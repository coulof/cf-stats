#!/usr/bin/env python3
"""
Probe the Cloudflare GraphQL Analytics API.

Validates ADR-001 assumptions before we build the full collector:
  - API token has the right scope
  - httpRequestsAdaptiveGroups returns rows on the free plan
  - clientRequestPath dimension is available
  - We understand the shape of a row

Usage:
  export CF_API_TOKEN=...      # token with Zone Analytics:Read on the zone
  export CF_ZONE_ID=...        # 32-char hex zone tag
  python3 probe.py             # last 24h, adaptive dataset
  python3 probe.py --hours 6
  python3 probe.py --dataset httpRequests1hGroups   # fallback dataset
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

ENDPOINT = "https://api.cloudflare.com/client/v4/graphql"

QUERY_ADAPTIVE = """
query ($zoneTag: String!, $since: Time!, $until: Time!) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      httpRequestsAdaptiveGroups(
        limit: 50
        filter: {datetime_geq: $since, datetime_lt: $until}
      ) {
        count
        dimensions {
          datetimeHour
          clientRequestPath
          clientCountryName
          edgeResponseStatus
        }
        sum { edgeResponseBytes }
        avg { sampleInterval }
      }
    }
  }
}
"""

QUERY_1H = """
query ($zoneTag: String!, $since: Time!, $until: Time!) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      httpRequests1hGroups(
        limit: 50
        filter: {datetime_geq: $since, datetime_lt: $until}
        orderBy: [datetime_DESC]
      ) {
        dimensions { datetime }
        sum {
          requests
          bytes
          countryMap { clientCountryName requests }
          responseStatusMap { edgeResponseStatus requests }
        }
      }
    }
  }
}
"""

QUERIES = {
    "httpRequestsAdaptiveGroups": QUERY_ADAPTIVE,
    "httpRequests1hGroups": QUERY_1H,
}


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def run(token: str, zone: str, dataset: str, hours: int) -> dict:
    until = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    since = until - timedelta(hours=hours)
    body = json.dumps(
        {
            "query": QUERIES[dataset],
            "variables": {"zoneTag": zone, "since": iso(since), "until": iso(until)},
        }
    ).encode()
    req = urllib.request.Request(
        ENDPOINT,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        sys.stderr.write(f"HTTP {e.code}: {e.read().decode()}\n")
        sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument(
        "--dataset",
        choices=list(QUERIES),
        default="httpRequestsAdaptiveGroups",
    )
    ap.add_argument(
        "--raw", action="store_true", help="print full JSON response and exit"
    )
    args = ap.parse_args()

    token = os.environ.get("CF_API_TOKEN")
    zone = os.environ.get("CF_ZONE_ID")
    if not token or not zone:
        sys.stderr.write("CF_API_TOKEN and CF_ZONE_ID must be set\n")
        sys.exit(2)

    result = run(token, zone, args.dataset, args.hours)

    if args.raw:
        json.dump(result, sys.stdout, indent=2)
        print()
        return

    if "errors" in result and result["errors"]:
        print("GraphQL errors:")
        for err in result["errors"]:
            print(f"  - {err.get('message')}")
        sys.exit(1)

    zones = result["data"]["viewer"]["zones"]
    if not zones:
        print("No zones returned — token scope or zone id is wrong.")
        sys.exit(1)

    groups = zones[0][args.dataset]
    print(f"dataset:  {args.dataset}")
    print(f"window:   last {args.hours}h")
    print(f"rows:     {len(groups)}")
    if not groups:
        print("(no data — either no traffic or the dataset is gated)")
        return

    print("\nfirst 10 rows:")
    for row in groups[:10]:
        dims = row["dimensions"]
        s = row.get("sum", {})
        # Adaptive groups expose row count as `count` + `avg.sampleInterval`;
        # 1h groups expose totals under `sum.requests`.
        n = row.get("count")
        if n is not None:
            sample = (row.get("avg") or {}).get("sampleInterval", 1)
            est = int(n * (sample or 1))
            print(f"  {dims} -> count={n} sampleInterval={sample} est_requests={est} bytes={s.get('edgeResponseBytes')}")
        else:
            print(f"  {dims} -> requests={s.get('requests')} bytes={s.get('edgeResponseBytes') or s.get('bytes')}")


if __name__ == "__main__":
    main()
