#!/usr/bin/env python3
"""
collector.py — pull Cloudflare adaptive analytics into DuckDB.

Idempotent: rerunning for the same window replaces existing rows in that
window, so cron overlap is safe.

Usage:
  export CF_API_TOKEN=... CF_ZONE_ID=...
  python collector.py                       # last 4 complete hours
  python collector.py --hours 24            # last 24 complete hours
  python collector.py --since 2026-04-27T00:00:00Z --until 2026-04-28T00:00:00Z
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

ENDPOINT = "https://api.cloudflare.com/client/v4/graphql"
LIMIT = 10000  # CF GraphQL Analytics per-query cap

QUERY = """
query ($zoneTag: String!, $since: Time!, $until: Time!, $limit: Int!) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      httpRequestsAdaptiveGroups(
        limit: $limit
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

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests_hourly (
  ts              TIMESTAMP NOT NULL,
  path            VARCHAR   NOT NULL,
  country         VARCHAR   NOT NULL,
  status          SMALLINT  NOT NULL,
  count           BIGINT    NOT NULL,   -- raw sampled hits
  sample_interval DOUBLE    NOT NULL,   -- adaptive multiplier
  est_requests    BIGINT    NOT NULL,   -- count * sample_interval
  bytes           BIGINT    NOT NULL,   -- edgeResponseBytes
  PRIMARY KEY (ts, path, country, status)
);
"""

ISO = "%Y-%m-%dT%H:%M:%SZ"


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def fetch(token: str, zone: str, since: datetime, until: datetime) -> list[dict]:
    body = json.dumps(
        {
            "query": QUERY,
            "variables": {
                "zoneTag": zone,
                "since": since.strftime(ISO),
                "until": until.strftime(ISO),
                "limit": LIMIT,
            },
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
            result = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"HTTP {e.code}: {e.read().decode()}")

    if result.get("errors"):
        raise SystemExit("GraphQL errors: " + json.dumps(result["errors"], indent=2))

    zones = result["data"]["viewer"]["zones"]
    if not zones:
        raise SystemExit("zone not found — check CF_ZONE_ID and token scope")
    return zones[0]["httpRequestsAdaptiveGroups"]


def to_records(rows: list[dict]) -> list[tuple]:
    out = []
    for r in rows:
        d = r["dimensions"]
        ts = datetime.strptime(d["datetimeHour"], ISO).replace(tzinfo=timezone.utc)
        n = int(r["count"])
        s = float((r.get("avg") or {}).get("sampleInterval") or 1.0)
        bytes_ = int((r.get("sum") or {}).get("edgeResponseBytes") or 0)
        out.append(
            (
                ts,
                d["clientRequestPath"] or "",
                d["clientCountryName"] or "",
                int(d["edgeResponseStatus"]),
                n,
                s,
                int(round(n * s)),
                bytes_,
            )
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=4,
                    help="hours back from the most recent complete hour (default 4)")
    ap.add_argument("--since", help="ISO8601 UTC, overrides --hours")
    ap.add_argument("--until", help="ISO8601 UTC, overrides --hours")
    ap.add_argument("--db", default=str(Path(__file__).parent.parent / "stats.duckdb"))
    args = ap.parse_args()

    token = os.environ.get("CF_API_TOKEN")
    zone = os.environ.get("CF_ZONE_ID")
    if not token or not zone:
        raise SystemExit("CF_API_TOKEN and CF_ZONE_ID must be set")

    # Default window: [now_hour - hours, now_hour). Excludes the current
    # in-progress hour so each bucket we ingest is complete.
    now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    until = parse_iso(args.until) if args.until else now_hour
    since = parse_iso(args.since) if args.since else until - timedelta(hours=args.hours)

    rows = fetch(token, zone, since, until)
    if len(rows) == LIMIT:
        sys.stderr.write(
            f"WARN: hit per-query limit ({LIMIT}); window likely truncated. "
            f"Reduce --hours or chunk by hour.\n"
        )

    records = to_records(rows)

    con = duckdb.connect(args.db)
    con.execute(SCHEMA)
    con.execute("BEGIN")
    con.execute(
        "DELETE FROM requests_hourly WHERE ts >= ? AND ts < ?",
        [since, until],
    )
    if records:
        con.executemany(
            "INSERT INTO requests_hourly VALUES (?,?,?,?,?,?,?,?)",
            records,
        )
    con.execute("COMMIT")

    total = con.execute("SELECT COUNT(*) FROM requests_hourly").fetchone()[0]
    span = con.execute(
        "SELECT MIN(ts), MAX(ts) FROM requests_hourly"
    ).fetchone()
    print(f"window:  {since.strftime(ISO)} → {until.strftime(ISO)}")
    print(f"fetched: {len(records)} rows")
    print(f"db:      {args.db}")
    print(f"  total rows: {total}")
    print(f"  span:       {span[0]} → {span[1]}")


if __name__ == "__main__":
    main()
