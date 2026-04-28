#!/usr/bin/env python3
"""
serve.py — local dashboard server for cf-stats.

Usage:
  task serve                      # default: 0.0.0.0:8080
  task serve -- --port 9090
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal

import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DB = Path(__file__).parent / "stats.duckdb"
WEB = Path(__file__).parent / "web"

Period = Literal["day", "week", "quarter", "semester", "year", "all"]
Granularity = Literal["hour", "day", "month"]

PERIOD_WHERE = {
    "day":      "ts > now() - INTERVAL 1 DAY",
    "week":     "ts > now() - INTERVAL 7 DAY",
    "quarter":  "ts > now() - INTERVAL 90 DAY",
    "semester": "ts > now() - INTERVAL 180 DAY",
    "year":     "ts > now() - INTERVAL 365 DAY",
    "all":      "1=1",
}

app = FastAPI(title="cf-stats", docs_url=None, redoc_url=None)


def _query(sql: str, params: list | None = None) -> list[dict]:
    if not DB.exists():
        raise HTTPException(
            status_code=503,
            detail="stats.duckdb not found — run `task collect` first",
        )
    con = duckdb.connect(str(DB), read_only=True)
    try:
        cur = con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()


@app.get("/api/health")
def health():
    return _query("""
        SELECT
            COUNT(*)                            AS rows,
            MIN(ts)                             AS since,
            MAX(ts)                             AS until,
            CAST(epoch(MAX(ts)) AS BIGINT)      AS until_ts
        FROM requests_hourly
    """)[0]


@app.get("/api/summary")
def summary(period: Period = "all"):
    where = PERIOD_WHERE[period]
    return _query(f"""
        SELECT category,
               SUM(est_requests) AS requests,
               ROUND(100.0 * SUM(est_requests) / SUM(SUM(est_requests)) OVER (), 1) AS pct
        FROM v_classified
        WHERE {where}
        GROUP BY 1
        ORDER BY requests DESC
    """)


@app.get("/api/traffic")
def traffic(period: Period = "week", granularity: Granularity = "day"):
    where = PERIOD_WHERE[period]
    trunc = granularity  # validated by Literal; used only as dict key above
    return _query(f"""
        SELECT
            CAST(epoch(date_trunc('{trunc}', ts)) * 1000 AS BIGINT) AS ts_ms,
            SUM(CASE WHEN category = 'blog' THEN est_requests ELSE 0 END) AS blog,
            SUM(CASE WHEN category = 'site' THEN est_requests ELSE 0 END) AS site,
            SUM(CASE WHEN category = 'spam' THEN est_requests ELSE 0 END) AS spam,
            SUM(est_requests) AS total
        FROM v_classified
        WHERE {where}
        GROUP BY 1
        ORDER BY 1
    """)


@app.get("/api/top-blog")
def top_blog(period: Period = "quarter"):
    where = PERIOD_WHERE[period]
    return _query(f"""
        SELECT path, SUM(est_requests) AS requests
        FROM v_classified
        WHERE category = 'blog'
          AND status IN (200, 304)
          AND {where}
        GROUP BY path
        ORDER BY requests DESC
        LIMIT 15
    """)


@app.get("/api/countries")
def countries(period: Period = "quarter"):
    where = PERIOD_WHERE[period]
    return _query(f"""
        SELECT country, SUM(est_requests) AS requests
        FROM v_classified
        WHERE category IN ('blog', 'site')
          AND status IN (200, 304)
          AND {where}
        GROUP BY country
        ORDER BY requests DESC
        LIMIT 15
    """)


@app.get("/favicon.svg", include_in_schema=False)
def favicon():
    path = Path(__file__).parent / "favicon.svg"
    if path.exists():
        return FileResponse(str(path), media_type="image/svg+xml")
    raise HTTPException(status_code=404)

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(WEB / "index.html")


app.mount("/vendor", StaticFiles(directory=WEB / "vendor"), name="vendor")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()
    uvicorn.run("serve:app", host=args.host, port=args.port, reload=False)
