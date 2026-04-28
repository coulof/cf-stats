# ADR-001: Long-term Cloudflare traffic stats with DuckDB and a local web dashboard

**Status:** Accepted
**Date:** 2026-04-28
**Deciders:** @gonzo

## Context

`lafabrique.ai` is hosted on Cloudflare Pages. The free plan retains traffic
analytics for only 30 days, and the data is locked in CF's UI. Goal: extend
retention indefinitely, run lightweight queries, and view a private dashboard
from a small home server. Hourly granularity, with breakdowns by request path
(popular posts) and country. Public sharing is a Phase 2 nice-to-have.

Constraints:

- Free Cloudflare plan — must use the GraphQL Analytics API.
- Hosted on a small local server, single user, no HA needed.
- DuckDB-friendly stack preferred.
- Phase 2: must promote to a public CF Pages dashboard with minimal rework.

## Decision

Build a three-piece local pipeline:

```
┌────────────┐   hourly cron    ┌──────────────┐    on-demand     ┌────────────┐
│ CF GraphQL │ ───────────────▶ │ stats.duckdb │ ───────────────▶ │ dashboard  │
│ Analytics  │   collector.py   │  (file, ~MB) │   queries → JSON │ HTML + HC  │
└────────────┘                  └──────────────┘                  └────────────┘
```

1. **Collector**: a single Python script (`collector.py`), run hourly via
   systemd timer or cron. Pulls the last 2–4 hours from
   `httpRequestsAdaptiveGroups` (with dimensions `datetimeHour`,
   `clientRequestPath`, `clientCountryName`, `edgeResponseStatus`) and
   `INSERT … ON CONFLICT` into DuckDB. Idempotent on
   `(ts, path, country, status)`.
2. **Storage**: a single `stats.duckdb` file on the server. One fact table plus
   a few materialized aggregate views. Backed up by `rsync` to a second disk
   or R2.
3. **Dashboard**: a tiny FastAPI app that runs DuckDB queries on the dashboard
   URL and returns JSON; static HTML + Highcharts on the front. Bound to LAN /
   Tailscale only. The same Highcharts page is what we'll later deploy to CF
   Pages, swapping the JSON source for a pre-baked file.

## Options Considered

### Option A — Local cron + DuckDB + FastAPI + Highcharts (chosen)

| Dimension        | Assessment                                                        |
|------------------|-------------------------------------------------------------------|
| Complexity       | Low — three small files                                           |
| Cost             | $0 (local box)                                                    |
| Phase-2 path     | Drop FastAPI, bake JSON, push static site to CF Pages             |
| Scalability      | Fine to ~years of hourly data (a few hundred MB max)              |
| Team familiarity | Python + DuckDB, both well-known to user                          |

**Pros**

- Single DuckDB file, easy to back up, easy to query ad hoc with `duckdb` CLI.
- FastAPI gives flexible "what-if" queries during exploration; can be replaced
  by static JSON later.
- Reuses the `Taskfile` style — `task collect`, `task serve`, `task backup`.

**Cons**

- Server must stay on; gaps if it's down >30 days (acceptable; alert via cron
  failure email).
- Two artifacts to deploy in Phase 2 (HTML + JSON), but that's fine for static
  hosting.

### Option B — Cloudflare Worker cron + R2 Parquet + DuckDB-WASM

**Pros:** zero servers, scales forever, dashboard works from any browser
unmodified.
**Cons:** introduces R2 + Worker quotas now for a one-user workflow; harder to
poke at locally; auth for "private" requires CF Access setup. Overkill for
Phase 1.

### Option C — Plain SQLite + Grafana

**Pros:** Grafana gives a time-series UI for free.
**Cons:** heavier than the project warrants; loses DuckDB analytical
ergonomics; Grafana is a service to babysit.

## Trade-off Analysis

The critical trade-off is **simplicity now vs. Phase-2 reuse**. Option A keeps
everything in user-space on the server, but the dashboard is intentionally
written so the "data layer" is replaceable: today it's `fetch('/api/stats')`
hitting FastAPI, tomorrow it's `fetch('./stats.json')` hitting CF Pages. The
Highcharts code, the DuckDB SQL, and the collector are all unchanged.

We pay one small price: in Phase 1 we run a process; we don't get the
all-static elegance of B until Phase 2. Acceptable.

## Schema (revised after probe)

The probe (2026-04-28) confirmed `httpRequestsAdaptiveGroups` works on the
free plan with `clientRequestPath` populated, but the dataset is **adaptively
sampled**: a row reports `count` (sampled hits) and `avg.sampleInterval`
(multiplier). Real volume = `count × sampleInterval`. We store all three so
we can detect when sampling is active and recompute estimates if the
methodology changes.

```sql
CREATE TABLE requests_hourly (
  ts              TIMESTAMP NOT NULL,   -- hour bucket, UTC
  path            VARCHAR   NOT NULL,
  country         VARCHAR   NOT NULL,   -- ISO-2
  status          SMALLINT  NOT NULL,
  count           BIGINT    NOT NULL,   -- raw sampled hits
  sample_interval DOUBLE    NOT NULL,   -- adaptive multiplier (≥ 1)
  est_requests    BIGINT    NOT NULL,   -- count * sample_interval
  bytes           BIGINT    NOT NULL,   -- edgeResponseBytes
  PRIMARY KEY (ts, path, country, status)
);
```

## Classification: blog / site / spam (decided after probe)

Probe revealed that the bulk of incoming traffic is opportunistic scanners
(`/wp-admin/install.php`, `/.aws/config`, random `*.php`). Two principles:

- **Store raw, classify at query time.** Don't filter at the GraphQL API —
  the spam volume is itself a useful metric, and classification rules will
  evolve as new scanner patterns appear. Re-classifying a view is free;
  re-fetching deleted data is impossible.
- **Categories live in `views.sql`** as a `CASE` expression on `path`.
  Three buckets: `blog` (`/blog/%`), `site` (homepage, assets, generated
  pages), `spam` (everything else). User edits the `CASE` whenever a new
  pattern surfaces.

Downstream views consume `v_classified` instead of `requests_hourly`
directly, so the dashboard is decoupled from path patterns.

## Consequences

**Becomes easier**

- Querying historical traffic with plain SQL.
- Adding new metrics — extend the GraphQL query and add a column.
- Phase 2 promotion: same Highcharts page, swap data source.

**Becomes harder**

- Server uptime now matters (mitigated by cron alerting).
- Schema evolution requires DuckDB migrations (use a numbered `migrations/`
  folder, simplest possible thing).

**To revisit at Phase 2**

- Auth model (CF Access vs. just-public).
- Whether to keep FastAPI for "ad hoc query" mode or go fully static.
- Move `stats.duckdb` (or its Parquet export) to R2 if dashboard latency
  matters.

## References

**Cloudflare GraphQL Analytics API**

- Overview: https://developers.cloudflare.com/analytics/graphql-api/
- Getting started + authentication:
  https://developers.cloudflare.com/analytics/graphql-api/getting-started/
- Datasets reference (look for `httpRequestsAdaptiveGroups`,
  `httpRequests1hGroups`):
  https://developers.cloudflare.com/analytics/graphql-api/features/data-sets/
- Query limits & quotas (free-plan retention is the constraint we're working
  around): https://developers.cloudflare.com/analytics/graphql-api/limits/
- Endpoint: `POST https://api.cloudflare.com/client/v4/graphql`

**Auth**

- Create a scoped API token (`Analytics: Read` for the zone):
  https://developers.cloudflare.com/fundamentals/api/get-started/create-token/

**Probe query** — used by `collector/probe.py` to confirm path-level dimensions
work on the free plan:

```graphql
query ($zoneTag: String!, $since: Time!, $until: Time!) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      httpRequestsAdaptiveGroups(
        limit: 1000
        filter: {datetime_geq: $since, datetime_lt: $until}
      ) {
        dimensions {
          datetimeHour
          clientRequestPath
          clientCountryName
          edgeResponseStatus
        }
        sum { requests edgeResponseBytes }
      }
    }
  }
}
```

If `clientRequestPath` is Pro-gated, fall back to `httpRequests1hGroups` (no
path dimension) for country + status only, or revisit the source (CF Pages Web
Analytics, Workers logs) before deciding.

## Action Items

1. [x] Confirm CF GraphQL dataset works on the free plan
       (`httpRequestsAdaptiveGroups` ✅, `clientRequestPath` ✅, sampled).
2. [x] Build the collector (`collector/collector.py`, `collector/views.sql`).
3. [x] Build `serve.py` (FastAPI) and `web/index.html` (Highcharts).
       Dashboard: period + granularity selectors, spam toggle, 4 themes ×
       light/dark, world map (log scale) + bar for countries, clickable
       post links.
4. [x] Set up systemd user service + hourly timer (`systemd/`).
       Install with `task systemd:install && task systemd:enable`.
5. [ ] Run collector; let data accumulate and verify no gaps.
6. [ ] Bind the dashboard to Tailscale/LAN only.
7. [ ] After ~2 weeks of accumulation, evaluate query latency and decide on
       Phase 2 timing (static JSON export → Cloudflare Pages).
