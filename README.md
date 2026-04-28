# cf-stats

Long-term, queryable archive of `lafabrique.ai` Cloudflare HTTP traffic stats.

The Cloudflare free plan retains analytics for only 30 days. This project
pulls `httpRequestsAdaptiveGroups` hourly into a local DuckDB file so the
data accumulates indefinitely, ready for ad-hoc SQL today and a Highcharts
dashboard later.

Architecture, alternatives considered, and trade-offs:
[`docs/adr/0001-cf-stats-architecture.md`](docs/adr/0001-cf-stats-architecture.md).

## Quick start

1. Create a Cloudflare API token with **Zone → Analytics: Read** scoped to the
   target zone: <https://dash.cloudflare.com/profile/api-tokens>.

2. Configure credentials:

   ```bash
   cp .env.example .env
   # fill in CF_API_TOKEN and CF_ZONE_ID
   ```

3. Backfill the past day, apply views, peek at the result:

   ```bash
   task collect -- --hours 24
   task views
   task summary
   ```

## Daily commands

`task --list` is the menu. The useful ones:

| Command         | Purpose                                                       |
|-----------------|---------------------------------------------------------------|
| `task collect`  | Pull the last 4 hours into DuckDB (idempotent)                |
| `task views`    | (Re)apply classification rules from `collector/views.sql`     |
| `task summary`  | Headline blog / site / spam split with percentages            |
| `task top-blog` | Most-read blog posts in the last 30 days                      |
| `task top-spam` | Top scanner targets — use to refine the rules in `views.sql`  |
| `task shell`    | Interactive DuckDB shell on `stats.duckdb`                    |
| `task backup`   | Snapshot the DB to `backups/<timestamp>/` as Parquet          |

## Layout

```
collector/
  probe.py       one-shot diagnostic against the CF GraphQL API
  collector.py   idempotent ingester (DELETE + INSERT per window)
  views.sql      classification + dashboard aggregates — edit freely
docs/adr/        architecture decision records
stats.duckdb     the database (gitignored)
.env             CF credentials (gitignored)
```

## Status

Phase 1 of [ADR-001](docs/adr/0001-cf-stats-architecture.md):

- [x] Collector + classification views + Taskfile
- [ ] Hourly systemd timer
- [ ] FastAPI + Highcharts dashboard
- [ ] Phase 2: public dashboard via Cloudflare Pages
