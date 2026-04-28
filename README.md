# cf-stats

Long-term, queryable archive of `lafabrique.ai` Cloudflare HTTP traffic stats.

The Cloudflare free plan retains analytics for only 30 days. This project
pulls `httpRequestsAdaptiveGroups` hourly into a local DuckDB file so the
data accumulates indefinitely. A Highcharts dashboard is served locally at
`http://localhost:8080`.

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

3. Backfill, apply views, open the dashboard:

   ```bash
   task collect -- --hours 24
   task views
   task serve          # → http://localhost:8080
   ```

## Docker (recommended for server deployment)

```bash
cp .env.example .env          # fill in CF_API_TOKEN and CF_ZONE_ID
task docker:build
task docker:up                # starts web (:8080) + collector
task docker:logs              # follow both services
task docker:backfill -- --hours 720   # one-off 30-day backfill
```

The `stats` Docker volume holds `stats.duckdb` and survives container restarts.
`task docker:down` stops containers without touching data;
`task docker:destroy` removes containers **and** the volume.

## Hourly collection (systemd, alternative)

```bash
task systemd:install   # symlink units into ~/.config/systemd/user/
task systemd:enable    # enable linger + start timer
task systemd:status    # check it's running
task systemd:logs      # tail recent runs
```

## Task reference

`task --list` is the menu. Key tasks:

| Task | Purpose |
|---|---|
| `task collect` | Pull last 4h into DuckDB (idempotent) |
| `task views` | (Re)apply classification rules from `collector/views.sql` |
| `task serve` | Start dashboard at `http://localhost:8080` |
| `task summary` | Headline blog / site / spam split |
| `task top-blog` | Most-read posts (last 30d) |
| `task top-spam` | Top scanner targets — use to refine `views.sql` |
| `task shell` | Interactive DuckDB shell |
| `task backup` | Snapshot DB to `backups/<timestamp>/` as Parquet |
| `task systemd:run` | Trigger a one-shot collection immediately |

## Layout

```
collector/
  probe.py        one-shot API diagnostic
  collector.py    idempotent hourly ingester
  views.sql       blog/site/spam classification — edit freely
docker/
  collector-entrypoint.sh   hourly loop with clock alignment + jitter
systemd/
  cf-stats.service          oneshot collector unit
  cf-stats.timer            hourly schedule (RandomizedDelaySec=300)
  cf-stats-web.service      web dashboard service
web/
  index.html      Highcharts dashboard (single file, no build step)
serve.py          FastAPI — /api/* endpoints + serves web/
Dockerfile        single image for both services
docker-compose.yml
tests/            pytest suite (61 tests)
docs/adr/         architecture decision records
stats.duckdb      the database (gitignored)
.env              CF credentials (gitignored)
```

## Status

Phase 1 of [ADR-001](docs/adr/0001-cf-stats-architecture.md):

- [x] Collector + classification views + Taskfile
- [x] Hourly systemd timer
- [x] FastAPI + Highcharts dashboard
- [x] Test suite (61 tests)
- [x] Containerize (Docker Compose — collector + web + DuckDB volume)
- [ ] Phase 2: public dashboard via Cloudflare Pages
