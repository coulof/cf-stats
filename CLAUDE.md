# CLAUDE.md

Context for future Claude sessions in this repo.

## What this is

A small local pipeline pulling `lafabrique.ai`'s Cloudflare traffic analytics
into DuckDB to extend the free-plan 30-day retention indefinitely. Single
user, single host. Read
[`docs/adr/0001-cf-stats-architecture.md`](docs/adr/0001-cf-stats-architecture.md)
first — it has the architecture, schema rationale, and trade-offs against
alternatives. Don't duplicate that content here.

## Environment

- Python venv at `.venv/` (Python 3.13 + `duckdb` package).
- DuckDB CLI binary lives at `~/.duckdb/cli/latest/duckdb` —
  **not on `PATH`**. The `Taskfile.yml` exposes it as the `DUCKDB` var; reuse
  that variable rather than hard-coding the path.
- CF credentials (`CF_API_TOKEN`, `CF_ZONE_ID`) are in `.env`, auto-loaded by
  Task via `dotenv:`. The Python scripts read them from the environment.

## Conventions

- **Taskfile is the entry point.** Wrap any new flow as a task; don't add bare
  scripts users have to remember the path of. Match the style already in
  `Taskfile.yml`.
- **Store raw, classify at query time.** The collector ingests every row CF
  returns, including spam. Classification (`blog` / `site` / `spam`) is a
  `CASE` expression in `collector/views.sql`. To refine rules, edit the
  `CASE` and run `task views` — never filter at the GraphQL API.
- **Adaptive sampling: real volume = `count × sample_interval`.**
  `httpRequestsAdaptiveGroups` samples high-volume buckets. Always use
  `est_requests` for traffic numbers; raw `count` is sampled hits.
- **Idempotent collection.** `collector.py` does DELETE-then-INSERT in a
  transaction over the requested window. Cron overlap and manual reruns are
  safe; the latest in-progress hour is intentionally excluded.
- **Times are UTC.** `datetimeHour` from CF is UTC; the DuckDB `ts` column is
  naive `TIMESTAMP` storing UTC wall-clock.

## What's not built yet

The action-item checklist at the bottom of the ADR is the source of truth.
