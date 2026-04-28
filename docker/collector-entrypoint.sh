#!/bin/bash
# Collector entrypoint: apply views, run one initial collection, then loop hourly.
set -euo pipefail

DB="${CF_STATS_DB:-/data/stats.duckdb}"

log() { echo "[$(date -u +%FT%TZ)] $*"; }

log "applying classification views to $DB..."
python - <<EOF
import duckdb, pathlib
con = duckdb.connect("$DB")
con.execute(pathlib.Path("collector/views.sql").read_text())
log_msg = "views applied"
EOF
log "views OK"

log "initial collection (last 4h)..."
python collector/collector.py || log "WARN: initial collection failed, will retry on next cycle"

log "starting hourly loop..."
while true; do
    # Sleep until the top of the next hour, then add up to 5 min of jitter
    # so we don't hammer the CF API exactly on the hour.
    NOW=$(date +%s)
    NEXT_HOUR=$(( (NOW / 3600 + 1) * 3600 ))
    JITTER=$(( RANDOM % 300 ))
    SLEEP=$(( NEXT_HOUR - NOW + JITTER ))
    log "next collection in ${SLEEP}s (at $(date -u -d "@$NEXT_HOUR" +%H:%M) UTC + ${JITTER}s jitter)"
    sleep "$SLEEP"
    log "collecting..."
    python collector/collector.py || log "WARN: collection failed, will retry next cycle"
done
