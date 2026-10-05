#!/usr/bin/env bash
# Samples a running Portage engine every INTERVAL seconds and appends one CSV
# row per sample: process RSS (ps), goroutines and engine counters (/metrics),
# and the size of the file record and River job tables (Postgres).
#
#   METRICS_URL=http://127.0.0.1:9090/metrics PIDFILE=/run/portage.pid \
#   PSQL='psql postgres://.../portage' bench/soak/sample.sh samples.csv
#
# PIDFILE holds the engine's PID; it is re-read every sample so a restarted
# engine is picked up. PSQL is any command that runs SQL from stdin, e.g.
# 'docker exec -i portage-postgres-1 psql -U portage -d portage'.
# Stop with Ctrl-C / SIGTERM.
set -u

out=${1:?usage: sample.sh OUT.csv}
interval=${INTERVAL:-30}
metrics_url=${METRICS_URL:?set METRICS_URL}
pidfile=${PIDFILE:?set PIDFILE}
psql_cmd=${PSQL:?set PSQL}

if [ ! -s "$out" ]; then
  echo "ts,pid,rss_kb,go_goroutines,go_heap_inuse_bytes,queue_depth,oldest_pending_s,bytes_copied,bytes_uploaded,files_synced,files_already_synced,files_stale,files_retry,files_failed,events_event,events_reconcile,reconcile_runs,file_record_rows,file_record_pending,file_record_bytes,river_job_rows,river_completed,river_available,river_running,river_retryable,river_scheduled,river_discarded,river_cancelled,river_job_bytes,db_bytes" >"$out"
fi

# metric NAME [LABEL_REGEX]: sum of matching series in the scrape.
metric() {
  awk -v n="$1" -v re="${2:-}" '
    $0 !~ /^#/ {
      name = $1; sub(/\{.*/, "", name)
      if (name == n && (re == "" || $1 ~ re)) s += $NF
    }
    END { printf "%.0f", s }' "$scrape"
}

scrape=$(mktemp)
trap 'rm -f "$scrape"; exit 0' INT TERM

while :; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  pid=$(cat "$pidfile" 2>/dev/null || true)
  rss=""
  if [ -n "$pid" ]; then rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' '); fi
  if ! curl -fsS --max-time 10 "$metrics_url" >"$scrape" 2>/dev/null; then : >"$scrape"; fi

  db=$($psql_cmd -At -F, 2>/dev/null <<'SQL' | tr '\n' ',' | sed 's/,$//'
SELECT
  (SELECT count(*) FROM file_record),
  (SELECT count(*) FROM file_record WHERE status <> 'synced'),
  pg_total_relation_size('file_record'),
  (SELECT count(*) FROM river_job),
  (SELECT count(*) FILTER (WHERE state = 'completed') FROM river_job),
  (SELECT count(*) FILTER (WHERE state = 'available') FROM river_job),
  (SELECT count(*) FILTER (WHERE state = 'running') FROM river_job),
  (SELECT count(*) FILTER (WHERE state = 'retryable') FROM river_job),
  (SELECT count(*) FILTER (WHERE state = 'scheduled') FROM river_job),
  (SELECT count(*) FILTER (WHERE state = 'discarded') FROM river_job),
  (SELECT count(*) FILTER (WHERE state = 'cancelled') FROM river_job),
  pg_total_relation_size('river_job'),
  pg_database_size(current_database());
SQL
)
  [ -n "$db" ] || db=",,,,,,,,,,,,"

  echo "$ts,$pid,$rss,$(metric go_goroutines),$(metric go_memstats_heap_inuse_bytes),$(metric portage_queue_depth),$(metric portage_oldest_pending_seconds),$(metric portage_bytes_copied_total),$(metric portage_bytes_uploaded_total),$(metric portage_files_total 'outcome="synced"'),$(metric portage_files_total 'outcome="already_synced"'),$(metric portage_files_total 'outcome="stale"'),$(metric portage_files_total 'outcome="retry"'),$(metric portage_files_total 'outcome="failed"'),$(metric portage_events_total 'source="event"'),$(metric portage_events_total 'source="reconcile"'),$(metric portage_reconcile_runs_total),$db" >>"$out"
  sleep "$interval" &
  wait $!
done
