#!/usr/bin/env bash
# Samples active PostgreSQL backends and prints where time goes, a poor man's
# profiler. pg_stat_activity shows what each backend is doing right now, so many
# samples add up to a picture of what the database spends its time on.
#
#   scripts/pgsample.sh <port> <database> <seconds>
set -euo pipefail
PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
port="$1"; database="$2"; seconds="$3"
query="select coalesce(wait_event_type||':'||wait_event, 'on-cpu') || ' | ' ||
       left(regexp_replace(query, '\s+', ' ', 'g'), 70)
       from pg_stat_activity
       where state = 'active' and pid <> pg_backend_pid() and datname = '$database'"
end=$(( $(date +%s) + seconds ))
samples=0
while [ "$(date +%s)" -lt "$end" ]; do
  psql -h 127.0.0.1 -p "$port" -U hook -d "$database" -Atc "$query"
  samples=$((samples + 1))
done | sort | uniq -c | sort -rn | head -14
