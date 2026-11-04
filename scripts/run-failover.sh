#!/usr/bin/env bash
# Runs docs/preregistration-failover.md.
#
#   scripts/run-failover.sh [results directory]
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$ROOT/results/failover}"
QUIET=(--quiet-cpu 500 --quiet-wait 30m)
cd "$ROOT"
make build
scripts/pg.sh up
mkdir -p "$OUT"

./bin/harness failover --mode sync --repetitions 5 --out "$OUT" "${QUIET[@]}" | tee "$OUT/failover.txt"
./bin/harness failover --mode async --repetitions 3 --out "$OUT" "${QUIET[@]}" | tee -a "$OUT/failover.txt"

scripts/pg-ha.sh create sync
./bin/harness bench --name replication-cost-r1000-w4 --rate 1000 --workers 4 --concurrency 64 --max-in-flight 512 \
  --duration 60s --warmup 10s --repetitions 3 --drain-deadline 120s --out "$OUT" "${QUIET[@]}" \
  --engine-url 'postgres://hook@127.0.0.1:54340,127.0.0.1:54341/hookd?target_session_attrs=read-write' | tee "$OUT/replication-cost.txt"
scripts/pg-ha.sh destroy
