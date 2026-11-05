#!/usr/bin/env bash
# Repeats the two tests the final run failed, with a drive flush probe beside them.
# This takes about 50 minutes. See docs/preregistration-retest.md.
#
#   scripts/run-retest.sh [results directory]
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$ROOT/results/retest}"
QUIET=(--quiet-cpu 500 --quiet-wait 30m)
cd "$ROOT"
make build
scripts/pg.sh down
scripts/pg.sh up
mkdir -p "$OUT"
log() { echo "[$(date '+%H:%M:%S')] $*"; }

python3 scripts/flush_probe.py "$OUT/flush-probe.csv" "${HOOK_RUN_DIR:-$ROOT/.run}/flush-probe.scratch" &
PROBE=$!
trap 'kill "$PROBE" 2>/dev/null' EXIT

log "ten minute runs"
./bin/harness bench --concurrency 64 --max-in-flight 512 --drain-deadline 120s "${QUIET[@]}" \
  --name target-r2000-w4 --rate 2000 --workers 4 --duration 10m --warmup 10s --repetitions 3 \
  --out "$OUT/target" | tee "$OUT/target.txt"

log "isolation"
./bin/harness recovery --name slow-limits-on --mode slow --max-in-flight 64 --breaker 5 --out "$OUT/recovery" "${QUIET[@]}" | tee "$OUT/recovery.txt"
./bin/harness recovery --name slow-limits-off --mode slow --max-in-flight 1024 --breaker 1000000 --out "$OUT/recovery" "${QUIET[@]}" | tee -a "$OUT/recovery.txt"
./bin/harness recovery --name offline-limits-on --mode offline --max-in-flight 64 --breaker 5 --out "$OUT/recovery" "${QUIET[@]}" | tee -a "$OUT/recovery.txt"

python3 scripts/compact_raw.py "$OUT"
log "finished"
