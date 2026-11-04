#!/usr/bin/env bash
# Runs every published experiment in docs/preregistration.md. This takes hours, most
# of it waiting for a quiet machine.
#
#   scripts/run-benchmarks.sh [results directory]
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$ROOT/results/published}"
QUIET=(--quiet-cpu 500 --quiet-wait 30m)
cd "$ROOT"
make build
scripts/pg.sh down
scripts/pg.sh up
mkdir -p "$OUT"
log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "generator limit"
./bin/harness generator --out "$OUT/generator" --rates 1000,2000,4000,8000,16000

log "failure run"
./bin/harness faults --out "$OUT/faults" | tee "$OUT/faults.txt"

log "H2 ladder"
for workers in 1 2 4; do
  for rate in 1000 1500 2000 2500 3000; do
    ./bin/harness bench --name "ladder-w$workers-r$rate" --rate "$rate" --workers "$workers" \
      --concurrency 64 --max-in-flight 512 --duration 60s --warmup 10s --repetitions 3 \
      --drain-deadline 120s --out "$OUT/ladder" "${QUIET[@]}" | tee -a "$OUT/ladder.txt"
  done
done

log "H1 target"
./bin/harness bench --name target-r1000-w2 --rate 1000 --workers 2 --concurrency 64 --max-in-flight 512 \
  --duration 10m --warmup 10s --repetitions 3 --drain-deadline 120s --out "$OUT/target" "${QUIET[@]}" | tee "$OUT/target.txt"

log "H4 isolation"
./bin/harness recovery --name slow-limits-on --mode slow --max-in-flight 64 --breaker 5 --out "$OUT/recovery" "${QUIET[@]}" | tee "$OUT/recovery.txt"
./bin/harness recovery --name slow-limits-off --mode slow --max-in-flight 1024 --breaker 1000000 --out "$OUT/recovery" "${QUIET[@]}" | tee -a "$OUT/recovery.txt"
./bin/harness recovery --name offline-limits-on --mode offline --max-in-flight 64 --breaker 5 --out "$OUT/recovery" "${QUIET[@]}" | tee -a "$OUT/recovery.txt"

log "H5 flush cost"
scripts/pg.sh down
HOOK_WAL_SYNC=open_datasync scripts/pg.sh up
./bin/harness bench --name flush-default --rate 1500 --workers 2 --concurrency 64 --max-in-flight 512 --duration 60s \
  --warmup 10s --repetitions 3 --drain-deadline 120s --out "$OUT/flush" "${QUIET[@]}" | tee "$OUT/flush.txt"
scripts/pg.sh down
scripts/pg.sh up
./bin/harness bench --name flush-writethrough --rate 1500 --workers 2 --concurrency 64 --max-in-flight 512 --duration 60s \
  --warmup 10s --repetitions 3 --drain-deadline 120s --out "$OUT/flush" "${QUIET[@]}" | tee -a "$OUT/flush.txt"

log "finished"
