#!/usr/bin/env bash
# Runs every experiment from one commit. This takes about 3.5 hours.
#
#   scripts/run-final.sh [results directory]
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$ROOT/results/final}"
QUIET=(--quiet-cpu 500 --quiet-wait 30m)
TLA="/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar"
HA_URL='postgres://hook@127.0.0.1:54340,127.0.0.1:54341/hookd?target_session_attrs=read-write'
cd "$ROOT"
make build
scripts/pg.sh down
scripts/pg.sh up
mkdir -p "$OUT"
log() { echo "[$(date '+%H:%M:%S')] $*"; }
bench() { ./bin/harness bench --concurrency 64 --max-in-flight 512 --drain-deadline 120s "${QUIET[@]}" "$@"; }

log "generator limit"
./bin/harness generator --out "$OUT/generator" --rates 1000,2000,4000,8000,16000

log "failure run"
./bin/harness faults --out "$OUT/faults" | tee "$OUT/faults.txt"

log "ladder"
for workers in 1 2 4; do
  for rate in 1000 1500 2000 2500 3000; do
    bench --name "ladder-w$workers-r$rate" --rate "$rate" --workers "$workers" --duration 60s --warmup 10s \
      --repetitions 3 --out "$OUT/ladder" | tee -a "$OUT/ladder.txt"
  done
done

log "ten minute runs"
bench --name target-r1000-w2 --rate 1000 --workers 2 --duration 10m --warmup 10s --repetitions 3 --out "$OUT/target" | tee "$OUT/target.txt"
bench --name target-r2000-w4 --rate 2000 --workers 4 --duration 10m --warmup 10s --repetitions 3 --out "$OUT/target" | tee -a "$OUT/target.txt"

log "isolation"
./bin/harness recovery --name slow-limits-on --mode slow --max-in-flight 64 --breaker 5 --out "$OUT/recovery" "${QUIET[@]}" | tee "$OUT/recovery.txt"
./bin/harness recovery --name slow-limits-off --mode slow --max-in-flight 1024 --breaker 1000000 --out "$OUT/recovery" "${QUIET[@]}" | tee -a "$OUT/recovery.txt"
./bin/harness recovery --name offline-limits-on --mode offline --max-in-flight 64 --breaker 5 --out "$OUT/recovery" "${QUIET[@]}" | tee -a "$OUT/recovery.txt"

log "flush cost"
scripts/pg.sh down
HOOK_WAL_SYNC=open_datasync scripts/pg.sh up
bench --name flush-default --rate 1500 --workers 2 --duration 60s --warmup 10s --repetitions 3 --out "$OUT/flush" | tee "$OUT/flush.txt"
scripts/pg.sh down
scripts/pg.sh up
bench --name flush-writethrough --rate 1500 --workers 2 --duration 60s --warmup 10s --repetitions 3 --out "$OUT/flush" | tee -a "$OUT/flush.txt"

log "failover"
./bin/harness failover --mode sync --repetitions 5 --out "$OUT/failover" "${QUIET[@]}" | tee "$OUT/failover.txt"
./bin/harness failover --mode async --repetitions 3 --out "$OUT/failover" "${QUIET[@]}" | tee -a "$OUT/failover.txt"
scripts/pg-ha.sh create sync
bench --name replication-cost-r1000-w4 --rate 1000 --workers 4 --duration 60s --warmup 10s --repetitions 3 \
  --out "$OUT/failover" --engine-url "$HA_URL" | tee "$OUT/replication-cost.txt"
scripts/pg-ha.sh destroy

log "model check"
python3 scripts/check_spec.py "$TLA" | tee "$OUT/tlc.txt"
cp results/spec/tlc.json "$OUT/tlc.json"

log "mutation check"
python3 scripts/mutants.py | tee "$OUT/mutants.txt"

python3 scripts/compact_raw.py "$OUT"
log "finished"
