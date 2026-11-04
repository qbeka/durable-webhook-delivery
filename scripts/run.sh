#!/usr/bin/env bash
# Runs the whole service locally with one command.
#
#   scripts/run.sh start   start everything and register a demo route
#   scripts/run.sh stop    stop the services and the databases
#   scripts/run.sh send    send one synthetic receipt and show its ledger row
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="$ROOT/.run/service"
PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"

TENANT=koba
SOURCE=mail-forwarder
SOURCE_SECRET=whsec_local_source
DESTINATION_SECRET=whsec_local_destination

start_process() {
  local name="$1"
  shift
  nohup "$@" >"$RUN_DIR/$name.log" 2>&1 &
  echo $! >"$RUN_DIR/$name.pid"
}

wait_for() {
  for _ in $(seq 1 100); do
    if curl -sf "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "$1 did not become ready. See $RUN_DIR" >&2
  exit 1
}

case "${1:-}" in
  start)
    mkdir -p "$RUN_DIR"
    (cd "$ROOT" && go build -o bin/ ./cmd/...)
    "$ROOT/scripts/pg.sh" up >/dev/null
    "$ROOT/bin/hookctl" migrate
    start_process receiver "$ROOT/bin/receiver" --secret "$DESTINATION_SECRET" --effect-log "$RUN_DIR/effects.csv"
    start_process api "$ROOT/bin/hookd" api
    start_process worker "$ROOT/bin/hookd" worker --id worker-1
    wait_for http://127.0.0.1:9000/healthz
    wait_for http://127.0.0.1:9101/healthz
    "$ROOT/bin/hookctl" source --tenant "$TENANT" --source "$SOURCE" --secret "$SOURCE_SECRET"
    destination="$("$ROOT/bin/hookctl" destination --tenant "$TENANT" --name receipts \
      --url http://127.0.0.1:9101/webhooks/receipts --secret "$DESTINATION_SECRET")"
    "$ROOT/bin/hookctl" subscribe --tenant "$TENANT" --source "$SOURCE" --destination "$destination"
    echo "Running. Ingress: http://127.0.0.1:9000. Receiver: http://127.0.0.1:9101."
    echo "Send a receipt: make send. Stop everything: make stop."
    ;;
  stop)
    for name in worker api receiver; do
      if [ -f "$RUN_DIR/$name.pid" ]; then
        kill "$(cat "$RUN_DIR/$name.pid")" 2>/dev/null || true
        rm -f "$RUN_DIR/$name.pid"
      fi
    done
    "$ROOT/scripts/pg.sh" down
    echo "Stopped."
    ;;
  send)
    event_id="evt-$(date +%s)-$RANDOM"
    body="{\"receipt_id\":\"rcpt-$RANDOM\",\"amount_cents\":$((RANDOM % 9000 + 100)),\"currency\":\"CAD\",\"merchant\":\"Synthetic Coffee\",\"arrival_unix_ns\":0}"
    "$ROOT/bin/hookctl" send --tenant "$TENANT" --source "$SOURCE" --secret "$SOURCE_SECRET" --event-id "$event_id" --body "$body"
    sleep 0.3
    psql -h 127.0.0.1 -p 54331 -U hook -d receipts -c \
      "SELECT delivery_key, event_id, amount_cents, currency, merchant, committed_at FROM expense_ledger ORDER BY id DESC LIMIT 1"
    ;;
  *)
    echo "usage: scripts/run.sh start|stop|send" >&2
    exit 2
    ;;
esac
