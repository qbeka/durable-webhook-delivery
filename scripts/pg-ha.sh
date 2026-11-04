#!/usr/bin/env bash
# Runs an engine primary with one streaming standby, for the failover experiment.
#
#   scripts/pg-ha.sh create sync|async   new primary and new standby
#   scripts/pg-ha.sh freeze              stop the standby receiving WAL
#   scripts/pg-ha.sh thaw                let it continue
#   scripts/pg-ha.sh crash               stop the primary with no clean shutdown
#   scripts/pg-ha.sh promote             make the standby the primary
#   scripts/pg-ha.sh destroy             stop both and delete their data
set -euo pipefail
export LC_ALL="${LC_ALL:-en_US.UTF-8}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="${HOOK_RUN_DIR:-$ROOT/.run}/pg-ha"
PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
PRIMARY_PORT=54340
STANDBY_PORT=54341

start_server() {
  local data="$1" port="$2"
  shift 2
  pg_ctl -D "$data" -w -l "$data.log" -o "-p $port -k /tmp -c wal_sync_method=fsync_writethrough $*" start >/dev/null
}

stop_server() {
  [ -d "$1" ] && pg_ctl -D "$1" status >/dev/null 2>&1 && pg_ctl -D "$1" -m immediate -w stop >/dev/null
  return 0
}

receiver_pid() {
  psql -h 127.0.0.1 -p "$STANDBY_PORT" -U hook -d postgres -Atc "select pid from pg_stat_wal_receiver"
}

case "${1:-}" in
  create)
    "$0" destroy
    mkdir -p "$RUN_DIR"
    initdb -D "$RUN_DIR/primary" -U hook --auth=trust --encoding=UTF8 --no-locale >/dev/null
    echo "include '$ROOT/config/postgresql.conf'" >>"$RUN_DIR/primary/postgresql.conf"
    start_server "$RUN_DIR/primary" "$PRIMARY_PORT"
    createdb -h 127.0.0.1 -p "$PRIMARY_PORT" -U hook hookd
    pg_basebackup -h 127.0.0.1 -p "$PRIMARY_PORT" -U hook -D "$RUN_DIR/standby" -R -X stream
    start_server "$RUN_DIR/standby" "$STANDBY_PORT"
    for _ in $(seq 1 100); do
      [ -n "$(receiver_pid)" ] && break
      sleep 0.1
    done
    # Last, because a synchronous primary's commits wait for a streaming standby.
    if [ "$2" = sync ]; then
      psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U hook -d postgres -q \
        -c "ALTER SYSTEM SET synchronous_standby_names = '*'" -c "SELECT pg_reload_conf()" >/dev/null
    fi
    ;;
  freeze) kill -STOP "$(receiver_pid)" ;;
  thaw) pkill -CONT -f 'walreceiver' || true ;;
  crash) pg_ctl -D "$RUN_DIR/primary" -m immediate -w stop >/dev/null ;;
  promote) pg_ctl -D "$RUN_DIR/standby" -w promote >/dev/null ;;
  destroy)
    pkill -CONT -f 'walreceiver' || true
    stop_server "$RUN_DIR/primary"
    stop_server "$RUN_DIR/standby"
    rm -rf "$RUN_DIR"
    ;;
  *)
    echo "usage: scripts/pg-ha.sh create sync|async|freeze|thaw|crash|promote|destroy" >&2
    exit 2
    ;;
esac
