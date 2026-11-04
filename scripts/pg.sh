#!/usr/bin/env bash
# Manages two local PostgreSQL clusters: one for the engine, one for the receiver.
# They are separate so restarting the engine database leaves the ledger running.
#
#   scripts/pg.sh up                  start both, creating them on first use
#   scripts/pg.sh down                stop both
#   scripts/pg.sh destroy             stop both and delete their data
#   scripts/pg.sh crash engine        stop one cluster with no clean shutdown
#   scripts/pg.sh start engine        start one cluster and wait for it
set -euo pipefail

# PostgreSQL refuses to start on macOS without a valid locale in the environment.
export LC_ALL="${LC_ALL:-en_US.UTF-8}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="${HOOK_RUN_DIR:-$ROOT/.run}"
# fsync_writethrough is the macOS setting that flushes the drive cache.
WAL_SYNC="${HOOK_WAL_SYNC:-fsync_writethrough}"

if command -v initdb >/dev/null 2>&1; then
  PG_BIN="$(dirname "$(command -v initdb)")"
elif [ -x /opt/homebrew/opt/postgresql@17/bin/initdb ]; then
  PG_BIN=/opt/homebrew/opt/postgresql@17/bin
else
  echo "PostgreSQL not found. Install it with: brew install postgresql@17" >&2
  exit 1
fi

port_of() { case "$1" in engine) echo 54330 ;; receiver) echo 54331 ;; esac; }
db_of() { case "$1" in engine) echo hookd ;; receiver) echo receipts ;; esac; }
data_of() { echo "$RUN_DIR/pg/$1"; }

is_running() { "$PG_BIN/pg_ctl" -D "$(data_of "$1")" status >/dev/null 2>&1; }

create_cluster() {
  local name="$1" data
  data="$(data_of "$name")"
  mkdir -p "$RUN_DIR/pg"
  "$PG_BIN/initdb" -D "$data" -U hook --auth=trust --encoding=UTF8 --no-locale >/dev/null
  echo "include '$ROOT/config/postgresql.conf'" >>"$data/postgresql.conf"
}

start_cluster() {
  local name="$1" data port
  data="$(data_of "$name")"
  port="$(port_of "$name")"
  [ -d "$data" ] || create_cluster "$name"
  if ! is_running "$name"; then
    "$PG_BIN/pg_ctl" -D "$data" -w -l "$RUN_DIR/pg/$name.log" \
      -o "-p $port -k /tmp -c wal_sync_method=$WAL_SYNC" start >/dev/null
  fi
  if ! "$PG_BIN/psql" -h 127.0.0.1 -p "$port" -U hook -d postgres -Atc \
    "select 1 from pg_database where datname='$(db_of "$name")'" | grep -q 1; then
    "$PG_BIN/createdb" -h 127.0.0.1 -p "$port" -U hook "$(db_of "$name")"
  fi
}

stop_cluster() {
  local name="$1" mode="$2"
  if is_running "$name"; then
    "$PG_BIN/pg_ctl" -D "$(data_of "$name")" -m "$mode" -w stop >/dev/null
  fi
}

case "${1:-}" in
  up)
    start_cluster engine
    start_cluster receiver
    echo "engine:   postgres://hook@127.0.0.1:54330/hookd"
    echo "receiver: postgres://hook@127.0.0.1:54331/receipts"
    echo "wal_sync_method: $WAL_SYNC"
    ;;
  down)
    stop_cluster engine fast
    stop_cluster receiver fast
    ;;
  destroy)
    stop_cluster engine immediate
    stop_cluster receiver immediate
    rm -rf "$RUN_DIR/pg"
    ;;
  crash) stop_cluster "$2" immediate ;;
  start) start_cluster "$2" ;;
  *)
    echo "usage: scripts/pg.sh up|down|destroy|crash <cluster>|start <cluster>" >&2
    exit 2
    ;;
esac
