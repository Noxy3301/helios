#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/build/server/helios-storage"
# The server reads helios.cnf, then helios.local.cnf when it exists, then any
# --config given here; a later file overrides an earlier one.
CONF=(--config "$ROOT_DIR/helios.cnf")
if [ -f "$ROOT_DIR/helios.local.cnf" ]; then
  CONF+=(--config "$ROOT_DIR/helios.local.cnf")
fi
LOG_DIR="$ROOT_DIR/helios_logs"
TS=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/helios_storage_$TS.log"
PID_FILE="/tmp/helios_storage.pid"

while [ $# -gt 0 ]; do
  case "$1" in
    --config) CONF+=(--config "$2"); shift 2 ;;
    *) echo "Usage: start_server.sh [--config <path>]" >&2; exit 1 ;;
  esac
done

mkdir -p "$LOG_DIR"

# jemalloc: use LD_PRELOAD to replace glibc malloc
JEMALLOC="/lib/x86_64-linux-gnu/libjemalloc.so.2"
if [ -f "$JEMALLOC" ]; then
  export LD_PRELOAD="$JEMALLOC"
else
  echo "WARNING: jemalloc not found, using system malloc (apt install libjemalloc2)" >&2
fi

if [ ! -x "$BIN" ]; then
  echo "ERROR: binary not found: $BIN" >&2
  echo "Hint: build it via: bash scripts/build.sh (or build_partial.sh)" >&2
  exit 1
fi

if pgrep -f "/build/server/helios-storage" >/dev/null 2>&1; then
  echo "helios-storage already running. Use scripts/stop_server.sh to stop it." >&2
  exit 0
fi

echo "Starting helios-storage (${CONF[*]}) ..."
ulimit -n 1048576 2>/dev/null || ulimit -n 65535 2>/dev/null || true
nohup "$BIN" "${CONF[@]}" > "$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"

echo "Started helios-storage with PID $PID"
echo "Logs: $LOG_FILE"
echo "PID file: $PID_FILE"
