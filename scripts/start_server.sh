#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/build/server/lineairdb-server"
LOG_DIR="$ROOT_DIR/lineairdb_logs"
TS=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/lineairdb_server_$TS.log"
PID_FILE="/tmp/lineairdb_server.pid"

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

if pgrep -f "/build/server/lineairdb-server" >/dev/null 2>&1; then
  echo "lineairdb-server already running. Use scripts/stop_server.sh to stop it." >&2
  exit 0
fi

echo "Starting lineairdb-server (port 9999) ..."
ulimit -n 1048576 2>/dev/null || ulimit -n 65535 2>/dev/null || true
nohup "$BIN" > "$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"

echo "Started lineairdb-server with PID $PID"
echo "Logs: $LOG_FILE"
echo "PID file: $PID_FILE"
