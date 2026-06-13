#!/bin/bash
# Start the InnoDB reference mysqld on port 3308 (datadir build/data_3308).
# Plain mysqld: no LineairDB plugin, default engine = InnoDB. Disk-persistent,
# so it survives restarts; reload only when the scale factor changes.
#
# Fair-comparison config: 16G buffer pool fully caches SF=1 so the matrix
# measures execution-engine differences, not disk I/O.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR/build"

PORT=3308
DATA_DIR="./data_${PORT}"
SOCKET="/tmp/mysql_${PORT}.sock"
PID_FILE="/tmp/mysql_${PORT}.pid"

LOG_DIR="$ROOT_DIR/lineairdb_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/mysqld_${PORT}_$(date +%Y%m%d_%H%M%S).log"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "InnoDB ref already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

if [ ! -d "$DATA_DIR" ] || [ ! -f "$DATA_DIR/ibdata1" ]; then
  echo "Initializing InnoDB ref data directory..."
  ./runtime_output_directory/mysqld --initialize-insecure --user="$USER" --datadir="$DATA_DIR"
fi

echo "Starting InnoDB ref mysqld (port $PORT)..."
nohup ./runtime_output_directory/mysqld --datadir="$DATA_DIR" --socket="$SOCKET" --port="$PORT" \
  --pid-file="$PID_FILE" \
  --innodb-buffer-pool-size=16G \
  --max-connections=16384 \
  --open-files-limit=65535 \
  --table-open-cache=8192 \
  --disable-log-bin >> "$LOG_FILE" 2>&1 &
disown

until ./runtime_output_directory/mysqladmin ping -u root --socket="$SOCKET" >/dev/null 2>&1; do
  sleep 1
done

echo "InnoDB ref running: port=$PORT socket=$SOCKET pid=$(cat "$PID_FILE") log=$LOG_FILE"
