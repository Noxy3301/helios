#!/bin/bash

set -euo pipefail

SERVER_HOST="127.0.0.1"
SERVER_PORT=9999
MYSQLD_PORT=3307

usage() {
  cat <<USAGE
Usage: $0 [--mysqld-port N] [--server-host HOST] [--server-port PORT]
Defaults: mysqld-port=3307, server=127.0.0.1:9999
Data dir / socket are derived from mysqld-port (3307 -> data,/tmp/mysql.sock; others -> data_PORT,/tmp/mysql_PORT.sock)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysqld-port) MYSQLD_PORT="$2"; shift 2;;
    --server-host) SERVER_HOST="$2"; shift 2;;
    --server-port) SERVER_PORT="$2"; shift 2;;
    --help|-h) usage; exit 0;;
    --) shift; break;;
    -*) echo "Unknown option: $1" >&2; usage; exit 2;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/build"

# jemalloc: use LD_PRELOAD to replace glibc malloc
JEMALLOC="/lib/x86_64-linux-gnu/libjemalloc.so.2"
if [ -f "$JEMALLOC" ]; then
  export LD_PRELOAD="$JEMALLOC"
else
  echo "WARNING: jemalloc not found, using system malloc (apt install libjemalloc2)" >&2
fi

DATA_DIR="./data"
SOCKET="/tmp/mysql.sock"
PID_FILE="/tmp/mysql.pid"
if [ "$MYSQLD_PORT" != "3307" ]; then
  DATA_DIR="./data_${MYSQLD_PORT}"
  SOCKET="/tmp/mysql_${MYSQLD_PORT}.sock"
  PID_FILE="/tmp/mysql_${MYSQLD_PORT}.pid"
fi

# Per-instance log so the background mysqld does not inherit the caller's
# stdout/stderr (otherwise subprocess.run() in benchrun.py blocks forever
# waiting for the inherited pipe to close).
MYSQL_LOG_DIR="$ROOT_DIR/lineairdb_logs"
mkdir -p "$MYSQL_LOG_DIR"
MYSQL_LOG_FILE="$MYSQL_LOG_DIR/mysqld_${MYSQLD_PORT}_$(date +%Y%m%d_%H%M%S).log"

# Step 1: Initialize if data directory doesn't exist
if [ ! -d "$DATA_DIR" ] || [ ! -f "$DATA_DIR/ibdata1" ]; then
  echo "Step 1/5: Initializing MySQL data directory..."
  ./runtime_output_directory/mysqld --initialize-insecure --user="$USER" --datadir="$DATA_DIR"
fi

echo "Step 2/5: Starting MySQL with InnoDB..."
./runtime_output_directory/mysqld --datadir="$DATA_DIR" --socket="$SOCKET" --port="$MYSQLD_PORT" \
  --pid-file="$PID_FILE" \
  --max-connections=16384 \
  --open-files-limit=65535 \
  --table-open-cache=8192 \
  --skip-name-resolve \
  --disable-log-bin >> "$MYSQL_LOG_FILE" 2>&1 &
BOOT_PID=$!

echo "Step 3/5: Waiting for MySQL to be ready..."
until ./runtime_output_directory/mysqladmin ping -u root --socket="$SOCKET" --port="$MYSQLD_PORT" >/dev/null 2>&1; do
  sleep 1
done

echo "Step 4/5: Installing LineairDB plugin..."
./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$MYSQLD_PORT" \
  -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true
./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$MYSQLD_PORT" \
  -e "INSTALL PLUGIN lineairdb_columnar SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true

# --skip-name-resolve makes TCP clients match accounts by IP literal only;
# 'root'@'localhost' stays socket-only, so create loopback root accounts.
./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$MYSQLD_PORT" \
  -e "CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY '';
      GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;
      CREATE USER IF NOT EXISTS 'root'@'::1' IDENTIFIED WITH mysql_native_password BY '';
      GRANT ALL PRIVILEGES ON *.* TO 'root'@'::1' WITH GRANT OPTION;
      FLUSH PRIVILEGES;" 2>/dev/null || true

echo "Step 5/5: Stopping MySQL and restarting with LineairDB as default..."
kill "$BOOT_PID" 2>/dev/null || true
wait "$BOOT_PID" 2>/dev/null || true
sleep 3

nohup ./runtime_output_directory/mysqld --datadir="$DATA_DIR" --socket="$SOCKET" --port="$MYSQLD_PORT" \
  --pid-file="$PID_FILE" --default-storage-engine=lineairdb \
  --max-connections=16384 \
  --open-files-limit=65535 \
  --table-open-cache=8192 \
  --skip-name-resolve \
  --disable-log-bin >> "$MYSQL_LOG_FILE" 2>&1 &
MYSQL_PID=$!
disown "$MYSQL_PID" 2>/dev/null || true

until ./runtime_output_directory/mysqladmin ping -u root --socket="$SOCKET" --port="$MYSQLD_PORT" >/dev/null 2>&1; do
  sleep 1
done

./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$MYSQLD_PORT" \
  -e "SET GLOBAL lineairdb_server_host='${SERVER_HOST}'; SET GLOBAL lineairdb_server_port=${SERVER_PORT};" >/dev/null

# Prove the loopback root account authenticates over TCP: every check above
# runs on the socket and would report success even if provisioning failed.
./runtime_output_directory/mysql -u root --protocol=TCP --host=127.0.0.1 --port="$MYSQLD_PORT" \
  -e "SELECT 1;" >/dev/null

echo "MySQL running with LineairDB"
echo "PID       : $MYSQL_PID"
echo "Port      : $MYSQLD_PORT"
echo "Data dir  : $DATA_DIR"
echo "Socket    : $SOCKET"
echo "Server    : ${SERVER_HOST}:${SERVER_PORT}"
echo "Log       : $MYSQL_LOG_FILE"
