#!/bin/bash
# B1 close-out: OLTP regression with final binary (gates OFF = default path).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

restart_services() {
  ./scripts/stop_server.sh || true
  sleep 2
  ./scripts/start_server.sh
  sleep 2
  if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
  sleep 2
  ./scripts/start_mysql.sh
}

echo "##### TPCC autogen #####"
restart_services
python3 bench/bin/benchrun.py tpcc --external-server --time 60 --terminals 1 --prefetch-stmt
echo "##### TATP autogen #####"
restart_services
python3 bench/bin/benchrun.py tatp --external-server --time 60 --terminals 1 --prefetch-stmt
echo "OLTP REG DONE"
