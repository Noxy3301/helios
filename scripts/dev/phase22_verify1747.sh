#!/bin/bash
# Verify the suspicious COST_V2-only TPC-C T1 = 1747 req/s. Capture not just
# throughput but GOODPUT (committed), Server Retry, and Unexpected Errors, and
# repeat — to distinguish a real plan speedup from wrong-results/skipped-work
# inflation. stock vs cost_v2-only, AGG off, one load, mysqld restart per run.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
TIME_S="${1:-30}"; TERMINALS="${2:-1}"; RUNS="${3:-2}"
MYSQL="build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock

start_cfg(){ [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  env $1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
  until $MYSQL -u root --socket=$SOCK -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done; }
one(){ # $1=label $2=env -> prints metrics line
  start_cfg "$2"
  local log; log=$(mktemp)
  python3 bench/bin/benchrun.py tpcc --external-server --no-setup --time "$TIME_S" --terminals "$TERMINALS" --prefetch-stmt > "$log" 2>&1
  local tp rt er res gp
  tp=$(grep "Throughput:" "$log" | grep -oE "[0-9]+\.[0-9]+" | head -1)
  rt=$(grep -oE "Server Retry: [0-9]+" "$log" | grep -oE "[0-9]+" | head -1)
  er=$(grep -oE "Unexpected Errors: [0-9]+" "$log" | grep -oE "[0-9]+" | head -1)
  res=$(grep -oE "Results: +[^ ]+/TPCC" "$log" | awk '{print $2}')
  gp=""
  [ -n "$res" ] && [ -f "$res/summary.csv" ] && gp=$(awk -F, -v t="$TERMINALS" '$1==t{print $3}' "$res/summary.csv")
  printf "  %-12s throughput=%-9s goodput=%-9s retry=%-8s errors=%s\n" "$1" "${tp:-FAIL}" "${gp:-?}" "${rt:-?}" "${er:-?}"
  rm -f "$log"
}

echo "=== fresh server + load TPC-C SF1 (T=$TERMINALS, ${TIME_S}s x${RUNS}) ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true
./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
./scripts/start_server.sh >/dev/null 2>&1; sleep 2; ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until $MYSQL -u root --socket=$SOCK -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpcc --external-server --no-exec 2>&1 | grep -E "Load time|ERROR" || true
start_cfg ""; python3 bench/bin/benchrun.py tpcc --external-server --no-setup --time "$TIME_S" --terminals "$TERMINALS" --prefetch-stmt >/dev/null 2>&1  # warmup

STOCK="HELIOS_COST_V2=0 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
CV2="HELIOS_COST_V2=1 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
for r in $(seq 1 "$RUNS"); do one "stock#$r"   "$STOCK"; done
for r in $(seq 1 "$RUNS"); do one "cost_v2#$r" "$CV2";   done
echo "VERIFY1747_DONE"
