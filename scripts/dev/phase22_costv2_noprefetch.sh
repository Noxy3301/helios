#!/bin/bash
# Plan B (isolate the cost_v2 TPC-C failure): run cost_v2 WITHOUT prefetch, so
# the autogen prefetch-compiler is never invoked. If cost_v2-no-prefetch has
# ~0 errors and stock-like goodput, the 46-69k errors were PURELY the autogen
# coverage gap (cost_v2 plans -> unsupported QEP -> ERROR 1235), and the fix is
# graceful fallback (plan A). If it still errors / loses goodput, cost_v2's plan
# CHOICE itself is also broken. Metric = goodput + errors (throughput lies under
# partial failure). Full setup per run (fresh server + ANALYZE).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
TIME_S="${1:-30}"; T="${2:-1}"
MY="build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"

run(){ # $1=label $2=gate-env  (NO --prefetch-stmt; prefetch forced OFF)
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  ./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
  ./scripts/start_server.sh >/dev/null 2>&1; sleep 2
  env $2 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
  until $MY -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
  $MY -e "SET GLOBAL lineairdb_prefetch_execution=OFF;" 2>/dev/null || true
  local log; log=$(mktemp)
  python3 bench/bin/benchrun.py tpcc --external-server --time "$TIME_S" --terminals "$T" > "$log" 2>&1
  local tp gp rt er res
  tp=$(grep "Throughput:" "$log" | grep -oE "[0-9]+\.[0-9]+" | head -1)
  rt=$(grep -oE "Server Retry: [0-9]+" "$log" | grep -oE "[0-9]+" | head -1)
  er=$(grep -oE "Unexpected Errors: [0-9]+" "$log" | grep -oE "[0-9]+" | head -1)
  res=$(grep -oE "Results saved: [^ ]+/TPCC" "$log" | awk '{print $3}')
  gp=""; [ -n "$res" ] && [ -f "$res/summary.csv" ] && gp=$(awk -F, -v t="$T" '$1==t{print $3}' "$res/summary.csv")
  printf "  %-22s throughput=%-9s goodput=%-9s retry=%-6s errors=%s\n" "$1" "${tp:-FAIL}" "${gp:-?}" "${rt:-?}" "${er:-?}"
  rm -f "$log"
}

echo "=== Plan B: cost_v2 WITHOUT prefetch (T=$T, ${TIME_S}s, full setup per run) ==="
run "stock / no-prefetch"   "HELIOS_COST_V2=0 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
run "cost_v2 / no-prefetch" "HELIOS_COST_V2=1 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
echo "NOPREFETCH_DONE"
