#!/bin/bash
# Phase-22 default-ON OLTP safety check on the SHIPPING binary: confirm the
# default config (COST_V2+OPT_STATS+SEMIJOIN+RANGE_HIST auto-ON, AGG auto-OFF)
# is at TPC-C throughput parity with stock (all gates =0). Loads TPC-C SF1 once,
# then restarts ONLY mysqld per config (server data preserved in memory).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
TIME_S="${1:-60}"; TERMINALS="${2:-1}"

start_mysqld_cfg(){ # $1 = gate env string
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  env $1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
  until build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
}
OL_EXPLAIN="EXPLAIN SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id=1 AND ol_d_id=1 AND ol_o_id<3000 AND ol_o_id>=2980 AND s_w_id=1 AND s_i_id=ol_i_id AND s_quantity<15"
explain_ol(){ # prints order_line access type — proves whether COST_V2 is active
  build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock benchbase -N -e "$OL_EXPLAIN" 2>/dev/null \
    | awk -F'\t' 'tolower($0) ~ /order_line/ {print "    order_line access="$0}' | head -1
}
run_tpcc(){ # $1=label -> prints "tp=.. retry=.."
  local log; log=$(mktemp)
  python3 bench/bin/benchrun.py tpcc --external-server --no-setup --time "$TIME_S" --terminals "$TERMINALS" --prefetch-stmt > "$log" 2>&1
  local tp; tp=$(grep "Throughput:" "$log" | grep -oE "[0-9]+\.[0-9]+" | head -1)
  local rt; rt=$(grep -oE "Server Retry: [0-9]+" "$log" | grep -oE "[0-9]+" | head -1)
  echo "tp=${tp:-FAIL} retry=${rt:-?}"
  rm -f "$log"
}

echo "=== fresh server + load TPC-C SF1 ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true
./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
./scripts/start_server.sh >/dev/null 2>&1; sleep 2
./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpcc --external-server --no-exec 2>&1 | grep -E "Load time|ERROR" || true

echo "=== warmup (default config, discarded — absorbs cold-start/first-config bias) ==="
start_mysqld_cfg ""
run_tpcc warmup >/dev/null

echo "=== default config (COST_V2+OPT_STATS+SEMIJOIN auto-ON; RANGE_HIST+AGG off) ==="
start_mysqld_cfg ""
explain_ol   # default-ON proof: COST_V2 active changes the order_line row estimate
echo "  default: $(run_tpcc default)"
echo "=== stock (all gates =0) ==="
start_mysqld_cfg "HELIOS_COST_V2=0 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
explain_ol   # stock: no cost model => baseline order_line row estimate
echo "  stock:   $(run_tpcc stock)"
echo "=== default config AGAIN (ordering-bias check: should match the first default) ==="
start_mysqld_cfg ""
echo "  default2: $(run_tpcc default2)"
echo "TPCC_PARITY_DONE"
