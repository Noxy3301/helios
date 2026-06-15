#!/bin/bash
# Phase-22 OLTP gate isolation: the default bundle showed a reproducible ~27x
# TPC-C regression at terminals=1, contradicting an earlier (terminals=8?)
# isolation. Decompose: which gate flips throughput, and does RANGE_HIST rescue
# the order_line records_in_range overestimate (15002 vs 273)? One load, mysqld
# restart per config (server data preserved). AGG OFF throughout.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
TIME_S="${1:-30}"; TERMINALS="${2:-1}"
MYSQL="build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock
OL_EXPLAIN="EXPLAIN SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id=1 AND ol_d_id=1 AND ol_o_id<3000 AND ol_o_id>=2980 AND s_w_id=1 AND s_i_id=ol_i_id AND s_quantity<15"

start_cfg(){ [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  env $1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
  until $MYSQL -u root --socket=$SOCK -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done; }
ol_rows(){ $MYSQL -u root --socket=$SOCK benchbase -N -e "$OL_EXPLAIN" 2>/dev/null \
  | awk -F'\t' 'tolower($0)~/order_line/{print $5"/rows="$10; exit}'; }
tpcc(){ local log; log=$(mktemp)
  python3 bench/bin/benchrun.py tpcc --external-server --no-setup --time "$TIME_S" --terminals "$TERMINALS" --prefetch-stmt > "$log" 2>&1
  grep "Throughput:" "$log" | grep -oE "[0-9]+\.[0-9]+" | head -1; rm -f "$log"; }
run(){ start_cfg "$2"; printf "  %-26s ol=%-18s tp=%s\n" "$1" "$(ol_rows)" "$(tpcc)"; }

echo "=== fresh server + load TPC-C SF1 (terminals=$TERMINALS, ${TIME_S}s/cfg) ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true
./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
./scripts/start_server.sh >/dev/null 2>&1; sleep 2; ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until $MYSQL -u root --socket=$SOCK -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpcc --external-server --no-exec 2>&1 | grep -E "Load time|ERROR" || true
start_cfg ""; tpcc >/dev/null   # warmup

ALLOFF="HELIOS_COST_V2=0 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
run "stock (all off)"        "$ALLOFF"
run "cost_v2 only"           "HELIOS_COST_V2=1 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
run "cv2+opt_stats"          "HELIOS_COST_V2=1 HELIOS_OPT_STATS=1 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
run "cv2+opt+semijoin(DEF)"  "HELIOS_COST_V2=1 HELIOS_OPT_STATS=1 HELIOS_ENABLE_SEMIJOIN=1 HELIOS_RANGE_HIST=0 HELIOS_AGG_PUSHDOWN=0"
run "DEF + range_hist"       "HELIOS_COST_V2=1 HELIOS_OPT_STATS=1 HELIOS_ENABLE_SEMIJOIN=1 HELIOS_RANGE_HIST=1 HELIOS_AGG_PUSHDOWN=0"
echo "GATEISO_DONE"
