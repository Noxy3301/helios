#!/bin/bash
# D1 verify: cost_v2+OPT_STATS (the path where the /2 bug bit). Expect the
# order_line composite-key trailing-range estimate 17802 -> ~200, and TPC-C
# clean (goodput ~390, errors 0, reject 0) now that A2a (eq_ref) + D1 (range
# cardinality) both apply. Fresh server + full load (one execute).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
TIME_S="${1:-30}"
MY="build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"
OL="EXPLAIN SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id=1 AND ol_d_id=1 AND ol_o_id<3000 AND ol_o_id>=2980 AND s_w_id=1 AND s_i_id=ol_i_id AND s_quantity<15"
LOGDIR="$ROOT/lineairdb_logs"

echo "=== fresh server + cost_v2+opt_stats+prefetch mysqld + TPC-C full load ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
./scripts/start_server.sh >/dev/null 2>&1; sleep 2
env HELIOS_COST_V2=1 HELIOS_OPT_STATS=1 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 \
    HELIOS_AGG_PUSHDOWN=0 HELIOS_REJECT_DEBUG=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
until $MY -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpcc --external-server --no-exec 2>&1 | grep -E "Load time|ERROR" || true
bash scripts/dev/prewarm_stats.sh /tmp/mysql.sock benchbase >/dev/null 2>&1 || true

echo "=== D1: order_line range estimate (cost_v2+opt_stats) — expect ~200, was 17802 ==="
$MY benchbase -e "${OL}\G" 2>&1 | grep -E "table:|type:|key:|rows:" | head -8

echo "=== cost_v2+opt_stats+prefetch TPC-C ${TIME_S}s (goodput+errors+reject) ==="
LOGF=$(ls -t "$LOGDIR"/mysqld_3307_*.log 2>/dev/null | head -1)
log=$(mktemp)
python3 bench/bin/benchrun.py tpcc --external-server --no-setup --time "$TIME_S" --terminals 1 --prefetch-stmt > "$log" 2>&1
tp=$(grep "Throughput:" "$log" | grep -oE "[0-9]+\.[0-9]+" | head -1)
er=$(grep -oE "Unexpected Errors: [0-9]+" "$log" | grep -oE "[0-9]+" | head -1)
res=$(grep -oE "Results saved: [^ ]+/TPCC" "$log" | awk '{print $3}')
gp=""; [ -n "$res" ] && [ -f "$res/summary.csv" ] && gp=$(awk -F, '$1==1{print $3}' "$res/summary.csv")
printf "  throughput=%s goodput=%s errors=%s\n" "${tp:-FAIL}" "${gp:-?}" "${er:-?}"
echo "  reject lines: $(grep -hcE '\[REJECT-' "$LOGF" 2>/dev/null || echo 0)"
rm -f "$log"
echo "D1_VERIFY_DONE"
