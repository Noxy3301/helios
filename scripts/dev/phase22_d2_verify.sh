#!/bin/bash
# D2 verify: AGG ON + the full default bundle. Before D2, AGG hijacked the TPC-C
# Delivery SUM into a ~300k full scan -> 16.6 req/s catastrophe. After D2, the
# bounded PK-prefix aggregate is left to MySQL's normal pipeline (override
# skipped) -> TPC-C should recover (goodput ~390, errors 0). Fresh server + load.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
TIME_S="${1:-30}"
MY="build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"
LOGDIR="$ROOT/lineairdb_logs"

echo "=== fresh server + cost_v2+opt_stats+semijoin+AGG+prefetch mysqld + TPC-C load ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
./scripts/start_server.sh >/dev/null 2>&1; sleep 2
env HELIOS_COST_V2=1 HELIOS_OPT_STATS=1 HELIOS_ENABLE_SEMIJOIN=1 HELIOS_RANGE_HIST=0 \
    HELIOS_AGG_PUSHDOWN=1 HELIOS_FE_DEBUG=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
until $MY -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpcc --external-server --no-exec 2>&1 | grep -E "Load time|ERROR" || true
bash scripts/dev/prewarm_stats.sh /tmp/mysql.sock benchbase >/dev/null 2>&1 || true
LOGF=$(ls -t "$LOGDIR"/mysqld_3307_*.log 2>/dev/null | head -1)

echo "=== AGG-on TPC-C ${TIME_S}s (was 16.6 req/s catastrophe; expect ~390 goodput, 0 errors) ==="
log=$(mktemp)
python3 bench/bin/benchrun.py tpcc --external-server --no-setup --time "$TIME_S" --terminals 1 --prefetch-stmt > "$log" 2>&1
tp=$(grep "Throughput:" "$log" | grep -oE "[0-9]+\.[0-9]+" | head -1)
er=$(grep -oE "Unexpected Errors: [0-9]+" "$log" | grep -oE "[0-9]+" | head -1)
res=$(grep -oE "Results saved: [^ ]+/TPCC" "$log" | awk '{print $3}')
gp=""; [ -n "$res" ] && [ -f "$res/summary.csv" ] && gp=$(awk -F, '$1==1{print $3}' "$res/summary.csv")
printf "  throughput=%s goodput=%s errors=%s\n" "${tp:-FAIL}" "${gp:-?}" "${er:-?}"
echo "=== [AGGPD] override decisions (skip = D2 working; Delivery aggregate bounded) ==="
echo "  skip override:    $(grep -hc 'skip override' "$LOGF" 2>/dev/null || echo 0)"
echo "  install override: $(grep -hc 'installing override' "$LOGF" 2>/dev/null || echo 0)"
rm -f "$log"
echo "D2_VERIFY_DONE"
