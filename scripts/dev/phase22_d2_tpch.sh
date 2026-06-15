#!/bin/bash
# D2 TPC-H check: AGG ON + full bundle. Verify D2 did NOT break the AGG benefit on
# TPC-H — q1/q6/q18 are full-scan aggregates (large estimate) so the override must
# still FIRE (install, not skip). Expect md5 22/22 and 31.75s-class suite. Fresh
# helios load (the DB stack was stopped); InnoDB ref :3308 reused for md5.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; IS=/tmp/mysql_3308.sock; DB=benchbase
LOGDIR="$ROOT/lineairdb_logs"

echo "=== fresh helios server + TPC-H SF1 load (fullidx), AGG ON ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 2
env HELIOS_COST_V2=1 HELIOS_OPT_STATS=1 HELIOS_ENABLE_SEMIJOIN=1 HELIOS_RANGE_HIST=0 \
    HELIOS_AGG_PUSHDOWN=1 HELIOS_FE_DEBUG=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR" || true
"$MYSQL" -u root --socket="$HS" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"
bash "$ROOT/scripts/dev/prewarm_stats.sh" "$HS" "$DB" >/dev/null 2>&1 || true
LOGF=$(ls -t "$LOGDIR"/mysqld_3307_*.log 2>/dev/null | head -1)

echo "  InnoDB ref(3308) lineitem=$("$MYSQL" -u root --socket="$IS" -N -e "SELECT COUNT(*) FROM $DB.lineitem" 2>/dev/null || echo DOWN)"
echo "=== AGG-on TPC-H fullidx md5 (helios vs InnoDB) ==="
bash bench/bin/tpch_md5.sh --target-socket "$HS" --ref-socket "$IS" --db "$DB" 2>&1 | tail -3
echo "=== AGG-on TPC-H fullidx suite (expect 31.75s-class via AGG) ==="
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/d2_full_warm 150 >/dev/null 2>&1
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/d2_full 150 >/dev/null 2>&1
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "  FULLIDX OK-sum=%.2fs (%d/22)\n",s,n}' /tmp/d2_full/summary.csv
echo "=== [AGGPD] on TPC-H (install = AGG firing on full-scan aggs; skip = bounded) ==="
echo "  install override: $(grep -hc 'installing override' "$LOGF" 2>/dev/null || echo 0)"
echo "  skip override:    $(grep -hc 'skip override' "$LOGF" 2>/dev/null || echo 0)"
echo "D2_TPCH_DONE"
