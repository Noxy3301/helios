#!/bin/bash
# Audit B2 (corrected): a FRESH noidx SF1 load (no indexes ever created) is the
# valid in-session noidx baseline -- drop-from-fullidx gives stale-stats plans
# (q17/q20 timeout, q9 56s) that misrepresent true noidx. Same M5 build, governor
# +C6 pinned, prefetch ON. Compare to the same-build fullidx 31.75s.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; DB=benchbase
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
echo "=== fresh stack + load SF1 (NO indexes) ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true
./scripts/stop_server.sh 2>/dev/null || true; sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 3
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
until "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR" || true
"$MYSQL" -u root --socket="$HS" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
bash scripts/dev/prewarm_stats.sh "$HS" "$DB" >/dev/null 2>&1 || true
echo "  fresh noidx: lineitem=$("$MYSQL" -u root --socket="$HS" -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem'") sec_idx=$("$MYSQL" -u root --socket="$HS" -N -e "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"
echo "=== fresh-noidx suite (warm then measured, 150s timeout) ==="
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/b2_freshnoidx_warm 150 >/dev/null 2>&1
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/b2_freshnoidx 150
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "  FRESH-NOIDX OK-sum=%.2fs (%d/22 ok)\n",s,n}' /tmp/b2_freshnoidx/summary.csv
awk -F, 'NR>1 && $3!="OK"{printf "    NON-OK: %s %s\n",$1,$3}' /tmp/b2_freshnoidx/summary.csv
echo "B2_FRESH_DONE"
