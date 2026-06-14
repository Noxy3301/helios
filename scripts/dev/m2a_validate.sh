#!/bin/bash
# Phase-22 M2a validate: the access-class gate (helios_charge_materialise) should
# WIDEN the kC_materialise safe band by excluding the bulk/agg-served single-table
# grouped shape (q15) from the per-row materialise charge. Test: sweep
# HELIOS_C_MATERIALISE {8,11,16,24} on the live SF1 stack (mysqld restart only,
# server stays up) and confirm q15 stays FAST at ALL values (was: regressed at
# >=11 pre-gate) while the JOIN regressors q3/q5/q7/q8 stay flipped/fast and the
# collateral does not regress. The .so is already built with the gate; this only
# reloads SF1 + sweeps. Heavy (~35min reload+backfill, then ~6min sweep).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
MH(){ "$MYSQL" -u root --socket="$SOCK" "$DB" "$@"; }
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
[ -f "$POSTLOAD" ] || unzip -o -q "$ROOT/bench/benchbase-mysql/benchbase.jar" benchmarks/tpch/postload-mysql.sql -d /tmp/bb_tpch 2>/dev/null
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
timeq(){ local q=$1 S E T rc; S=$(date +%s.%N); timeout 150 "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$ROOT/bench/queries/q${q}.sql" >/dev/null 2>&1; rc=$?
  E=$(date +%s.%N); T=$(echo "$E-$S"|bc); local st=OK;[ $rc -eq 124 ]&&st=TMO;[ $rc -ne 0 ]&&[ $rc -ne 124 ]&&st=ERR; printf "%.2f(%s)" "$T" "$st"; }
qtree(){ grep -v '^--' "$ROOT/bench/queries/q${1}.sql" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }
factacc(){ "$MYSQL" -u root --socket="$SOCK" "$DB" -N -e "EXPLAIN FORMAT=TREE $(qtree "$1")" 2>&1 | sed 's/\\n/\n/g' | grep -oE '(Table scan on (lineitem|orders)|range scan on (lineitem|orders) using [A-Za-z0-9_]+)' | head -1; }

echo "################ fresh stack + load SF1 ################"
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 3
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR" || true
"$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true
echo "## backfill standard 23-index set ##"
( time MH < "$POSTLOAD" ) 2>&1 | tail -3
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true
echo "## SF1 indexed ready: lineitem=$(MH -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem'") idx=$(MH -N -e "SELECT COUNT(DISTINCT CONCAT(table_name,index_name)) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'") ##"

echo "################ WIDEN-WINDOW SWEEP (gate ON): q15 should stay fast at ALL values ################"
echo "  pre-gate M1: q15 broke at >=11 (1.47s@8 -> 9.96s@11). With the gate q15 should stay ~1.5s."
for X in 8 11 16 24; do
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  HELIOS_C_MATERIALISE=$X HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
  until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
  "$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
  for q in 3 5 7 8 15 1 9 13 21; do "$MYSQL" -u root --socket="$SOCK" "$DB" -N < bench/queries/q${q}.sql >/dev/null 2>&1; done  # warm
  echo "==== HELIOS_C_MATERIALISE=$X ===="
  printf "  q15(agg)=%s  | regressors q3=%s q5=%s q7=%s q8=%s | collateral q1=%s q9=%s q13=%s q21=%s\n" \
    "$(timeq 15)" "$(timeq 3)" "$(timeq 5)" "$(timeq 7)" "$(timeq 8)" "$(timeq 1)" "$(timeq 9)" "$(timeq 13)" "$(timeq 21)"
  printf "  fact-access: q7=%s q15=%s\n" "$(factacc 7)" "$(factacc 15)"
done

echo "################ FULL 22-SUITE at HELIOS_C_MATERIALISE=16 (catch false-skip under-pricing, Codex BI-1) ################"
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
HELIOS_C_MATERIALISE=16 HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
"$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
OUT=/tmp/m2a; rm -rf "$OUT"; mkdir -p "$OUT"
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/warm" 150 >/dev/null 2>&1
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/suite" 150
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "  C_mat=16 fullidx OK-sum=%.2fs (%d/22 ok)\n",s,n}' "$OUT/suite/summary.csv"
awk -F, 'NR>1 && $3!="OK"{printf "    NON-OK: %s %s\n",$1,$3}' "$OUT/suite/summary.csv"
echo "M2A_DONE"
