#!/bin/bash
# Phase-22 M1 validate: AFTER the read_cost (non-covering) cost fix is in the
# source, rebuild + fresh SF1 stack + standard 23-index set, then check the
# regressors flip to full-scan+prefetch and recover, the full suite does not
# regress, and results stay correct. engine_cost stays at DEFAULT (we are
# testing the helios_ref_cost/read_cost code fix, not engine_cost).
# Heavy (~35min: build + load + 30min backfill). Run detached.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
MH(){ "$MYSQL" -u root --socket="$SOCK" "$DB" "$@"; }
MROOT(){ "$MYSQL" -u root --socket="$SOCK" "$@"; }
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
OUT=/tmp/m1; rm -rf "$OUT"; mkdir -p "$OUT"
REG="3 5 7 8 10 18"
qtree(){ grep -v '^--' "$ROOT/bench/queries/q${1}.sql" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }
rss_gb(){ awk '/VmRSS/{printf "%.1f",$2/1048576}' /proc/"$(cat /tmp/mysql.pid 2>/dev/null)"/status 2>/dev/null || echo "?"; }
# M0 SF1 baselines (range-scan regression) and full-scan recovery targets:
declare -A BASE=( [3]=6.10 [5]=3.57 [7]=13.24 [8]=8.73 [18]=23.34 )
declare -A TGT=(  [3]=1.20 [5]=0.99 [7]=1.74  [8]=2.89  [18]=23.34 )

echo "################ [0] BUILD (services down first) ################"
./scripts/stop_mysql.sh 2>/dev/null || { [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null; }
./scripts/stop_server.sh 2>/dev/null || true; sleep 2
( time bash scripts/build_partial.sh ) 2>&1 | tail -6
[ -f build/plugin_output_directory/ha_lineairdb_storage_engine.so ] || { echo "BUILD FAILED"; exit 1; }

echo "################ [1] FRESH STACK + LOAD SF1 ################"
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 3
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR" || true
MROOT -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
echo "  env:"; tr '\0' '\n' < /proc/"$(cat /tmp/mysql.pid)"/environ | grep -iE 'HELIOS' | sort | sed 's/^/    /'
echo "  HELIOS_C_MATERIALISE=$(tr '\0' '\n' < /proc/"$(cat /tmp/mysql.pid)"/environ | grep -i C_MATERIALISE || echo '(default in code)')"
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true

echo "################ [2] BACKFILL standard 23-index set ################"
( time MH < "$POSTLOAD" ) 2>&1 | tail -3
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true
echo "  SF1 INDEXED READY rss=$(rss_gb)GB"

echo "################ [3] PLAN-SHAPE: do regressors now choose full scan? ################"
for q in $REG; do
  s=$(MH -N -e "EXPLAIN FORMAT=TREE $(qtree "$q")" 2>&1 | sed 's/\\n/\n/g' | grep -oE '(Table scan on (lineitem|orders)|range scan on (lineitem|orders) using [A-Za-z0-9_]+)' | tr '\n' '|')
  printf "  q%-3s %s\n" "$q" "$s"
done

echo "################ [4] TIMED regressors (fullidx) vs M0 baseline / target ################"
for q in $REG; do MH -N < "$ROOT/bench/queries/q${q}.sql" >/dev/null 2>&1; done   # warm
for q in 3 5 7 8 18; do
  S=$(date +%s.%N); timeout 150 "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$ROOT/bench/queries/q${q}.sql" >"$OUT/q$q.out" 2>"$OUT/q$q.err"; rc=$?
  E=$(date +%s.%N); T=$(echo "$E-$S"|bc); st=OK; [ $rc -eq 124 ]&&st=TIMEOUT; [ $rc -ne 0 ]&&[ $rc -ne 124 ]&&st=ERR
  printf "  q%-3s %8.2fs %-7s  (M0 range=%ss  target≈%ss)\n" "$q" "$T" "$st" "${BASE[$q]}" "${TGT[$q]}"
done

echo "################ [5] FULL 22-suite timing (regression check) ################"
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/suite" 150
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "  fullidx OK-sum=%.2fs (%d ok)\n",s,n}' "$OUT/suite/summary.csv"
awk -F, 'NR>1 && $3!="OK"{printf "    %s %s\n",$1,$3}' "$OUT/suite/summary.csv"

echo "################ [6] CORRECTNESS md5 vs InnoDB:3308 (if SF1-loaded) ################"
if "$MYSQL" -u root --socket=/tmp/mysql_3308.sock -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem'" 2>/dev/null | grep -q '[0-9]'; then
  bash bench/bin/tpch_md5.sh --target-socket "$SOCK" --ref-socket /tmp/mysql_3308.sock --db "$DB" 2>&1 | tail -26
else
  echo "  (InnoDB:3308 not SF1-loaded — md5 skipped; cost-only change cannot alter results, full-scan plans already md5-verified in noidx config)"
fi
echo "M1_VALIDATE_DONE rss=$(rss_gb)GB"
