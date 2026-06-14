#!/bin/bash
# Phase-22 M1 calibration sweep: HELIOS_C_MATERIALISE controls the per-row PK-
# materialisation charge on a NON-covering range/ref (read_cost). M0 trace shows
# q7's l_sd range is estimated at ~300k rows / cost 330k vs lineitem table_scan
# 3.31M, so the regressors only flip to full scan once the per-row charge is
# large. Sweep the env constant by RESTARTING mysqld only (the in-memory server
# + SF1 data + 23 indexes stay up — NO rebuild, NO re-backfill). For each value:
# does the regressor flip to full scan, does its time recover, and do the
# index-benefit / other queries (collateral) NOT regress?
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
FLIP="3 5 7 8 10"; TIMED="3 5 7 8"; COLLAT="1 9 13 21 18"
qtree(){ grep -v '^--' "$ROOT/bench/queries/q${1}.sql" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }
sig(){ "$MYSQL" -u root --socket="$SOCK" "$DB" -N -e "EXPLAIN FORMAT=TREE $(qtree "$1")" 2>&1 | sed 's/\\n/\n/g' \
  | grep -oE '(Table scan on (lineitem|orders)|range scan on (lineitem|orders) using [A-Za-z0-9_]+)' | tr '\n' '|'; }
timeq(){ local q=$1 S E T rc; S=$(date +%s.%N); timeout 150 "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$ROOT/bench/queries/q${q}.sql" >/dev/null 2>&1; rc=$?
  E=$(date +%s.%N); T=$(echo "$E-$S"|bc); local st=OK;[ $rc -eq 124 ]&&st=TMO;[ $rc -ne 0 ]&&[ $rc -ne 124 ]&&st=ERR; printf "%s(%ss)" "$T" "$st"; }
restart_mysqld(){ # $1 = kC_materialise
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  HELIOS_C_MATERIALISE="$1" HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 \
    ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
  until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
  "$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
}

echo "baselines (M0 SF1 range-scan): q3=6.10 q5=3.57 q7=13.24 q8=8.73 ; fullscan targets: q3=1.20 q5=0.99 q7=1.74 q8=2.89"
for X in 0.5 4 11 24 60; do
  restart_mysqld "$X"
  envv=$(tr '\0' '\n' < /proc/"$(cat /tmp/mysql.pid)"/environ | grep -i C_MATERIALISE | cut -d= -f2)
  echo "================ HELIOS_C_MATERIALISE=$envv ================"
  echo "  -- plan shape (fact-table access) --"
  for q in $FLIP; do printf "    q%-3s %s\n" "$q" "$(sig "$q")"; done
  # warm
  for q in $TIMED $COLLAT; do "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$ROOT/bench/queries/q${q}.sql" >/dev/null 2>&1; done
  printf "  regressors:"; for q in $TIMED; do printf " q%s=%s" "$q" "$(timeq "$q")"; done; echo
  printf "  collateral:"; for q in $COLLAT; do printf " q%s=%s" "$q" "$(timeq "$q")"; done; echo
done
echo "SWEEP_DONE"
