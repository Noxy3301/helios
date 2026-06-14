#!/bin/bash
# Phase-22 M2b §7.1 acceptance: pre-registered hold-out C_materialise sweep on the
# FULLIDX SF1 suite (full deployment: prefetch+agg+semijoin ON). Demonstrates the
# physical-vs-steering gap: physical C_mat (~0.27 cu) regresses the JOIN regressors
# (q3/q5/q7/q8); the steering value 8.0 holds. Finds the lower band edge where the
# regressors flip, confirming the ~15-30x gap on the actual suite.
#
# Requires: fullidx SF1 live (server up + 23-idx backfilled). Restarts ONLY mysqld
# per C_mat value (server/data preserved). Run AFTER m2b_taxonomy backfill.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; DB=benchbase
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
MH(){ "$MYSQL" -u root --socket="$HS" "$DB" -N -e "$1" 2>/dev/null; }
QDIR=bench/queries

restart(){ # $1 = HELIOS_C_MATERIALISE value
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  HELIOS_C_MATERIALISE="$1" HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 \
    ./scripts/start_mysql.sh >/dev/null 2>&1
  for i in $(seq 1 30); do "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
  "$MYSQL" -u root --socket="$HS" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';" 2>/dev/null
}
qt(){ # time query file $1 (warm once, median of 3), prints seconds
  local f="$QDIR/$1.sql"; [ -f "$f" ] || { echo "NA"; return; }
  "$MYSQL" -u root --socket="$HS" "$DB" < "$f" >/dev/null 2>&1
  local a b c
  for v in a b c; do local s=$(date +%s.%N); "$MYSQL" -u root --socket="$HS" "$DB" < "$f" >/dev/null 2>&1; local e=$(date +%s.%N); eval $v=$(echo "$e-$s"|bc); done
  printf '%s\n' "$a $b $c" | tr ' ' '\n' | sort -n | sed -n 2p
}

REGRESSORS="q3 q5 q7 q8"; GATE_SKIP="q15 q1 q6"
echo "C_MAT | q3 q5 q7 q8 (regressors) | q15 q1 q6 (gate-skip) | note"
for CM in 0.27 1 2 4 8; do
  restart "$CM"
  line=""
  for q in $REGRESSORS; do line="$line $(qt $q)"; done
  gline=""
  for q in $GATE_SKIP; do gline="$gline $(qt $q)"; done
  echo "$CM |$line |$gline |"
done
echo "M2B_ACCEPT_DONE"
