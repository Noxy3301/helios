#!/bin/bash
# Phase-22 cardinality CO-TUNING headline: with the range histogram ON
# (HELIOS_RANGE_HIST=1), sweep C_materialise on the fullidx SF1 regressors. Question:
# once range cardinality is corrected, can C_materialise drop BELOW the M2b lower band
# edge (~8) while the join regressors stay fast? (If yes, the steering inflation
# partially retires; if it stays ~8, the residual is join-NLJ, not cardinality.)
# Compare directly to docs/data/m2b_sweep.log (histogram OFF). Restarts ONLY mysqld
# per value (server/data preserved). $1 = "ON" (default) or "OFF" (gate off, control).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; DB=benchbase
GATE="${1:-ON}"; QDIR=bench/queries
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true

restart(){ # $1 = C_materialise
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  local extra=""; [ "$GATE" = "ON" ] && extra="HELIOS_RANGE_HIST=1"
  env $extra HELIOS_C_MATERIALISE="$1" HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 \
    HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1
  for i in $(seq 1 30); do "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
  "$MYSQL" -u root --socket="$HS" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';" 2>/dev/null
  bash scripts/dev/prewarm_stats.sh "$HS" "$DB" >/dev/null 2>&1 || true  # build/seed histograms
}
qt(){ local f="$QDIR/$1.sql"; [ -f "$f" ] || { echo NA; return; }
  "$MYSQL" -u root --socket="$HS" "$DB" < "$f" >/dev/null 2>&1
  local a b c; for v in a b c; do local s=$(date +%s.%N); "$MYSQL" -u root --socket="$HS" "$DB" < "$f" >/dev/null 2>&1; local e=$(date +%s.%N); eval $v=$(echo "$e-$s"|bc); done
  printf '%s\n' "$a $b $c" | tr ' ' '\n' | sort -n | sed -n 2p; }

echo "=== co-tuning sweep (HELIOS_RANGE_HIST=$GATE) | q3 q5 q7 q8 | q15 q1 q6 ==="
for CM in 0.27 1 2 4 8; do
  restart "$CM"
  r=""; for q in q3 q5 q7 q8; do r="$r $(qt $q)"; done
  g=""; for q in q15 q1 q6; do g="$g $(qt $q)"; done
  echo "C_mat=$CM |$r |$g"
done
echo "CARD_ACCEPT_DONE_$GATE"
