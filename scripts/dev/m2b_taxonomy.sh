#!/bin/bash
# Phase-22 M2b grounding: measure the RPC taxonomy per access-class x prefetch
# regime (docs/phase22_m2b_taxonomy.md) AND the pivotal full-scan-vs-range latency
# crossover under prefetch ON (docs/phase22_m2b_design.md premise).
#
# Restarts ONLY mysqld (server stays up, SF1 data preserved). COST_V2 on,
# AGG_PUSHDOWN/SEMIJOIN OFF to isolate the raw access paths. Two phases:
#   [TAX]   ENABLE_RPC_TRACE on, 6 probes x {prefetch ON, OFF} -> summary_by_type
#   [PIVOT] trace off, full-scan vs 2ary-range latency across selectivity (ON)
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; DB=benchbase
TRACE=/tmp/m2b_tax.jsonl
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
MH(){ "$MYSQL" -u root --socket="$HS" "$DB" "$@"; }

restart_mysqld(){ # $1 = extra env string (e.g. "ENABLE_RPC_TRACE=1 ...")
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  env $1 HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 ./scripts/start_mysql.sh >/dev/null 2>&1
  for i in $(seq 1 30); do "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
  "$MYSQL" -u root --socket="$HS" -e "SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';" 2>/dev/null
}
prefetch(){ "$MYSQL" -u root --socket="$HS" -e "SET GLOBAL lineairdb_prefetch_execution=$1; SET GLOBAL lineairdb_prefetch_ro_novalidate=$1;" 2>/dev/null; }

# ---------- [TAX] ----------
echo "=== [TAX] restart mysqld with RPC trace ==="
rm -f "$TRACE"
restart_mysqld "ENABLE_RPC_TRACE=1 ENABLE_RPC_TRACE_PATH=$TRACE"
run_probes(){ local m="$1"; prefetch "$m"
  MH -N -e "SELECT SUM(s_acctbal) FROM supplier IGNORE INDEX(s_nationkey)" >/dev/null 2>&1
  MH -N -e "SELECT COUNT(s_nationkey) FROM (SELECT s_nationkey FROM supplier FORCE INDEX(s_nationkey) WHERE s_nationkey BETWEEN 5 AND 9) t" >/dev/null 2>&1
  MH -N -e "SELECT SUM(s_acctbal) FROM supplier FORCE INDEX(s_nationkey) WHERE s_nationkey BETWEEN 5 AND 9" >/dev/null 2>&1
  MH -N -e "SELECT STRAIGHT_JOIN SUM(o.o_totalprice) FROM (SELECT o_orderkey FROM orders WHERE o_orderkey BETWEEN 1 AND 2000) d STRAIGHT_JOIN orders o ON o.o_orderkey=d.o_orderkey" >/dev/null 2>&1
  MH -N -e "SELECT s_acctbal FROM supplier WHERE s_suppkey=42" >/dev/null 2>&1
  MH -N -e "SELECT STRAIGHT_JOIN SUM(s.s_acctbal) FROM nation n STRAIGHT_JOIN supplier s FORCE INDEX(s_nationkey) ON s.s_nationkey=n.n_nationkey WHERE n.n_nationkey BETWEEN 5 AND 9" >/dev/null 2>&1
}
run_probes ON; run_probes OFF; sleep 1
echo "trace -> $TRACE ($(wc -l < "$TRACE") tx). Parse with the python in docs/phase22_m2b_taxonomy.md."

# ---------- [PIVOT] ----------
echo "=== [PIVOT] restart mysqld WITHOUT trace; full-scan vs 2ary-range latency (prefetch ON) ==="
restart_mysqld ""
prefetch ON
t(){ local sql="$1" a b c; MH -N -e "$sql" >/dev/null 2>&1
  for v in a b c; do local s=$(date +%s.%N); MH -N -e "$sql" >/dev/null 2>&1; local e=$(date +%s.%N); eval $v=$(echo "$e-$s"|bc); done
  printf '%s\n' "$a $b $c" | tr ' ' '\n' | sort -n | sed -n 2p; }
echo "full_scan = $(t "SELECT SUM(l_extendedprice) FROM lineitem IGNORE INDEX(l_partkey)")s"
printf "%-10s %-9s %-12s %-12s\n" "partkey<=" "R_rows" "range_noncov" "range_cov"
for P in 2000 20000 60000 120000; do
  R=$(MH -N -e "SELECT COUNT(*) FROM lineitem WHERE l_partkey BETWEEN 1 AND $P")
  nc=$(t "SELECT SUM(l_extendedprice) FROM lineitem FORCE INDEX(l_partkey) WHERE l_partkey BETWEEN 1 AND $P")
  cv=$(t "SELECT SUM(l_partkey) FROM lineitem FORCE INDEX(l_partkey) WHERE l_partkey BETWEEN 1 AND $P")
  printf "%-10s %-9s %-12s %-12s\n" "$P" "$R" "${nc}s" "${cv}s"
done
echo "M2B_TAXONOMY_DONE"
