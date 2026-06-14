#!/bin/bash
# Phase-22 cardinality: q-error check. Captures the records_in_range estimate the
# optimizer uses for the q3/q10 date ranges (via optimizer_trace, visible even when
# the range is not the chosen plan) and compares to the known actuals. Run with
# HELIOS_RANGE_HIST unset (baseline /10,/20) vs set (histogram). $1 = label.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; DB=benchbase
LABEL="${1:-?}"
MH(){ "$MYSQL" -u root --socket="$HS" "$DB" "$@"; }

# (predicate description | forced-index query | known actual rows)
probe(){
  local desc="$1" q="$2" actual="$3"
  MH -e "SET SESSION optimizer_trace='enabled=on'; SET SESSION optimizer_trace_max_mem_size=8388608; EXPLAIN $q; SELECT TRACE FROM information_schema.OPTIMIZER_TRACE\G" 2>/dev/null \
    > /tmp/card_trace.txt
  # the range_scan_alternatives "rows" for the forced index (first numeric "rows" in a ranges block)
  local est
  est=$(grep -oE '"rows": [0-9]+' /tmp/card_trace.txt | head -3 | grep -oE '[0-9]+' | sort -rn | head -1)
  [ -z "$est" ] && est="NA"
  local qerr="NA"
  if [ "$est" != "NA" ] && [ "$actual" -gt 0 ] 2>/dev/null; then
    qerr=$(echo "scale=2; if ($est>$actual) $est/$actual else $actual/$est" | bc 2>/dev/null)
  fi
  printf "  [%s] %-34s est=%-9s actual=%-9s q-error=%s\n" "$LABEL" "$desc" "$est" "$actual" "$qerr"
}

echo "=== q-error ($LABEL) ==="
probe "orders o_orderdate<1995-03-15 (1side)" \
  "SELECT o_orderkey FROM orders FORCE INDEX(o_od) WHERE o_orderdate < '1995-03-15'" 727305
probe "lineitem l_shipdate>1995-03-15 (1side)" \
  "SELECT l_orderkey FROM lineitem FORCE INDEX(l_sd) WHERE l_shipdate > '1995-03-15'" 3241776
probe "orders o_orderdate 1993Q4 (2side)" \
  "SELECT o_orderkey FROM orders FORCE INDEX(o_od) WHERE o_orderdate BETWEEN '1993-10-01' AND '1993-12-31'" 57069
echo "QERROR_DONE_$LABEL"
