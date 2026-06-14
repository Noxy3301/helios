#!/bin/bash
# Phase-22 M0 addendum: does the engine_cost lever (M1) OVER-CORRECT?
# Raising LINEAIRDB engine_cost makes ALL probe access expensive -> may push the
# index-NEEDERS (q17/q20/q9 correlated subqueries) OFF their required index back
# to full-scan = regression, while fixing the regressor (q18). Measures the
# index-needers vs regressors at SF0.1 fullidx under a sweep of engine_cost.
# READ-ONLY (pure SQL lever, no recompile). Stack must be on /tmp/mysql.sock,
# SF0.1, FULLIDX already applied.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
MROOT(){ "$MYSQL" -u root --socket="$SOCK" "$@"; }
PROBE="3 7 9 17 18 20"   # regressors {3,7,18} + index-needers {9,17,20}
qfile(){ echo "$ROOT/bench/queries/q${1}.sql"; }
qtree(){ grep -v '^--' "$(qfile "$1")" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }
OUT=/tmp/m0_lever; rm -rf "$OUT"; mkdir -p "$OUT"

MROOT -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"

set_ec(){ # $1 = value, or 'none'
  if [ "$1" = none ]; then MROOT -e "DELETE FROM mysql.engine_cost WHERE engine_name='LINEAIRDB'; FLUSH OPTIMIZER_COSTS;"
  else MROOT -e "INSERT INTO mysql.engine_cost(engine_name,device_type,cost_name,cost_value) VALUES
        ('LINEAIRDB',0,'io_block_read_cost',$1),('LINEAIRDB',0,'memory_block_read_cost',$1)
        ON DUPLICATE KEY UPDATE cost_value=VALUES(cost_value); FLUSH OPTIMIZER_COSTS;"
  fi
}
timeq(){ # $1=q  -> seconds (timeout 90), prints "qN  T  status"
  local q="$1" S E T rc
  S=$(date +%s.%N)
  timeout 45 "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$(qfile "$q")" > "$OUT/q${q}.$2.out" 2>"$OUT/q${q}.$2.err"; rc=$?
  E=$(date +%s.%N); T=$(echo "$E - $S" | bc)
  local st=OK; [ $rc -eq 124 ] && st=TIMEOUT; [ $rc -ne 0 ] && [ $rc -ne 124 ] && st=ERR
  printf "  q%-3s %8.2fs %s\n" "$q" "$T" "$st"
}
access1(){ # one-line access summary for a query under current cost
  "$MYSQL" -u root --socket="$SOCK" "$DB" -N -e "EXPLAIN FORMAT=TREE $(qtree "$1")" 2>&1 | sed 's/\\n/\n/g' \
    | grep -oE '(Table scan on [a-z0-9_]+|index lookup on [a-z0-9_]+ using [A-Za-z0-9_]+|range scan on [a-z0-9_]+ using [A-Za-z0-9_]+)' | tr '\n' '|'; echo
}

for V in none 2 5 50; do
  echo "################ engine_cost(LINEAIRDB) io=mem=$V ################"
  set_ec "$V"
  # warm
  for q in $PROBE; do timeq "$q" warm >/dev/null; done
  # measure
  for q in $PROBE; do timeq "$q" "$V"; done
  echo "  -- access shapes --"
  for q in $PROBE; do printf "  q%-3s %s\n" "$q" "$(access1 "$q")"; done
done
echo "## restore (no LINEAIRDB engine_cost) ##"
set_ec none
echo "LEVER_DONE"
