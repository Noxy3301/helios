#!/bin/bash
# Phase-22 M0 (SF1, live stack): is the regression caused by the optimizer
# choosing an index RANGE SCAN on the big fact tables (lineitem l_sd / orders
# o_od) over full-scan+prefetch? Test: drop those range-scan indexes -> does the
# regressor TIME recover toward the noidx fast path? If yes, the real lever is
# helios_ref_cost (charge non-covering range/ref the per-row RPC cost so full
# scan wins), NOT engine_cost. READ-ONLY w.r.t. cost code. Runs on the live
# SF1-fullidx stack (/tmp/mysql.sock).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
MH(){ "$MYSQL" -u root --socket="$SOCK" "$DB" "$@"; }
PROBE="3 5 7 8"
qfile(){ echo "$ROOT/bench/queries/q${1}.sql"; }
qtree(){ grep -v '^--' "$(qfile "$1")" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }
OUT=/tmp/m0_sf1_fs; rm -rf "$OUT"; mkdir -p "$OUT"
sig(){ "$MYSQL" -u root --socket="$SOCK" "$DB" -N -e "EXPLAIN FORMAT=TREE $(qtree "$1")" 2>&1 | sed 's/\\n/\n/g' \
        | grep -oE '(Table scan on [a-z0-9_]+|index scan on [a-z0-9_]+ using [A-Za-z0-9_]+|range scan on [a-z0-9_]+ using [A-Za-z0-9_]+)' | tr '\n' '|'; }
timeq(){ local q=$1 tag=$2 S E T rc; S=$(date +%s.%N)
  timeout 150 "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$(qfile "$q")" >"$OUT/q$q.$tag.out" 2>"$OUT/q$q.$tag.err"; rc=$?
  E=$(date +%s.%N); T=$(echo "$E-$S"|bc); local st=OK; [ $rc -eq 124 ]&&st=TIMEOUT; [ $rc -ne 0 ]&&[ $rc -ne 124 ]&&st=ERR
  printf "  q%-3s %8.2fs %s\n" "$q" "$T" "$st"; }

"$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON;SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';DELETE FROM mysql.engine_cost WHERE engine_name='LINEAIRDB';FLUSH OPTIMIZER_COSTS;" 2>/dev/null

echo "################ BASELINE (full standard index set) ################"
for q in $PROBE; do printf "  q%-3s %s\n" "$q" "$(sig "$q")"; done
echo "  -- warm + timed --"; for q in $PROBE; do timeq "$q" warm0 >/dev/null; done; for q in $PROBE; do timeq "$q" base; done

echo "################ DROP fact-table range-scan indexes (l_sd, l_cd, l_rd, o_od) ################"
for ix in "l_sd ON lineitem" "l_cd ON lineitem" "l_rd ON lineitem" "o_od ON orders"; do
  MH -e "DROP INDEX ${ix%% *} ON ${ix##*ON }" 2>/dev/null && echo "  dropped ${ix}" || echo "  (skip ${ix})"
done
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true

echo "################ AFTER (date range-scan indexes gone -> expect full scan + prefetch) ################"
for q in $PROBE; do printf "  q%-3s %s\n" "$q" "$(sig "$q")"; done
echo "  -- warm + timed --"; for q in $PROBE; do timeq "$q" warm1 >/dev/null; done; for q in $PROBE; do timeq "$q" after; done

echo "(compare the 'base' vs 'after' timed lines above per query)"
echo "FS_DONE"
