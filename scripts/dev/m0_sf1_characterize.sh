#!/bin/bash
# Phase-22 M0 (SF1 extension, per Codex safe protocol): reproduce the regression
# PLAN SHAPE at SF1 where the per-row NLJ pathology actually lives, and test
# whether the engine_cost lever (M1) improves the regressors' TIME at SF1.
# SAFE: never executes the OOM hazard set (q17/q20 noidx). Only EXPLAIN (no exec)
# for plan shapes + timed INDEXED regressors (joins, memory-bounded) with an
# mysqld-RSS guard. READ-ONLY w.r.t. cost code (no recompile).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
MH(){ "$MYSQL" -u root --socket="$SOCK" "$DB" "$@"; }
MROOT(){ "$MYSQL" -u root --socket="$SOCK" "$@"; }
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
OUT=/tmp/m0_sf1; rm -rf "$OUT"; mkdir -p "$OUT"
REG_EXPLAIN="3 5 7 8 10 18"     # plan-shape capture
REG_TIME="3 5 7 8 18"           # timed indexed regressors (memory-bounded joins)
RSS_KILL_GB=34                  # skip a timed query if mysqld RSS already exceeds this
qfile(){ echo "$ROOT/bench/queries/q${1}.sql"; }
qtree(){ grep -v '^--' "$(qfile "$1")" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }
rss_gb(){ awk '/VmRSS/{printf "%.1f",$2/1048576}' /proc/"$(cat /tmp/mysql.pid 2>/dev/null)"/status 2>/dev/null || echo "?"; }
[ -f "$POSTLOAD" ] || unzip -o -q "$ROOT/bench/benchbase-mysql/benchbase.jar" benchmarks/tpch/postload-mysql.sql -d /tmp/bb_tpch 2>/dev/null
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true

echo "################ [1] FRESH STACK (3307 only; InnoDB 3308 untouched) ################"
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true
./scripts/stop_server.sh || true; sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 3
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
echo "  stack up. mysqld env:"; tr '\0' '\n' < /proc/"$(cat /tmp/mysql.pid)"/environ | grep -iE 'HELIOS' | sort | sed 's/^/    /'

echo "################ [2] LOAD SF1 ################"
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR" || echo "  (load output filtered)"
MROOT -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"
MROOT -e "SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
echo "  optimizer_switch (logged):"; MH -N -e "SELECT @@global.optimizer_switch;" | tr ',' '\n' | grep -iE 'hypergraph|mrr|batched_key|hash_join' | sed 's/^/    /'
echo "  lineitem rows:"; MH -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem';" | sed 's/^/    /'
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true

echo "################ [3] BACKFILL standard 23-index set (slow at SF1) ################"
( time MH < "$POSTLOAD" ) 2>&1 | tail -4
MH -N -e "SELECT CONCAT('secondary_index_count=',COUNT(DISTINCT CONCAT(table_name,index_name))) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY';" | sed 's/^/  /'
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true
echo "  SF1 INDEXED READY. mysqld RSS=$(rss_gb)GB"

echo "################ [4] CAPTURES: EXPLAIN (plan) + TIMED indexed regressors, engine_cost none vs 50 ################"
for V in none 50; do
  echo "==== engine_cost(LINEAIRDB) io=mem=$V ===="
  if [ "$V" = none ]; then MROOT -e "DELETE FROM mysql.engine_cost WHERE engine_name='LINEAIRDB'; FLUSH OPTIMIZER_COSTS;"
  else MROOT -e "INSERT INTO mysql.engine_cost(engine_name,device_type,cost_name,cost_value) VALUES
        ('LINEAIRDB',0,'io_block_read_cost',$V),('LINEAIRDB',0,'memory_block_read_cost',$V)
        ON DUPLICATE KEY UPDATE cost_value=VALUES(cost_value); FLUSH OPTIMIZER_COSTS;"; fi
  mkdir -p "$OUT/explain_$V"
  echo "  -- plan shapes --"
  for q in $REG_EXPLAIN; do
    MH -N -e "EXPLAIN FORMAT=TREE $(qtree "$q")" 2>&1 | sed 's/\\n/\n/g' > "$OUT/explain_$V/q${q}.tree"
    sig=$(grep -oE '(Table scan on [a-z0-9_]+|index lookup on [a-z0-9_]+ using [A-Za-z0-9_]+|index scan on [a-z0-9_]+ using [A-Za-z0-9_]+|range scan on [a-z0-9_]+ using [A-Za-z0-9_]+|Hash)' "$OUT/explain_$V/q${q}.tree" | tr '\n' '|')
    printf "    q%-3s %s\n" "$q" "$sig"
  done
  echo "  -- timed indexed regressors (RSS-guarded, timeout 150) --"
  for q in $REG_TIME; do
    cur=$(rss_gb)
    if awk "BEGIN{exit !($cur > $RSS_KILL_GB)}" 2>/dev/null; then printf "    q%-3s SKIP (RSS=%sGB > %dGB)\n" "$q" "$cur" "$RSS_KILL_GB"; continue; fi
    S=$(date +%s.%N)
    timeout 150 "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$(qfile "$q")" > "$OUT/q${q}.$V.out" 2> "$OUT/q${q}.$V.err"; rc=$?
    E=$(date +%s.%N); T=$(echo "$E - $S" | bc)
    st=OK; [ $rc -eq 124 ] && st=TIMEOUT; [ $rc -ne 0 ] && [ $rc -ne 124 ] && st=ERR
    printf "    q%-3s %8.2fs %s  (RSS now %sGB)\n" "$q" "$T" "$st" "$(rss_gb)"
  done
done
MROOT -e "DELETE FROM mysql.engine_cost WHERE engine_name='LINEAIRDB'; FLUSH OPTIMIZER_COSTS;"

echo "################ [5] PLAN-SHAPE DIFF none vs 50 (did the lever flip the regressors at SF1?) ################"
for q in $REG_EXPLAIN; do
  if diff -q "$OUT/explain_none/q${q}.tree" "$OUT/explain_50/q${q}.tree" >/dev/null 2>&1; then echo "  q$q: unchanged"
  else echo "  q$q: FLIPPED"; fi
done
echo "SF1_DONE  (mysqld RSS=$(rss_gb)GB)"
