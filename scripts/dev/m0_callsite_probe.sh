#!/bin/bash
# Phase-22 M0: reproduce the secondary-index plan regression at SF0.1, prove the
# per-site call-target (eq_ref via Cost_model_table::page_read_cost=engine_cost
# vs non-covering secondary ref via handler page_read_cost), and capture
# baselines + cardinality health. READ-ONLY w.r.t. cost code (NO recompile).
# Assumes the standard measurement stack is already up on /tmp/mysql.sock with
# SF0.1 data, HELIOS_COST_V2=1 etc. (verified live before running).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"
SOCK=/tmp/mysql.sock; DB=benchbase
MH(){ "$MYSQL" -u root --socket="$SOCK" "$DB" "$@"; }
MROOT(){ "$MYSQL" -u root --socket="$SOCK" "$@"; }
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
OUT=/tmp/m0; rm -rf "$OUT"; mkdir -p "$OUT"
REG="3 5 7 8 10 18"   # regressors (q3/5/7/8/18) + canary (q10) + q3 canary
CARD="3 7 10"         # lighter set for per-row EXPLAIN ANALYZE
qsql(){ grep -v '^--' "$ROOT/bench/queries/q${1}.sql" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }

source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || echo "(cstate_guard skipped)"

explain_capture(){ # $1=tag
  local tag="$1" q sql; mkdir -p "$OUT/explain_$tag"
  for q in $REG; do sql="$(qsql "$q")"
    MH -N -e "EXPLAIN FORMAT=TREE $sql"  2>&1 | sed 's/\\n/\n/g' > "$OUT/explain_$tag/q${q}.tree"
    MH    -e "EXPLAIN FORMAT=JSON $sql"  2>&1                    > "$OUT/explain_$tag/q${q}.json"
  done
}
access_sig(){ # $1=tree file -> compact per-table access signature
  grep -oE '(Table scan on|Index lookup on|Covering index lookup on|Single-row index lookup on|Index range scan on|Index scan on|Hash|Nested loop|Filter|Aggregate)[^,(]*' "$1" 2>/dev/null | sed 's/[[:space:]]*$//' | head -40
}

echo "################ M0 PROVENANCE ################"
echo "## mysqld env ##"; tr '\0' '\n' < /proc/"$(cat /tmp/mysql.pid)"/environ | grep -iE 'HELIOS' | sort
echo "## data scale ##"; MH -N -e "SELECT CONCAT('lineitem_rows=',table_rows) FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem';"
echo "## optimizer_switch (mrr/bka/hypergraph) BEFORE reset ##"
MH -N -e "SELECT @@global.optimizer_switch;" | tr ',' '\n' | grep -iE 'hypergraph|mrr|batched_key'
echo "## engine_cost rows ##"; MROOT -N -e "SELECT engine_name,cost_name,cost_value,default_value FROM mysql.engine_cost;"

echo "################ RESET optimizer_switch to MySQL stock (pre-M1 baseline) ################"
MROOT -e "SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
MH -N -e "SELECT @@global.optimizer_switch;" | tr ',' '\n' | grep -iE 'hypergraph|mrr|batched_key'
MROOT -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"

echo "################ [A] NOIDX BASELINE ################"
echo "## dropping all secondary indexes ##"
MH -N -e "SELECT CONCAT('DROP INDEX \`',index_name,'\` ON \`',table_name,'\`;') FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY' GROUP BY table_name,index_name;" \
  | while read -r stmt; do [ -n "$stmt" ] && MH -e "$stmt" 2>/dev/null; done
echo "## remaining secondary indexes (want: none) ##"
MH -N -e "SELECT CONCAT(table_name,'.',index_name) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY';" | sed 's/^/   /' || echo "   (none)"
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true
echo "## noidx timing (warm, then measured) ##"
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/noidx_warm" 120 >/dev/null 2>&1
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/noidx" 120
explain_capture noidx

echo "################ [B] FULLIDX (standard 23-index set) ################"
echo "## applying postload backfill ##"
( time MH < "$POSTLOAD" ) 2>&1 | tail -3
MH -N -e "SELECT CONCAT('secondary_index_count=',COUNT(DISTINCT CONCAT(table_name,index_name))) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY';"
bash scripts/dev/prewarm_stats.sh "$SOCK" "$DB" >/dev/null 2>&1 || true
echo "## fullidx timing (warm, then measured) ##"
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/fullidx_warm" 120 >/dev/null 2>&1
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/fullidx" 120
explain_capture fullidx

echo "################ [B2] PLAN-SHAPE DIFF (noidx vs fullidx) ################"
for q in $REG; do
  echo "== q$q =="
  diff <(access_sig "$OUT/explain_noidx/q${q}.tree") <(access_sig "$OUT/explain_fullidx/q${q}.tree") \
    | grep -E '^[<>]' | sed 's/^</  noidx:/; s/^>/  fullidx:/' | head -20 || echo "  (no access-method change)"
done

echo "################ [C] CARDINALITY HEALTH (prefetch OFF, plan unchanged) ################"
MROOT -e "SET GLOBAL lineairdb_prefetch_execution=OFF;"
for q in $CARD; do sql="$(qsql "$q")"
  echo "== q$q (top rows: estimated vs actual) =="
  timeout 90 "$MYSQL" -u root --socket="$SOCK" "$DB" -N -e "EXPLAIN ANALYZE $sql" 2>&1 | sed 's/\\n/\n/g' > "$OUT/analyze_q${q}.txt" || echo "  (analyze failed/timeout)"
  grep -oE '\(cost=[0-9.]+ rows=[0-9.]+\) \(actual time=[0-9.]+\.\.[0-9.]+ rows=[0-9.]+' "$OUT/analyze_q${q}.txt" | head -6 | sed 's/^/  /'
done
MROOT -e "SET GLOBAL lineairdb_prefetch_execution=ON;"

echo "################ [D] CALL-TARGET SENTINEL (engine_cost reaches probe sites?) ################"
mkdir -p "$OUT/sentinel"
for q in $REG; do MH -N -e "EXPLAIN FORMAT=TREE $(qsql "$q")" 2>&1 | sed 's/\\n/\n/g' > "$OUT/sentinel/q${q}.before"; done
echo "## raise LINEAIRDB engine_cost io+memory_block_read_cost 1.0/0.25 -> 50/50 (eq_ref lever; w/o page_read_cost override this ALSO delegates to :163 per BI-2) ##"
MROOT -e "INSERT INTO mysql.engine_cost(engine_name,device_type,cost_name,cost_value) VALUES
 ('LINEAIRDB',0,'io_block_read_cost',50.0),('LINEAIRDB',0,'memory_block_read_cost',50.0)
 ON DUPLICATE KEY UPDATE cost_value=VALUES(cost_value);"
MROOT -e "FLUSH OPTIMIZER_COSTS;"
for q in $REG; do MH -N -e "EXPLAIN FORMAT=TREE $(qsql "$q")" 2>&1 | sed 's/\\n/\n/g' > "$OUT/sentinel/q${q}.after"; done
echo "## which query plans MOVED when engine_cost raised (= engine_cost reaches that query's probe pricing) ##"
for q in $REG; do
  if diff -q "$OUT/sentinel/q${q}.before" "$OUT/sentinel/q${q}.after" >/dev/null; then echo "  q$q: unchanged"
  else echo "  q$q: PLAN MOVED"; diff <(access_sig "$OUT/sentinel/q${q}.before") <(access_sig "$OUT/sentinel/q${q}.after") | grep -E '^[<>]' | sed 's/^</    was:/; s/^>/    now:/' | head -10; fi
done
echo "## restore engine_cost ##"
MROOT -e "DELETE FROM mysql.engine_cost WHERE engine_name='LINEAIRDB'; FLUSH OPTIMIZER_COSTS;"

echo "################ M0 TOTALS ################"
for d in noidx fullidx; do
  awk -F, 'NR>1 && $3=="OK"{s+=$2; n++} END{printf "  %-8s OK-sum=%.2fs (%d ok)\n","'"$d"'",s,n}' "$OUT/$d/summary.csv"
  awk -F, 'NR>1 && $3!="OK"{printf "    %s %s\n",$1,$3}' "$OUT/$d/summary.csv"
done
echo "## per-query noidx -> fullidx (focus rows) ##"
join -t, -1 1 -2 1 <(tail -n +2 "$OUT/noidx/summary.csv" | cut -d, -f1,2 | sort) \
                   <(tail -n +2 "$OUT/fullidx/summary.csv" | cut -d, -f1,2 | sort) 2>/dev/null \
  | awk -F, '{printf "  %-4s noidx=%6.2fs fullidx=%6.2fs  %s\n",$1,$2,$3,($3>$2*1.3?"<-- REGRESSED":($3<$2*0.77?"<-- improved":""))}'
echo "M0_DONE"
