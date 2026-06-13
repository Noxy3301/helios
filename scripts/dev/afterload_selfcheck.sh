#!/bin/bash
# Phase-21 afterload step (Option A): helios self-consistency at SF=<sf>.
# Load once (standard DDL: PK + FK auto-indexes only), capture the all-22 md5
# baseline, then build the FULL standard secondary-index set (postload-mysql.sql,
# 24 indexes incl 8 UNIQUE) via the chunk-COMMIT backfill, and re-capture the
# all-22 md5. Indexes never change results, so every query's md5 MUST be
# unchanged -> proves the backfill is correct for the full set. A changed md5 is
# a backfill or prefetch×SI bug (q15 expected per Step ④). Run with prefetch ON.
set -uo pipefail
SF="${1:-0.1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"
DB=benchbase
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
SET_PF="SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"

# Ensure the postload SQL is available (extracted from the benchbase jar).
if [ ! -f "$POSTLOAD" ]; then
  unzip -o -q "$ROOT/bench/benchbase-mysql/benchbase.jar" \
    benchmarks/tpch/postload-mysql.sql -d /tmp/bb_tpch
fi

md5_all() { # $1 = label ; prints "Qn md5" per query
  for q in $(seq 1 22); do
    local qf="$ROOT/bench/queries/q${q}.sql"
    [ -f "$qf" ] || continue
    $MYSQL -e "$SET_PF" >/dev/null 2>&1
    local out
    out=$($MYSQL $DB -N --batch < "$qf" 2>&1)
    if [ $? -ne 0 ]; then
      echo "q${q} ERROR:$(echo "$out" | head -1 | cut -c1-60)"
    else
      echo "q${q} $(echo "$out" | sort | md5sum | cut -d' ' -f1)"
    fi
  done
}

echo "=== restart fresh helios stack (SF=$SF) ==="
if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
./scripts/stop_server.sh || true
sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh
sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh > /dev/null
sleep 3

echo "=== load TPC-H SF=$SF (standard DDL, no afterLoad) ==="
python3 bench/bin/benchrun.py tpch --scalefactor "$SF" --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR"
$MYSQL -e "$SET_PF"
echo -n "lineitem indexes BEFORE: "
$MYSQL $DB -N -e "SELECT GROUP_CONCAT(DISTINCT index_name ORDER BY index_name SEPARATOR ' ') FROM information_schema.statistics WHERE table_schema='$DB' AND table_name='lineitem';"
bash "$ROOT/scripts/dev/prewarm_stats.sh" /tmp/mysql.sock "$DB" >/dev/null 2>&1 || true

echo "=== BASELINE md5 (no secondary indexes) ==="
md5_all > /tmp/afterload_baseline.txt
cat /tmp/afterload_baseline.txt

echo "=== build full standard index set (24 indexes) via backfill ==="
time $MYSQL $DB < "$POSTLOAD"
echo -n "lineitem indexes AFTER: "
$MYSQL $DB -N -e "SELECT GROUP_CONCAT(DISTINCT index_name ORDER BY index_name SEPARATOR ' ') FROM information_schema.statistics WHERE table_schema='$DB' AND table_name='lineitem';"
bash "$ROOT/scripts/dev/prewarm_stats.sh" /tmp/mysql.sock "$DB" >/dev/null 2>&1 || true

echo "=== FULL-INDEX md5 ==="
md5_all > /tmp/afterload_fullidx.txt
cat /tmp/afterload_fullidx.txt

echo "=== DIFF (baseline vs full-index) — any line = result changed by an index ==="
diff /tmp/afterload_baseline.txt /tmp/afterload_fullidx.txt && echo "ALL 22 IDENTICAL (backfill correct for full set)" || echo "^ differences above (q15 expected per Step ④; others = bug)"
echo "DONE"
