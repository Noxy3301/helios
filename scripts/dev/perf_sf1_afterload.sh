#!/bin/bash
# Phase-21 perf milestone: TPC-H suite LATENCY at SF1, prefetch ON.
# Measures helios with NO secondary indexes (current measured config) vs helios
# with the standard 23-index set (Phase-21 afterLoad backfill) vs InnoDB(3308)
# with the same standard set. Confirms q21 improves (SIP), q1/q6 do NOT regress
# (F' keeps agg-pushdown), and the overall suite. cstate must be pinned
# (performance governor + deep C-states off) — see cstate_guard.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
MH="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"
MI="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql_3308.sock"
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
SET_PF="SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"
OUT=/tmp/perf_sf1; mkdir -p "$OUT"
[ -f "$POSTLOAD" ] || unzip -o -q "$ROOT/bench/benchbase-mysql/benchbase.jar" benchmarks/tpch/postload-mysql.sql -d /tmp/bb_tpch
source "$ROOT/scripts/dev/cstate_guard.sh"; cstate_guard

run_matrix() { # $1=label $2=sock
  $ROOT/build/runtime_output_directory/mysql -u root --socket="$2" benchbase \
    -e "DROP VIEW IF EXISTS revenue0; DROP TABLE IF EXISTS revenue0;" 2>/dev/null
  # warm once (prefetch staging / caches), then measure
  MYSQL_SOCK="$2" bash bench/bin/tpch_matrix.sh "$OUT/$1_warm" 300 >/dev/null 2>&1
  $ROOT/build/runtime_output_directory/mysql -u root --socket="$2" benchbase \
    -e "DROP VIEW IF EXISTS revenue0; DROP TABLE IF EXISTS revenue0;" 2>/dev/null
  MYSQL_SOCK="$2" bash bench/bin/tpch_matrix.sh "$OUT/$1" 300
}

echo "=== helios: fresh stack + load SF1 ==="
if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
./scripts/stop_server.sh || true; sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR"
$MH -e "$SET_PF"
bash scripts/dev/prewarm_stats.sh /tmp/mysql.sock benchbase >/dev/null 2>&1 || true

echo "=== [A] helios NO secondary indexes (current config) ==="
run_matrix helios_noidx /tmp/mysql.sock

echo "=== build standard 23-index set on helios (backfill, ~30min SF1) ==="
time $MH benchbase < "$POSTLOAD" 2>&1 | tail -1
bash scripts/dev/prewarm_stats.sh /tmp/mysql.sock benchbase >/dev/null 2>&1 || true

echo "=== [B] helios FULL 23-index set ==="
run_matrix helios_fullidx /tmp/mysql.sock

echo "=== InnoDB(3308): reload SF1 + standard 23-index set (native) ==="
bash scripts/dev/start_innodb_ref.sh 2>/dev/null | tail -1 || bash scripts/start_innodb_ref.sh | tail -1
python3 bench/bin/benchrun.py tpch --scalefactor 1 --terminals 1 --no-exec --external-server --mysql-port 3308 2>&1 | grep -E "Load time|ERROR"
$MI benchbase -e "DROP VIEW IF EXISTS revenue0;" 2>/dev/null
$MI benchbase < "$POSTLOAD" 2>&1 | tail -1
echo "=== [C] InnoDB FULL 23-index set ==="
run_matrix innodb_fullidx /tmp/mysql_3308.sock

echo "=== SUMMARY: q,time helios_noidx | helios_fullidx | innodb_fullidx ==="
paste -d' ' \
  <(tail -n +2 "$OUT/helios_noidx/summary.csv") \
  <(tail -n +2 "$OUT/helios_fullidx/summary.csv" | cut -d, -f2-) \
  <(tail -n +2 "$OUT/innodb_fullidx/summary.csv" | cut -d, -f2-)
echo "totals:"
for d in helios_noidx helios_fullidx innodb_fullidx; do
  awk -F, 'NR>1{s+=$2} END{printf "  %s: %.2fs\n", "'"$d"'", s}' "$OUT/$d/summary.csv"
done
echo "PERF_DONE"
