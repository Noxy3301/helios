#!/bin/bash
# Phase-21 perf: can the q21 SIP win be captured WITHOUT the full-set regression?
# Build ONLY l_sk (+ l_sk_pk) on helios SF1 and measure the suite. Compare to the
# no-index baseline (39.14s) and the full-23-index set (77.19s). If l_sk-only
# gives q21 improved AND no q3/q5/q7/q8/q18 regression, selective indexes are a
# net win for helios.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
MH="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"
SET_PF="SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"
OUT=/tmp/perf_sf1; mkdir -p "$OUT"
source "$ROOT/scripts/dev/cstate_guard.sh"; cstate_guard

echo "=== helios: fresh stack + load SF1 + l_sk/l_sk_pk only ==="
if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
./scripts/stop_server.sh || true; sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR"
$MH -e "$SET_PF"
echo "build l_sk + l_sk_pk..."; time $MH benchbase -e "CREATE INDEX l_sk ON lineitem (l_suppkey ASC); CREATE INDEX l_sk_pk ON lineitem (l_suppkey ASC, l_partkey ASC);" 2>&1 | tail -1
bash scripts/dev/prewarm_stats.sh /tmp/mysql.sock benchbase >/dev/null 2>&1 || true

$MH benchbase -e "DROP VIEW IF EXISTS revenue0; DROP TABLE IF EXISTS revenue0;" 2>/dev/null
MYSQL_SOCK=/tmp/mysql.sock bash bench/bin/tpch_matrix.sh "$OUT/helios_lsk_warm" 300 >/dev/null 2>&1
$MH benchbase -e "DROP VIEW IF EXISTS revenue0; DROP TABLE IF EXISTS revenue0;" 2>/dev/null
echo "=== helios l_sk-only suite ==="
MYSQL_SOCK=/tmp/mysql.sock bash bench/bin/tpch_matrix.sh "$OUT/helios_lsk" 300

echo "=== compare: q, noidx | lsk_only | fullidx ==="
paste -d' ' \
  <(tail -n +2 "$OUT/helios_noidx/summary.csv") \
  <(tail -n +2 "$OUT/helios_lsk/summary.csv" | cut -d, -f2-) \
  <(tail -n +2 "$OUT/helios_fullidx/summary.csv" | cut -d, -f2-)
for d in helios_noidx helios_lsk helios_fullidx; do
  awk -F, 'NR>1{s+=$2} END{printf "  %s: %.2fs\n", "'"$d"'", s}' "$OUT/$d/summary.csv" 2>/dev/null
done
echo "LSK_DONE"
