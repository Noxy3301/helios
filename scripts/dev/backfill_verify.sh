#!/bin/bash
# Phase-21 Step-2: chunk-COMMIT + read-chunked backfill verification.
# Usage: backfill_verify.sh <scalefactor>   (e.g. 0.1 or 1)
# Loads TPC-H at <sf> WITHOUT l_sk in the DDL, captures the q21 ground-truth md5
# (no index), exercises the CREATE INDEX backfill (measuring wall time), and
# re-checks the q21 md5 -> must equal the ground-truth (backfilled index is
# correct/complete). The ground-truth is captured per-run (scale-independent).
set -uo pipefail
SF="${1:-0.1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"
DB=benchbase

echo "=== restart fresh helios stack (SF=$SF) ==="
if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
./scripts/stop_server.sh || true
sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh
sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh > /dev/null
sleep 3

echo "=== load TPC-H SF=$SF (no l_sk in DDL) ==="
python3 bench/bin/benchrun.py tpch --scalefactor "$SF" --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR"

$MYSQL -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"
echo -n "lineitem rows: "
$MYSQL $DB -N -e "SELECT COUNT(*) FROM lineitem WHERE l_orderkey >= 0;" 2>/dev/null || echo "(count blocked)"
echo -n "lineitem indexes BEFORE: "
$MYSQL $DB -N -e "SELECT GROUP_CONCAT(DISTINCT index_name ORDER BY index_name SEPARATOR ' ') FROM information_schema.statistics WHERE table_schema='$DB' AND table_name='lineitem';"

echo "=== q21 ground-truth (no l_sk index) ==="
GT=$($MYSQL $DB -N --batch < bench/queries/q21.sql 2>&1 | sort | md5sum | cut -d' ' -f1)
echo "q21 md5 (no index): $GT"

echo "=== CREATE INDEX l_sk (chunk-COMMIT + read-chunk backfill) ==="
time $MYSQL $DB -e "CREATE INDEX l_sk ON lineitem (l_suppkey ASC);"
echo "=== CREATE INDEX l_sk_pk (composite) ==="
time $MYSQL $DB -e "CREATE INDEX l_sk_pk ON lineitem (l_suppkey ASC, l_partkey ASC);"

echo -n "lineitem indexes AFTER: "
$MYSQL $DB -N -e "SELECT GROUP_CONCAT(DISTINCT index_name ORDER BY index_name SEPARATOR ' ') FROM information_schema.statistics WHERE table_schema='$DB' AND table_name='lineitem';"

echo "=== q21 WITH backfilled index ==="
BF=$($MYSQL $DB -N --batch < bench/queries/q21.sql 2>&1 | sort | md5sum | cut -d' ' -f1)
echo "q21 md5 (backfilled): $BF"
echo "q21 md5 (no index)  : $GT"
if [ "$BF" = "$GT" ] && [ -n "$BF" ] && [[ "$BF" != ERROR* ]]; then
  echo "RESULT: CORRECT (backfilled == ground-truth)"
else
  echo "RESULT: MISMATCH (correctness regression!)"
fi
echo "DONE"
