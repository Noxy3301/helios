#!/bin/bash
# Phase-21 afterload step (Option B): helios vs InnoDB(3308) at SF=<sf>, both
# with the FULL standard secondary-index set (helios via chunk-COMMIT backfill,
# InnoDB native). md5 all-22 between the two endpoints. Expect 21/22 (q15 is the
# known prefetch×SI bug = Step 4). InnoDB ref is reloaded at this SF (approved).
set -uo pipefail
SF="${1:-0.1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
MYSQL_H="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"
MYSQL_I="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql_3308.sock"
DB=benchbase
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
SET_PF="SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"

[ -f "$POSTLOAD" ] || unzip -o -q "$ROOT/bench/benchbase-mysql/benchbase.jar" benchmarks/tpch/postload-mysql.sql -d /tmp/bb_tpch

echo "=== helios: fresh stack + load SF=$SF + backfill 24 indexes ==="
if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
./scripts/stop_server.sh || true
sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh
sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh > /dev/null
sleep 3
python3 bench/bin/benchrun.py tpch --scalefactor "$SF" --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR"
$MYSQL_H -e "$SET_PF"
echo "helios building indexes..."; time $MYSQL_H $DB < "$POSTLOAD"
bash "$ROOT/scripts/dev/prewarm_stats.sh" /tmp/mysql.sock "$DB" >/dev/null 2>&1 || true

echo "=== InnoDB(3308): ensure up + reload SF=$SF + native 24 indexes ==="
bash scripts/dev/start_innodb_ref.sh 2>/dev/null || bash scripts/start_innodb_ref.sh
python3 bench/bin/benchrun.py tpch --scalefactor "$SF" --terminals 1 --no-exec --external-server --mysql-port 3308 2>&1 | grep -E "Load time|ERROR"
echo "InnoDB building indexes..."; $MYSQL_I $DB < "$POSTLOAD"

echo "=== md5 helios(target) vs InnoDB(ref), prefetch ON ==="
bash bench/bin/tpch_md5.sh --target-set "$SET_PF" 2>&1 | tail -28
echo "DONE"
