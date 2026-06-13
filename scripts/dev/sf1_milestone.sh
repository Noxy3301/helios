#!/bin/bash
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"

echo "=== [1/5] fresh helios stack ==="
if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
./scripts/stop_server.sh || true
sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh
sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh > /dev/null
sleep 3

echo "=== [2/5] helios SF=1 load ==="
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR"

echo "=== [3/5] InnoDB ref (3308) SF=1 reload ==="
python3 bench/bin/benchrun.py tpch --scalefactor 1 --terminals 1 --no-exec --external-server --mysql-port 3308 2>&1 | grep -E "Load time|ERROR"

echo "=== [4/5] matrix ==="
$MYSQL -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"
bash bench/bin/tpch_matrix.sh /tmp/final_matrix_sf1_v2 420
SRV=$(pgrep -x lineairdb-serve | head -1)
echo "server RSS after matrix: $(ps -o rss= -p $SRV | awk '{printf "%.0fMB", $1/1024}')"
echo "mysqld RSS after matrix: $(ps -o rss= -p $(cat /tmp/mysql.pid) | awk '{printf "%.0fMB", $1/1024}')"

echo "=== [5/5] md5 ==="
bash bench/bin/tpch_md5.sh --target-set "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;" 2>&1 | tail -3
echo "MILESTONE DONE"
