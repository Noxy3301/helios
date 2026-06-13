#!/bin/bash
# Plan stability test: 2 cycles of fresh-load -> time q17/q18/q20 -> full
# matrix -> time again. Stable = pre/post and cross-cycle times agree.
set -uo pipefail
ROOT=/home/noxy/helios
cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"

for cycle in 1 2; do
  echo "##### CYCLE $cycle #####"
  if [ -f /tmp/mysql.pid ]; then kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; fi
  ./scripts/stop_server.sh || true
  sleep 2
  HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh > /dev/null
  sleep 2
  HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 HELIOS_STATS_DEBUG=1 ./scripts/start_mysql.sh > /dev/null
  sleep 3
  python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR"
  $MYSQL -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;"
  $MYSQL benchbase -e "DROP VIEW IF EXISTS revenue0" 2>/dev/null || true
  echo "--- PRE (right after load) ---"
  for q in 17 18 20; do
    /usr/bin/time -f "q$q PRE %es" $MYSQL benchbase -N < bench/queries/q$q.sql > /dev/null 2>/tmp/pst.time; tail -1 /tmp/pst.time
  done
  echo "--- full matrix (warm + stats re-sync) ---"
  bash bench/bin/tpch_matrix.sh /tmp/pst_matrix_c$cycle 420 > /dev/null 2>&1
  echo "--- POST (after full suite) ---"
  for q in 17 18 20; do
    /usr/bin/time -f "q$q POST %es" $MYSQL benchbase -N < bench/queries/q$q.sql > /dev/null 2>/tmp/pst.time; tail -1 /tmp/pst.time
  done
  grep -E "^q1[78]|^q20" /tmp/pst_matrix_c$cycle/summary.csv | sed 's/^/  matrix: /'
done
echo "PST DONE"
