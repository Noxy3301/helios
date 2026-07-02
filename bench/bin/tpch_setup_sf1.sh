#!/bin/bash
# Load TPC-H SF=1 into a RUNNING helios stack (server:9999 + mysqld:3307),
# apply the standard 23-index postload set, ANALYZE, and set the
# champion-scorecard session conditions (prefetch ON + ro_novalidate ON +
# subquery_to_derived=off). Idempotent per fresh server start.
# Usage: tpch_setup_sf1.sh [--sf 1] [--loader-threads 16]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SF=1; LOADERS=16
while [ $# -gt 0 ]; do case "$1" in
  --sf) SF=$2; shift 2;; --loader-threads) LOADERS=$2; shift 2;;
  *) echo "unknown arg $1" >&2; exit 1;;
esac; done
MYSQL="$ROOT/build/runtime_output_directory/mysql"
SOCK=/tmp/mysql.sock

echo "== [1/4] benchbase create+load (SF=$SF) =="
python3 "$ROOT/bench/bin/benchrun.py" tpch --scalefactor "$SF" --no-exec \
  --external-server --loader-threads "$LOADERS"

echo "== [2/4] postload 23-index set =="
time "$MYSQL" -u root --socket=$SOCK benchbase \
  < "$ROOT/third_party/benchbase/src/main/resources/benchmarks/tpch/postload-mysql.sql"

echo "== [3/4] ANALYZE =="
"$MYSQL" -u root --socket=$SOCK benchbase \
  -e "ANALYZE TABLE customer, lineitem, nation, orders, part, partsupp, region, supplier;" >/dev/null

echo "== [4/4] champion measurement conditions =="
"$MYSQL" -u root --socket=$SOCK -e "
  SET GLOBAL lineairdb_prefetch_execution=ON;
  SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;
  SET GLOBAL optimizer_switch='batched_key_access=on,mrr_cost_based=off,subquery_to_derived=off';
  SET GLOBAL join_buffer_size=1073741824;"
"$MYSQL" -u root --socket=$SOCK -e "SELECT COUNT(*) AS lineitem_rows FROM benchbase.lineitem;"
echo "setup done"
