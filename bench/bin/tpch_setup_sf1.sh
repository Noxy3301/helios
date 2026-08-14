#!/bin/bash
# Load TPC-H into a running helios stack, then apply the postload indexes,
# ANALYZE statistics, session settings, and the secondary-engine attach
# used by the wall-clock runner.
# Usage: tpch_setup_sf1.sh [--sf 1] [--loader-threads 16] [--socket /tmp/mysql.sock]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SF=1
LOADERS=16
SOCKET=/tmp/mysql.sock

while [ $# -gt 0 ]; do
  case "$1" in
    --sf)
      SF=$2
      shift 2
      ;;
    --loader-threads)
      LOADERS=$2
      shift 2
      ;;
    --socket)
      SOCKET=$2
      shift 2
      ;;
    *)
      echo "unknown arg $1" >&2
      exit 1
      ;;
  esac
done

MYSQL="$ROOT/build/runtime_output_directory/mysql"
[ -x "$MYSQL" ] || MYSQL=$(command -v mysql)

echo "== [1/5] benchbase create+load (SF=$SF) =="
python3 "$ROOT/bench/bin/benchrun.py" tpch --scalefactor "$SF" --no-exec \
  --external-server --loader-threads "$LOADERS"

echo "== [2/5] postload 23-index set =="
time "$MYSQL" -u root --socket="$SOCKET" benchbase \
  < "$ROOT/third_party/benchbase/src/main/resources/benchmarks/tpch/postload-mysql.sql"

echo "== [3/5] ANALYZE =="
# ANALYZE intermittently hits ERROR 1213 right after a bulk load; retry
# instead of failing the whole setup under set -e.
for attempt in 1 2 3 4 5; do
  if "$MYSQL" -u root --socket="$SOCKET" benchbase \
    -e "ANALYZE TABLE customer, lineitem, nation, orders, part, partsupp, region, supplier;" >/dev/null; then
    break
  fi
  if [ "$attempt" = 5 ]; then
    echo "ANALYZE failed after 5 attempts" >&2
    exit 1
  fi
  echo "ANALYZE attempt $attempt failed; retrying"
  sleep 2
done

"$MYSQL" -u root --socket="$SOCKET" -e "SELECT COUNT(*) AS lineitem_rows FROM benchbase.lineitem;"

echo "== [4/5] measurement conditions =="
"$MYSQL" -u root --socket="$SOCKET" -e "
  SET GLOBAL lineairdb_prefetch_execution=ON;
  SET GLOBAL optimizer_switch='batched_key_access=on,mrr_cost_based=off,subquery_to_derived=off';
  SET GLOBAL join_buffer_size=1073741824;"

echo "== [5/5] secondary engine attach =="
for t in customer lineitem nation orders part partsupp region supplier; do
  "$MYSQL" -u root --socket="$SOCKET" benchbase -e \
    "ALTER TABLE $t SECONDARY_ENGINE = LINEAIRDB_COLUMNAR; ALTER TABLE $t SECONDARY_LOAD;"
done

echo "setup done"
