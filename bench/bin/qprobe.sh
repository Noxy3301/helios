#!/bin/bash
# qprobe.sh: latency probe for a single TPC-H query, oneshot (with @_ldb_plan)
# vs NLJ, run N times in ONE connection to amortize connect overhead.
# Usage: qprobe.sh <iters> <plan|-> <<<"SELECT ...;"   (query on stdin)
#   plan = "-" means NLJ (no @_ldb_plan). Prints per-iter median-ish (mean) ms.
set -u
ITERS="${1:-10}"
PLAN="${2:--}"
MYSQL="/home/noxy/helios/build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock benchbase"
Q="$(cat)"
# build the multi-statement script
SQL=""
for i in $(seq 1 "$ITERS"); do
  if [ "$PLAN" != "-" ]; then SQL+="SET @_ldb_plan='$PLAN'; "; fi
  SQL+="$Q "
done
# warmup once (not timed)
if [ "$PLAN" != "-" ]; then $MYSQL -e "SET @_ldb_plan='$PLAN'; $Q" >/dev/null 2>&1; else $MYSQL -e "$Q" >/dev/null 2>&1; fi
t0=$(date +%s%N)
$MYSQL -e "$SQL" >/dev/null 2>&1
rc=$?
t1=$(date +%s%N)
total=$(( (t1-t0)/1000000 ))
echo "iters=$ITERS total_ms=$total per_query_ms=$(( total / ITERS )) rc=$rc plan=${PLAN}"
