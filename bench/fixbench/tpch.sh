#!/bin/bash
# Run the 22 TPC-H queries of bench/queries/tpch on a freshly loaded stack.
#
#   tpch.sh native    [sf] [runs]   wall and CPU time per query (default SF 0.1, 3 runs)
#   tpch.sh callgrind [sf]          instructions per query: the storage server runs under
#                                   callgrind and each query is dumped as part <q>
#
# Each run loads with BenchBase and attaches the DuckDB secondary engine without a
# restart. Under callgrind, one unmeasured pass runs first so DuckDB's caches are warm.
set -u
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$D/../.." && pwd)"
WORK=${FIXBENCH_WORK:-$ROOT/bench/results/fixbench}
mode=${1:?usage: tpch.sh native|callgrind [sf] [runs]}; sf=${2:-0.1}; runs=${3:-3}
export USER=${USER:-root} LOGNAME=${LOGNAME:-${USER:-root}}
bash "$D/stack.sh" wipe
if [ "$mode" = native ]; then
  bash "$D/stack.sh" start native >/dev/null || exit 1
  python3 "$D/load.py" tpch "$sf" >/dev/null 2>&1 || { echo "load failed" >&2; exit 1; }
  SP=$(cat /tmp/helios_storage.pid); MP=$(pgrep -x mysqld | head -1)
  python3 "$D/tpch_run.py" --sf "$sf" --runs "$runs" --pids "mysqld=$MP,storage=$SP"
else
  OUT=${OUT:-$WORK/cg/tpch}; rm -rf "$OUT"; mkdir -p "$OUT"
  CG_OUT=$OUT/storage MYSQLD_MODE=native bash "$D/stack.sh" start callgrind >/dev/null || exit 1
  python3 "$D/load.py" tpch "$sf" > "$OUT/load.log" 2>&1 || { echo "load failed" >&2; exit 1; }
  SP=$(cat /tmp/helios_storage.pid)
  python3 "$D/tpch_run.py" --sf "$sf" --runs 1 > "$OUT/warm.jsonl"
  python3 "$D/tpch_run.py" --sf "$sf" --runs 1 \
    --before "callgrind_control -i on $SP >/dev/null" \
    --after "callgrind_control -d q{q} $SP >/dev/null; callgrind_control -i off $SP >/dev/null"
  echo "callgrind output: $OUT (part <n> is query n; summarize with cgreport.py)"
fi
bash "$D/stack.sh" stop
