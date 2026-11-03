#!/bin/bash
# Measure one fixed TPC-C sample.
#
#   run.sh native    <sample> [limit]   wall and CPU time per transaction
#   run.sh callgrind <sample> [limit]   instructions per transaction (see cgreport.py)
#
# <sample> is a name under samples/ (neworder, payment, orderstatus, delivery,
# stocklevel) or a path. Each run starts from an empty stack, loads TPC-C SF 1
# with BenchBase and replays without a restart, so the optimizer statistics are
# the ones a benchrun.py run sees (a storage restart loses them).
#
# Environment: WARM (default 20) transactions run before measuring; MYSQLD_MODE
# (default callgrind for the callgrind mode); OUT (callgrind output directory).
set -u
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$D/../.." && pwd)"
WORK=${FIXBENCH_WORK:-$ROOT/bench/results/fixbench}
mode=${1:?usage: run.sh native|callgrind <sample> [limit]}
sample=${2:?usage: run.sh native|callgrind <sample> [limit]}
limit=${3:-0}
[ -f "$sample" ] || sample=$D/samples/tpcc_sf1_$sample.json.gz
export USER=${USER:-root} LOGNAME=${LOGNAME:-${USER:-root}}

bash "$D/stack.sh" wipe
if [ "$mode" = native ]; then
  bash "$D/stack.sh" start native >/dev/null || exit 1
  python3 "$D/load.py" tpcc >/dev/null 2>&1 || { echo "load failed" >&2; exit 1; }
  SP=$(cat /tmp/helios_storage.pid); MP=$(pgrep -x mysqld | head -1)
  python3 "$D/replay.py" "$sample" --warm "${WARM:-20}" --limit "$limit" --pids "mysqld=$MP,storage=$SP"
else
  OUT=${OUT:-$WORK/cg/$(basename "$sample" .json.gz)}; rm -rf "$OUT"; mkdir -p "$OUT"
  CG_OUT=$OUT/storage CG_MOUT=$OUT/mysqld MYSQLD_MODE=${MYSQLD_MODE:-callgrind} \
    bash "$D/stack.sh" start callgrind >/dev/null || exit 1
  python3 "$D/load.py" tpcc > "$OUT/load.log" 2>&1 || { echo "load failed" >&2; exit 1; }
  SP=$(cat /tmp/helios_storage.pid); MP=$(cat /tmp/mysqld_cg.pid 2>/dev/null || pgrep -x mysqld | head -1)
  python3 "$D/replay.py" "$sample" --warm "${WARM:-20}" --limit "$limit" --pids "mysqld=$MP,storage=$SP" \
    --before "callgrind_control -i on $SP >/dev/null; callgrind_control -i on $MP >/dev/null 2>&1 || true" \
    --after "callgrind_control -d $SP >/dev/null; callgrind_control -d $MP >/dev/null 2>&1 || true"
  echo "callgrind output: $OUT"
fi
bash "$D/stack.sh" stop
