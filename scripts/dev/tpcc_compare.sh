#!/bin/bash
# Reproducible TPC-C throughput measurement, pinned so ad-hoc shell variance
# cannot drift the conditions. Encodes the rules that bit us before:
#   - lineairdb-server is IN-MEMORY: stale tables accumulate across loads and
#     make CREATE fail, so we STOP+START the server before EVERY load.
#   - mysqld is restarted from a fixed env (no TPC-H gates; TPC-C doesn't use
#     them) so the loaded binary is deterministic.
#   - The running binary is verified to be newer than the last build.
#
# Usage:
#   scripts/dev/tpcc_compare.sh LABEL [MODE] [RUNS] [TIME_S] [TERMINALS]
#     MODE: prefetch-stmt (autogen, default) | prefetch (DSL @_tx_plan) | none
#     RUNS: repetitions (default 3)
# Appends one CSV row per run to /tmp/tpcc_compare_<LABEL>.csv and prints a
# summary (median). Run it ONCE per built binary; it does not build.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

LABEL="${1:?usage: tpcc_compare.sh LABEL [prefetch-stmt|prefetch|none] [RUNS] [TIME_S] [TERMINALS]}"
MODE="${2:-prefetch-stmt}"
RUNS="${3:-3}"
TIME_S="${4:-20}"
TERMINALS="${5:-1}"

case "$MODE" in
  prefetch-stmt) FLAG="--prefetch-stmt" ;;
  prefetch)      FLAG="--prefetch" ;;
  none)          FLAG="" ;;
  *) echo "unknown MODE: $MODE" >&2; exit 2 ;;
esac

OUT="/tmp/tpcc_compare_${LABEL}.csv"
[ -f "$OUT" ] || echo "label,mode,run,terminals,throughput,goodput,retry,errors,load_s,sysload" > "$OUT"

PLUGIN_MTIME=$(stat -c %Y build/lib/plugin/ha_lineairdb_storage_engine.so 2>/dev/null || echo 0)
SERVER_MTIME=$(stat -c %Y build/server/lineairdb-server 2>/dev/null || echo 0)

restart_stack() {
  # STOP both, then START fresh — the only safe way to clear the in-mem server
  # before a load. Verify the new processes started after the last build.
  [ -f /tmp/mysql.pid ] && kill "$(cat /tmp/mysql.pid)" 2>/dev/null
  sleep 2
  ./scripts/stop_server.sh >/dev/null 2>&1
  sleep 1
  ./scripts/start_server.sh >/dev/null 2>&1
  sleep 2
  ./scripts/start_mysql.sh >/dev/null 2>&1
  sleep 2
  local srv mysd
  srv=$(pgrep -f "/build/server/lineairdb-server" | head -1)
  mysd=$(cat /tmp/mysql.pid 2>/dev/null)
  local srv_start mysd_start
  srv_start=$(ps -o lstart= -p "$srv" 2>/dev/null | xargs -0 -I{} date -d {} +%s 2>/dev/null || echo 0)
  mysd_start=$(ps -o lstart= -p "$mysd" 2>/dev/null | xargs -0 -I{} date -d {} +%s 2>/dev/null || echo 0)
  if [ "$srv_start" -lt "$SERVER_MTIME" ] || [ "$mysd_start" -lt "$PLUGIN_MTIME" ]; then
    echo "  WARNING: a running binary predates the last build (stale process?)" >&2
  fi
}

echo "=== TPC-C compare: label=$LABEL mode=$MODE runs=$RUNS time=${TIME_S}s terminals=$TERMINALS ==="
for run in $(seq 1 "$RUNS"); do
  restart_stack
  SYSLOAD=$(awk '{print $1}' /proc/loadavg)
  LOG=$(mktemp)
  python3 bench/bin/benchrun.py tpcc --external-server --time "$TIME_S" \
      --terminals "$TERMINALS" $FLAG > "$LOG" 2>&1
  TP=$(grep "Throughput:" "$LOG" | grep -oE "[0-9]+\.[0-9]+" | head -1)
  LOADS=$(grep -oE "Load time: [0-9.]+s" "$LOG" | grep -oE "[0-9.]+" | head -1)
  # Pull goodput/retry/errors from the saved summary.csv if present.
  RES=$(grep -oE "Results: +[^ ]+/TPCC" "$LOG" | awk '{print $2}')
  GP=""; RT=""; ER=""
  if [ -n "$RES" ] && [ -f "$RES/summary.csv" ]; then
    read GP RT ER < <(awk -F, -v t="$TERMINALS" '$1==t{print $3, $4, $5}' "$RES/summary.csv")
  fi
  echo "${LABEL},${MODE},${run},${TERMINALS},${TP:-FAIL},${GP},${RT},${ER},${LOADS},${SYSLOAD}" >> "$OUT"
  echo "  run$run: throughput=${TP:-FAIL} goodput=${GP:-?} retry=${RT:-?} (load=${SYSLOAD})"
  rm -f "$LOG"
done

echo "--- summary for $LABEL/$MODE ---"
grep -E "^${LABEL},${MODE}," "$OUT" | awk -F, '$5!="FAIL"{s+=$5; n++; if(mn==""||$5<mn)mn=$5; if($5>mx)mx=$5}
  END{ if(n==0){print "no successful runs"; exit}
       printf "mean=%.1f  min=%.1f  max=%.1f  n=%d\n", s/n, mn, mx, n }'
