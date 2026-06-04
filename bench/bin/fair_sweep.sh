#!/usr/bin/env bash
# Fair-sweep harness: run a TPC-C terminal sweep, restarting server + mysqld
# (and rerunning load) between every iteration so each measurement sees a
# clean LineairDB state. Eliminates the cross-iteration contamination that
# made the existing benchrun --sweep mode unfair to later (higher-terminal)
# runs.
#
# Usage:
#   fair_sweep.sh [--mode prefetch|stateful] [--sf N] [--time S] [--terms LIST]
#                 [--label TAG]
#
# Output:
#   bench/results/fair-sweep-<label>-<timestamp>/
#     summary.csv     terminals,throughput,goodput,retry,wall,drain,load
#     log/iter_<N>.log  full benchrun output per terminal count
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPTS="$ROOT/scripts"

MODE=prefetch          # prefetch | stateful
SF=1
TIME=30
TERMS="1,4,16,32,64,128,256,384,512"
LABEL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)  MODE="$2"; shift 2 ;;
    --sf)    SF="$2"; shift 2 ;;
    --time)  TIME="$2"; shift 2 ;;
    --terms) TERMS="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,/^set/p' "$0" | sed 's/^# //'; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ "$MODE" =~ ^(prefetch|stateful)$ ]] || { echo "Invalid --mode" >&2; exit 2; }

ts=$(date +%Y%m%d_%H%M%S)
tag="${LABEL:+${LABEL}-}${MODE}-sf${SF}"
OUT_DIR="$ROOT/bench/results/fair-sweep-${tag}-${ts}"
mkdir -p "$OUT_DIR/log"
SUMMARY="$OUT_DIR/summary.csv"
echo "terminals,throughput,goodput,retry,wall_s,drain_s,load_s" > "$SUMMARY"

PREFETCH_FLAG=""
[[ "$MODE" == "prefetch" ]] && PREFETCH_FLAG="--prefetch"

echo "fair_sweep: mode=$MODE sf=$SF time=${TIME}s terms=$TERMS"
echo "output: $OUT_DIR"
echo

force_stop() {
  pkill -9 -f benchbase.jar 2>/dev/null || true
  pkill -9 -f benchrun.py   2>/dev/null || true
  "$SCRIPTS/stop_mysql.sh"  >/dev/null 2>&1 || true
  pkill -9 mysqld           2>/dev/null || true
  # stop_server.sh handles SIGTERM; force-kill as fallback.
  if [[ -f /tmp/lineairdb_server.pid ]]; then
    kill -9 "$(cat /tmp/lineairdb_server.pid)" 2>/dev/null || true
  fi
  pkill -9 lineairdb-serve  2>/dev/null || true
  sleep 1
}

trap force_stop EXIT

# Avoid carrying lineairdb_logs across iterations: each fresh server start
# would race on the directory anyway and we already wipe build/data.
IFS=',' read -ra TERM_LIST <<< "$TERMS"
for term in "${TERM_LIST[@]}"; do
  log="$OUT_DIR/log/iter_${term}.log"
  echo "[$(date +%H:%M:%S)] === t=${term} ===" | tee -a "$SUMMARY".meta
  force_stop
  rm -rf "$ROOT/build/data" "$ROOT/build/lineairdb_logs" 2>/dev/null || true

  "$SCRIPTS/start_server.sh" >>"$log" 2>&1 &
  for _ in $(seq 1 20); do
    ss -tln 2>/dev/null | grep -q ':9999' && break
    sleep 0.5
  done
  "$SCRIPTS/start_mysql.sh" --mysqld-port 3307 \
      --server-host 127.0.0.1 --server-port 9999 >>"$log" 2>&1
  for _ in $(seq 1 20); do
    ss -tln 2>/dev/null | grep -q ':3307' && break
    sleep 0.5
  done

  start_ns=$(date +%s%N)
  set +e
  python3 "$ROOT/bench/bin/benchrun.py" tpcc \
    --terminals "$term" --time "$TIME" --scalefactor "$SF" \
    --external-server $PREFETCH_FLAG \
    >>"$log" 2>&1
  rc=$?
  set -e
  end_ns=$(date +%s%N)
  wall_s=$(awk "BEGIN{printf \"%.1f\", ($end_ns - $start_ns)/1e9}")

  # Parse the per-iteration benchrun.py output.
  load_s=$(grep -oE 'Load time: [0-9.]+s' "$log" | tail -1 | grep -oE '[0-9.]+' | head -1)
  tput=$(grep   -oE 'Throughput:[[:space:]]+[0-9.]+' "$log" | tail -1 | awk '{print $2}')
  goodput=$(awk '/SUMMARY/{found=1} found && /^[[:space:]]*[0-9]+[[:space:]]/ {print $3; exit}' "$log")
  # benchrun.py prints "Server Retry: N | Unexpected Errors: M"
  retry=$(grep -oE 'Server Retry:[[:space:]]+[0-9]+' "$log" | tail -1 | awk '{print $3}')

  load_s=${load_s:-0}
  tput=${tput:-0}
  goodput=${goodput:-0}
  retry=${retry:-0}
  drain_s=$(awk "BEGIN{printf \"%.1f\", $wall_s - $load_s - $TIME}")

  printf "%s,%s,%s,%s,%s,%s,%s\n" "$term" "$tput" "$goodput" "$retry" "$wall_s" "$drain_s" "$load_s" \
    | tee -a "$SUMMARY"
  if (( rc != 0 )); then
    echo "  WARN: benchrun.py rc=$rc (see $log)" | tee -a "$SUMMARY".meta
  fi
done

force_stop
trap - EXIT

echo
echo "=== sweep finished: $OUT_DIR ==="
column -t -s, "$SUMMARY"
