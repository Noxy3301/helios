#!/bin/bash
# M2b (d)+(e): build cal_* true-cardinality probe tables, hit point/scan/range
# probes under ENABLE_RPC_TRACE -> /tmp/m2b_struct.jsonl, then run the JOINT NNLS +
# identifiability gate (m2b_nnls.py). Restart-only mysqld (server preserved);
# start_server ONLY if none is up. NEVER stop_server (operational trap).
# Usage: m2b_struct_bench.sh ["1000 10000 100000"]   (cal_* N sizes; dev default)
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; DB=benchbase
TRACE="${TRACE:-/tmp/m2b_struct.jsonl}"; GEN="$ROOT/scripts/dev/m2b_probe_gen.py"
SIZES="${1:-1000 10000 100000}"
MH(){ "$MYSQL" -u root --socket="$HS" "$DB" "$@"; }
MHroot(){ "$MYSQL" -u root --socket="$HS" "$@"; }
wait_up(){ for i in $(seq 1 30); do "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1 && return; sleep 1; done; }
restart(){ [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  env $1 HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 ./scripts/start_mysql.sh >/dev/null 2>&1; wait_up
  MHroot -e "SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';" 2>/dev/null; }

echo "=== ensure server up (start_server only if none; NEVER stop_server) ==="
pgrep -x lineairdb-serve >/dev/null || { HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh >/dev/null 2>&1; sleep 2; }

# NOTE: default probe path is CONTIGUOUS (k=pk dense). The scattered variant
# (m2b_probe_gen.py --shuffle, k=LCG perm) is the contiguity-confound control measured
# separately in docs/data/m2b_bench.txt:29-40 (1.10x premium, in-memory => negligible).
# This driver intentionally uses the contiguous path for the main NNLS fit.
echo "=== build cal_* probe tables (sizes: $SIZES) ==="
restart ""
MHroot -e "CREATE DATABASE IF NOT EXISTS $DB;" 2>/dev/null || true
for N in $SIZES; do
  python3 "$GEN" --kind n --n "$N" | MH
  python3 "$GEN" --kind w --n "$N" | MH
  echo "  cal_n_$N / cal_w_$N loaded (rows=$(MH -N -e "SELECT COUNT(*) FROM cal_w_$N" 2>/dev/null))"
done

echo "=== hit probes under RPC trace -> $TRACE ==="
rm -f "$TRACE"
restart "ENABLE_RPC_TRACE=1 ENABLE_RPC_TRACE_PATH=$TRACE"
MHroot -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;" 2>/dev/null
for rep in 1 2 3; do
 for N in $SIZES; do
  MH -N -e "SELECT k FROM cal_w_$N WHERE pk=42" >/dev/null 2>&1                                  # point
  MH -N -e "SELECT k FROM cal_n_$N IGNORE INDEX(sk)" >/dev/null 2>&1                              # scan_n
  for frac in 10 50 90; do
    R=$(( N * frac / 100 )); [ "$R" -lt 1 ] && continue
    MH -N -e "SELECT pad FROM cal_w_$N IGNORE INDEX(sk) WHERE k BETWEEN 1 AND $R" >/dev/null 2>&1 # fullscan_filter
    MH -N -e "SELECT pad FROM cal_w_$N FORCE INDEX(sk) WHERE k BETWEEN 1 AND $R" >/dev/null 2>&1  # noncov
    MH -N -e "SELECT k   FROM cal_w_$N FORCE INDEX(sk) WHERE k BETWEEN 1 AND $R" >/dev/null 2>&1  # cov
  done
 done
done
sleep 1
echo "  trace: $(wc -l < "$TRACE" 2>/dev/null || echo 0) tx"
echo "=== (d)+(e) joint NNLS + identifiability gate ==="
python3 "$ROOT/scripts/dev/m2b_nnls.py" "$TRACE"
echo "STRUCT_BENCH_DONE"
