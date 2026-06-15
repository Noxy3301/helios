#!/bin/bash
# M2b (g) scale-invariance: re-fit the physical constants at multiple probe scales.
# The cal_* size N is the calibration's own scale axis; hardware constants
# (C_rpc, C_byte, S_scan) MUST be size-invariant. Each N = an independent struct
# bench + joint NNLS. Stable S_scan/B_ship/C_rpc across N => physical (hardware) =>
# (g) PASS; size-dependent => not physical (e.g. a steering value). Restart-only
# mysqld; NEVER stop_server.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
SIZES="${1:-1000 10000 100000}"
echo "=== (g) scale-invariance: per-size physical-constant re-fit ==="
printf "%-10s %-12s %-14s %-16s %-6s %s\n" "N" "S_scan(ns)" "B_ship(ns/B)" "C_rpc(point,ns)" "gate" "cond"
declare -a SS BB CC
for N in $SIZES; do
  TRACE=/tmp/m2b_struct_n${N}.jsonl bash scripts/dev/m2b_struct_bench.sh "$N" >/tmp/m2b_msf_${N}.log 2>&1 || true
  j=$(grep NNLS_JSON /tmp/m2b_msf_${N}.log | tail -1 | sed 's/.*NNLS_JSON //')
  python3 - "$N" "$j" <<'PY'
import json, sys
N = sys.argv[1]; j = sys.argv[2]
try:
    d = json.loads(j)
    print(f"{N:<10} {d['S_scan_ns']:<12.1f} {d['B_ship_ns']:<14.3f} {d['C_rpc_point_ns']:<16.0f} {str(d['gate_ok']):<6} {d['cond']:.2f}")
except Exception as e:
    print(f"{N:<10} PARSE_FAIL {e}")
PY
done
echo "MULTISF_DONE (stable S_scan/B_ship/C_rpc across N => physical/hardware = (g) PASS)"
