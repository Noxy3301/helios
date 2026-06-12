#!/bin/bash
# Run bench/queries/q1..q22.sql against the target endpoint, recording per-query
# wall time, status, and the FULL error text (stderr) to an output directory.
#
# Usage: bash bench/bin/tpch_matrix.sh OUTDIR [TIMEOUT_S] [QUERIES...]
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MYSQL="$ROOT/build/runtime_output_directory/mysql"
SOCK=${MYSQL_SOCK:-/tmp/mysql.sock}
DB=${MYSQL_DB:-benchbase}
OUT="${1:?usage: tpch_matrix.sh OUTDIR [TIMEOUT_S] [QUERIES...]}"
TMO="${2:-120}"
shift || true; shift || true
QUERIES=${*:-$(seq 1 22)}
mkdir -p "$OUT"
: > "$OUT/summary.csv"
echo "query,time_s,status" >> "$OUT/summary.csv"
for q in $QUERIES; do
  qf="$ROOT/bench/queries/q${q}.sql"
  S=$(date +%s.%N)
  timeout "$TMO" "$MYSQL" -u root --socket="$SOCK" "$DB" -N \
      < "$qf" > "$OUT/q${q}.out" 2> "$OUT/q${q}.err"
  RC=$?
  E=$(date +%s.%N)
  T=$(echo "$E - $S" | bc)
  if [ $RC -eq 124 ]; then ST="TIMEOUT"
  elif [ $RC -ne 0 ]; then ST="ERROR"
  else ST="OK"; fi
  printf "q%s,%.2f,%s\n" "$q" "$T" "$ST" >> "$OUT/summary.csv"
  printf "q%-3s %8.2fs  %s\n" "$q" "$T" "$ST"
done
