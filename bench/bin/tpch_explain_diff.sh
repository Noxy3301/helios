#!/bin/bash
# Capture EXPLAIN FORMAT=TREE for q1..q22 on Helios and InnoDB side by side,
# to compare join orders / access methods (plan-equivalence check).
# Usage: bash bench/bin/tpch_explain_diff.sh OUTDIR
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MYSQL="$ROOT/build/runtime_output_directory/mysql"
HSOCK=${HELIOS_SOCK:-/tmp/mysql.sock}
ISOCK=${INNODB_SOCK:-/tmp/mysql_3308.sock}
DB=${MYSQL_DB:-benchbase}
OUT="${1:?usage: tpch_explain_diff.sh OUTDIR}"
mkdir -p "$OUT"
for q in $(seq 1 22); do
  qf="$ROOT/bench/queries/q${q}.sql"
  [ -f "$qf" ] || continue
  # q15 contains CREATE VIEW/DROP VIEW; EXPLAIN only the middle SELECT crudely:
  sql=$(grep -v '^--' "$qf" | tr '\n' ' ')
  if [ "$q" = 15 ]; then
    echo "(q15 skipped: multi-statement view)" > "$OUT/q${q}.helios.plan"
    cp "$OUT/q${q}.helios.plan" "$OUT/q${q}.innodb.plan"
    continue
  fi
  sql=${sql%;*}
  "$MYSQL" -u root --socket="$HSOCK" "$DB" -N \
    -e "EXPLAIN FORMAT=TREE $sql" 2>&1 | sed 's/\\n/\n/g' > "$OUT/q${q}.helios.plan"
  "$MYSQL" -u root --socket="$ISOCK" "$DB" -N \
    -e "EXPLAIN FORMAT=TREE $sql" 2>&1 | sed 's/\\n/\n/g' > "$OUT/q${q}.innodb.plan"
done
echo "plans in $OUT/"
