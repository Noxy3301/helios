#!/bin/bash
# Pre-warm the optimizer NDV / row-count cache before a measurement run so the
# one-time-per-table stats RPC (TX_GET_TABLE_STATS) does not land INSIDE the
# measured queries and show up as noise.
#
# Why this works (and why no code change is needed): under HELIOS_OPT_STATS the
# proxy fetches per-index NDV + row counts once and seeds them into the GLOBAL
# TABLE_SHARE (share->index_ndv_loaded_). The fetch is share-gated, so after the
# first info() per table NO connection re-fetches for the mysqld's lifetime.
# EXPLAIN runs the optimizer (which calls info()) WITHOUT executing the query,
# so it seeds every table's share with ZERO row transfer. Verified on TPC-H
# SF=1: after this pass q9/q21/q22 each issue exactly one TX_EXECUTE_READ_PLAN
# RPC and zero stats RPCs (Phase-19).
#
# Run once right after (re)starting mysqld, before measuring. mysqld must be up
# with HELIOS_OPT_STATS=1 (the standard measurement env).
#
# Usage: scripts/dev/prewarm_stats.sh [SOCKET] [DB]
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SOCK="${1:-/tmp/mysql.sock}"
DB="${2:-benchbase}"
MYSQL="$ROOT/build/runtime_output_directory/mysql -u root --socket=$SOCK"

mapfile -t TABLES < <($MYSQL -N -e "SHOW TABLES FROM \`$DB\`;" 2>/dev/null)
if [ "${#TABLES[@]}" -eq 0 ]; then
  echo "  [prewarm_stats] no tables in $DB (is it loaded? socket=$SOCK)" >&2
  exit 1
fi
n=0
for t in "${TABLES[@]}"; do
  if $MYSQL "$DB" -e "EXPLAIN SELECT * FROM \`$t\`;" >/dev/null 2>&1; then
    n=$((n + 1))
  fi
done
echo "  [prewarm_stats] seeded NDV/row-count for $n/${#TABLES[@]} tables in $DB (socket=$SOCK)"
