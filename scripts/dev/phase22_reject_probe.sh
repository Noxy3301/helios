#!/bin/bash
# Plan A ① — capture WHICH QEP shapes cost_v2 makes the autogen/prefetch path
# reject on TPC-C, to decide A1 (extend autogen coverage to compile them in 2
# RPCs) vs A2 (steer cost_v2 to autogen-compatible plans). Fresh server + full
# load + cost_v2 + prefetch + HELIOS_REJECT_DEBUG, short run, then aggregate the
# [REJECT-*] lines by type/reason (sql/query_id stripped) from the mysqld log.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
TIME_S="${1:-10}"; LOGDIR="$ROOT/lineairdb_logs"
MY="build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock"

echo "=== fresh server + cost_v2 + REJECT_DEBUG mysqld + full load ==="
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
./scripts/stop_server.sh >/dev/null 2>&1 || true; sleep 1
./scripts/start_server.sh >/dev/null 2>&1; sleep 2
env HELIOS_COST_V2=1 HELIOS_OPT_STATS=0 HELIOS_ENABLE_SEMIJOIN=0 HELIOS_RANGE_HIST=0 \
    HELIOS_AGG_PUSHDOWN=0 HELIOS_REJECT_DEBUG=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 3
until $MY -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
LOGF=$(ls -t "$LOGDIR"/mysqld_3307_*.log 2>/dev/null | head -1)
echo "  log: $LOGF"

echo "=== cost_v2 + prefetch TPC-C ${TIME_S}s (errors expected) ==="
python3 bench/bin/benchrun.py tpcc --external-server --time "$TIME_S" --terminals 1 --prefetch-stmt 2>&1 \
  | grep -iE "Goodput|Unexpected Errors|Throughput|Load time" | tail -5

echo "=== REJECT aggregation by type/reason (top 25) ==="
grep -hE "\[REJECT-(AUTOGEN|PREFETCH)\]" "$LOGF" 2>/dev/null \
  | sed -E 's/ sql=.*$//; s/ query_id=[0-9]+//' \
  | sort | uniq -c | sort -rn | head -25
echo "  (total reject lines: $(grep -hcE "\[REJECT-(AUTOGEN|PREFETCH)\]" "$LOGF" 2>/dev/null || echo 0))"
echo "REJECT_PROBE_DONE"
