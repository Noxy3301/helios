#!/bin/bash
# M1 final check (continuation): .so already built with default kC_materialise=11.
# Restart mysqld WITHOUT C_MATERIALISE env (code default 11), validate full suite
# on the live SF1 server (data survives mysqld restart).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
OUT=/tmp/m1_final; rm -rf "$OUT"; mkdir -p "$OUT"
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
"$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
echo "C_MATERIALISE env: $(tr '\0' '\n' < /proc/"$(cat /tmp/mysql.pid)"/environ | grep -i C_MATERIALISE || echo 'ABSENT -> code default 11')"
echo "data: lineitem=$("$MYSQL" -u root --socket="$SOCK" -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem'") sec_idx=$("$MYSQL" -u root --socket="$SOCK" -N -e "SELECT COUNT(DISTINCT CONCAT(table_name,index_name)) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/warm" 150 >/dev/null 2>&1
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/suite" 150
echo "----"
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "fullidx OK-sum=%.2fs (%d/22 ok)\n",s,n}' "$OUT/suite/summary.csv"
awk -F, 'NR>1 && $3!="OK"{printf "  NON-OK: %s %s\n",$1,$3}' "$OUT/suite/summary.csv"
echo "FINALCHECK_DONE"
