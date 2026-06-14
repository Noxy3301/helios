#!/bin/bash
# Phase-22 M1 final check: rebuild with the new CODE DEFAULT (kC_materialise=11),
# restart mysqld WITHOUT HELIOS_C_MATERIALISE in env (so the code default is what
# runs), and validate the full 22-query suite on the live SF1 server (the
# in-memory server + 23 indexes survive the mysqld restart; NO re-backfill).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
OUT=/tmp/m1_final; rm -rf "$OUT"; mkdir -p "$OUT"

echo "################ rebuild (default kC_materialise=11) ################"
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
( time bash scripts/build_partial.sh ) 2>&1 | tail -3
strings build/plugin_output_directory/ha_lineairdb_storage_engine.so | grep -q HELIOS_C_MATERIALISE && echo "  .so has materialise term" || { echo "BUILD missing symbol"; exit 1; }

echo "################ restart mysqld (NO C_MATERIALISE env -> code default 11) ################"
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
"$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
echo "  C_MATERIALISE in env: $(tr '\0' '\n' < /proc/"$(cat /tmp/mysql.pid)"/environ | grep -i C_MATERIALISE || echo 'ABSENT -> code default 11')"
echo "  data present: lineitem=$("$MYSQL" -u root --socket="$SOCK" -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem'")  secondary_idx=$("$MYSQL" -u root --socket="$SOCK" -N -e "SELECT COUNT(DISTINCT CONCAT(table_name,index_name)) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"

echo "################ full 22-suite (warm then measured) ################"
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/warm" 150 >/dev/null 2>&1
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/suite" 150
echo "----"
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "fullidx OK-sum=%.2fs (%d/22 ok)\n",s,n}' "$OUT/suite/summary.csv"
awk -F, 'NR>1 && $3!="OK"{printf "  NON-OK: %s %s\n",$1,$3}' "$OUT/suite/summary.csv"
echo "regressor recovery (M0 range -> now): q3 6.06->$(awk -F, '$1=="q3"{print $2}' "$OUT/suite/summary.csv") q5 3.61->$(awk -F, '$1=="q5"{print $2}' "$OUT/suite/summary.csv") q7 14.23->$(awk -F, '$1=="q7"{print $2}' "$OUT/suite/summary.csv") q8 9.31->$(awk -F, '$1=="q8"{print $2}' "$OUT/suite/summary.csv")"
echo "FINALCHECK_DONE"
