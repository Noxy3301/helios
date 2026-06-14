#!/bin/bash
# Phase-22 M5 validate: the GS-skip full-scan-shape predicate should let q18's
# full l_ok secondary scan be served by GroupedSummary (recover ~3s) WITHOUT
# re-breaking q15 (l_sd range stays staged) / q1 / q6, and WITHOUT changing
# results. Runs on the LIVE SF1 server (data preserved; only mysqld restarted).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; SOCK=/tmp/mysql.sock; DB=benchbase
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until "$MYSQL" -u root --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
"$MYSQL" -u root --socket="$SOCK" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
echo "data: lineitem=$("$MYSQL" -u root --socket="$SOCK" -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem'") idx=$("$MYSQL" -u root --socket="$SOCK" -N -e "SELECT COUNT(DISTINCT CONCAT(table_name,index_name)) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"
timeq(){ local f=$1 S E rc; "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$f" >/dev/null 2>&1; S=$(date +%s.%N); timeout 150 "$MYSQL" -u root --socket="$SOCK" "$DB" -N < "$f" >/dev/null 2>&1; rc=$?; E=$(date +%s.%N); printf "%.2f" "$(echo "$E-$S"|bc)"; }
md5q(){ "$MYSQL" -u root --socket="$SOCK" "$DB" -N --batch < "$1" 2>/dev/null | sort | md5sum | cut -d' ' -f1; }

echo "################ q18: recovered via GS? ################"
echo "  q18 plan (still l_ok? GS-skip is execution-layer):"
"$MYSQL" -u root --socket="$SOCK" "$DB" -N -e "EXPLAIN FORMAT=TREE $(grep -v '^--' bench/queries/q18.sql | tr '\n' ' ' | sed 's/;[[:space:]]*$//')" 2>&1 | sed 's/\\n/\n/g' | grep -E 'Index scan on lineitem' | sed 's/^/    /'
echo "  q18 time: $(timeq bench/queries/q18.sql)s  (was 24.66s; forced-PRIMARY=3.07s)"
echo "  q18 md5 == forced-PRIMARY(known-correct) md5?  q18=$(md5q bench/queries/q18.sql)  ign=$(md5q /tmp/q18_ign.sql)"

echo "################ non-regression: q15/q1/q6 ################"
printf "  q15=%ss q1=%ss q6=%ss\n" "$(timeq bench/queries/q15.sql)" "$(timeq bench/queries/q1.sql)" "$(timeq bench/queries/q6.sql)"

echo "################ full 22-suite ################"
OUT=/tmp/m5; rm -rf "$OUT"; mkdir -p "$OUT"
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/warm" 150 >/dev/null 2>&1
MYSQL_SOCK=$SOCK bash bench/bin/tpch_matrix.sh "$OUT/suite" 150
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "  fullidx OK-sum=%.2fs (%d/22 ok)\n",s,n}' "$OUT/suite/summary.csv"
awk -F, 'NR>1 && $3!="OK"{printf "    NON-OK: %s %s\n",$1,$3}' "$OUT/suite/summary.csv"
echo "M5_DONE"
