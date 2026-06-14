#!/bin/bash
# Phase-22 rigorous re-verification (closes logic-audit B1 + B2) on the M5 build.
# B1: full 22-query md5, helios(fullidx, prefetch ON) vs InnoDB:3308 SF1.
# B2: in-session noidx-vs-fullidx suite timing, SAME build + governor + C6 pinned.
# Helios server stays up (SF1 data preserved); only mysqld restarted.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; IS=/tmp/mysql_3308.sock; DB=benchbase
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
MH(){ "$MYSQL" -u root --socket="$HS" "$DB" "$@"; }
MI(){ "$MYSQL" -u root --socket="$IS" "$DB" "$@"; }

echo "################ [1] InnoDB:3308 SF1 + standard indexes (md5 reference) ################"
bash scripts/dev/start_innodb_ref.sh 2>/dev/null | tail -1 || bash scripts/start_innodb_ref.sh 2>/dev/null | tail -1 || true
sleep 3
python3 bench/bin/benchrun.py tpch --scalefactor 1 --terminals 1 --no-exec --external-server --mysql-port 3308 2>&1 | grep -E "Load time|ERROR" || true
MI -e "DROP VIEW IF EXISTS revenue0;" 2>/dev/null || true
MI < "$POSTLOAD" 2>&1 | tail -1 || true
echo "  InnoDB lineitem=$(MI -N -e "SELECT COUNT(*) FROM lineitem" 2>/dev/null)"

echo "################ [2] Helios mysqld restart (M5 build, default kC_materialise=8) ################"
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
"$MYSQL" -u root --socket="$HS" -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
echo "  helios fullidx: lineitem=$(MH -N -e "SELECT table_rows FROM information_schema.tables WHERE table_schema='$DB' AND table_name='lineitem'") sec_idx=$(MH -N -e "SELECT COUNT(DISTINCT CONCAT(table_name,index_name)) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"

echo "################ [B1] FULLIDX md5 (helios prefetch ON vs InnoDB SF1) ################"
bash bench/bin/tpch_md5.sh --target-socket "$HS" --ref-socket "$IS" --db "$DB" 2>&1 | tail -26
echo "################ [B2a] FULLIDX suite timing (in-session, M5 build) ################"
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/v_full_warm 150 >/dev/null 2>&1
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/v_full 150 >/dev/null 2>&1
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "  FULLIDX OK-sum=%.2fs (%d/22)\n",s,n}' /tmp/v_full/summary.csv

echo "################ [3] Helios -> NOIDX (drop all secondary indexes, same session) ################"
MH -N -e "SELECT CONCAT('DROP INDEX \`',index_name,'\` ON \`',table_name,'\`;') FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY' GROUP BY table_name,index_name;" \
  | while read -r s; do [ -n "$s" ] && MH -e "$s" 2>/dev/null; done
bash scripts/dev/prewarm_stats.sh "$HS" "$DB" >/dev/null 2>&1 || true
echo "  helios noidx: sec_idx=$(MH -N -e "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"
echo "################ [B1-noidx] NOIDX md5 (helios prefetch ON vs InnoDB SF1) ################"
bash bench/bin/tpch_md5.sh --target-socket "$HS" --ref-socket "$IS" --db "$DB" 2>&1 | tail -3
echo "################ [B2b] NOIDX suite timing (in-session, governor+C6 pinned) ################"
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/v_noidx_warm 150 >/dev/null 2>&1
MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh /tmp/v_noidx 150 >/dev/null 2>&1
awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "  NOIDX OK-sum=%.2fs (%d/22)\n",s,n}' /tmp/v_noidx/summary.csv
echo "################ VERDICT ################"
echo "  in-session: fullidx=$(awk -F, 'NR>1 && $3=="OK"{s+=$2} END{printf "%.2f",s}' /tmp/v_full/summary.csv)s  noidx=$(awk -F, 'NR>1 && $3=="OK"{s+=$2} END{printf "%.2f",s}' /tmp/v_noidx/summary.csv)s"
echo "VERIFY_DONE"
