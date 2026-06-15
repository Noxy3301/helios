#!/bin/bash
# Phase-22 default-ON evidence sweep (Codex review #1/#2/#3: justify each gate
# independently). Loads TPC-H SF1 ONCE, then sweeps optimizer-gate configs by
# restarting ONLY mysqld (helios server data is preserved in memory). AGG stays
# OFF throughout (it is the OLTP-catastrophic feature, kept opt-in). Reports
# per-config OK-sum for fullidx and noidx, plus md5(22) for base & full.
#
# Configs (all relative to default-ON binary; we DISABLE via =0 to subset):
#   stockcost : COST_V2=0 OPT_STATS=0 RANGE_HIST=0 SEMIJOIN=0  (no cost model)
#   base      : RANGE_HIST=0 SEMIJOIN=0                        (COST_V2+OPT_STATS)
#   hist      : SEMIJOIN=0                                     (base + RANGE_HIST)
#   sj        : RANGE_HIST=0                                   (base + SEMIJOIN)
#   full      : (nothing)                                      (base + HIST + SJ)
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
MYSQL="$ROOT/build/runtime_output_directory/mysql"; HS=/tmp/mysql.sock; IS=/tmp/mysql_3308.sock; DB=benchbase
POSTLOAD=/tmp/bb_tpch/benchmarks/tpch/postload-mysql.sql
source "$ROOT/scripts/dev/cstate_guard.sh" 2>/dev/null && cstate_guard 2>/dev/null || true
MH(){ "$MYSQL" -u root --socket="$HS" "$DB" "$@"; }
MI(){ "$MYSQL" -u root --socket="$IS" "$DB" "$@"; }
TMO=150

start_mysqld_cfg(){ # $1=env-string (gate overrides)
  [ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true; sleep 2
  env $1 HELIOS_AGG_PUSHDOWN=0 ./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
  until "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
  MH -e "SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON; SET GLOBAL optimizer_switch='mrr_cost_based=on,batched_key_access=off';"
  bash "$ROOT/scripts/dev/prewarm_stats.sh" "$HS" "$DB" >/dev/null 2>&1 || true
}
oksum(){ awk -F, 'NR>1 && $3=="OK"{s+=$2;n++} END{printf "%.2fs (%d/22)",s,n}' "$1/summary.csv"; }
measure(){ # $1=outtag  -> prints OK-sum
  MYSQL_SOCK=$HS bash bench/bin/tpch_matrix.sh "/tmp/eval_$1" "$TMO" >/dev/null 2>&1
  oksum "/tmp/eval_$1"
}

echo "############ [1] fresh helios server + TPC-H SF1 load (fullidx) ############"
[ -f /tmp/mysql.pid ] && kill -9 "$(cat /tmp/mysql.pid)" 2>/dev/null || true
./scripts/stop_server.sh || true; sleep 2
HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1 ./scripts/start_server.sh; sleep 2
./scripts/start_mysql.sh >/dev/null 2>&1; sleep 2
until "$MYSQL" -u root --socket="$HS" -e "SELECT 1" >/dev/null 2>&1; do sleep 1; done
python3 bench/bin/benchrun.py tpch --scalefactor 1 --no-exec --external-server --loader-threads 16 2>&1 | grep -E "Load time|ERROR" || true
echo "  helios sec_idx=$(MH -N -e "SELECT COUNT(DISTINCT CONCAT(table_name,index_name)) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"

echo "############ [1b] InnoDB:3308 SF1 reload + standard indexes (md5 ref) ############"
bash scripts/dev/start_innodb_ref.sh 2>/dev/null | tail -1 || true; sleep 3
python3 bench/bin/benchrun.py tpch --scalefactor 1 --terminals 1 --no-exec --external-server --mysql-port 3308 2>&1 | grep -E "Load time|ERROR" || true
MI -e "DROP VIEW IF EXISTS revenue0;" 2>/dev/null || true
MI < "$POSTLOAD" 2>&1 | tail -1 || true
echo "  InnoDB lineitem=$(MI -N -e "SELECT COUNT(*) FROM lineitem" 2>/dev/null)"

echo "############ [2] FULLIDX config sweep (AGG off) ############"
start_mysqld_cfg "";  measure full_warm >/dev/null   # throwaway warm (server caches)
declare -A FULL
start_mysqld_cfg "HELIOS_COST_V2=0 HELIOS_OPT_STATS=0 HELIOS_RANGE_HIST=0 HELIOS_ENABLE_SEMIJOIN=0"; FULL[stockcost]=$(measure f_stock)
start_mysqld_cfg "HELIOS_RANGE_HIST=0 HELIOS_ENABLE_SEMIJOIN=0";                                     FULL[base]=$(measure f_base)
start_mysqld_cfg "HELIOS_ENABLE_SEMIJOIN=0";                                                         FULL[hist]=$(measure f_hist)
start_mysqld_cfg "HELIOS_RANGE_HIST=0";                                                              FULL[sj]=$(measure f_sj)
start_mysqld_cfg "";                                                                                 FULL[full]=$(measure f_full)
echo "  fullidx stockcost = ${FULL[stockcost]}"
echo "  fullidx base      = ${FULL[base]}"
echo "  fullidx +hist     = ${FULL[hist]}"
echo "  fullidx +sj       = ${FULL[sj]}"
echo "  fullidx full      = ${FULL[full]}"

echo "############ [3] md5 (fullidx) — base & full must be 22/22 vs InnoDB ############"
start_mysqld_cfg "HELIOS_RANGE_HIST=0 HELIOS_ENABLE_SEMIJOIN=0"
echo "  [base] $(bash bench/bin/tpch_md5.sh --target-socket "$HS" --ref-socket "$IS" --db "$DB" 2>&1 | tail -1)"
start_mysqld_cfg ""
echo "  [full] $(bash bench/bin/tpch_md5.sh --target-socket "$HS" --ref-socket "$IS" --db "$DB" 2>&1 | tail -1)"

echo "############ [4] NOIDX (drop secondary indexes) reference ############"
MH -N -e "SELECT CONCAT('DROP INDEX \`',index_name,'\` ON \`',table_name,'\`;') FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY' GROUP BY table_name,index_name;" \
  | while read -r s; do [ -n "$s" ] && MH -e "$s" 2>/dev/null; done
echo "  helios sec_idx now=$(MH -N -e "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema='$DB' AND index_name<>'PRIMARY'")"
declare -A NO
start_mysqld_cfg "";  measure n_warm >/dev/null
start_mysqld_cfg "HELIOS_COST_V2=0 HELIOS_OPT_STATS=0 HELIOS_RANGE_HIST=0 HELIOS_ENABLE_SEMIJOIN=0"; NO[stockcost]=$(measure n_stock)
start_mysqld_cfg "HELIOS_RANGE_HIST=0 HELIOS_ENABLE_SEMIJOIN=0";                                     NO[base]=$(measure n_base)
start_mysqld_cfg "";                                                                                 NO[full]=$(measure n_full)
echo "  noidx stockcost = ${NO[stockcost]}"
echo "  noidx base      = ${NO[base]}"
echo "  noidx full      = ${NO[full]}"

echo "############ VERDICT ############"
echo "  net-positive iff fullidx(cfg) < noidx(cfg). Target: fullidx < ~39s."
echo "EVAL_DONE"
