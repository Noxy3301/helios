#!/bin/bash
# Per-query TIME BREAKDOWN via proxy TIMEPROF + HANDLER_TIMEPROF, prefetch path.
# Restarts ONLY helios mysqld (3307) with the timeprof envs; server(9999) stays
# (no reload). InnoDB(3308) untouched. Per-query timeout. Extracts, per query:
#   rpc_exec (server scan+serialize+network), ingest (proxy row materialize),
#   commit, and set_fields/rnd_next/idx_read from HTIMEPROF.
set -uo pipefail
BUILD=/home/noxy/helios/build
MYSQL=$BUILD/runtime_output_directory/mysql
HS=/tmp/mysql.sock
JE=/lib/x86_64-linux-gnu/libjemalloc.so.2
LOG=/tmp/mysqld_timeprof.log
TMO=${TMO:-120}
log(){ echo "[$(date +%T)] $*"; }

# restart helios mysqld with timeprof envs (PID via /proc, no self-match)
MPID=""
for p in $(pgrep mysqld); do cl=$(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null); case "$cl" in *port=3307*) MPID=$p;; esac; done
[ -n "$MPID" ] && { kill $MPID; for i in $(seq 1 40); do kill -0 $MPID 2>/dev/null||break; sleep 0.5; done; }
LD_PRELOAD=$JE HELIOS_RO_NOVALIDATE=1 HELIOS_OPT_STATS=1 HELIOS_TIMEPROF=1 HELIOS_HANDLER_TIMEPROF=1 \
  $BUILD/runtime_output_directory/mysqld --datadir=$BUILD/data --socket=$HS --port=3307 \
  --pid-file=/tmp/mysql.pid --default-storage-engine=lineairdb --max-connections=16384 \
  --open-files-limit=65535 --table-open-cache=8192 --disable-log-bin > $LOG 2>&1 &
for i in $(seq 1 60); do $MYSQL -u root --socket=$HS --port=3307 -e "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
$MYSQL -u root --socket=$HS --port=3307 -e "SET GLOBAL lineairdb_oneshot_execution=ON;" >/dev/null 2>&1
$MYSQL -u root --socket=$HS --port=3307 benchbase -e "SELECT COUNT(*) FROM lineitem;" >/dev/null 2>&1
log "mysqld up with TIMEPROF, oneshot ON"

OUT=/tmp/timeprof_per_query.txt
printf "%-4s %8s %9s %8s %8s %10s %s\n" "Q" "total" "rpc_exec" "ingest" "commit" "set_fields" "status" > $OUT
for n in $(seq 1 22); do
  q=/tmp/v_q$n.sql; [ -f "$q" ]||continue
  mark=$(wc -l < $LOG)
  t0=$(date +%s%N); timeout $TMO $MYSQL -u root --socket=$HS --port=3307 benchbase < $q >/dev/null 2>&1; rc=$?; t1=$(date +%s%N)
  tot=$(((t1-t0)/1000000)); st="OK"; [ $rc -eq 124 ] && { tot=">$TMO""s"; st="TIMEOUT"; }
  seg=$(tail -n +$((mark+1)) $LOG)
  # take the LARGEST rpc_exec TIMEPROF line for this query (the main SELECT tx)
  tp=$(echo "$seg" | grep -F "[TIMEPROF]" | sort -t= -k3 -g | tail -1)
  ht=$(echo "$seg" | grep -F "[HTIMEPROF]" | tail -1)
  rpc=$(echo "$tp"|grep -oE "rpc_exec=[0-9.]+"|cut -d= -f2); ing=$(echo "$tp"|grep -oE "ingest=[0-9.]+"|cut -d= -f2)
  com=$(echo "$tp"|grep -oE "commit=[0-9.]+"|cut -d= -f2); sf=$(echo "$ht"|grep -oE "set_fields=[0-9.]+"|cut -d= -f2)
  printf "q%-3s %7sms %8sms %7sms %7sms %9sms %s\n" "$n" "$tot" "${rpc:-?}" "${ing:-?}" "${com:-?}" "${sf:-?}" "$st" >> $OUT
  log "q$n total=${tot} rpc=${rpc:-?} ingest=${ing:-?} commit=${com:-?} setf=${sf:-?}"
done
echo "===TIMEPROF DONE===" >> $OUT
cat $OUT
