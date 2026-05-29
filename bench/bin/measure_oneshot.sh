#!/bin/bash
# Measure all 22 TPC-H via the PREFETCH path (lineairdb_oneshot_execution=ON).
# Assumes server(9999)+helios mysqld(3307, NEW plugin, OPT_STATS=1)+InnoDB(3308) up.
# Per-query timeout so breakage is visible immediately. NEVER touches InnoDB data.
set -uo pipefail
BUILD=/home/noxy/helios/build
MYSQL=$BUILD/runtime_output_directory/mysql
HS=/tmp/mysql.sock; IS=/tmp/mysql_innodb.sock
TMO=${TMO:-120}   # per-query hard timeout (s)
log(){ echo "[$(date +%T)] $*"; }

# Enable prefetch (oneshot) on helios. InnoDB unaffected.
$MYSQL -u root --socket=$HS --port=3307 -e "SET GLOBAL lineairdb_oneshot_execution=ON;" 2>&1
$MYSQL -u root --socket=$HS --port=3307 benchbase -e "SELECT COUNT(*) FROM lineitem;" >/dev/null 2>&1

# best-of-2 under timeout; prints ms or TIMEOUT
best(){ local s=$1 p=$2 q=$3 b=99999999 i t0 t1 d rc; for i in 1 2; do
  t0=$(date +%s%N); timeout $TMO $MYSQL -u root --socket=$s --port=$p benchbase < $q >/dev/null 2>&1; rc=$?; t1=$(date +%s%N)
  [ $rc -eq 124 ] && { echo "TIMEOUT"; return; }
  d=$(((t1-t0)/1000000)); [ $d -lt $b ]&&b=$d; done; echo $b; }

OUT=/tmp/cmp_oneshot.txt
printf "%-4s %9s %9s %7s %s\n" "Q" "helios" "innodb" "ratio" "md5" > $OUT
htot=0; itot=0
for n in $(seq 1 22); do
  q=/tmp/v_q$n.sql; [ -f "$q" ]||continue
  # md5 (single run, timeout)
  timeout $TMO $MYSQL -u root --socket=$HS --port=3307 benchbase < $q > /tmp/ho_$n.out 2>/dev/null
  hm=$(md5sum /tmp/ho_$n.out|awk '{print $1}'); im=$(md5sum /tmp/iout_q$n.txt 2>/dev/null|awk '{print $1}')
  md=$([ "$hm" = "$im" ]&&echo OK||echo DIFF)
  h=$(best $HS 3307 $q); i=$(best $IS 3308 $q)
  if [ "$h" = "TIMEOUT" ]; then r="-"; else htot=$((htot+h)); fi
  [ "$i" != "TIMEOUT" ] && itot=$((itot+i))
  if [ "$h" != "TIMEOUT" ] && [ "$i" != "TIMEOUT" ] && [ "$i" -gt 0 ]; then r=$(awk "BEGIN{printf \"%.1f\",$h/$i}"); else r="-"; fi
  printf "q%-3s %8sms %8sms %6sx %s\n" "$n" "$h" "$i" "$r" "$md" >> $OUT
  log "q$n helios=${h} innodb=${i} md5=$md"
done
printf "%-4s %8sms %8sms %6sx\n" "TOT" "$htot" "$itot" "$(awk "BEGIN{if($itot>0)printf \"%.1f\",$htot/$itot}")" >> $OUT
echo "===ONESHOT MEASURE DONE===" >> $OUT
cat $OUT
