#!/bin/bash
# Per-query memory + time profiler for helios vs InnoDB.
# Splits helios memory between the lineairdb-server process (in-memory KV /
# resident dataset) and the mysqld process (proxy ingest cache + MySQL exec).
# Samples RSS of all processes at 10 Hz into per-query time-series CSVs, and
# emits a summary table. helios mysqld is restarted per query for a clean
# ingest-cache baseline; the server stays up (restart would need a reload), so
# its idle RSS just before each query is that query's resident-dataset baseline
# and the in-query peak minus idle is the server-side working set.
#
# Usage: memprofile_per_query.sh [q1 q2 ...]   (default: all 22)
set +u
cd /home/noxy/helios
BIN=/home/noxy/helios/build/runtime_output_directory
SOCK=/tmp/mysql.sock
ISOCK=/tmp/mysql_innodb.sock
JEMALLOC=/lib/x86_64-linux-gnu/libjemalloc.so.2
OUT=/tmp/memprof
mkdir -p "$OUT"
RES="$OUT/summary.tsv"
QUERIES=("$@"); [ ${#QUERIES[@]} -eq 0 ] && QUERIES=(q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22)

SVPID=$(ps -eo pid,comm | awk '$2=="lineairdb-serve"{print $1}' | head -1)
IPID=$(ps -eo pid,args | awk '/[m]ysqld/ && /data_innodb/{print $1}' | head -1)
echo "lineairdb-server pid=$SVPID  innodb-mysqld pid=$IPID" >&2

rss_kb() { awk '/VmRSS/{print $2}' /proc/$1/status 2>/dev/null; }
g() { awk "BEGIN{printf \"%.2f\", ${1:-0}/1048576}"; }

restart_helios_mysqld() {
  local oldpid=$(cat /tmp/mysql.pid 2>/dev/null)
  [ -n "$oldpid" ] && kill "$oldpid" 2>/dev/null
  for i in $(seq 1 15); do ps -p $oldpid >/dev/null 2>&1 || break; sleep 1; done
  ps -p $oldpid >/dev/null 2>&1 && kill -9 $oldpid 2>/dev/null
  sleep 2
  cd /home/noxy/helios/build
  nohup env LD_PRELOAD="$JEMALLOC" MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000 \
    HELIOS_PIN_TTL_MS=1800000 \
    ./runtime_output_directory/mysqld --datadir=./data --socket=$SOCK --port=3307 \
    --pid-file=/tmp/mysql.pid --default-storage-engine=lineairdb \
    --max-connections=16384 --open-files-limit=65535 --table-open-cache=8192 \
    --disable-log-bin >> "$OUT/mysqld.log" 2>&1 &
  disown
  cd /home/noxy/helios
  for i in $(seq 1 40); do $BIN/mysqladmin ping -u root --socket=$SOCK 2>/dev/null | grep -q alive && break; sleep 1; done
  $BIN/mysql -u root --socket=$SOCK -e \
    "SET GLOBAL lineairdb_server_host='127.0.0.1'; SET GLOBAL lineairdb_server_port=9999; SET GLOBAL lineairdb_oneshot_execution=ON;" >/dev/null 2>&1
}

# sampler: $1=csv $2..=pids ; header written by caller
sampler() {
  local csv="$1"; shift; local pids=("$@")
  local s0=$(date +%s%N)
  while true; do
    local now=$(( ($(date +%s%N)-s0)/1000000 )) line="$now"
    for p in "${pids[@]}"; do line="$line,$(rss_kb $p)"; done
    echo "$line" >> "$csv"
    sleep 0.1
  done
}

printf "Q\thel_ms\tinn_ms\tratio\tmy_idleGB\tmy_peakGB\tmy_dGB\tsv_idleGB\tsv_peakGB\tsv_dGB\tinn_idleGB\tinn_peakGB\trows\tmd5\n" > "$RES"

for q in "${QUERIES[@]}"; do
  echo "=== $q ===" >&2
  restart_helios_mysqld
  MPID=$(cat /tmp/mysql.pid)
  [ -z "$MPID" ] && { echo "no mysqld for $q" >&2; continue; }
  $BIN/mysql -u root --socket=$SOCK -e "SELECT 1;" >/dev/null 2>&1   # warm conn
  sleep 1
  my_idle=$(rss_kb $MPID); sv_idle=$(rss_kb $SVPID)

  # ---- helios run with time-series sampling (mysqld + server) ----
  CSV="$OUT/ts_${q}_helios.csv"; echo "t_ms,mysqld_kb,server_kb" > "$CSV"
  sampler "$CSV" "$MPID" "$SVPID" & SAMP=$!
  t0=$(date +%s%N)
  timeout 200 $BIN/mysql -u root --socket=$SOCK benchbase < /tmp/v_$q.sql > "$OUT/hout_$q.txt" 2>"$OUT/herr_$q.txt"
  t1=$(date +%s%N)
  sleep 0.2; kill $SAMP 2>/dev/null; wait $SAMP 2>/dev/null
  hel_ms=$(( (t1-t0)/1000000 ))
  my_peak=$(awk -F, 'NR>1 && $2!=""{if($2>m)m=$2}END{print m+0}' "$CSV")
  sv_peak=$(awk -F, 'NR>1 && $3!=""{if($3>m)m=$3}END{print m+0}' "$CSV")
  rows=$(wc -l < "$OUT/hout_$q.txt")
  hmd5=$(md5sum "$OUT/hout_$q.txt" | awk '{print $1}')
  imd5=$(md5sum /tmp/iout_$q.txt 2>/dev/null | awk '{print $1}')
  md5m="?"; [ "$hmd5" = "$imd5" ] && md5m="OK" || md5m="DIFF"

  # ---- InnoDB run with sampling ----
  i_idle=$(rss_kb $IPID)
  ICSV="$OUT/ts_${q}_innodb.csv"; echo "t_ms,innodb_kb" > "$ICSV"
  sampler "$ICSV" "$IPID" & ISAMP=$!
  it0=$(date +%s%N)
  timeout 200 $BIN/mysql -u root --socket=$ISOCK benchbase < /tmp/v_$q.sql > "$OUT/i2out_$q.txt" 2>/dev/null
  it1=$(date +%s%N)
  sleep 0.2; kill $ISAMP 2>/dev/null; wait $ISAMP 2>/dev/null
  inn_ms=$(( (it1-it0)/1000000 ))
  i_peak=$(awk -F, 'NR>1 && $2!=""{if($2>m)m=$2}END{print m+0}' "$ICSV")

  ratio=$(awk "BEGIN{if($inn_ms>0)printf \"%.1f\", $hel_ms/$inn_ms; else print \"-\"}")
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$q" "$hel_ms" "$inn_ms" "$ratio" \
    "$(g $my_idle)" "$(g $my_peak)" "$(g $((my_peak-my_idle)))" \
    "$(g $sv_idle)" "$(g $sv_peak)" "$(g $((sv_peak-sv_idle)))" \
    "$(g $i_idle)" "$(g $i_peak)" "$rows" "$md5m" >> "$RES"
  printf "  %s hel=%sms inn=%sms my_peak=%sGB sv_peak=%sGB inn_peak=%sGB %s\n" \
    "$q" "$hel_ms" "$inn_ms" "$(g $my_peak)" "$(g $sv_peak)" "$(g $i_peak)" "$md5m" >&2
done
echo "MEMPROFILE_DONE" >> "$RES"
column -t -s $'\t' "$RES"
