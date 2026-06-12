#!/bin/bash
# Head-to-head TPC-H comparison: Helios (LineairDB, prefetch) vs InnoDB.
# For each query: wall time, result md5 (sorted), and peak RSS during the
# query (Helios = mysqld+lineairdb-server combined; InnoDB = its mysqld).
#
# Usage:
#   bash bench/bin/tpch_compare.sh OUTDIR [TIMEOUT_S] [QUERIES...]
# Env: HELIOS_SOCK (default /tmp/mysql.sock), INNODB_SOCK (default /tmp/mysql_3308.sock)
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MYSQL="$ROOT/build/runtime_output_directory/mysql"
HSOCK=${HELIOS_SOCK:-/tmp/mysql.sock}
ISOCK=${INNODB_SOCK:-/tmp/mysql_3308.sock}
DB=${MYSQL_DB:-benchbase}
OUT="${1:?usage: tpch_compare.sh OUTDIR [TIMEOUT_S] [QUERIES...]}"
TMO="${2:-600}"
shift || true; shift || true
QUERIES=${*:-$(seq 1 22)}
mkdir -p "$OUT"

# Sample peak RSS (kB) of processes matching a pattern while a command runs.
run_measured() { # pattern, sock, qfile, out, err -> "time_s rss_peak_kb status"
  local pattern="$1" sock="$2" qfile="$3" outf="$4" errf="$5"
  local peakfile; peakfile=$(mktemp)
  ( peak=0
    while :; do
      r=$(ps -eo rss,args 2>/dev/null | grep -E "$pattern" | grep -v grep |
          awk '{s+=$1} END{print s+0}')
      [ "$r" -gt "$peak" ] && peak=$r
      echo "$peak" > "$peakfile"
      sleep 0.3
    done ) & local SAMP=$!
  local S E RC
  S=$(date +%s.%N)
  timeout "$TMO" "$MYSQL" -u root --socket="$sock" "$DB" -N \
      < "$qfile" > "$outf" 2> "$errf"
  RC=$?
  E=$(date +%s.%N)
  kill $SAMP 2>/dev/null; wait $SAMP 2>/dev/null
  local T PK ST
  T=$(echo "$E - $S" | bc)
  PK=$(cat "$peakfile" 2>/dev/null || echo 0)
  rm -f "$peakfile"
  if [ $RC -eq 124 ]; then ST=TIMEOUT; elif [ $RC -ne 0 ]; then ST=ERROR; else ST=OK; fi
  echo "$T $PK $ST"
}

CSV="$OUT/compare.csv"
echo "query,helios_s,helios_peak_gb,innodb_s,innodb_peak_gb,speedup,md5_match" > "$CSV"
printf "%-5s %10s %10s %12s %12s %9s %6s\n" Q helios_s innodb_s helios_pk_gb innodb_pk_gb speedup md5
for q in $QUERIES; do
  qf="$ROOT/bench/queries/q${q}.sql"
  [ -f "$qf" ] || continue
  read -r HT HP HS <<<"$(run_measured 'mysqld --datadir=./data |mysqld --datadir=/.*helios/build/data |lineairdb-server' "$HSOCK" "$qf" "$OUT/q${q}.helios.out" "$OUT/q${q}.helios.err")"
  read -r IT IP IS <<<"$(run_measured 'data_innodb_ref' "$ISOCK" "$qf" "$OUT/q${q}.innodb.out" "$OUT/q${q}.innodb.err")"
  MD5=NA
  if [ "$HS" = OK ] && [ "$IS" = OK ]; then
    h=$(sort "$OUT/q${q}.helios.out" | md5sum | cut -d' ' -f1)
    i=$(sort "$OUT/q${q}.innodb.out" | md5sum | cut -d' ' -f1)
    [ "$h" = "$i" ] && MD5=OK || MD5=MISMATCH
  fi
  HG=$(echo "scale=1; $HP/1048576" | bc); IG=$(echo "scale=1; $IP/1048576" | bc)
  SP=NA
  [ "$HS" = OK ] && [ "$IS" = OK ] && SP=$(echo "scale=2; $IT/$HT" | bc)
  [ "$HS" = OK ] || HT="$HS"; [ "$IS" = OK ] || IT="$IS"
  echo "q$q,$HT,$HG,$IT,$IG,$SP,$MD5" >> "$CSV"
  printf "%-5s %10s %10s %12s %12s %9s %6s\n" "q$q" "$HT" "$IT" "$HG" "$IG" "$SP" "$MD5"
done
echo "CSV: $CSV"
