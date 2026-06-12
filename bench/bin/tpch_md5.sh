#!/bin/bash
# Compare TPC-H query results (bench/queries/q1..q22.sql, fixed qualification
# parameters) between two MySQL endpoints and report md5 per query.
#
# Usage:
#   bash bench/bin/tpch_md5.sh [--target-socket /tmp/mysql.sock] \
#       [--ref-socket /tmp/mysql_3308.sock] [--db benchbase] [--queries "1 2 6"]
#
# The reference endpoint is typically an InnoDB-backed mysqld loaded with the
# same scale factor. Output rows are normalized (sorted) before hashing so
# result-order differences do not matter; column order and values must match.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MYSQL="$ROOT/build/runtime_output_directory/mysql"
TARGET_SOCK=/tmp/mysql.sock
REF_SOCK=/tmp/mysql_3308.sock
DB=benchbase
QUERIES="$(seq 1 22)"

TARGET_SET=""   # SQL executed before each target query (e.g. SET GLOBAL ...=ON)
REF_SET=""      # SQL executed before each reference query

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-socket) TARGET_SOCK="$2"; shift 2;;
    --ref-socket) REF_SOCK="$2"; shift 2;;
    --target-set) TARGET_SET="$2"; shift 2;;
    --ref-set) REF_SET="$2"; shift 2;;
    --db) DB="$2"; shift 2;;
    --queries) QUERIES="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

run_one() { # socket, setup_sql, qfile -> md5 or ERROR
  local sock="$1" setup="$2" qfile="$3"
  local out
  if [[ -n $setup ]]; then
    "$MYSQL" -u root --socket="$sock" -e "$setup" >/dev/null 2>&1
  fi
  out=$("$MYSQL" -u root --socket="$sock" "$DB" -N --batch < "$qfile" 2>&1)
  if [[ $? -ne 0 ]]; then
    echo "ERROR:$(echo "$out" | head -1)"
    return
  fi
  echo "$out" | sort | md5sum | cut -d' ' -f1
}

pass=0; fail=0; err=0
printf "%-5s %-34s %-34s %s\n" "Q" "target" "reference" "verdict"
for q in $QUERIES; do
  qf="$ROOT/bench/queries/q${q}.sql"
  [[ -f $qf ]] || { echo "missing $qf" >&2; continue; }
  t=$(run_one "$TARGET_SOCK" "$TARGET_SET" "$qf")
  r=$(run_one "$REF_SOCK" "$REF_SET" "$qf")
  if [[ $t == ERROR:* || $r == ERROR:* ]]; then
    v="ERR"; err=$((err+1))
  elif [[ $t == "$r" ]]; then
    v="OK"; pass=$((pass+1))
  else
    v="MISMATCH"; fail=$((fail+1))
  fi
  printf "%-5s %-34s %-34s %s\n" "q$q" "${t:0:32}" "${r:0:32}" "$v"
done
echo "----"
echo "OK=$pass MISMATCH=$fail ERR=$err"
[[ $fail -eq 0 && $err -eq 0 ]]
