#!/bin/bash
# Run per-query TPC-H wall-clock timing against a running mysqld.
# Method: single stream, one discarded warm run, N timed runs, and external
# mysql-client wall-clock timing.
# The md5 is over sorted output lines: it checks result multiset equality and
# deliberately ignores row order, which the spec leaves engine-defined among ties.
# Usage: tpch_wall.sh <label> [--runs 3] [--db benchbase] [--socket /tmp/mysql.sock]
#        [--queries bench/queries/tpch] [--out <dir>] [--timeout 120] [--only "1 6 18"]
#        [--sf 1]
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LABEL=${1:?usage: tpch_wall.sh <label> [options]}
shift

RUNS=3
DB=benchbase
SOCKET=/tmp/mysql.sock
QDIR="$ROOT/bench/queries/tpch"
OUT="$ROOT/bench/results/tpch_wall"
TIMEOUT=120
ONLY=""
SF=1

while [ $# -gt 0 ]; do
  case "$1" in
    --runs)
      RUNS=$2
      shift 2
      ;;
    --db)
      DB=$2
      shift 2
      ;;
    --socket)
      SOCKET=$2
      shift 2
      ;;
    --queries)
      QDIR=$2
      shift 2
      ;;
    --out)
      OUT=$2
      shift 2
      ;;
    --timeout)
      TIMEOUT=$2
      shift 2
      ;;
    --only)
      ONLY=$2
      shift 2
      ;;
    --sf)
      SF=$2
      shift 2
      ;;
    *)
      echo "unknown arg $1" >&2
      exit 1
      ;;
  esac
done

QLIST=${ONLY:-$(seq 1 22)}

# Refuse rather than auto-substitute the fraction: the measured input must stay a checked-in file.
case " $(echo $QLIST) " in *" 11 "*)
  if [ "$SF" != "1" ] && [ -f "$QDIR/q11.sql" ] && sed 's/--.*//' "$QDIR/q11.sql" | grep -E '(^|[^0-9])0\.0001([^0-9]|$)' >/dev/null; then
    echo "q11 in $QDIR is the SF=1 instantiation (FRACTION = 0.0001 / SF); pass an SF-adjusted --queries dir" >&2
    exit 1
  fi
esac

MYSQL="$ROOT/build/runtime_output_directory/mysql"
[ -x "$MYSQL" ] || MYSQL=$(command -v mysql)
mkdir -p "$OUT"

RESULT="$OUT/${LABEL}.tsv"
: > "$RESULT"
echo "# label=$LABEL runs=$RUNS db=$DB queries=$QDIR sf=$SF $(date -Is)" >> "$RESULT"

for q in $QLIST; do
  # q15 creates and drops a view; a killed run leaves it behind and every
  # later q15 fails at CREATE. Clean up defensively before each query.
  if ! "$MYSQL" -u root --socket="$SOCKET" "$DB" -e "DROP VIEW IF EXISTS revenue0;" \
      2>"$OUT/${LABEL}_q${q}.err"; then
    echo -e "q$q\tERR\tview-cleanup-failed: $(head -c 120 "$OUT/${LABEL}_q${q}.err" | tr '\t\n' '  ')" >> "$RESULT"
    continue
  fi
  SQL_FILE="$QDIR/q$q.sql"
  if [ ! -f "$SQL_FILE" ]; then
    echo "q$q MISSING" >> "$RESULT"
    continue
  fi

  timeout "$TIMEOUT" "$MYSQL" -u root --socket="$SOCKET" "$DB" -B -N \
    < "$SQL_FILE" > /dev/null 2>"$OUT/${LABEL}_q${q}.err" \
    || {
      echo -e "q$q\tERR\t$(head -c 120 "$OUT/${LABEL}_q${q}.err" | tr '\t\n' '  ')" >> "$RESULT"
      continue
    }

  TIMES=()
  for _ in $(seq 1 "$RUNS"); do
    T0=$(python3 -c 'import time; print(int(time.time()*1000))')
    timeout "$TIMEOUT" "$MYSQL" -u root --socket="$SOCKET" "$DB" -B -N \
      < "$SQL_FILE" > "$OUT/${LABEL}_q${q}.out" 2>/dev/null
    RC=$?
    T1=$(python3 -c 'import time; print(int(time.time()*1000))')
    [ $RC -ne 0 ] && {
      TIMES=()
      break
    }
    TIMES+=( $((T1 - T0)) )
  done

  if [ ${#TIMES[@]} -eq 0 ]; then
    echo -e "q$q\tERR\trun-failed" >> "$RESULT"
    continue
  fi

  if ! MD5=$(LC_ALL=C sort "$OUT/${LABEL}_q${q}.out" | md5sum | cut -d' ' -f1); then
    echo -e "q$q\tERR\thash-failed" >> "$RESULT"
    continue
  fi
  echo -e "q$q\t$(IFS=,; echo "${TIMES[*]}")\tmd5=$MD5" >> "$RESULT"
  echo "[$LABEL] q$q: ${TIMES[*]} ms"
done

echo "results -> $RESULT"
