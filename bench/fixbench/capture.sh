#!/bin/bash
# Record a new fixed sample: load TPC-C SF 1, run BenchBase with one transaction
# type at 1 terminal while the MySQL general log is on, and turn the log into
# samples/tpcc_sf1_<name>.json.gz (the first MAXN transactions, default 300).
#
#   capture.sh <type-index 0..4> <name> <seconds>
#   (0 NewOrder, 1 Payment, 2 OrderStatus, 3 Delivery, 4 StockLevel)
#
# The SQL is BenchBase's own, so the sample changes only when BenchBase or
# bench/config/tpcc.xml changes. JDBC sends client-side prepared statements as
# plain text, so the log holds the literal statements.
set -u
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$D/../.." && pwd)"
WORK=${FIXBENCH_WORK:-$ROOT/bench/results/fixbench}; mkdir -p "$WORK"
idx=${1:?}; name=${2:?}; secs=${3:?}
w=(0 0 0 0 0); w[$idx]=100; weights=$(IFS=,; echo "${w[*]}")
sed -e "s#<time>[0-9]*</time>#<time>$secs</time>#" -e "s#<weights>[0-9,]*</weights>#<weights>$weights</weights>#" \
  "$ROOT/bench/config/tpcc.xml" > "$WORK/cap_$name.xml"
bash "$D/stack.sh" wipe
bash "$D/stack.sh" start native >/dev/null || exit 1
python3 "$D/load.py" tpcc >/dev/null 2>&1 || { echo "load failed" >&2; exit 1; }
M="$ROOT/build/runtime_output_directory/mysql -uroot -S /tmp/mysql.sock"
rm -f "$WORK/glog_$name.log"
$M -e "SET GLOBAL general_log_file='$WORK/glog_$name.log'; SET GLOBAL general_log=1"
(cd "$ROOT/bench/benchbase-mysql" && java -jar benchbase.jar -b tpcc -c "$WORK/cap_$name.xml" --execute=true \
  > "$WORK/cap_$name.out" 2>&1)
$M -e "SET GLOBAL general_log=0"
bash "$D/stack.sh" stop
python3 "$D/parse_glog.py" "$WORK/glog_$name.log" "$WORK/$name.json" --max "${MAXN:-300}"
gzip -9 -c "$WORK/$name.json" > "$D/samples/tpcc_sf1_$name.json.gz"
