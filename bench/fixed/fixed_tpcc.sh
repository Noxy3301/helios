#!/bin/bash
# Fixed-sample TPC-C: every run loads TPC-C SF 1 afresh (BenchBase's loader is
# deterministic per unit), then replays the same seeded transaction sequence
# on the same server processes, so a run does the same work as the last one.
#
# The load is repeated rather than restored from a copy of the data dirs: the
# storage server keeps its per-table row counts in memory only and does not
# rebuild them on recovery, so after a restart the optimizer sees 2-row tables
# and StockLevel turns into a hash join over all of stock.
#
#   fixed_tpcc.sh template <dir>                 initialize a MySQL data dir with the plugin installed
#   fixed_tpcc.sh run <dir> <out.json> [opts]    load, replay, stop
#
# run options:
#   --plan <spec>       blocks, e.g. mix:300,NewOrder:300,... (FixedTpcc --plan)
#   --seed <n>
#   --valgrind <what>   storage|mysqld: run that process under callgrind with
#                       instrumentation off until the replay; one dump per
#                       block lands next to <out.json>
#   --rpc-trace         start mysqld with helios_rpc_trace=ON
#   --durability <m>    sync|async for the replay (default: sync)
#   --url <jdbc url>    the driver's JDBC URL (default: tpcc.xml's)
#   --pin               pin storage to cpu 0, mysqld to cpu 1-2, driver to cpu 3
#   --perf <what>       storage|mysqld: perf record -F 999 -g on that process
#
# environment: STORAGE_BIN (storage binary), PLUGIN_DIR (mysqld --plugin-dir),
# both from build_version.sh to run another commit.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD="$ROOT/build"
BIN="$BUILD/runtime_output_directory"
FIXED="$ROOT/bench/fixed"
# BenchBase builds for Java 23; JAVA_HOME names it when java on PATH is older.
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
CP="$FIXED/classes:$ROOT/bench/benchbase-mysql/benchbase.jar:$ROOT/bench/benchbase-mysql/lib/*"
SOCK=/tmp/mysql.sock
JEMALLOC=/lib/x86_64-linux-gnu/libjemalloc.so.2
MYSQL=("$BIN/mysql" -u root --socket="$SOCK")

stop_all() {
  if [ -S "$SOCK" ]; then
    "$BIN/mysqladmin" -u root --socket="$SOCK" shutdown >/dev/null 2>&1 || true
  fi
  for _ in $(seq 1 120); do
    pgrep -f "runtime_output_directory/mysqld" >/dev/null || break
    sleep 1
  done
  pkill -9 -f "runtime_output_directory/mysqld" 2>/dev/null || true
  # SIGINT lets callgrind write its last dump; the storage server's own
  # shutdown path is not exercised by these runs.
  pkill -INT -f "helios-storage --config" 2>/dev/null || true
  for _ in $(seq 1 60); do
    pgrep -f "helios-storage --config" >/dev/null || break
    sleep 1
  done
  pkill -9 -f "helios-storage --config" 2>/dev/null || true
  rm -f "$SOCK" /tmp/mysql.pid /tmp/helios_storage.pid
}

# wait_port <port> <pgrep pattern of the process that should open it>
wait_port() {
  for _ in $(seq 1 900); do
    (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null && return 0
    if ! pgrep -f "$2" >/dev/null; then
      echo "the process for port $1 exited" >&2
      return 1
    fi
    sleep 1
  done
  echo "port $1 did not open" >&2
  return 1
}

# start_storage <durability> <valgrind:0|1> <cpus|"">
start_storage() {
  local dur=$1 vg=$2 cpus=$3 out=${4:-}
  mkdir -p "$ROOT/helios_data/logs"
  local conf="$ROOT/helios_data/fixed.cnf"
  printf '[helios-storage]\ncommit_durability = %s\n' "$dur" > "$conf"
  local cmd=("${STORAGE_BIN:-$ROOT/build/server/helios-storage}" --config "$ROOT/helios.cnf" --config "$conf")
  if [ "$vg" = 1 ]; then
    cmd=(valgrind --tool=callgrind --separate-threads=no --dump-instr=no --instr-atstart=no
         --callgrind-out-file="$out.storage.cg" "${cmd[@]}")
  fi
  [ -n "$cpus" ] && cmd=(taskset -c "$cpus" "${cmd[@]}")
  (cd "$ROOT" && { ulimit -n 65535 2>/dev/null || ulimit -n "$(ulimit -Hn)"; } || true; LD_PRELOAD="$JEMALLOC" exec setsid "${cmd[@]}" \
      >> "$ROOT/helios_data/logs/helios.log" 2>&1 < /dev/null) &
  sleep 1
  wait_port 9999 "helios-storage --config"
}

# start_mysqld <valgrind:0|1> <cpus|""> <extra args...>
start_mysqld() {
  local vg=$1 cpus=$2 out=$3
  shift 3
  local cmd=("$BIN/mysqld" --datadir="$BUILD/data" --socket="$SOCK" --port=3307
             --pid-file=/tmp/mysql.pid --default-storage-engine=helios
             --max-connections=16384 --open-files-limit=65535 --table-open-cache=8192
             --skip-name-resolve --disable-log-bin --user=root
             ${PLUGIN_DIR:+--plugin-dir="$PLUGIN_DIR"} "$@")
  if [ "$vg" = 1 ]; then
    cmd=(valgrind --tool=callgrind --separate-threads=no --dump-instr=no --instr-atstart=no
         --callgrind-out-file="$out.mysqld.cg" "${cmd[@]}")
  fi
  [ -n "$cpus" ] && cmd=(taskset -c "$cpus" "${cmd[@]}")
  mkdir -p "$ROOT/helios_data/logs"
  (cd "$BUILD" && LD_PRELOAD="$JEMALLOC" exec setsid "${cmd[@]}" \
      >> "$ROOT/helios_data/logs/mysqld_3307.log" 2>&1 < /dev/null) &
  sleep 1
  wait_port 3307 "runtime_output_directory/mysqld"
  for _ in $(seq 1 600); do
    "${MYSQL[@]}" -e "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

storage_pid() { pgrep -f "helios-storage --config" | head -1; }
mysqld_pid() { pgrep -f "runtime_output_directory/mysqld" | head -1; }

cmd=${1:?usage}
dir=${2:?usage}
shift 2

case "$cmd" in
template)
  stop_all
  rm -rf "$ROOT/helios_data" "$BUILD/data"
  (cd "$ROOT" && LD_PRELOAD="" ./scripts/start_server.sh) >/dev/null
  sleep 1
  wait_port 9999 "build/server/helios-storage"
  (cd "$ROOT" && USER=root MYSQLD_EXTRA_ARGS=--user=root ./scripts/start_mysql.sh) >/dev/null
  stop_all
  rm -rf "$dir"
  mkdir -p "$dir"
  cp -a "$BUILD/data" "$dir/mysql_data"
  echo "template in $dir"
  ;;
run)
  out=${1:?out.json}
  shift
  plan="mix:300,NewOrder:300,Payment:300,OrderStatus:300,Delivery:60,StockLevel:300"
  seed=20261102
  vg=""
  perf=""
  trace=0
  dur=sync
  pin=0
  url=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --plan) plan=$2; shift 2 ;;
      --seed) seed=$2; shift 2 ;;
      --valgrind) vg=$2; shift 2 ;;
      --perf) perf=$2; shift 2 ;;
      --rpc-trace) trace=1; shift ;;
      --durability) dur=$2; shift 2 ;;
      --pin) pin=1; shift ;;
      --url) url=$2; shift 2 ;;
      *) echo "unknown $1" >&2; exit 1 ;;
    esac
  done
  out=$(realpath -m "$out")
  mkdir -p "$(dirname "$out")"
  stop_all
  rm -rf "$ROOT/helios_data" "$BUILD/data"
  cp -a "$dir/mysql_data" "$BUILD/data"
  scpu=""; mcpu=""; dcpu=""
  if [ "$pin" = 1 ]; then scpu=0; mcpu=1-2; dcpu=3; fi
  start_storage "$dur" "$([ "$vg" = storage ] && echo 1 || echo 0)" "$scpu" "$out"
  extra=()
  if [ "$trace" = 1 ]; then
    extra+=(--helios-rpc-trace=ON --helios-rpc-trace-path="$out.rpc.jsonl")
  fi
  start_mysqld "$([ "$vg" = mysqld ] && echo 1 || echo 0)" "$mcpu" "$out" "${extra[@]}"
  "${MYSQL[@]}" -e "SET GLOBAL helios_read_path='plan'"
  # Load under Async, as benchrun's --load-durability async does, then switch
  # to the replay's contract.
  "${MYSQL[@]}" -e "SET GLOBAL helios_commit_durability='async'"
  python3 "$ROOT/bench/bin/benchrun.py" tpcc --scalefactor 1 --no-exec --external-server \
      --keep-helios-data --analyze > "$out.load.log" 2>&1
  "${MYSQL[@]}" -e "SET GLOBAL helios_commit_durability='$dur'"
  spid=$(storage_pid)
  mpid=$(mysqld_pid)
  # The real process under valgrind is the valgrind launcher's pid itself.
  hook=""
  if [ -n "$vg" ]; then
    vpid=$([ "$vg" = storage ] && echo "$spid" || echo "$mpid")
    hook="$FIXED/cg_hook.sh $vpid"
  fi
  perfpid=""
  if [ -n "$perf" ]; then
    ppid=$([ "$perf" = storage ] && echo "$spid" || echo "$mpid")
    "${PERF:-$(ls /usr/lib/linux-tools/*/perf 2>/dev/null | head -1)}" record -F 999 -g -p "$ppid" -o "$out.$perf.perf.data" >/dev/null 2>&1 &
    perfpid=$!
    sleep 1
  fi
  driver=("$JAVA" -cp "$CP" com.oltpbenchmark.api.FixedTpcc --seed "$seed" --plan "$plan"
          --pid storage="$spid" --pid mysqld="$mpid" --out "$out")
  [ -n "$hook" ] && driver+=(--hook "$hook")
  [ -n "$url" ] && driver+=(--url "$url")
  [ "$pin" = 1 ] && driver=(taskset -c "$dcpu" "${driver[@]}")
  "${driver[@]}" 2> >(grep -v '^Picked up' >&2)
  [ -n "$perfpid" ] && { kill -INT "$perfpid"; wait "$perfpid" || true; }
  # A content digest of the final state, to check that two runs did the same work.
  "${MYSQL[@]}" benchbase -N -e "
    SELECT 'district', SUM(d_next_o_id), SUM(d_ytd) FROM district;
    SELECT 'orders', COUNT(*), SUM(o_c_id) FROM oorder;
    SELECT 'new_order', COUNT(*), SUM(no_o_id) FROM new_order;
    SELECT 'order_line', COUNT(*), SUM(ol_quantity) FROM order_line;
    SELECT 'customer', SUM(c_balance), SUM(c_payment_cnt), SUM(c_delivery_cnt) FROM customer;
    SELECT 'history', COUNT(*) FROM history;" > "$out.digest" 2>&1 || true
  stop_all
  ;;
tpch)
  # tpch <template> <outdir> [--sf 1] [--runs 3] [--valgrind storage|mysqld]
  # Loads TPC-H with the 23 postload indexes and the DuckDB secondary
  # engine, then runs the checked-in Q1-Q22 on both engines (fixed_tpch.py).
  outd=$(realpath -m "${1:?outdir}")
  shift
  sf=1; runs=3; vg=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --sf) sf=$2; shift 2 ;;
      --runs) runs=$2; shift 2 ;;
      --valgrind) vg=$2; shift 2 ;;
      *) echo "unknown $1" >&2; exit 1 ;;
    esac
  done
  mkdir -p "$outd"
  stop_all
  rm -rf "$ROOT/helios_data" "$BUILD/data"
  cp -a "$dir/mysql_data" "$BUILD/data"
  start_storage sync "$([ "$vg" = storage ] && echo 1 || echo 0)" "" "$outd/tpch"
  start_mysqld "$([ "$vg" = mysqld ] && echo 1 || echo 0)" "" "$outd/tpch"
  "${MYSQL[@]}" -e "SET GLOBAL helios_commit_durability='async'"
  # Peak and current memory of both processes after the setup, the
  # queries and each step of the setup the log marks.
  mem() {
    for p in "helios-storage --config" "runtime_output_directory/mysqld"; do
      local pid
      pid=$(pgrep -f "$p" | head -1)
      [ -n "$pid" ] && echo "$1 ${p%% *} $(grep -E 'VmHWM|VmRSS' /proc/"$pid"/status | tr -s ' \t\n' ' ')"
    done >> "$outd/memory.txt"
  }
  ( while sleep 5; do mem poll; done ) &
  mempoll=$!
  bash "$ROOT/bench/bin/tpch_setup_sf1.sh" --sf "$sf" --loader-threads 4 > "$outd/setup.log" 2>&1 || true
  mem after_setup
  "${MYSQL[@]}" -e "SET GLOBAL helios_commit_durability='sync'"
  grep -E "real|lineitem_rows" -A1 "$outd/setup.log" > "$outd/setup.summary" || true
  hook=""
  if [ -n "$vg" ]; then
    vpid=$([ "$vg" = storage ] && storage_pid || mysqld_pid)
    hook="$FIXED/cg_hook.sh $vpid"
  fi
  # TPCH_PRE: SQL run before each query in its session (for example
  # "SET SESSION secondary_engine_cost_threshold=0;" so that a small scale
  # factor sends every query to DuckDB, as SF 1 does).
  python3 "$FIXED/fixed_tpch.py" "$outd/duckdb.json" --runs "$runs" --engine duckdb \
      ${hook:+--hook "$hook"} --pre "${TPCH_PRE:-}" > "$outd/duckdb.txt" 2>&1
  mem after_duckdb
  if [ "$sf" = 0.1 ] || [ "${TPCH_PLAN:-0}" = 1 ]; then
    python3 "$FIXED/fixed_tpch.py" "$outd/plan.json" --runs "$runs" --engine plan \
        ${hook:+--hook "$hook"} > "$outd/plan.txt" 2>&1
  fi
  kill "$mempoll" 2>/dev/null || true
  stop_all
  ;;
*)
  echo "unknown command $cmd" >&2
  exit 1
  ;;
esac
