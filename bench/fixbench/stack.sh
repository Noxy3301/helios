#!/bin/bash
# Start and stop the Helios stack for fixed-sample measurements.
#
#   stack.sh wipe                 stop everything and delete helios_data and build/data
#   stack.sh start [native|callgrind]
#                                 start the storage server (native, or the profiling
#                                 build under callgrind) and then mysqld
#   stack.sh stop
#
# Environment:
#   MYSQLD_MODE=native|callgrind  run mysqld under callgrind as well (default native)
#   CG_OUT, CG_MOUT               callgrind output prefixes for storage and mysqld
#   PROF_STORAGE                  storage binary used under callgrind
#                                 (default build-prof/server/helios-storage; see README)
#
# Callgrind runs with instrumentation off; the caller switches it on around the
# measured part with callgrind_control. The storage pid is in /tmp/helios_storage.pid
# and a callgrind mysqld's pid in /tmp/mysqld_cg.pid.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK=${FIXBENCH_WORK:-$ROOT/bench/results/fixbench}
PROF_STORAGE=${PROF_STORAGE:-$ROOT/build-prof/server/helios-storage}
BIN=$ROOT/build/runtime_output_directory
export USER=${USER:-root} LOGNAME=${LOGNAME:-${USER:-root}}
if [ "$(id -u)" = 0 ]; then export MYSQLD_EXTRA_ARGS="${MYSQLD_EXTRA_ARGS:-} --user=root"; fi

stop() {
  $BIN/mysqladmin -uroot -S /tmp/mysql.sock shutdown >/dev/null 2>&1
  for _ in $(seq 1 60); do pgrep -x mysqld >/dev/null || break; sleep 0.5; done
  # scripts/stop_server.sh also stops the storage server with SIGKILL.
  for p in $(cat /tmp/helios_storage.pid 2>/dev/null) $(pgrep -x helios-storage) \
           $(pgrep -f "^valgrind.*helios-storage"); do kill -9 "$p" 2>/dev/null; done
  for p in $(cat /tmp/mysqld_cg.pid 2>/dev/null) $(pgrep -x mysqld); do kill -9 "$p" 2>/dev/null; done
  for _ in $(seq 1 40); do pgrep -x helios-storage >/dev/null || break; sleep 0.25; done
  rm -f /tmp/helios_storage.pid /tmp/mysql.pid /tmp/mysqld_cg.pid
}

wait_port() {
  for _ in $(seq 1 1200); do (echo > /dev/tcp/127.0.0.1/"$1") 2>/dev/null && return 0; sleep 0.5; done
  return 1
}

case "${1:-}" in
  wipe)
    stop
    rm -rf "$ROOT/helios_data" "$ROOT/build/data"
    ;;
  start)
    mkdir -p "$ROOT/helios_data/logs" "$WORK"
    cd "$ROOT" || exit 1
    if [ "${2:-native}" = native ]; then
      bash scripts/start_server.sh >/dev/null
    else
      OUT=${CG_OUT:-$WORK/cg/storage}; mkdir -p "$(dirname "$OUT")"
      nohup valgrind --tool=callgrind --instr-atstart=no --separate-threads=yes \
        --callgrind-out-file="$OUT.%p" "$PROF_STORAGE" --config "$ROOT/helios.cnf" \
        >> "$ROOT/helios_data/logs/helios.log" 2>&1 &
      echo $! > /tmp/helios_storage.pid
    fi
    wait_port 9999 || { echo "storage did not start" >&2; exit 1; }
    if [ "${MYSQLD_MODE:-native}" = native ]; then
      bash scripts/start_mysql.sh >/dev/null 2>&1 || { echo "mysqld did not start" >&2; exit 1; }
    else
      # start_mysql.sh initializes the data directory and installs the plugins.
      if [ ! -f "$ROOT/build/data/ibdata1" ]; then
        bash scripts/start_mysql.sh >/dev/null 2>&1 || { echo "mysqld init failed" >&2; exit 1; }
        $BIN/mysqladmin -uroot -S /tmp/mysql.sock shutdown
        for _ in $(seq 1 60); do pgrep -x mysqld >/dev/null || break; sleep 0.5; done
      fi
      cd "$ROOT/build" || exit 1
      MOUT=${CG_MOUT:-$WORK/cg/mysqld}; mkdir -p "$(dirname "$MOUT")"
      # Same arguments as the last step of scripts/start_mysql.sh.
      nohup valgrind --tool=callgrind --instr-atstart=no --separate-threads=yes \
        --callgrind-out-file="$MOUT.%p" ./runtime_output_directory/mysqld --datadir=./data \
        --socket=/tmp/mysql.sock --port=3307 --pid-file=/tmp/mysql.pid --default-storage-engine=helios \
        --max-connections=16384 --open-files-limit=65535 --table-open-cache=8192 --skip-name-resolve \
        --disable-log-bin ${MYSQLD_EXTRA_ARGS:-} >> "$ROOT/helios_data/logs/mysqld_cg.log" 2>&1 &
      echo $! > /tmp/mysqld_cg.pid
      for _ in $(seq 1 1200); do $BIN/mysqladmin ping -uroot -S /tmp/mysql.sock >/dev/null 2>&1 && break; sleep 0.5; done
      $BIN/mysql -uroot -S /tmp/mysql.sock \
        -e "SET GLOBAL helios_server_host='127.0.0.1'; SET GLOBAL helios_server_port=9999;"
    fi
    # bench/bin/benchrun.py's default read path.
    $BIN/mysql -uroot -S /tmp/mysql.sock -e "SET GLOBAL helios_read_path='plan'"
    ;;
  stop) stop ;;
  *) echo "usage: stack.sh wipe | start [native|callgrind] | stop" >&2; exit 2 ;;
esac
