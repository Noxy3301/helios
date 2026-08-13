#!/bin/bash

# Stop build/server/lineairdb-server. SIGTERM first: with
# HELIOS_RPC_TIMING=1 this lets the server log its per-opcode RPC timing
# before exiting; without it, SIGTERM just terminates the process as
# before. Any PID still alive after the grace period gets -9.
PIDS=$(ps aux | grep "build/server/lineairdb-server" | grep -v grep | awk '{print $2}')

if [ -z "$PIDS" ]; then
  exit 0
fi

echo "$PIDS" | xargs -r kill -TERM

for _ in $(seq 1 50); do
  REMAINING=$(ps aux | grep "build/server/lineairdb-server" | grep -v grep | awk '{print $2}')
  [ -z "$REMAINING" ] && exit 0
  sleep 0.1
done

REMAINING=$(ps aux | grep "build/server/lineairdb-server" | grep -v grep | awk '{print $2}')
[ -n "$REMAINING" ] && echo "$REMAINING" | xargs -r kill -9
exit 0
