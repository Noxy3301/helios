#!/bin/bash
# FixedTpcc / fixed_tpch hook for a process under callgrind:
#   cg_hook.sh <pid> begin <label>   zero the counters (dump and drop what came before)
#   cg_hook.sh <pid> end <label>     dump the block's counters under <label>
pid=$1 phase=$2 label=$3
case "$phase" in
  begin) callgrind_control -i on "$pid" >/dev/null; callgrind_control -z "$pid" >/dev/null ;;
  end) callgrind_control -d "$label" "$pid" >/dev/null ;;
esac
