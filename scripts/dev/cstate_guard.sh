#!/bin/bash
# Sourceable guard that WARNS (does not fail) when the CPU is not pinned for
# low-latency benchmarking. A deep C-state left enabled silently throttles
# latency-bound runs (single-terminal OLTP especially): on XG6326-2U, C6's
# 170us exit latency cut TPC-C T1 by ~3.7x (autogen 390->104) until disabled.
# Pin with:
#   sudo cpupower frequency-set -g performance
#   sudo cpupower idle-set -D 50      # disable C-states with exit latency >=50us
# Revert: sudo cpupower idle-set -E
#
# Usage:  source scripts/dev/cstate_guard.sh; cstate_guard
cstate_guard() {
  local gov warned=0
  gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null)
  if [ "$gov" != "performance" ]; then
    echo "  [cstate_guard] WARNING: scaling_governor=$gov (not 'performance')." >&2
    echo "                 -> sudo cpupower frequency-set -g performance" >&2
    warned=1
  fi
  local s lat dis nm
  for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
    [ -d "$s" ] || continue
    lat=$(cat "$s/latency" 2>/dev/null); dis=$(cat "$s/disable" 2>/dev/null)
    nm=$(cat "$s/name" 2>/dev/null)
    if [ "${lat:-0}" -ge 100 ] && [ "${dis:-0}" != "1" ]; then
      echo "  [cstate_guard] WARNING: deep C-state $nm (exit ${lat}us) ENABLED" \
           "— latency-bound throughput will be throttled." >&2
      echo "                 -> sudo cpupower idle-set -D 50" >&2
      warned=1
    fi
  done
  if [ "$warned" = "0" ]; then
    echo "  [cstate_guard] OK: performance governor, deep C-states disabled."
  fi
  return 0
}
