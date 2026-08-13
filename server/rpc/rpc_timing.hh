#ifndef HELIOS_RPC_RPC_TIMING_HH
#define HELIOS_RPC_RPC_TIMING_HH

// Opt-in per-opcode RPC handler execution-time distributions, gated by env
// HELIOS_RPC_TIMING=1. Zero cost when unset: time_call() degrades to a
// single cached-bool branch around fn(), with no clock read. This is the
// sanctioned home for measurement code in this codebase; extend it rather
// than adding one-off timing hacks elsewhere.
//
// Each handler-executing thread accumulates into a thread_local table (no
// locking on the hot path), merged into a global mutex-protected table when
// the thread exits. SIGTERM/SIGINT are handled by a dedicated sigwait()
// thread (not a signal handler, so mutex/file/log calls are safe), which
// writes the accumulated per-opcode table to
// /tmp/helios_rpc_timing_<pid>.txt and logs the same table before exiting
// the process.
//
// Samples from threads still alive at dump time are not included (no
// cross-thread sweep of live thread_locals): connections must close before
// shutdown for complete data.

#include <cstdint>
#include <ctime>

#include "../protocol/message.hh"

namespace rpc_timing {

// True iff HELIOS_RPC_TIMING is set to exactly "1".
bool enabled();

// Records one handler execution's elapsed time (microseconds) under `type`.
void record(MessageType type, uint64_t elapsed_us);

// Times fn() with CLOCK_MONOTONIC and records it under `type` when timing is
// enabled; otherwise just calls fn(). Excludes whatever the caller does
// outside fn() (e.g. socket recv/send).
template <typename Fn>
inline void time_call(MessageType type, Fn &&fn) {
  if (!enabled()) {
    fn();
    return;
  }
  struct timespec t0{};
  struct timespec t1{};
  clock_gettime(CLOCK_MONOTONIC, &t0);
  fn();
  clock_gettime(CLOCK_MONOTONIC, &t1);
  // Borrow across the second boundary in signed space before widening to
  // uint64_t: a negative tv_nsec delta cast straight to unsigned wraps to a
  // ~2^64 garbage value.
  int64_t sec_diff = static_cast<int64_t>(t1.tv_sec) - static_cast<int64_t>(t0.tv_sec);
  int64_t nsec_diff = static_cast<int64_t>(t1.tv_nsec) - static_cast<int64_t>(t0.tv_nsec);
  if (nsec_diff < 0) {
    sec_diff -= 1;
    nsec_diff += 1000000000LL;
  }
  uint64_t elapsed_us = static_cast<uint64_t>(sec_diff) * 1000000ULL +
                        static_cast<uint64_t>(nsec_diff) / 1000ULL;
  record(type, elapsed_us);
}

// No-op unless timing is enabled. Blocks SIGTERM/SIGINT on the calling
// thread (inherited by every thread spawned afterwards) and starts a
// detached sigwait() thread that dumps the accumulated per-opcode timing
// (file + log) and exits the process on receipt.
void install_shutdown_handler();

}  // namespace rpc_timing

#endif  // HELIOS_RPC_RPC_TIMING_HH
