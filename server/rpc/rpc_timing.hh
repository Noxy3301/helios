#ifndef HELIOS_RPC_RPC_TIMING_HH
#define HELIOS_RPC_RPC_TIMING_HH

// Opt-in per-opcode RPC handler execution-time distributions, gated by env
// HELIOS_RPC_TIMING=1. When unset, the wrapper work (clock reads, request
// parsing, classification, table writes) degrades to a single cached-bool
// branch around fn(); the two handler-reported facts below are the one
// exception and are accumulated even when timing is disabled. This is the
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
//
// TX_VALIDATE_AND_COMMIT and TX_EXECUTE_READ_PLAN additionally get a variant
// breakdown (fence/entries/frame-size/response-size/stats-size for commit;
// steps/scan_limit/inspected-rows/response-size for read plan), because
// opcode-level aggregates dilute cheap and expensive request shapes
// together. The variant key is derived by parsing the request (and reading
// the already-populated response) once per call, only under
// HELIOS_RPC_TIMING=1, so the extra parse is opt-in overhead, never a
// hot-path cost.
//
// Two per-call facts can't be read back from the request/response bytes
// alone, so the handlers report them explicitly via a single-shot
// thread_local write, read by record_variant() right after fn() returns
// (same thread, strict happens-before via the handler's own return). The
// handlers accumulate these unconditionally (relaxed atomic adds during
// scans/probes, one ByteSizeLong pass per commit):
//   - TX_EXECUTE_READ_PLAN's inspected/probed row count (note_inspected_rows)
//   - TX_VALIDATE_AND_COMMIT's table-stats-snapshot response sub-size
//     (note_commit_stats_bytes)

#include <cstdint>
#include <ctime>
#include <string>

#include "../protocol/message.hh"

namespace rpc_timing {

// True iff HELIOS_RPC_TIMING is set to exactly "1".
bool enabled();

// Records one handler execution's elapsed time (microseconds) under `type`.
void record(MessageType type, uint64_t elapsed_us);

// Same, but additionally classifies `message` (request bytes) / `result`
// (response bytes, already filled by the handler) into a variant bin for
// TX_VALIDATE_AND_COMMIT / TX_EXECUTE_READ_PLAN. Falls back to the
// opcode-level record() above for any other opcode.
void record_variant(MessageType type, uint64_t elapsed_us, const std::string &message,
                     const std::string &result);

// TX_EXECUTE_READ_PLAN: report this call's total inspected/probed row count
// (server/rpc boundary: rows pulled from scan results across all steps and
// branches, serial and parallel-worker, before row_passes/semijoin
// filtering). Called once per call, typically via RAII at handler scope exit
// so every return path reports. A no-op call (0) is fine when unused.
void note_inspected_rows(uint64_t rows);

// TX_VALIDATE_AND_COMMIT: report this call's table-stats-snapshot response
// sub-size in bytes (the repeated TableRowCount field's serialized weight),
// separate from the response's total size.
void note_commit_stats_bytes(uint64_t bytes);

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

// Overload used at the two variant-tracked call sites: `message` is the raw
// request frame, `result` is the caller's response out-param (read after
// fn() runs, once the handler has filled it in).
template <typename Fn>
inline void time_call(MessageType type, const std::string &message, std::string &result,
                       Fn &&fn) {
  if (!enabled()) {
    fn();
    return;
  }
  // Defensive reset: record_variant() reads these after fn() returns, but
  // resetting first means a call site that (incorrectly) never reports one
  // can't inherit a stale value from an earlier call on this thread.
  note_inspected_rows(0);
  note_commit_stats_bytes(0);
  struct timespec t0{};
  struct timespec t1{};
  clock_gettime(CLOCK_MONOTONIC, &t0);
  fn();
  clock_gettime(CLOCK_MONOTONIC, &t1);
  int64_t sec_diff = static_cast<int64_t>(t1.tv_sec) - static_cast<int64_t>(t0.tv_sec);
  int64_t nsec_diff = static_cast<int64_t>(t1.tv_nsec) - static_cast<int64_t>(t0.tv_nsec);
  if (nsec_diff < 0) {
    sec_diff -= 1;
    nsec_diff += 1000000000LL;
  }
  uint64_t elapsed_us = static_cast<uint64_t>(sec_diff) * 1000000ULL +
                        static_cast<uint64_t>(nsec_diff) / 1000ULL;
  record_variant(type, elapsed_us, message, result);
}

// No-op unless timing is enabled. Blocks SIGTERM/SIGINT on the calling
// thread (inherited by every thread spawned afterwards) and starts a
// detached sigwait() thread that dumps the accumulated per-opcode timing
// (file + log) and exits the process on receipt.
void install_shutdown_handler();

}  // namespace rpc_timing

#endif  // HELIOS_RPC_RPC_TIMING_HH
