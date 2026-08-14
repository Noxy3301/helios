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
// locking on the hot path) and registers that table in a small global
// registry on first use. The table is merged into a global, mutex-protected
// table when the thread exits (removing it from the registry too) and,
// right before the shutdown dump, a sweep additionally merges every table
// still in the registry -- i.e. threads that are still alive. This matters
// for HELIOS_TRANSPORT=reactor: reactor threads are long-lived (one thread
// serves many connections over the process lifetime) and never hit the
// exit-time merge, so without the sweep their fast-path opcodes would be
// invisible in the dump. SIGTERM/SIGINT are handled by a dedicated
// sigwait() thread (not a signal handler, so mutex/file/log calls are
// safe), which writes the accumulated per-opcode table to
// /tmp/helios_rpc_timing_<pid>.txt and logs the same table before exiting
// the process.
//
// Correctness note: the sweep reads a still-live thread's counters without
// synchronizing with its writes, so it is only safe once RPC traffic has
// quiesced. The measurement workflow always stops mysqld first -- which
// closes every RPC connection, so no handler thread is mid-update -- before
// signaling the server, so this is not a gap in practice. Concurrent thread
// exit is handled independently of quiescence: exit finalization and the
// sweep both hold the registry mutex for their entire decide-then-merge
// step (registry mutex acquired before the global mutex, consistently), and
// each table records whether the sweep already merged it, so a thread's
// samples land in the global table exactly once whether it exits before,
// during, or after the sweep runs.
//
// TX_VALIDATE_AND_COMMIT and TX_EXECUTE_READ_PLAN additionally get a variant
// breakdown (fence/entries/frame-size/response-size/stats-size for commit;
// steps/scan_limit/inspected-rows/response-size for read plan), because
// opcode-level aggregates dilute cheap and expensive request shapes
// together. The string path (legacy transport and the kSlow helper pool)
// derives the variant by parsing the request (and reading the
// already-populated response) once per call, only under
// HELIOS_RPC_TIMING=1, so the extra parse is opt-in overhead there. The
// reactor fast path instead supplies the request classify_rpc already
// parsed via time_call_parsed()/the typed record_variant() overloads below,
// so timing adds no parse on that path.
//
// Two per-call facts can't be read back from the request/response bytes
// alone, so the handlers report them explicitly via a single-shot
// thread_local write, read by record_variant() right after fn() returns
// (same thread, strict happens-before via the handler's own return). The
// handlers accumulate these unconditionally (relaxed atomic adds during
// scans/probes, one ByteSizeLong pass per commit):
//   - TX_EXECUTE_READ_PLAN's inspected/probed row count (note_inspected_rows)
//   - TX_VALIDATE_AND_COMMIT's touched-table stats response sub-size
//     (note_commit_stats_bytes), framing included.

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <string>

#include "../protocol/message.hh"

// Forward declarations only: the reactor fast-path call sites already have
// the full generated types in scope (they hold a parsed request); this
// header stays independent of lineairdb.pb.h. The nested `::Request` names
// used elsewhere in this codebase (TxValidateAndCommit::Request etc.) are
// typedefs for these same classes.
namespace LineairDB {
namespace Protocol {
class TxValidateAndCommit_Request;
class TxExecuteReadPlan_Request;
}  // namespace Protocol
}  // namespace LineairDB

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

// Typed overloads for the reactor fast path: same binning as the string
// overload above, but the request is already parsed, so no call here parses
// it again. `request_bytes` is the wire payload size (used by the commit
// variant key's frame-size bin; the read-plan overload ignores it, kept
// only so both call sites share one shape via time_call_parsed()).
void record_variant(MessageType type, uint64_t elapsed_us,
                     const LineairDB::Protocol::TxValidateAndCommit_Request &request,
                     size_t request_bytes, const std::string &result);
void record_variant(MessageType type, uint64_t elapsed_us,
                     const LineairDB::Protocol::TxExecuteReadPlan_Request &request,
                     size_t request_bytes, const std::string &result);

// TX_EXECUTE_READ_PLAN: report this call's total inspected/probed row count
// (server/rpc boundary: rows pulled from scan results across all steps and
// branches, serial and parallel-worker, before row_passes/semijoin
// filtering). Called once per call, typically via RAII at handler scope exit
// so every return path reports. A no-op call (0) is fine when unused.
void note_inspected_rows(uint64_t rows);

// TX_VALIDATE_AND_COMMIT: report this call's touched-table stats response
// sub-size in bytes (the repeated TableRowCount field's serialized weight,
// tag and length framing included), separate from the response's total size.
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

// Reactor fast-path overload: `req` is the request classify_rpc already
// parsed, so -- unlike the string time_call() overload above -- this adds no
// parse under HELIOS_RPC_TIMING=1. Mirrors the string overload's clock
// arithmetic and defensive note_* resets; records via the typed
// record_variant() overload for Req's type. `request_bytes` is the wire
// payload size; the commit variant key uses it, the read-plan one doesn't
// (kept for a uniform call shape across both call sites).
template <typename Req, typename Fn>
inline void time_call_parsed(MessageType type, const Req &req, size_t request_bytes,
                              std::string &result, Fn &&fn) {
  if (!enabled()) {
    fn();
    return;
  }
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
  record_variant(type, elapsed_us, req, request_bytes, result);
}

// No-op unless timing is enabled. Blocks SIGTERM/SIGINT on the calling
// thread (inherited by every thread spawned afterwards) and starts a
// detached sigwait() thread that dumps the accumulated per-opcode timing
// (file + log) and exits the process on receipt.
void install_shutdown_handler();

}  // namespace rpc_timing

#endif  // HELIOS_RPC_RPC_TIMING_HH
