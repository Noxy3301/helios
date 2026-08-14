#ifndef HELIOS_RPC_BUDGET_HH
#define HELIOS_RPC_BUDGET_HH

#include <cstddef>
#include <cstdint>

// Frozen fast-path caps: exceeding either aborts the execution in place and
// the caller re-dispatches the same request to the general helper pool,
// which runs it unbounded (see Reactor::RpcDispatcher::handle_fast_rpc).
constexpr uint64_t kFastReadPlanRowBudget = 4096;        // rows returned by storage scans
constexpr uint64_t kFastReadPlanByteBudget = 64 * 1024;  // accumulated response bytes
// Conservative per-element wire framing charged on top of raw string bytes
// for every repeated-field entry a row or group contributes.
constexpr size_t kBudgetFieldOverhead = 8;

/** @brief Per-execution row/byte budget for a fast-path read plan. */
struct RpcExecutionBudget {
  uint64_t rows_remaining;
  uint64_t bytes_remaining;
  bool exceeded = false;

  // Counts one row returned by a storage scan or point read, charged before
  // the row joins any step result. The storage layer filters tombstones and
  // dead index members before returning and its row limit counts live rows
  // only, so traversal over filtered entries is bounded by neither this
  // budget nor the capped limit.
  bool charge_row() {
    if (exceeded) return false;
    if (rows_remaining == 0) {
      exceeded = true;
      return false;
    }
    rows_remaining--;
    return true;
  }

  // Charges `n` bytes before they are appended to a step result: every key,
  // value, group boundary, tid, and per-field overhead the response will
  // carry, so a within-budget execution can never assemble a response far
  // beyond the byte budget.
  bool charge_bytes(size_t n) {
    if (exceeded) return false;
    if (n > bytes_remaining) {
      exceeded = true;
      return false;
    }
    bytes_remaining -= n;
    return true;
  }
};

namespace rpc_budget_detail {
inline thread_local RpcExecutionBudget *tl_current_budget = nullptr;
}  // namespace rpc_budget_detail

// Thread-local current budget; null = unbounded (legacy/helper paths).
inline RpcExecutionBudget *current_rpc_budget() {
  return rpc_budget_detail::tl_current_budget;
}
inline void set_current_rpc_budget(RpcExecutionBudget *b) {
  rpc_budget_detail::tl_current_budget = b;
}

#endif  // HELIOS_RPC_BUDGET_HH
