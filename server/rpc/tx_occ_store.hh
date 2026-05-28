// Per-transaction OCC state retained on the server across the 2 RPCs of a
// prefetch+commit (helios's disaggregated OCC). See the design doc
// `.note/session/2026-05-28_node_version_server_retain_plan.md` and the
// background `.note/reference/rcu_epoch_background.md`.
//
// Step 2 scope: storage scaffolding + connection-close hook. NOT populated
// by any caller yet (no-op). Filled in by Steps 3-6.
//
// Lifecycle:
//   TX_EXECUTE_READ_PLAN handler (Step 5) -> Insert(state)
//   TX_VALIDATE_AND_COMMIT handler  (Step 6) -> Take(key) and validate
//   LineairDBServer::handle_client end of loop -> ReleaseConnection(fd)
//   MasstreeAdvanceEpoch tick                  -> SweepExpired(now_ns) so
//     stalled tx (proxy crash, network split) cannot pin masstree reclaim
//     forever.
//
// Concurrency: one mutex protects the map. Acceptable because pin
// register/release runs at RPC boundaries (10s-100s/s), not per-row.
// Sharding can come later if profiling demands.

#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <lineairdb/stateless.h>  // ExternalRange/IndexValidationEntry

namespace LineairDB { class Database; }

namespace helios {

// Stable identifier for a logical transaction across the prefetch and
// commit RPCs. Carried on the wire in the read-plan response (Step 5)
// and echoed by the proxy in TX_VALIDATE_AND_COMMIT (Step 6).
using TxKey = std::uint64_t;

struct TxOccState {
  TxKey tx_key = 0;
  int connection_fd = -1;             // which client owns this state
  std::uint64_t pinned_epoch = 0;     // captured at prefetch
  std::uint64_t expires_at_ns = 0;    // monotonic; 0 = no TTL
  // Verbatim copies of what the existing LineairDB ValidateAndCommit path
  // takes today — passing these in unchanged at commit (Step 6) reuses the
  // physical-validation code at database_impl.h:1122 etc., so we are NOT
  // writing a new validator. The only architectural change is WHERE these
  // live (server-side TxOccState instead of the proxy's
  // range_validation_set_).
  std::vector<LineairDB::ExternalRangeValidationEntry> range_reads;
  std::vector<LineairDB::ExternalIndexValidationEntry> index_reads;  // incl tombstones
  // Filter-rejected rows captured during the prefetch. At commit they go
  // through ValidateAndCommit as stateless point reads (TID re-read). The
  // table_name is REQUIRED — Q9's filtered rows come from `part` while the
  // last range_read's table is `orders/nation/...`; relying on the latter
  // produces validation against the wrong table → false aborts.
  struct FilteredRow {
    std::string table_name;
    std::string key;
    std::uint64_t tid;
  };
  std::vector<FilteredRow> filtered_rows;

  // helios Phase-6 range-hash OCC. For each primary full-range scan in a
  // read-only txn, the server captures a 32-byte footprint digest at prefetch.
  // At commit it re-derives the digest over the same range and compares; a
  // mismatch (value update / insert / delete in the range) aborts. This
  // replaces the proxy's O(rows) per-row read set with O(1) per range.
  struct RangeHash {
    std::string table_name;
    std::string start_key;
    std::string end_key;
    std::uint64_t row_limit = 0;
    bool reverse_scan = false;
    std::uint8_t root[32] = {0};
  };
  std::vector<RangeHash> range_hashes;
};

class TxOccStore {
 public:
  // Register a new tx state. Overwrites if `key` already exists (defensive;
  // should not happen with monotonic TxKey assignment).
  void Insert(TxOccState state) {
    std::lock_guard<std::mutex> lg(mtx_);
    map_[state.tx_key] = std::move(state);
  }

  // Remove and return the state. Used by commit to perform validation and
  // by abort to release. Returns nullopt if not present (already expired or
  // already taken).
  std::optional<TxOccState> Take(TxKey key) {
    std::lock_guard<std::mutex> lg(mtx_);
    auto it = map_.find(key);
    if (it == map_.end()) return std::nullopt;
    TxOccState s = std::move(it->second);
    map_.erase(it);
    return s;
  }

  // Release everything owned by `fd`. Called from
  // LineairDBServer::handle_client after the recv loop exits (proxy
  // disconnect / crash). Returns the released TxKeys so the caller can
  // call `db->ReleaseTxEpochPin(key)` for each — keeping the OCC store
  // dependency-free of LineairDB internals.
  std::vector<TxKey> ReleaseConnection(int fd) {
    std::lock_guard<std::mutex> lg(mtx_);
    std::vector<TxKey> released;
    for (auto it = map_.begin(); it != map_.end();) {
      if (it->second.connection_fd == fd) {
        released.push_back(it->first);
        it = map_.erase(it);
      } else {
        ++it;
      }
    }
    return released;
  }

  // TTL sweep: erase any state whose expires_at_ns <= now_ns. Returns the
  // released TxKeys so the caller can release their masstree pins.
  // Intended to be invoked from the masstree epoch tick (~40 ms).
  std::vector<TxKey> SweepExpired(std::uint64_t now_ns) {
    std::lock_guard<std::mutex> lg(mtx_);
    std::vector<TxKey> released;
    for (auto it = map_.begin(); it != map_.end();) {
      if (it->second.expires_at_ns != 0 &&
          it->second.expires_at_ns <= now_ns) {
        released.push_back(it->first);
        it = map_.erase(it);
      } else {
        ++it;
      }
    }
    return released;
  }

  std::size_t Size() const {
    std::lock_guard<std::mutex> lg(mtx_);
    return map_.size();
  }

 private:
  mutable std::mutex mtx_;
  std::unordered_map<TxKey, TxOccState> map_;
};

// Process-global singleton. The store has no internal LineairDB
// dependencies, so a free function returning a static instance is
// sufficient. (Sharding can be added later if profiling demands it.)
inline TxOccStore& GlobalTxOccStore() {
  static TxOccStore inst;
  return inst;
}

}  // namespace helios
