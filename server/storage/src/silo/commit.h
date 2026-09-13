/**
 * @file server/storage/src/silo/commit.h
 * The entry point of the commit protocol, and the entries a caller submits
 * as its read evidence and its writes.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_COMMIT_H
#define HELIOS_STORAGE_SRC_SILO_COMMIT_H

#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "pax/table.h"
#include "storage/commit.h"
#include "storage/config.h"
#include "storage/read.h"

namespace helios::storage {

class TableDictionary;
namespace epoch {
class Framework;
}  // namespace epoch

namespace wal {
class Logger;
}

namespace index {
class Reaper;
}

namespace silo {

/**
 * @brief One row change, decoded before entering the commit protocol.
 * @details Names and untyped field bytes borrow the caller's request.
 * Deletes use only the table name, key and operation.
 */
struct Write {
  std::string_view table_name;
  std::string_view key;
  pax::Row value;
  RowOp op = RowOp::kUpdate;
};

/**
 * @brief One transaction's request: what it observed and what it wants
 * installed. A call-site view; it does not own the vectors.
 */
struct CommitPayload {
  const std::vector<ExternalReadEntry> &reads;
  const std::vector<Write> &writes;
  const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops;
  const std::vector<ExternalRangeReadEntry> &range_reads;
};

/**
 * @brief Runs the Silo commit protocol for a transaction whose read and
 * write sets were assembled by the caller through the read API.
 *
 * @details
 * Writes already contain PAX cell values; this protocol does not decode
 * input bytes. Nothing in the entries points into storage
 * memory:
 *   - reads:       (key, observed TID, found)
 *   - writes:      (key, value | delete)
 *   - SI ops:      (secondary key, primary key, add | remove)
 *   - range reads: scan bounds plus the returned key list
 *
 * The protocol is Silo's commit protocol (paper §4.4), with a join at the
 * start, a leave and re-join at 1.2, and a leave after enqueue; [added]
 * marks steps beyond the paper, for by-key inputs and SQL insert / UNIQUE
 * semantics:
 *
 *   Resolve   R1  [added] map every key to its DataItem
 *             R2  [added] materialize absent slots for fresh write keys
 *             R3  [added] reject in-request UNIQUE duplicates
 *   Phase 1   1.1 [paper] lock the write set in address order
 *             1.2 [paper] re-read the global epoch (serialization point)
 *   Phase 2   2.1 [paper] exact reads: observed presence and TIDs unchanged
 *             2.2 [added] ranges: replay the scans, compare the key lists
 *                         in scan order and by count (row TIDs are
 *                         validated at 2.1)
 *             2.3 [added] inserts: the claimed key still holds no row
 *             2.4 [added] UNIQUE recheck after the lock wait
 *   Phase 3   3.0 [added] reserve PAX slots for all row writes
 *             3.1 [paper] write values; deletes become tombstones
 *             3.2 [paper] log entries before unlock (when logging)
 *             3.3 [paper] publish even TIDs stamped with the 1.2 epoch
 *             3.4 [added] hand slots left empty to the reaper for
 *                         deferred physical purge
 *             3.5 [paper] enqueue the log set, leave the epoch
 *
 * @note Read validation is logical: Phase 2 re-reads every key and
 * replays every scan, then requires the observed TIDs and the result
 * key lists to be unchanged. Silo instead guards ranges with Masstree
 * node versions (physical validation), but a node version is bound to a
 * node pointer, and this server keeps no per-transaction state that could
 * pin such a pointer across the RPC boundary, so its lifetime cannot be
 * guaranteed.
 *
 * @param durability Whether this commit's acknowledgement waits for its epoch
 * to reach the device. A logger that writes no records ignores it: step 3.5
 * has nothing to wait for.
 * @param[out] abort_reason When the attempt aborts,
 * receives a short label naming the failed check, such as
 * `exact_read_tid_moved`, `primary_range_result_changed`,
 * `duplicate_primary_key`, or `unique_si_exists_after_lock`.
 * @return true when the transaction committed; false on abort, after
 * every lock this attempt acquired has been released.
 */
bool Commit(TableDictionary &tables, std::shared_mutex &schema_mutex,
            epoch::Framework &epoch_framework, index::Reaper &reaper,
            wal::Logger &logger, const CommitPayload &payload,
            CommitDurability durability, std::string &abort_reason);

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_COMMIT_H
