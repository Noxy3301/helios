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

#include "lineairdb/commit.h"
#include "lineairdb/config.h"
#include "lineairdb/read.h"

#include "pax/table.h"

namespace helios::storage {

class TableDictionary;
struct Tidword;
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

// Maximum tid within one epoch; Tidword reserves 29 bits for it.
inline constexpr uint32_t kMaxTid = (1u << 29) - 1;

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
 *   - reads:       (key, observed TID word)
 *   - writes:      (key, value | delete)
 *   - SI ops:      (secondary key, primary key, add | remove)
 *   - range reads: scan bounds plus the returned key list
 *
 * The protocol locks the write set and reads the epoch, validates read
 * observations, then chooses one commit TID (Silo §4.2–4.4). Write constraints
 * and PAX slot allocation are completed before applying any values.
 * Each record is then updated, copied into the WAL, and unlocked by publishing
 * its TID. Empty records go to the reaper.
 *
 * Point reads are revalidated by key and TID. Ranges are rescanned to compare
 * their returned key lists; the caller also submits consumed rows as point
 * reads.
 *
 * @param last_commit_tid Last TID chosen by this worker for this database.
 * Updated when the transaction proceeds to write publication.
 * @param durability Whether acknowledgement waits for this commit's epoch
 * to become durable. Commits that enqueue no log records do not wait.
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
            Tidword &last_commit_tid, CommitDurability durability,
            std::string &abort_reason);

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_COMMIT_H
