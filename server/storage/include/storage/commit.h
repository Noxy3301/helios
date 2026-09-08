/**
 * @file server/storage/include/storage/commit.h
 * What a commit is assembled from: the evidence a transaction observed, the
 * writes it installs, and when it is acknowledged.
 */

#ifndef HELIOS_STORAGE_INCLUDE_STORAGE_COMMIT_H
#define HELIOS_STORAGE_INCLUDE_STORAGE_COMMIT_H

#include <cstdint>
#include <string>
#include <vector>

namespace helios::storage {

/**
 * @brief Point read to revalidate at commit.
 *
 * `tid` and `found` come from an earlier read or scan row. Commit
 * aborts when the row's TID moved; `found == false` asserts the key was
 * absent and aborts when a row appeared.
 */
struct ExternalReadEntry {
  std::string table_name;
  std::string key;
  uint64_t tid = 0;
  bool found = false;
};

/**
 * @brief What a write does to the row it names.
 *
 * @details kInsert asserts that the key holds no live row at commit; if it
 * does, Commit aborts with @ref kDuplicatePrimaryKeyAbortReason. kUpdate
 * installs the value whether or not one is there.
 */
enum class RowOp {
  kUpdate = 0,
  kInsert = 1,
  kDelete = 2,
};

/**
 * @brief Row write or delete to install during Commit.
 *
 * `value` is ignored for a kDelete, which removes the row.
 */
struct ExternalWriteEntry {
  std::string table_name;
  std::string key;
  std::string value;
  RowOp op = RowOp::kUpdate;
};

/**
 * @brief Abort reason Commit reports when an insert entry finds a live row.
 */
inline constexpr char kDuplicatePrimaryKeyAbortReason[] =
    "duplicate_primary_key";

/**
 * @brief Every abort reason for a refused UNIQUE secondary key starts with
 * this.
 */
inline constexpr char kDuplicateSecondaryKeyAbortPrefix[] = "unique_si_";

/**
 * @brief When a commit is acknowledged, relative to its record reaching
 * stable storage. Carried per commit.
 *
 * The equivalent settings elsewhere, to keep Async from being read as a
 * faster Sync: Sync is PostgreSQL's synchronous_commit=on, SQL Server's full
 * durability, Oracle's COMMIT WAIT; Async is synchronous_commit=off, delayed
 * durability, COMMIT NOWAIT.
 */
enum class CommitDurability {
  kSync,   // Acknowledged once the committer's own epoch is durable.
  kAsync,  // Acknowledged at precommit; a crash can lose it.
};

/**
 * @brief Secondary-index add or remove to install during Commit.
 *
 * @details When `is_delete` is true, `primary_key` is removed from
 * `secondary_key`. Uniqueness belongs to the named index, not to this entry:
 * an add that collides on a UNIQUE index aborts with a reason starting with
 * kDuplicateSecondaryKeyAbortPrefix.
 */
struct ExternalSecondaryIndexEntry {
  std::string table_name;
  std::string index_name;
  std::string secondary_key;
  std::string primary_key;
  bool is_delete = false;
};

/**
 * @brief Range read to revalidate at commit.
 *
 * The bounds, index, row limit and direction describe the scan to re-run.
 * `result_keys` (plus `result_primary_keys` on a secondary index) holds the
 * keys that scan returned, in scan order; Commit replays the scan and aborts
 * when that ordered list differs. Row TIDs are not part of this entry: each
 * row the caller consumed is also an ExternalReadEntry.
 */
struct ExternalRangeReadEntry {
  std::string table_name;
  std::string index_name;     // Empty marks a primary-index range.
  std::string start_key;      // Scan start (inclusive).
  std::string end_key;        // Scan end (exclusive). Must be non-empty.
  uint64_t row_limit = 0;     // Row cap applied during scan.
  bool reverse_scan = false;  // Scan direction.
  std::vector<std::string> result_keys;  // Observed key set.
  std::vector<std::string>
      result_primary_keys;  // Secondary index: paired primary keys.
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_STORAGE_COMMIT_H
