/**
 * @file server/storage/include/storage/read.h
 * What a read observes: the outcome of a point read and of every scan.
 */

#ifndef HELIOS_STORAGE_INCLUDE_STORAGE_READ_H
#define HELIOS_STORAGE_INCLUDE_STORAGE_READ_H

#include <cstdint>
#include <string>
#include <vector>

namespace helios::storage {

/**
 * @brief Outcome of a single Database::Read.
 *
 * `tid` is the packed (epoch:32 | tid:32) version observed at read time.
 * Resubmit the same `tid` through Commit inside an
 * ExternalReadEntry to assert that the row did not move before commit.
 */
struct ReadResult {
  bool found = false;  // True when the key existed and was non-empty.
  std::string value;   // Row payload, valid only when `found` is true.
  uint64_t tid = 0;    // Packed (epoch | tid) version observed at read time.
};

/**
 * @brief One row from a primary-index range scan.
 */
struct ScanRow {
  std::string key;
  std::string value;
  uint64_t tid = 0;  // Packed version observed for this row.
};

/**
 * @brief One row from a secondary-index range scan.
 *
 * `secondary_key` is the indexed key, `primary_key` is the base-table key
 * reached through it, and `value` is the corresponding base-table row.
 */
struct ScanIndexRow {
  std::string secondary_key;
  std::string primary_key;
  std::string value;
  uint64_t tid = 0;  // Packed version of the base row.
};

/**
 * @brief Outcome of Database::Scan.
 *
 * `ok` separates a genuine empty result from a scan that never ran: the
 * table does not exist, or the exclusive end bound is empty. On
 * `ok == false` the caller should abort the logical transaction.
 */
struct ScanResult {
  bool ok = false;
  std::vector<ScanRow> rows;
};

/**
 * @brief One row reference from a PAX primary-index range scan.
 *
 * @details `group` is a `pax::PaxGroup*` and `item` is a `DataItem*`, kept
 * opaque so this public header does not expose internal storage headers.
 * Both are non-null in a returned row, are owned by the database, and stay
 * valid until the calling thread releases its epoch. Callers read the cells
 * they need, then call CurrentTid() and compare the result with `tid` to
 * reject torn reads.
 */
struct ScanPaxRow {
  std::string key;
  const void *group = nullptr;
  uint32_t slot = 0;
  uint32_t row_size = 0;
  uint64_t tid = 0;
  const void *item = nullptr;
};

/**
 * @brief Outcome of a PAX primary-index range scan.
 *
 * @details `ok == false` means the caller must use the materializing Scan
 * path instead. This happens when the end bound is empty, the table is
 * missing, it has no PAX table, or it contains rows that overflowed to the
 * heap.
 */
struct ScanPaxResult {
  bool ok = false;
  std::vector<ScanPaxRow> rows;
};

/**
 * @brief Returns the current packed TID for a PAX row reference.
 *
 * @param row Row reference returned by Database::ScanPax.
 */
uint64_t CurrentTid(const ScanPaxRow &row);

/**
 * @brief Outcome of Database::ScanIndex.
 */
struct ScanIndexResult {
  bool ok = false;
  std::vector<ScanIndexRow> rows;
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_STORAGE_READ_H
