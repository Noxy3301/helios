/**
 * @file server/storage/include/lineairdb/read.h
 * What a read observes: the outcome of a point read and of every scan.
 */

#ifndef HELIOS_STORAGE_INCLUDE_LINEAIRDB_READ_H
#define HELIOS_STORAGE_INCLUDE_LINEAIRDB_READ_H

#include <cstdint>
#include <string>
#include <vector>

namespace helios::storage {

/**
 * @brief Outcome of a single Database::Read.
 *
 * The caller passes the same `tid` and `found` back in an ExternalReadEntry
 * so Commit can confirm the row did not move.
 */
struct ReadResult {
  // True when the key holds a live, non-empty payload.
  bool found = false;
  std::string value;  // Row payload, valid only when `found` is true.
  uint64_t tid = 0;   // Packed (`epoch:32 | tid:32`) version observed at read.
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
 * valid until the calling thread releases its epoch. `slot` is the row's
 * index inside that group and `row_size` is its stored payload length in
 * bytes. Callers read the cells they need, then call CurrentTid() and
 * compare the result with `tid` to reject torn reads.
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
 * @details `ok == false` means the scan cannot run: the end bound is empty,
 * the table is missing, or its PAX schema has not been installed.
 */
struct ScanPaxResult {
  bool ok = false;
  std::vector<ScanPaxRow> rows;
};

/**
 * @brief Returns the live packed TID of the row a PAX reference points at.
 *
 * @details Loads through `row.item`, which must be non-null. Packed as
 * ReadResult::tid is. This is not `row.tid`: compare the two to reject a row
 * a writer changed during the scan.
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

#endif  // HELIOS_STORAGE_INCLUDE_LINEAIRDB_READ_H
