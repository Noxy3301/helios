/**
 * @file server/storage/src/silo/read.h
 * The read side of the store: point reads and range scans. Each call takes a
 * shared lock on the schema, reads rows with the stable read, and returns the
 * packed TIDs a later commit validates as read-set evidence.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_READ_H
#define HELIOS_STORAGE_SRC_SILO_READ_H

#include <cstdint>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/read.h"

namespace helios::storage {

class TableDictionary;

namespace silo {

/**
 * @brief Reads one row.
 *
 * Takes a shared lock on the schema, resolves the primary-index slot, and
 * performs a stable read on the DataItem: load the TID, yield while the lock
 * bit (LSB) is set, copy the value, then re-load the TID and retry until it
 * has not moved. The caller keeps the returned `tid` and submits it through
 * Commit later.
 *
 * A miss, a tombstone and a table that does not exist all return a result
 * with `found == false`.
 *
 * @param selected_columns Null for the whole row; non-null, even when empty,
 * blanks every PAX field it does not name.
 */
ReadResult Read(TableDictionary &tables, std::shared_mutex &schema_mutex,
                std::string_view table_name, std::string_view key,
                const std::vector<uint32_t> *selected_columns = nullptr);

/**
 * @brief Reads several rows in one call.
 *
 * Reuses the point read per entry. Reads are independent, so this is purely a
 * transport optimization that lets a caller fold N point reads into one
 * RPC.
 *
 * @param keys One (table name, primary key) pair per read, answered in order.
 */
std::vector<ReadResult> BatchRead(
    TableDictionary &tables, std::shared_mutex &schema_mutex,
    const std::vector<std::pair<std::string, std::string>> &keys);

/**
 * @brief Range-scans the primary index and returns the rows observed in
 *        the range.
 *
 * Drives index::Scan / index::ScanReverse with a callback that, for each
 * hit, performs the same stable read used by the point read. The caller
 * assembles the commit-time ExternalRangeReadEntry from its own scan
 * arguments and the returned keys. Tombstones are skipped: key-list
 * validation catches any reuse of their slots without a per-entry TID.
 *
 * The range is `[start_key, end_key)` and a `row_limit` of 0 means every row
 * in it. `ok` distinguishes a genuine empty result from a scan that never
 * ran: the table does not exist, or `end_key` is empty. Callers should treat
 * `!ok` as an abort signal.
 *
 * @param selected_columns Null for whole rows; non-null, even when empty,
 * blanks every PAX field it does not name.
 */
ScanResult Scan(TableDictionary &tables, std::shared_mutex &schema_mutex,
                std::string_view table_name, std::string_view start_key,
                std::string_view end_key, uint64_t row_limit, bool reverse_scan,
                const std::vector<uint32_t> *selected_columns = nullptr);

/**
 * @brief Range-scans a secondary index and resolves each hit to its base
 *        row.
 *
 * For every secondary key in `[start_key, end_key)`, pins its immutable
 * primary-key list and, for each key in the view, performs the same stable
 * base read as the point read. The caller assembles the commit-time
 * ExternalRangeReadEntry from its own scan arguments and both returned
 * key lists. `ok == false` is the abort signal, as in the primary range read,
 * and covers a missing index as well as a missing table and an empty
 * `end_key`.
 *
 * @param selected_columns Null for whole rows; non-null, even when empty,
 * blanks every PAX field it does not name.
 */
ScanIndexResult ScanIndex(
    TableDictionary &tables, std::shared_mutex &schema_mutex,
    std::string_view table_name, std::string_view index_name,
    std::string_view start_key, std::string_view end_key, uint64_t row_limit,
    bool reverse_scan, const std::vector<uint32_t> *selected_columns = nullptr);

/**
 * @brief Range-scans the primary index and returns PAX cell references.
 *
 * @details The caller evaluates cells directly and re-checks each row TID
 * after reading. Each row's `group` and `item` are non-owning and stay valid
 * until this thread releases its epoch, so the caller must finish with them
 * before Database::ReleaseThreadEpoch.
 *
 * `ok == false` either refuses the scan outright (the table does not exist,
 * `end_key` is empty) or reports that strip-direct references cannot serve
 * this table because some row lives on the heap; a caller that can fall back
 * to the row-shaped range read should do so.
 */
ScanPaxResult ScanPax(TableDictionary &tables, std::shared_mutex &schema_mutex,
                      std::string_view table_name, std::string_view start_key,
                      std::string_view end_key, uint64_t row_limit,
                      bool reverse_scan);

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_READ_H
