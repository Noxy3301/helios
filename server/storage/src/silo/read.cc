/**
 * @file server/storage/src/silo/read.cc
 * The read side of Database: point reads and range scans over the primary
 * and secondary indexes.
 */

#include "lineairdb/database.h"

#include "index/data_item.h"
#include "index/secondary_index.h"
#include "pax/table.h"
#include "silo/stable_read.h"
#include "table/table.h"
#include "table/table_dictionary.h"

namespace helios::storage {

ReadResult Database::Read(const std::string_view table_name,
                          const std::string_view key,
                          const std::vector<uint32_t> *selected_columns) {
  auto table = GetTable(table_name);
  if (table == nullptr) return {};

  DataItem *item = table->GetPrimaryIndex().Get(key);
  if (item == nullptr) return {false, {}, Tidword::Absent().obj};

  auto row = silo::StableRead(*item, selected_columns);
  return {row.found, std::move(row.value), row.tid.obj};
}

std::vector<ReadResult> Database::BatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  std::vector<ReadResult> results;
  results.reserve(keys.size());
  for (const auto &[table_name, key] : keys) {
    results.emplace_back(Read(table_name, key));
  }
  return results;
}

ScanResult Database::Scan(const std::string_view table_name,
                          const std::string_view start_key,
                          const std::string_view end_key, uint64_t row_limit,
                          bool reverse_scan,
                          const std::vector<uint32_t> *selected_columns) {
  ScanResult result;
  if (end_key.empty()) return result;

  auto table = GetTable(table_name);
  if (table == nullptr) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  // Copy each live row from the DataItem found by the scan.
  auto append_scan_entry = [&](std::string_view key, DataItem &item) {
    auto row = silo::StableRead(item, selected_columns);
    if (row.found) {
      result.rows.push_back(
          {std::string(key), std::move(row.value), row.tid.obj});
      ++returned_rows;
    }
    // Count only live rows toward the limit; tombstones stay for later cleanup.
    return row_limit > 0 && returned_rows >= row_limit;
  };

  if (reverse_scan) {
    table->GetPrimaryIndex().ScanReverse(start_key, end_key, append_scan_entry);
  } else {
    table->GetPrimaryIndex().Scan(start_key, end_key, append_scan_entry);
  }
  return result;
}

uint64_t CurrentTid(const ScanPaxRow &row) {
  const auto *item = static_cast<const DataItem *>(row.item);
  return item->transaction_id.load().obj;
}

ScanIndexResult Database::ScanIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  ScanIndexResult result;
  if (end_key.empty()) return result;

  auto table = GetTable(table_name);
  if (table == nullptr) return result;

  index::SecondaryIndex *index = table->GetSecondaryIndex(index_name);
  if (index == nullptr) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  // Resolve each secondary hit to a stable copy of its primary row.
  auto append_base_row = [&](std::string_view secondary_key,
                             std::string_view primary_key) {
    DataItem *item = table->GetPrimaryIndex().Get(primary_key);
    if (item == nullptr) {
      return false;
    }

    auto row = silo::StableRead(*item, selected_columns);
    if (row.found) {
      result.rows.push_back({std::string(secondary_key),
                             std::string(primary_key), std::move(row.value),
                             row.tid.obj});
      ++returned_rows;
    }
    return row_limit > 0 && returned_rows >= row_limit;
  };

  // Keep the key list alive while reading the referenced rows.
  auto append_secondary_entry = [&](std::string_view key, DataItem &item) {
    const std::string secondary_key(key);
    const auto keys = silo::StableReadKeys(item);
    for (std::string_view primary_key : keys.primary_keys_view()) {
      if (append_base_row(secondary_key, primary_key)) return true;
    }
    return false;
  };

  if (reverse_scan) {
    index->tree.ScanReverse(start_key, end_key, append_secondary_entry);
  } else {
    index->tree.Scan(start_key, end_key, append_secondary_entry);
  }
  return result;
}

ScanPaxResult Database::ScanPax(const std::string_view table_name,
                                const std::string_view start_key,
                                const std::string_view end_key,
                                uint64_t row_limit, bool reverse_scan) {
  ScanPaxResult result;
  if (end_key.empty()) return result;

  Table *table = GetTable(table_name);
  if (table == nullptr) return result;

  auto *store = table->GetPaxTable();
  if (store == nullptr) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  auto append_pax_row = [&](std::string_view key, DataItem &item) {
    // Read an unlocked TID; the caller must recheck it after reading the cells.
    const Tidword tid = silo::StableTid(item);
    if (tid.absent) return false;

    // Return the PAX location for the caller to read directly.
    result.rows.push_back({std::string(key), item.pax_group(), item.pax_slot(),
                           static_cast<uint32_t>(item.size()), tid.obj, &item});
    ++returned_rows;
    return row_limit > 0 && returned_rows >= row_limit;
  };

  if (reverse_scan) {
    table->GetPrimaryIndex().ScanReverse(start_key, end_key, append_pax_row);
  } else {
    table->GetPrimaryIndex().Scan(start_key, end_key, append_pax_row);
  }
  return result;
}

}  // namespace helios::storage
