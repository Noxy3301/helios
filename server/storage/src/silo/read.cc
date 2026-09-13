/**
 * @file server/storage/src/silo/read.cc
 * Index slot resolution and the row copies behind the point read and the
 * three scans.
 */

#include "silo/read.h"

#include <mutex>

#include "index/data_item.h"
#include "index/secondary_index.h"
#include "pax/table.h"
#include "silo/packed_transaction_id.h"
#include "silo/stable_read.h"
#include "table/table.h"
#include "table/table_dictionary.h"

namespace helios::storage {
namespace silo {

ReadResult Read(TableDictionary &tables, std::shared_mutex &schema_mutex,
                const std::string_view table_name, const std::string_view key,
                const std::vector<uint32_t> *selected_columns) {
  // Keep the schema fixed while resolving slots and reading their values.
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (table == nullptr) return {};

  DataItem *item = table->GetPrimaryIndex().Get(key);
  if (item == nullptr) return {};

  auto row = StableRead(*item, selected_columns);
  return {row.found, std::move(row.value), PackTransactionId(row.tid)};
}

std::vector<ReadResult> BatchRead(
    TableDictionary &tables, std::shared_mutex &schema_mutex,
    const std::vector<std::pair<std::string, std::string>> &keys) {
  std::vector<ReadResult> results;
  results.reserve(keys.size());
  for (const auto &[table_name, key] : keys) {
    results.emplace_back(Read(tables, schema_mutex, table_name, key));
  }
  return results;
}

ScanResult Scan(TableDictionary &tables, std::shared_mutex &schema_mutex,
                const std::string_view table_name,
                const std::string_view start_key,
                const std::string_view end_key, uint64_t row_limit,
                bool reverse_scan,
                const std::vector<uint32_t> *selected_columns) {
  ScanResult result;
  if (end_key.empty()) return result;

  // Keep the schema fixed while resolving slots and reading their values.
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (table == nullptr) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  // The value-yielding Scan/ScanReverse overloads pass the DataItem the leaf
  // walk already resolved, so read it directly instead of re-fetching by key.
  auto append_scan_entry = [&](std::string_view key, DataItem &item) {
    auto row = StableRead(item, selected_columns);
    if (row.found) {
      result.rows.push_back(
          {std::string(key), std::move(row.value), PackTransactionId(row.tid)});
      ++returned_rows;
    }
    // Tombstones are skipped here: a commit leaves them in place, and the
    // reaper removes them an epoch later.
    return row_limit > 0 && returned_rows >= row_limit;
  };

  if (reverse_scan) {
    table->GetPrimaryIndex().ScanReverse(start_key, end_key, append_scan_entry);
  } else {
    table->GetPrimaryIndex().Scan(start_key, end_key, append_scan_entry);
  }
  return result;
}

}  // namespace silo

uint64_t CurrentTid(const ScanPaxRow &row) {
  const auto *item = static_cast<const DataItem *>(row.item);
  return silo::PackTransactionId(item->transaction_id.load());
}

namespace silo {

ScanIndexResult ScanIndex(TableDictionary &tables,
                          std::shared_mutex &schema_mutex,
                          const std::string_view table_name,
                          const std::string_view index_name,
                          const std::string_view start_key,
                          const std::string_view end_key, uint64_t row_limit,
                          bool reverse_scan,
                          const std::vector<uint32_t> *selected_columns) {
  ScanIndexResult result;
  if (end_key.empty()) return result;

  // Keep the schema fixed while resolving slots and reading their values.
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
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

    auto row = StableRead(*item, selected_columns);
    if (row.found) {
      result.rows.push_back({std::string(secondary_key),
                             std::string(primary_key), std::move(row.value),
                             PackTransactionId(row.tid)});
      ++returned_rows;
    }
    return row_limit > 0 && returned_rows >= row_limit;
  };

  // Pin the immutable key list until all its primary keys have been read.
  auto append_secondary_entry = [&](std::string_view key) {
    const std::string secondary_key(key);
    DataItem *item = index->tree.Get(key);
    if (item == nullptr) {
      return false;
    }

    const auto keys = StableReadKeys(*item);
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

ScanPaxResult ScanPax(TableDictionary &tables, std::shared_mutex &schema_mutex,
                      const std::string_view table_name,
                      const std::string_view start_key,
                      const std::string_view end_key, uint64_t row_limit,
                      bool reverse_scan) {
  ScanPaxResult result;
  if (end_key.empty()) return result;

  // Keep the schema fixed while resolving slots and reading their values.
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  Table *table = tables.GetTable(table_name);
  if (table == nullptr) return result;

  auto *store = table->GetPaxTable();
  if (store == nullptr) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  auto append_pax_row = [&](std::string_view key, DataItem &item) {
    // Observe the same stable unlocked TID that a materialized read would use.
    // The caller re-checks this TID after reading cells from the strip.
    const TransactionId tid = StableTid(item);

    const size_t size = item.size();
    if (size == 0) return false;
    // Return a reference to the PAX location instead of gathering row bytes.
    result.rows.push_back({std::string(key), item.pax_group(),
                           item.pax_slot(), static_cast<uint32_t>(size),
                           PackTransactionId(tid), &item});
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

}  // namespace silo
}  // namespace helios::storage
