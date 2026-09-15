/**
 * @file server/storage/src/silo/read_set.cc
 * Validation of point observations and replay of primary and secondary ranges.
 */

#include "silo/read_set.h"

#include <algorithm>
#include <string_view>

#include "index/data_item.h"
#include "index/secondary_index.h"
#include "silo/tidword.h"
#include "silo/write_set.h"
#include "table/table_dictionary.h"

namespace helios::storage::silo {
namespace {

std::string KeyHex(const std::string &key) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(key.size() * 2);
  for (unsigned char byte : key) {
    out.push_back(kHex[byte >> 4]);
    out.push_back(kHex[byte & 0x0F]);
  }
  return out;
}

std::string FormatReadAbortReason(const char *reason,
                                  const ExternalReadEntry &read) {
  std::string out(reason);
  out += ':';
  out += read.table_name;
  out += ":key=";
  out += KeyHex(read.key);
  return out;
}

// Rescan a primary range; stop at the first difference in live keys.
bool ReplayRange(TableDictionary &tables, const WriteSet &write_set,
                 const ExternalRangeReadEntry &range, Tidword &max_tid) {
  auto table = tables.GetTable(range.table_name);
  if (table == nullptr) return false;

  size_t result_pos = 0;
  bool aborted = false;
  bool matches = true;
  auto collect_key = [&](std::string_view key, DataItem &item) {
    const Tidword tid = item.transaction_id.load();
    if (tid.lock && !write_set.OwnsLock(&item)) {
      aborted = true;
      return true;
    }
    // Include tombstones too: their delete TIDs determine the state we saw.
    max_tid = std::max(max_tid, tid);
    if (!tid.absent) {
      if (result_pos >= range.result_keys.size() ||
          std::string_view(range.result_keys[result_pos]) != key) {
        matches = false;
        return true;
      }
      ++result_pos;
    }
    return range.row_limit > 0 && result_pos >= range.row_limit;
  };

  if (range.reverse_scan) {
    table->GetPrimaryIndex().ScanReverse(range.start_key, range.end_key,
                                         collect_key);
  } else {
    table->GetPrimaryIndex().Scan(range.start_key, range.end_key, collect_key);
  }
  if (aborted) return false;
  return matches && result_pos == range.result_keys.size();
}

// Rescan a secondary range and compare secondary/primary key pairs in order.
bool ReplayIndexRange(TableDictionary &tables, const WriteSet &write_set,
                      const ExternalRangeReadEntry &range, Tidword &max_tid) {
  auto table = tables.GetTable(range.table_name);
  if (table == nullptr) return false;
  auto *index = table->GetSecondaryIndex(range.index_name);
  if (index == nullptr) return false;

  size_t result_pos = 0;
  bool aborted = false;
  bool matches = true;
  auto collect_base_row = [&](const std::string &secondary_key,
                              std::string_view primary_key) {
    DataItem *item = table->GetPrimaryIndex().Get(primary_key);
    if (item == nullptr) return false;
    const Tidword tid = item->transaction_id.load();
    if (tid.lock && !write_set.OwnsLock(item)) {
      aborted = true;
      return true;
    }
    max_tid = std::max(max_tid, tid);
    if (!tid.absent) {
      if (result_pos >= range.result_keys.size() ||
          result_pos >= range.result_primary_keys.size() ||
          std::string_view(range.result_keys[result_pos]) !=
              std::string_view(secondary_key) ||
          std::string_view(range.result_primary_keys[result_pos]) !=
              primary_key) {
        matches = false;
        return true;
      }
      ++result_pos;
    }
    return range.row_limit > 0 && result_pos >= range.row_limit;
  };

  auto collect_secondary_key = [&](std::string_view key, DataItem &) {
    // Find the current entry; the item supplied by the scan may be stale.
    const std::string secondary_key(key);
    DataItem *item = index->tree.Get(key);
    if (item == nullptr) return false;

    const Tidword tid = item->transaction_id.load();
    if (tid.lock && !write_set.OwnsLock(item)) {
      aborted = true;
      return true;
    }

    // The key list is a separate load; abort if the word moved around it.
    auto primary_keys = std::atomic_load(&item->primary_keys_);
    if (item->transaction_id.load() != tid) {
      aborted = true;
      return true;
    }
    max_tid = std::max(max_tid, tid);

    for (std::string_view primary_key : PrimaryKeyList::View(primary_keys)) {
      if (collect_base_row(secondary_key, primary_key)) return true;
    }
    return false;
  };

  if (range.reverse_scan) {
    index->tree.ScanReverse(range.start_key, range.end_key,
                            collect_secondary_key);
  } else {
    index->tree.Scan(range.start_key, range.end_key, collect_secondary_key);
  }
  if (aborted) return false;
  return matches && result_pos == range.result_keys.size() &&
         result_pos == range.result_primary_keys.size();
}

}  // namespace

bool ReadSet::CheckRangeBounds(std::string &reason) const {
  for (const auto &range : range_reads_) {
    if (range.end_key.empty()) {
      reason = "range_end_key_missing";
      return false;
    }
  }
  return true;
}

bool ReadSet::Validate(TableDictionary &tables, const WriteSet &write_set,
                       Tidword &max_tid, std::string &reason) const {
  for (const auto &read : point_reads_) {
    auto *table = tables.GetTable(read.table_name);
    if (table == nullptr) {
      if (read.tid != 0) {
        reason = "read_table_missing";
        return false;
      }
      continue;
    }
    const Tidword observed(read.tid);
    DataItem *item = table->GetPrimaryIndex().Get(read.key);
    // A key with no record reads as the absent word.
    const Tidword current =
        item ? item->transaction_id.load() : Tidword::Absent();
    // Only our own lock may differ from the word the read observed.
    Tidword expected = observed;
    if (item != nullptr && write_set.OwnsLock(item)) expected.lock = true;
    if (current != expected) {
      reason = FormatReadAbortReason("exact_read_tid_moved", read);
      return false;
    }
    max_tid = std::max(max_tid, observed);
  }
  for (const auto &range : range_reads_) {
    const bool ok = range.index_name.empty()
                        ? ReplayRange(tables, write_set, range, max_tid)
                        : ReplayIndexRange(tables, write_set, range, max_tid);
    if (!ok) {
      reason = range.index_name.empty() ? "primary_range_result_changed"
                                        : "secondary_range_result_changed";
      return false;
    }
  }
  return true;
}

}  // namespace helios::storage::silo
