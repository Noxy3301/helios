/**
 * @file server/storage/src/index/stats.cc
 * Distinct-key counts and histograms for the query optimizer.
 */

#include <xmmintrin.h>

#include <algorithm>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "lineairdb/database.h"

#include "index/data_item.h"
#include "index/secondary_index.h"
#include "silo/stable_read.h"

namespace helios::storage {

namespace {

// Encoded SQL keys start with a null marker below 0xff, so this bounds them.
constexpr size_t kSupremumSize = 16;
const std::string kSupremum(kSupremumSize, '\xff');

}  // namespace

bool Database::IndexNdv(const std::string_view table_name,
                        const std::string_view index_name, uint32_t num_parts,
                        const KeyPartEnds &parts,
                        std::vector<uint64_t> &out_ndv) {
  out_ndv.assign(num_parts, 0);
  if (num_parts == 0) return false;

  std::shared_lock<std::shared_mutex> lk(schema_mutex_);
  auto table = GetTable(table_name);
  if (table == nullptr) return false;

  bool failed = false;
  bool first = true;
  // Index scans are key-ordered, so one previous key is enough for NDV.
  std::string prev_key;
  std::vector<size_t> prev_part_ends(num_parts, 0);

  // Stop if the key parser cannot locate every requested part.
  auto count_key = [&](std::string_view key) -> bool {
    std::vector<size_t> part_ends(num_parts, 0);
    if (!parts(key, num_parts, part_ends.data())) {
      failed = true;
      return true;
    }

    if (first) {
      // The first key contributes one distinct value for each prefix length.
      for (uint32_t part = 0; part < num_parts; ++part) out_ndv[part] = 1;
      first = false;
    } else {
      // Count each prefix that differs from the previous key.
      const std::string_view prev(prev_key);
      for (uint32_t part = 0; part < num_parts; ++part) {
        if (part_ends[part] != prev_part_ends[part] ||
            key.substr(0, part_ends[part]) !=
                prev.substr(0, prev_part_ends[part])) {
          ++out_ndv[part];
        }
      }
    }

    prev_key.assign(key.data(), key.size());
    prev_part_ends = std::move(part_ends);
    return false;
  };

  auto &primary_index = table->GetPrimaryIndex();

  if (index_name.empty()) {
    // Primary index entries are base rows, so count live rows directly.
    primary_index.Scan(std::string_view(), std::string_view(kSupremum),
                       [&](std::string_view key, DataItem &item) -> bool {
                         // The absent bit changes only when a commit
                         // publishes, so a locked word still shows the last
                         // committed state.
                         if (item.transaction_id.load().absent) return false;
                         return count_key(key);
                       });
  } else {
    index::SecondaryIndex *index = table->GetSecondaryIndex(index_name);
    if (index == nullptr) return false;

    // Secondary entries count only if one referenced base row is live.
    auto stable_live_secondary = [&](const DataItem &item) {
      const auto keys = silo::StableReadKeys(item);
      if (!keys.found) return false;
      for (std::string_view primary_key : keys.primary_keys_view()) {
        DataItem *base_item = primary_index.Get(primary_key);
        if (base_item != nullptr && !base_item->transaction_id.load().absent)
          return true;
      }
      return false;
    };

    index->tree.Scan(std::string_view(), std::string_view(kSupremum),
                     [&](std::string_view key, DataItem &item) -> bool {
                       if (!stable_live_secondary(item)) {
                         return false;
                       }
                       return count_key(key);
                     });
  }

  if (failed) {
    // Discard partial counts after a key-parsing failure.
    out_ndv.assign(num_parts, 0);
    return false;
  }
  return true;
}

bool Database::IndexHistogram(const std::string_view table_name,
                              const std::string_view index_name,
                              uint32_t buckets, const KeyPartEnds &parts,
                              std::vector<std::string> &out_bounds,
                              std::vector<uint64_t> &out_cum) {
  out_bounds.clear();
  out_cum.clear();
  if (buckets == 0) return false;
  std::shared_lock<std::shared_mutex> lk(schema_mutex_);
  auto table = GetTable(table_name);
  if (table == nullptr) return false;

  // Find the end of the first key part; zero means parsing failed.
  auto leading_end = [&parts](std::string_view key) -> size_t {
    size_t end = 0;
    return parts(key, 1, &end) ? end : 0;
  };

  // Weight each secondary key by the number of primary keys it references.
  const auto stable_pk_count = [](const DataItem &item) -> uint64_t {
    const auto keys = silo::StableReadKeys(item);
    return keys.found ? keys.primary_keys->count : 0;
  };
  // Visit keys with weight 1 for primary rows, or the list size for secondary keys.
  // Secondary weights do not recheck whether the referenced rows are live.
  bool failed = false;
  auto walk = [&](auto &&fn) {
    if (index_name.empty()) {
      table->GetPrimaryIndex().Scan(
          std::string_view(), std::string_view(kSupremum),
          [&](std::string_view key, DataItem &item) -> bool {
            if (item.transaction_id.load().absent) return false;
            if (leading_end(key) == 0) {
              failed = true;
              return true;
            }
            return fn(key, static_cast<uint64_t>(1));
          });
    } else {
      index::SecondaryIndex *index = table->GetSecondaryIndex(index_name);
      if (index == nullptr) {
        failed = true;
        return;
      }
      index->tree.Scan(std::string_view(), std::string_view(kSupremum),
                       [&](std::string_view key, DataItem &item) -> bool {
                         const uint64_t w = stable_pk_count(item);
                         if (w == 0) return false;  // dead/empty secondary entry
                         if (leading_end(key) == 0) {
                           failed = true;
                           return true;
                         }
                         return fn(key, w);
                       });
    }
  };

  // First pass: sum the row weights.
  uint64_t total = 0;
  walk([&](std::string_view, uint64_t w) -> bool {
    total += w;
    return false;
  });
  if (failed || total == 0) return false;

  // Second pass: add a boundary when the row count reaches the next stride.
  const uint64_t stride = std::max<uint64_t>(1, total / buckets);
  uint64_t seen = 0;
  uint64_t next = stride;
  // Keep the last key's first part for the final boundary.
  std::string last_bound;
  walk([&](std::string_view key, uint64_t w) -> bool {
    const size_t end = leading_end(key);
    seen += w;
    last_bound.assign(key.data(), end);
    if (seen >= next) {
      out_bounds.emplace_back(key.substr(0, end));
      out_cum.push_back(seen);
      while (seen >= next) next += stride;
    }
    return false;
  });
  if (failed) {
    out_bounds.clear();
    out_cum.clear();
    return false;
  }
  if (out_bounds.empty() || out_cum.back() != total) {
    // Include the last visited key as the final histogram boundary.
    out_bounds.push_back(last_bound);
    out_cum.push_back(total);
  }
  return !out_bounds.empty();
}

}  // namespace helios::storage
