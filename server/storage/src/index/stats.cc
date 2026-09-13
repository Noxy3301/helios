/**
 * @file server/storage/src/index/stats.cc
 * Index statistics a query layer turns into optimizer estimates: exact NDV
 * per key-part prefix and an equi-height histogram of the leading key part.
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

// Exclusive end of a whole-index scan: a stored key opens with the null
// marker of its first part, never with 0xFF. Longer than any key prefix a
// caller can pack into one part.
constexpr size_t kSupremumSize = 16;
const std::string kSupremum(kSupremumSize, '\xff');

using silo::StableLive;

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

  // A key parts refuses fails the whole NDV: the scan stops and the caller
  // keeps its previous estimate.
  auto count_key = [&](std::string_view key) -> bool {
    std::vector<size_t> part_ends(num_parts, 0);
    if (!parts(key, num_parts, part_ends.data())) {
      failed = true;
      return true;
    }

    if (first) {
      // The first live key starts one distinct prefix at every depth.
      for (uint32_t part = 0; part < num_parts; ++part) out_ndv[part] = 1;
      first = false;
    } else {
      // Count a new prefix whenever bytes up to that key-part boundary
      // differ.
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
                         if (!StableLive(item)) return false;
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
        if (base_item != nullptr && StableLive(*base_item)) return true;
      }
      return false;
    };

    index->Scan(std::string_view(), std::string_view(kSupremum),
                [&](std::string_view key) -> bool {
                  DataItem *item = index->Get(key);
                  if (item == nullptr || !stable_live_secondary(*item)) {
                    return false;
                  }
                  return count_key(key);
                });
  }

  if (failed) {
    // The caller keeps the old optimizer estimate.
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

  // Histogram bounds are compared as bytes, so a key the caller refuses ends
  // the pass.
  auto leading_end = [&parts](std::string_view key) -> size_t {
    size_t end = 0;
    return parts(key, 1, &end) ? end : 0;
  };

  // Secondary scans visit one entry per key, but the histogram is over rows.
  // Use the PK-list length as that key's row weight.
  const auto stable_pk_count = [](const DataItem &item) -> uint64_t {
    const auto keys = silo::StableReadKeys(item);
    return keys.found ? keys.primary_keys->count : 0;
  };
  // Walks one index in key order and hands fn each counted key with its row
  // weight: 1 for a live primary row, or the primary-key list length for a
  // secondary entry, whose base rows are not re-checked here.
  // A key the caller refuses ends the pass, so `fn` never sees one.
  bool failed = false;
  auto walk = [&](auto &&fn) {
    if (index_name.empty()) {
      table->GetPrimaryIndex().Scan(
          std::string_view(), std::string_view(kSupremum),
          [&](std::string_view key, DataItem &item) -> bool {
            if (!StableLive(item)) return false;
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
      index->Scan(std::string_view(), std::string_view(kSupremum),
                  [&](std::string_view key) -> bool {
                    DataItem *item = index->Get(key);
                    if (item == nullptr) return false;
                    const uint64_t w = stable_pk_count(*item);
                    if (w == 0) return false;  // dead/empty secondary entry
                    if (leading_end(key) == 0) {
                      failed = true;
                      return true;
                    }
                    return fn(key, w);
                  });
    }
  };

  // Pass 1 sums the row weight the index represents.
  uint64_t total = 0;
  walk([&](std::string_view, uint64_t w) -> bool {
    total += w;
    return false;
  });
  if (failed || total == 0) return false;

  // Pass 2 records a bound every stride rows.
  const uint64_t stride = std::max<uint64_t>(1, total / buckets);
  uint64_t seen = 0;
  uint64_t next = stride;
  // Only the leading part of the last key walked, which is what a bound is.
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
    // The last bound closes the histogram at the maximum key so the high end
    // is exact.
    out_bounds.push_back(last_bound);
    out_cum.push_back(total);
  }
  return !out_bounds.empty();
}

}  // namespace helios::storage
