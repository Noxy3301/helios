/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/**
 * @file server/storage/src/index/primary_index.h
 * The primary index: the key to DataItem map every base row lives in.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_PRIMARY_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_PRIMARY_INDEX_H

#include <cassert>
#include <functional>
#include <optional>
#include <string_view>
#include <utility>

#include "index/data_item.h"
#include "index/masstree_index.h"

namespace helios::storage {
namespace index {

class PrimaryIndex {
 public:
  PrimaryIndex() = default;

  /**
   * @brief Routes future primary-row placeholders through `store`.
   */
  void SetPaxTable(pax::PaxTable *store) { index_.SetPaxTable(store); }

  DataItem *Get(std::string_view key) { return index_.Get(key); }

  /**
   * @brief Returns the slot for key, inserting an absent DataItem when it is
   *        absent.
   *
   * @return Non-null.
   */
  DataItem *GetOrInsert(std::string_view key) {
    auto *item = index_.Get(key);
    if (item == nullptr) {
      index_.PutAbsent(key);
      item = index_.Get(key);
      assert(item != nullptr);
    }
    return item;
  }

  void Put(std::string_view key, DataItem &&value) {
    index_.Put(key, std::move(value));
  }

  void ForEach(std::function<bool(std::string_view, DataItem &)> operation) {
    index_.ForEach(std::move(operation));
  }

  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view)> operation) {
    return index_.Scan(begin, end, std::move(operation));
  }

  size_t Scan(std::string_view begin, std::string_view end,
              std::function<bool(std::string_view, DataItem &)> operation) {
    return index_.Scan(begin, end, std::move(operation));
  }

  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view)> operation) {
    return index_.ScanReverse(begin, end, std::move(operation));
  }

  size_t ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem &)> operation) {
    return index_.ScanReverse(begin, end, std::move(operation));
  }

  bool Purge(std::string_view key, DataItem *expected,
             TransactionId retired_tid = {}) {
    return index_.Purge(key, expected, retired_tid);
  }

 private:
  MasstreeIndex index_;
};
}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_PRIMARY_INDEX_H
