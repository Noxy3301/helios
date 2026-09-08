/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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
 * @file server/storage/src/index/data_item.h
 * What the index maps a key to: the Silo transaction id word and the row
 * payload behind it, or the primary keys a secondary key points at.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_DATA_ITEM_H
#define HELIOS_STORAGE_SRC_INDEX_DATA_ITEM_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "index/data_buffer.h"
#include "index/primary_key_list.h"
#include "silo/transaction_id.h"

namespace helios::storage {

/**
 * @brief What one index key maps to.
 *
 * @details A primary-index slot keeps the row payload in buffer; a
 * secondary-index slot keeps the primary keys that key points at in
 * primary_keys, published with atomic load and store. transaction_id is the
 * Silo version word both carry. value() is valid only while buffer is
 * not in PAX mode.
 */
struct DataItem {
  std::atomic<TransactionId> transaction_id;
  DataBuffer buffer;
  std::shared_ptr<const PrimaryKeyList> primary_keys_;

  // Direct byte access is invalid for PAX-resident rows (no contiguous
  // bytes); those callers must go through DataBuffer::GatherInto / copies.
  std::byte *value() {
    assert(!buffer.is_pax());
    return buffer.value;
  }
  const std::byte *value() const {
    assert(!buffer.is_pax());
    return buffer.value;
  }
  size_t size() const { return buffer.size; }
  bool IsLive() const {
    if (buffer.size != 0) return true;
    const auto primary_keys = std::atomic_load(&primary_keys_);
    return primary_keys && primary_keys->count != 0;
  }
  bool HasRow() const { return buffer.size != 0; }

  PrimaryKeyList::View primary_keys_view() const {
    return PrimaryKeyList::View(std::atomic_load(&primary_keys_));
  }

  std::vector<std::string> primary_keys_vector() const {
    // Holds the list for the walk: the view is a pointer, and a writer may
    // publish a replacement over the member at any point.
    const auto primary_keys = std::atomic_load(&primary_keys_);
    const PrimaryKeyList::View view(primary_keys);
    std::vector<std::string> keys;
    keys.reserve(view.size());
    for (std::string_view key : view) {
      keys.emplace_back(key.data(), key.size());
    }
    return keys;
  }

  void SetPrimaryKeys(const std::vector<std::string> &primary_keys) {
    assert(IsSortedDeduped(primary_keys));
    auto packed = PrimaryKeyList::FromSortedDeduped(primary_keys);
    std::atomic_store(&primary_keys_, std::move(packed));
  }

  DataItem() : transaction_id(TransactionId()) {}
  DataItem(const DataItem &rhs)
      : transaction_id(rhs.transaction_id.load()),
        primary_keys_(std::atomic_load(&rhs.primary_keys_)) {
    buffer.Reset(rhs.buffer);
  }

  DataItem &operator=(const DataItem &rhs) {
    transaction_id.store(rhs.transaction_id.load());
    buffer.Reset(rhs.buffer);
    std::atomic_store(&primary_keys_, std::atomic_load(&rhs.primary_keys_));
    return *this;
  }

  DataItem(DataItem &&rhs) noexcept
      : transaction_id(rhs.transaction_id.load()),
        buffer(std::move(rhs.buffer)),
        primary_keys_(std::move(rhs.primary_keys_)) {}

  DataItem &operator=(DataItem &&rhs) noexcept {
    transaction_id.store(rhs.transaction_id.load());
    buffer = std::move(rhs.buffer);
    std::atomic_store(&primary_keys_, std::move(rhs.primary_keys_));
    return *this;
  }

  void Reset(const std::byte *row, size_t len, TransactionId tid = {}) {
    buffer.Reset(row, len);
    if (!tid.IsEmpty()) transaction_id.store(tid);
  }

  void InsertPrimaryKey(const std::byte *key, size_t len) {
    const std::string_view new_key(reinterpret_cast<const char *>(key), len);
    auto current = std::atomic_load(&primary_keys_);
    auto next = PrimaryKeyList::Insert(current, new_key);
    if (next != current) {
      std::atomic_store(&primary_keys_, std::move(next));
    }
  }

  void DeletePrimaryKey(const std::byte *key, size_t len) {
    std::string_view target(reinterpret_cast<const char *>(key), len);
    auto current = std::atomic_load(&primary_keys_);
    auto next = PrimaryKeyList::Delete(current, target);
    if (next != current) {
      std::atomic_store(&primary_keys_, std::move(next));
    }
  }

 private:
  static bool IsSortedDeduped(const std::vector<std::string> &keys) {
    return std::adjacent_find(
               keys.begin(), keys.end(),
               [](const std::string &lhs, const std::string &rhs) {
                 return !(lhs < rhs);
               }) == keys.end();
  }
};

static_assert(sizeof(DataItem) == 48, "DataItem must remain 48 bytes");
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_INDEX_DATA_ITEM_H
