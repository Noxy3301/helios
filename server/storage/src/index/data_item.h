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

// Modified for Helios.

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
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "helios/pax.h"

#include "index/primary_key_list.h"
#include "silo/tidword.h"
#include "util/epoch.h"

namespace helios::storage {

/**
 * @brief What one index key maps to.
 *
 * @details A primary-index item refers to a PAX slot its table hands out on
 * the item's first install.
 * A UNIQUE secondary item owns the primary-key list published with atomic
 * load and store. A non-unique item keys one (secondary key, primary key)
 * pair and holds no list: its key ends with the primary key, whose length it
 * keeps. All use transaction_id as their Silo version word.
 */
struct DataItem {
  // Readers take `absent` from this word; the committer sets it at publish
  // from IsLive(), or for a pair from its last change.
  std::atomic<Tidword> transaction_id;
  std::shared_ptr<const PrimaryKeyList> primary_keys;

  size_t size() const { return size_; }
  size_t pk_len() const { return pk_len_.load(std::memory_order_relaxed); }
  void set_pk_len(size_t len) {
    pk_len_.store(static_cast<uint16_t>(len), std::memory_order_relaxed);
  }
  bool IsLive() const {
    if (size_ != 0) return true;
    const auto keys = std::atomic_load(&primary_keys);
    return keys && keys->count != 0;
  }

  void set_primary_keys(const std::vector<std::string> &keys) {
    assert(IsSortedDeduped(keys));
    auto packed = PrimaryKeyList::from_sorted_deduped(keys);
    std::atomic_store(&primary_keys, std::move(packed));
  }

  DataItem() : transaction_id(Tidword::Absent()) {}

  DataItem(const DataItem &) = delete;
  DataItem &operator=(const DataItem &) = delete;

  DataItem(DataItem &&rhs) noexcept
      : transaction_id(rhs.transaction_id.load()),
        primary_keys(std::move(rhs.primary_keys)),
        group_(std::exchange(rhs.group_, nullptr)),
        slot_(std::exchange(rhs.slot_, 0)),
        pk_len_(rhs.pk_len_.exchange(0, std::memory_order_relaxed)),
        size_(std::exchange(rhs.size_, 0)) {}

  DataItem &operator=(DataItem &&rhs) noexcept {
    transaction_id.store(rhs.transaction_id.load());
    group_ = std::exchange(rhs.group_, nullptr);
    slot_ = std::exchange(rhs.slot_, 0);
    pk_len_.store(rhs.pk_len_.exchange(0, std::memory_order_relaxed),
                  std::memory_order_relaxed);
    size_ = std::exchange(rhs.size_, 0);
    std::atomic_store(&primary_keys, std::move(rhs.primary_keys));
    return *this;
  }

  bool pax_allocated() const { return group_ != nullptr; }
  pax::PaxGroup *pax_group() const { return group_; }
  uint32_t pax_slot() const { return slot_; }

  /**
   * @brief Reserves a slot in `table` if this item does not already have one.
   * @return true once the item holds a slot, including one it already had;
   * false when the table's slot directory is full.
   */
  bool AllocateSlot(pax::PaxTable &table);

  /**
   * @brief Installs an unpacked row into this item's PAX slot.
   * @note The caller holds the TID lock and has allocated every write's slot.
   * Recovery calls this before readers start.
   * @param epoch Commit epoch of this install; epoch images are tagged with
   * it.
   * @return The PAX fields whose cells changed, bit `min(f, 63)` for field
   * `f`; every bit when the item held no row.
   */
  uint64_t InstallRow(const pax::Row &row, EpochNumber epoch);

  /**
   * @brief Hides this item's row: retires the slot from strip scans and
   * preserves its epoch image for an open read view. The slot stays
   * reserved for a later write.
   * @note The caller holds the TID lock; publishing the absent word is
   * separate.
   * @param epoch Commit epoch of this delete, the epoch image's writer epoch.
   */
  void DeleteRow(EpochNumber epoch);

  // A concurrent update may change size_; use the reader's buffer capacity.
  size_t GatherInto(std::byte *out, size_t capacity) const {
    assert(pax_allocated());
    return pax_group()->GatherRow(slot_, out, capacity);
  }

  /**
   * @brief Copies the PAX value into owned bytes; an absent item returns empty.
   * @note The caller holds the TID lock or rechecks the TID after copying.
   */
  std::string CopyValue() const;

  void insert_primary_key(const std::byte *key, size_t len) {
    const std::string_view new_key(reinterpret_cast<const char *>(key), len);
    auto current = std::atomic_load(&primary_keys);
    auto next = PrimaryKeyList::insert(current, new_key);
    if (next != current) {
      std::atomic_store(&primary_keys, std::move(next));
    }
  }

  void delete_primary_key(const std::byte *key, size_t len) {
    std::string_view target(reinterpret_cast<const char *>(key), len);
    auto current = std::atomic_load(&primary_keys);
    auto next = PrimaryKeyList::erase(current, target);
    if (next != current) {
      std::atomic_store(&primary_keys, std::move(next));
    }
  }

 private:
  pax::PaxGroup *group_ = nullptr;  // The group holding this item's slot.
  uint32_t slot_ = 0;               // Slot inside group_.
  // A pair's primary-key length: its key ends with the primary key. The TID
  // word orders it; relaxed access only keeps readers race-free.
  std::atomic<uint16_t> pk_len_ = 0;
  size_t size_ = 0;  // Row length in bytes; zero once deleted.

  void PreserveImage(EpochNumber epoch);

  static bool IsSortedDeduped(const std::vector<std::string> &keys) {
    return std::adjacent_find(
               keys.begin(), keys.end(),
               [](const std::string &lhs, const std::string &rhs) {
                 return !(lhs < rhs);
               }) == keys.end();
  }
};

static_assert(sizeof(DataItem) == 48, "DataItem must remain 48 bytes");

/**
 * @brief Calls `fn` with each primary key a live secondary record points at,
 *        until `fn` returns true.
 *
 * A record without a list is a non-unique pair, whose key ends with its one
 * primary key. The caller reads `pk_len` and `list` under one live version of
 * the record.
 *
 * @param key    The record's index key.
 * @param pk_len A pair's primary-key length; unused when `list` is set.
 * @param list   A UNIQUE record's primary-key list, or null for a pair.
 * @return true when `fn` stopped the walk.
 */
template <typename Fn>
bool for_each_primary_key(std::string_view key, size_t pk_len,
                          const PrimaryKeyList::Ptr &list, Fn &&fn) {
  if (!list) return fn(key.substr(key.size() - pk_len));
  for (std::string_view primary_key : PrimaryKeyList::View(list)) {
    if (fn(primary_key)) return true;
  }
  return false;
}
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_INDEX_DATA_ITEM_H
