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
#include <utility>
#include <vector>

#include "lineairdb/pax.h"

#include "index/primary_key_list.h"
#include "silo/tidword.h"
#include "util/epoch.h"

namespace helios::storage {

/**
 * @brief What one index key maps to.
 *
 * @details A primary-index item refers to a PAX slot its table hands out on
 * the item's first install.
 * A secondary-index item owns the primary-key list published with atomic
 * load and store. Both use transaction_id as their Silo version word.
 */
struct DataItem {
  // Readers take `absent` from this word; the committer sets it from IsLive()
  // at publish.
  std::atomic<Tidword> transaction_id;
  std::shared_ptr<const PrimaryKeyList> primary_keys_;

  size_t size() const { return size_; }
  bool IsLive() const {
    if (size_ != 0) return true;
    const auto primary_keys = std::atomic_load(&primary_keys_);
    return primary_keys && primary_keys->count != 0;
  }

  void SetPrimaryKeys(const std::vector<std::string> &primary_keys) {
    assert(IsSortedDeduped(primary_keys));
    auto packed = PrimaryKeyList::FromSortedDeduped(primary_keys);
    std::atomic_store(&primary_keys_, std::move(packed));
  }

  DataItem() : transaction_id(Tidword::Absent()) {}

  DataItem(const DataItem &) = delete;
  DataItem &operator=(const DataItem &) = delete;

  DataItem(DataItem &&rhs) noexcept
      : transaction_id(rhs.transaction_id.load()),
        primary_keys_(std::move(rhs.primary_keys_)),
        group_(std::exchange(rhs.group_, nullptr)),
        slot_(std::exchange(rhs.slot_, 0)),
        size_(std::exchange(rhs.size_, 0)) {}

  DataItem &operator=(DataItem &&rhs) noexcept {
    transaction_id.store(rhs.transaction_id.load());
    group_ = std::exchange(rhs.group_, nullptr);
    slot_ = std::exchange(rhs.slot_, 0);
    size_ = std::exchange(rhs.size_, 0);
    std::atomic_store(&primary_keys_, std::move(rhs.primary_keys_));
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
   * @brief Installs a decoded row into this item's PAX slot.
   * @note The caller holds the TID lock and has allocated every write's slot.
   * Recovery calls this before readers start.
   * @param epoch Commit epoch of this install; before-images are tagged with
   * it.
   */
  void InstallRow(const pax::Row &row, EpochNumber epoch);

  /**
   * @brief Hides this item's row: retires the slot from strip scans and
   * captures its before-image for an active read view. The slot stays
   * reserved for a later write.
   * @note The caller holds the TID lock; publishing the absent word is
   * separate.
   * @param epoch Commit epoch of this delete, the before-image's writer epoch.
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
  pax::PaxGroup *group_ = nullptr;  // The group holding this item's slot.
  uint32_t slot_ = 0;               // Slot inside group_.
  size_t size_ = 0;  // Row length in bytes; zero once deleted.

  void CaptureBeforeImage(EpochNumber epoch);

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
