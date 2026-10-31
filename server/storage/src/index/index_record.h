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
 * @file server/storage/src/index/index_record.h
 * What the index maps a key to: the Silo transaction id word and the row
 * payload behind it, or the primary key a secondary key points at.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_INDEX_RECORD_H
#define HELIOS_STORAGE_SRC_INDEX_INDEX_RECORD_H

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "helios/pax.h"

#include "silo/tidword.h"
#include "util/epoch.h"

namespace helios::storage {

/**
 * @brief The base of every index record: its Silo version word.
 *
 * @details A tree allocates and frees its records as the type its
 * RecordType names, so the base needs no virtual destructor.
 */
struct IndexRecord {
  // Readers take `absent` from this word; the committer sets it at publish
  // from IsLive(), or for a non-unique index record from its last change.
  std::atomic<Tidword> transaction_id{Tidword::Absent()};
};

/**
 * @brief The record type a tree maps its keys to.
 */
enum class RecordType : uint8_t {
  kRow,        // A primary index's RowRecord.
  kUnique,     // A UNIQUE index's UniqueRecord.
  kNonUnique,  // A non-unique index's NonUniqueRecord.
};

/**
 * @brief A non-unique index record: one per (secondary key, primary key),
 *        under the secondary key followed by the primary-key bytes it lacks,
 *        whose length it keeps.
 */
struct NonUniqueRecord : IndexRecord {
  size_t pk_len() const { return pk_len_.load(std::memory_order_relaxed); }
  void set_pk_len(size_t len) {
    pk_len_.store(static_cast<uint16_t>(len), std::memory_order_relaxed);
  }

 private:
  // The TID word orders it; relaxed access only keeps readers race-free.
  std::atomic<uint16_t> pk_len_ = 0;
};

static_assert(sizeof(NonUniqueRecord) == 16,
              "NonUniqueRecord must remain 16 bytes");

/**
 * @brief A UNIQUE index record: the one primary key its secondary key points
 *        at, or none.
 *
 * @details A commit refuses a second primary key for a UNIQUE key, so one is
 * all a record holds. The key lives in an immutable block that the committer
 * replaces under the TID lock and frees after the RCU grace period.
 */
struct UniqueRecord : IndexRecord {
  ~UniqueRecord();

  bool IsLive() const { return primary_key_.load() != nullptr; }

  /**
   * @brief Returns the primary key, empty when the record holds none.
   * @note Read it under one version of the TID word. The view stays valid
   * until the calling thread releases its epoch.
   */
  std::string_view primary_key() const;

  /**
   * @brief Publishes `key`, or no key for nullopt.
   * @note The caller holds the TID lock, or is recovery before readers start.
   */
  void set_primary_key(std::optional<std::string_view> key);

 private:
  // A uint32_t length then the key bytes, from std::malloc; null for none.
  std::atomic<char *> primary_key_{nullptr};
};

static_assert(sizeof(UniqueRecord) == 16, "UniqueRecord must remain 16 bytes");

/**
 * @brief A primary index record: it refers to a PAX slot its table hands out
 *        on the record's first install.
 *
 * @details The slot is one atomic address word: bit 31 set while a slot is
 * attached, 18 bits of directory index and 13 bits of slot. A reader loads
 * it once.
 */
struct RowRecord : IndexRecord {
  // Where the row lives; group is nullptr when no slot is attached.
  struct PaxAddress {
    pax::PaxGroup *group;
    uint32_t slot;
  };

  size_t size() const { return size_; }
  bool IsLive() const { return size_ != 0; }

  bool pax_allocated() const {
    return address_.load(std::memory_order_acquire) != 0;
  }
  PaxAddress pax_address() const;

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
    const PaxAddress at = pax_address();
    assert(at.group != nullptr);
    return at.group->GatherRow(at.slot, out, capacity);
  }

  /**
   * @brief Copies the PAX value into owned bytes; an absent item returns empty.
   * @note The caller holds the TID lock or rechecks the TID after copying.
   */
  std::string CopyValue() const;

 private:
  static constexpr uint32_t kAttached = 1u << 31;
  static constexpr uint32_t kSlotBits = 13;

  pax::PaxTable *table_ = nullptr;  // Set by the first AllocateSlot.
  std::atomic<uint32_t> address_{0};
  uint32_t size_ = 0;  // Row length in bytes; zero once deleted.

  void PreserveImage(PaxAddress at, bool visible, EpochNumber epoch);
};

static_assert(sizeof(RowRecord) == 24, "RowRecord must remain 24 bytes");

}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_INDEX_INDEX_RECORD_H
