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
 * @file server/storage/src/index/data_buffer.h
 * The row payload a DataItem owns, on the heap or in a PAX slot.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_DATA_BUFFER_H
#define HELIOS_STORAGE_SRC_INDEX_DATA_BUFFER_H

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "storage/pax.h"

namespace helios::storage {

/**
 * @brief Owns or references the row payload stored in a DataItem.
 *
 * @details Heap mode: `value` owns a new[] byte array of `size` bytes and
 * `capacity_or_slot` tracks that allocation. It is used for
 * transaction-local copies, non-PAX tables, and rows that overflowed from
 * PAX storage.
 *
 * PAX mode: the payload bytes live in a PaxGroup's column strips; this
 * buffer only references them. `value` carries a tagged pointer
 * (bit0 = PAX, bit1 = slot allocated): before the first install it points
 * to the table's PaxTable, afterwards to the owning PaxGroup with
 * `capacity_or_slot` = slot index. `size` keeps its meaning (payload byte size,
 * 0 = tombstone/blank), so every liveness check (`size != 0`) works
 * unchanged in both modes.
 *
 * Mode transitions are one-way per row: index layers create blank items in
 * PAX mode for PAX tables. A row overflows to heap mode for good when the
 * table has no free slot, or when its bytes do not fit the declared cell
 * widths.
 *
 * Copy semantics do the storage conversion implicitly: copying FROM a
 * PAX-mode buffer gathers the row into heap bytes (transaction-local
 * snapshots), assigning INTO a PAX-mode buffer scatters the bytes into the
 * strips (commit install under the row's TID lock).
 */
struct DataBuffer {
  static constexpr uintptr_t kPaxTag = 0x1;
  static constexpr uintptr_t kPaxAllocated = 0x2;
  static constexpr uintptr_t kPaxMask = kPaxTag | kPaxAllocated;

  std::byte *value;
  size_t size;
  // Heap mode: the size of the allocation. PAX mode: the slot number.
  size_t capacity_or_slot;

  DataBuffer() : value(nullptr), size(0), capacity_or_slot(0) {}
  ~DataBuffer() {
    if (value != nullptr && !is_pax()) delete[] value;
  }

  bool is_pax() const {
    return (reinterpret_cast<uintptr_t>(value) & kPaxTag) != 0;
  }
  bool pax_allocated() const {
    return (reinterpret_cast<uintptr_t>(value) & kPaxAllocated) != 0;
  }
  pax::PaxGroup *pax_group() const {
    return reinterpret_cast<pax::PaxGroup *>(
        reinterpret_cast<uintptr_t>(value) & ~kPaxMask);
  }
  pax::PaxTable *pax_table() const {
    return reinterpret_cast<pax::PaxTable *>(
        reinterpret_cast<uintptr_t>(value) & ~kPaxMask);
  }
  uint32_t pax_slot() const { return static_cast<uint32_t>(capacity_or_slot); }

  /**
   * @brief Initializes a fresh blank item as a PAX-resident row reference.
   *
   * @param store PaxTable that will allocate the concrete row slot on the
   * first non-empty install.
   */
  void InitPaxBlank(pax::PaxTable *store) {
    assert((reinterpret_cast<uintptr_t>(store) & kPaxMask) == 0);
    value = reinterpret_cast<std::byte *>(reinterpret_cast<uintptr_t>(store) |
                                          kPaxTag);
    size = 0;
    capacity_or_slot = 0;
  }

  DataBuffer(DataBuffer &&other) noexcept
      : value(other.value),
        size(other.size),
        capacity_or_slot(other.capacity_or_slot) {
    other.value = nullptr;
    other.size = 0;
    other.capacity_or_slot = 0;
  }

  DataBuffer &operator=(DataBuffer &&other) noexcept {
    if (this != &other) {
      if (value != nullptr && !is_pax()) delete[] value;
      value = other.value;
      size = other.size;
      capacity_or_slot = other.capacity_or_slot;
      other.value = nullptr;
      other.size = 0;
      other.capacity_or_slot = 0;
    }
    return *this;
  }

  DataBuffer(const DataBuffer &other)
      : value(nullptr), size(0), capacity_or_slot(0) {
    Reset(other);
  }

  DataBuffer &operator=(const DataBuffer &other) {
    if (this != &other) {
      Reset(other);
    }
    return *this;
  }

  /**
   * @brief Copies len bytes of row into this buffer.
   *
   * @details Does not take ownership of row. A null pointer or a zero length
   * empties the row without releasing the heap allocation, which is kept so a
   * similar-sized row does not reallocate. A PAX-mode buffer installs or
   * tombstones through ResetPax.
   */
  void Reset(const std::byte *row, const size_t len) {
    if (is_pax()) {
      ResetPax(row, len);
      return;
    }
    if (row == nullptr || len == 0) {
      size = 0;
      return;
    }
    EnsureHeapCapacity(len);
    size = len;
    std::memcpy(value, row, len);
  }

  /**
   * @brief Resets this buffer to the row payload represented by `rhs`.
   */
  void Reset(const DataBuffer &rhs);

  void Reset(const std::string &rhs) {
    Reset(reinterpret_cast<const std::byte *>(rhs.data()), rhs.size());
  }

  /**
   * @brief Reconstructs this row's payload bytes from PAX strips into `dst`.
   *
   * @param dst Destination buffer with room for `size` bytes.
   * @return Number of bytes written.
   */
  size_t GatherInto(std::byte *dst) const {
    assert(is_pax());
    assert(pax_allocated());
    assert(size > 0);
    return pax_group()->GatherRow(pax_slot(), dst, size);
  }

  std::string toString() const;

 private:
  // The heap allocation only grows: a shorter row reuses it.
  void EnsureHeapCapacity(size_t len) {
    if (capacity_or_slot >= len) return;
    delete[] value;
    value = new std::byte[len];
    capacity_or_slot = len;
  }

  /**
   * @brief Captures the pre-install row image while a columnar read view
   * is active.
   *
   * @details Must run before the first strip mutation of this install
   * (ScatterRow or RetireSlot). A zero commit epoch means the install is
   * outside an epoch-tagged commit (recovery replay); a read view observed
   * active in that state fails the capture for the generation, fail-closed.
   */
  void CaptureBeforeImage();

  /**
   * @brief Installs payload bytes into this PAX-mode buffer, or tombstones
   *        it.
   *
   * @details The caller holds the row's TID lock. This buffer permanently
   * switches to heap storage when the table has no free slot, or when the
   * row does not fit its declared cell widths.
   */
  void ResetPax(const std::byte *row, const size_t len);

  /**
   * @brief Moves this row to heap storage for good, and records it.
   *
   * @details A table with one such row is no longer a complete set of rows in
   * its strips, which is what the store's counter tells strip-direct readers.
   */
  void OverflowToHeap(pax::PaxTable *store, const char *why,
                      const std::byte *row, const size_t len);
};
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_INDEX_DATA_BUFFER_H
