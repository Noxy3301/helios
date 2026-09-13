/**
 * @file server/storage/src/silo/stable_read.h
 * The read of a row consistent with one version (Silo's tuple.h stable_read):
 * load the TID, yield while the lock bit is set, copy, then re-load the TID
 * and retry until it has not moved. The returned TID is that version.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_STABLE_READ_H
#define HELIOS_STORAGE_SRC_SILO_STABLE_READ_H

#include <xmmintrin.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/data_item.h"
#include "silo/transaction_id.h"

namespace helios::storage {
namespace silo {

struct StableValue {
  bool found = false;
  std::string value;
  TransactionId tid;
};

struct StablePrimaryKeys {
  bool found = false;
  PrimaryKeyList::Ptr primary_keys;

  // Valid while this struct lives: the view points into the list it pins.
  PrimaryKeyList::View primary_keys_view() const {
    return PrimaryKeyList::View(primary_keys);
  }
};

// Bit 0 of a transaction id: set while a committer holds the row.
inline constexpr uint32_t kLockBit = 1u;

/**
 * @brief Reads the transaction id once no committer holds the row (Silo's
 * stable version): spins while the lock bit is set.
 */
inline TransactionId StableTid(const DataItem &item) {
  for (;;) {
    const TransactionId tid = item.transaction_id.load();
    if (!(tid.tid & kLockBit)) return tid;
    _mm_pause();
  }
}

/**
 * @brief Stable read of a base row's liveness, without copying its payload.
 */
inline bool StableLive(const DataItem &item) {
  for (;;) {
    const TransactionId tid = StableTid(item);
    const bool live = item.HasRow();
    if (item.transaction_id.load() == tid) return live;
  }
}

/**
 * @brief Reads a value, retrying if its TID changes during the copy.
 * @param selected_columns Columns needed for projection pushdown, given as
 * ascending zero-based MySQL column numbers. nullptr selects all columns;
 * an empty list selects no data columns. Null flags are always retained.
 * @details PAX copies the selected columns and leaves empty markers in the
 * others. The current non-PAX path ignores the selection and copies the
 * whole value.
 * @return The value and its TID, with found == false if the slot has no live
 * value.
 */
inline StableValue StableRead(
    const DataItem &item,
    const std::vector<uint32_t> *selected_columns = nullptr) {
  for (;;) {
    // Observe an unlocked version before copying its live row.
    const TransactionId tid = StableTid(item);
    const bool found = item.HasRow();
    std::string value;
    if (found) {
      if (item.buffer.is_pax()) {
        // Gather either all fields or the requested columns from PAX strips.
        if (selected_columns == nullptr) {
          value.resize(item.size());
          item.buffer.GatherInto(reinterpret_cast<std::byte *>(value.data()));
        } else {
          item.buffer.pax_group()->GatherRowMasked(
              item.buffer.pax_slot(), selected_columns->data(),
              selected_columns->size(), value);
        }
      } else {
        value.assign(reinterpret_cast<const char *>(item.value()), item.size());
      }
    }

    // Accept the copy only if the same version is still current.
    if (item.transaction_id.load() == tid) {
      return {found, std::move(value), tid};
    }
  }
}

/**
 * @brief Stable read of a secondary-index DataItem, pinning its immutable
 * primary-key list.
 *
 * `found` is false when the slot is uninitialized or the list is empty.
 */
inline StablePrimaryKeys StableReadKeys(const DataItem &item) {
  for (;;) {
    const TransactionId tid = StableTid(item);
    auto primary_keys = std::atomic_load(&item.primary_keys_);
    const bool found = primary_keys && primary_keys->count != 0;

    if (item.transaction_id.load() == tid) {
      return {found, std::move(primary_keys)};
    }
  }
}

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_STABLE_READ_H
