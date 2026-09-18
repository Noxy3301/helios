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
#include "silo/tidword.h"

namespace helios::storage {
namespace silo {

struct StableValue {
  bool found = false;
  std::string value;
  Tidword tid;
};

struct StablePrimaryKeys {
  bool found = false;
  PrimaryKeyList::Ptr primary_keys;

  // Valid while this struct lives: the view points into the list it pins.
  PrimaryKeyList::View primary_keys_view() const {
    return PrimaryKeyList::View(primary_keys);
  }
};

/**
 * @brief Reads the transaction id once no committer holds the row (Silo's
 * stable version): spins while the lock bit is set.
 */
inline Tidword StableTid(const DataItem &item) {
  for (;;) {
    const Tidword tid = item.transaction_id.load();
    if (!tid.lock) return tid;
    _mm_pause();
  }
}

/**
 * @brief Reads a value, retrying if its TID changes during the copy.
 * @param selected_columns Columns needed for projection pushdown, given as
 * ascending zero-based MySQL column numbers. nullptr selects all columns;
 * an empty list selects no data columns. Null flags are always retained.
 * @details PAX copies the selected columns and leaves empty markers in the
 * others. Every stored value is read from its PAX slot.
 * @return The value and its TID, with `found == false` if the slot has no live
 * value.
 */
inline StableValue StableRead(
    const DataItem &item,
    const std::vector<uint32_t> *selected_columns = nullptr) {
  for (;;) {
    // Observe an unlocked version before copying its live row.
    const Tidword tid = StableTid(item);
    const bool found = !tid.absent;
    std::string value;
    if (found) {
      if (selected_columns == nullptr) {
        value = item.CopyValue();
      } else {
        item.pax_group()->GatherRowMasked(item.pax_slot(),
                                          selected_columns->data(),
                                          selected_columns->size(), value);
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
 * `found` is false when the word's absent bit is set, which the committer
 * publishes for an emptied list.
 */
inline StablePrimaryKeys StableReadKeys(const DataItem &item) {
  for (;;) {
    const Tidword tid = StableTid(item);
    // Keep this immutable list alive even if a writer replaces it.
    auto primary_keys = std::atomic_load(&item.primary_keys);
    const bool found = !tid.absent;

    if (item.transaction_id.load() == tid) {
      return {found, std::move(primary_keys)};
    }
  }
}

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_STABLE_READ_H
