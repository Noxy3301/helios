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

#include "helios/index.h"

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
  Tidword tid;        // The version the list and the length belong to.
  size_t pk_len = 0;  // A pair's primary-key length, as DataItem keeps it.
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
 * @param keys_only When true, no value is copied and `value` stays empty.
 * @return The value and its TID, with `found == false` if the slot has no live
 * value.
 */
inline StableValue StableRead(const DataItem &item, bool keys_only = false) {
  for (;;) {
    // Observe an unlocked version before copying its live row.
    const Tidword tid = StableTid(item);
    const bool found = !tid.absent;
    std::string value;
    if (found && !keys_only) value = item.CopyValue();

    // Accept the copy only if the same version is still current.
    if (item.transaction_id.load() == tid) {
      return {found, std::move(value), tid};
    }
  }
}

/**
 * @brief Stable read of a secondary-index DataItem, pinning a UNIQUE record's
 * immutable primary-key list and taking a pair's primary-key length.
 *
 * `found` is false when the word's absent bit is set, which the committer
 * publishes for an emptied list or a removed pair.
 */
inline StablePrimaryKeys StableReadKeys(const DataItem &item,
                                        IndexConstraint constraint) {
  for (;;) {
    const Tidword tid = StableTid(item);
    // Keep this immutable list alive even if a writer replaces it; a pair
    // holds none.
    PrimaryKeyList::Ptr primary_keys;
    if (constraint == IndexConstraint::kUnique)
      primary_keys = std::atomic_load(&item.primary_keys);
    const size_t pk_len = item.pk_len();
    const bool found = !tid.absent;

    if (item.transaction_id.load() == tid) {
      return {found, std::move(primary_keys), tid, pk_len};
    }
  }
}

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_STABLE_READ_H
