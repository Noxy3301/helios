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

#include "index/index_record.h"
#include "index/secondary_index.h"
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
  Tidword tid;  // The version the two keys belong to.
  std::string_view secondary_key;
  std::string_view primary_key;  // Meaningful only when `found`.
};

/**
 * @brief Reads the transaction id once no committer holds the row (Silo's
 * stable version): spins while the lock bit is set.
 */
inline Tidword StableTid(const IndexRecord &item) {
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
inline StableValue StableRead(const RowRecord &item, bool keys_only = false) {
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
 * @brief Stable read of a secondary index record: its secondary key and the
 * primary key it points at, under one version of its TID word.
 *
 * `found` is false when the word's absent bit is set, which the committer
 * publishes for an emptied UNIQUE key or a removed non-unique index record.
 * The keys view `key` and the record's block, which stays valid until the
 * thread releases its epoch.
 */
inline StablePrimaryKeys StableReadKeys(const index::SecondaryIndex &index,
                                        std::string_view key,
                                        const IndexRecord &item) {
  for (;;) {
    const Tidword tid = StableTid(item);
    const auto [secondary_key, primary_key] =
        index::split_entry(index, key, item);
    if (item.transaction_id.load() == tid) {
      return {!tid.absent, tid, secondary_key, primary_key};
    }
  }
}

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_STABLE_READ_H
