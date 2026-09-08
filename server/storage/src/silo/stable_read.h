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
  PackedPrimaryKeys::Ptr primary_keys;
  TransactionId tid;

  // Valid while this struct lives: the view points into the list it pins.
  PackedPrimaryKeysView primary_keys_view() const {
    return PackedPrimaryKeysView(primary_keys);
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
 * @brief Stable read of a base-row DataItem.
 *
 * `found` is false for tombstones and uninitialized slots.
 */
inline StableValue StableReadValue(const DataItem &item) {
  for (;;) {
    const TransactionId tid = StableTid(item);
    const bool found = item.HasRow();
    std::string value;
    if (found) {
      if (item.buffer.is_pax()) {
        // Gather the row from its PAX strips; a torn gather (concurrent
        // install) is rejected by the TID re-check below, same as a torn
        // pointer copy would be.
        value.resize(item.size());
        item.buffer.GatherInto(reinterpret_cast<std::byte *>(value.data()));
      } else {
        value.assign(reinterpret_cast<const char *>(item.value()), item.size());
      }
    }

    if (item.transaction_id.load() == tid) {
      return {found, std::move(value), tid};
    }
  }
}

/**
 * @brief Stable-read a row while masking unselected PAX columns.
 *
 * @details PAX-resident rows keep their full field shape, but unlisted columns
 * are emitted as one-byte empty fields. Heap rows are returned in full, so
 * masked reads remain an optimization and do not change row semantics.
 *
 * @param item Primary row slot.
 * @param columns Zero-based MySQL column indexes to materialize, in ascending
 * order.
 * @param n_columns Number of entries in `columns`.
 */
inline StableValue StableReadValueMasked(const DataItem &item,
                                         const uint32_t *columns,
                                         size_t n_columns) {
  for (;;) {
    const TransactionId tid = StableTid(item);
    const bool found = item.HasRow();
    std::string value;
    if (found) {
      if (item.buffer.is_pax()) {
        item.buffer.pax_group()->GatherRowMasked(item.buffer.pax_slot(),
                                                 columns, n_columns, value);
      } else {
        value.assign(reinterpret_cast<const char *>(item.value()), item.size());
      }
    }

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
      return {found, std::move(primary_keys), tid};
    }
  }
}

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_STABLE_READ_H
