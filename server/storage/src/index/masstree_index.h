/**
 * @file server/storage/src/index/masstree_index.h
 * Ordered indexes backed by Masstree.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_MASSTREE_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_MASSTREE_INDEX_H

#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include "index/data_item.h"

namespace helios::storage {
namespace index {

/**
 * @brief Maps ordered keys to DataItems.
 * @details DataItem pointers stay valid until this thread calls
 * release_thread_epoch().
 */
class MasstreeIndex final {
 public:
  MasstreeIndex();
  ~MasstreeIndex();

  DataItem *Get(std::string_view key);
  void Put(std::string_view key, DataItem &&value);

  /**
   * @brief Gets the entry for key, creating an absent DataItem if needed.
   * @details Commit must recheck the key after locking the item.
   * @return Non-null; allocation failure throws.
   */
  DataItem *GetOrInsert(std::string_view key);

  /**
   * @brief Walks [begin, end) upwards; nullopt for `end` means no upper bound.
   * @details The key is valid only during the callback. Return true to stop.
   * Use stable reads for values; a scan is not a snapshot.
   * @return Visited key count, including the key that stopped the scan.
   */
  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view, DataItem &)> operation);

  /**
   * @brief Walks the same half-open range downwards, from below `end`;
   *        nullopt for `end` starts at the largest key.
   * @details Same callback contract and same count as Scan.
   */
  size_t ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view, DataItem &)> operation);

  // Visits all keys unless the callback returns true to stop.
  void ForEach(std::function<bool(std::string_view, DataItem &)> operation);

  /**
   * @brief Removes a tombstone after the reaper locks it and checks its TID.
   * @details Publishes retired_tid before queuing the item for RCU deletion.
   * @return False if key no longer points to expected; true after removal.
   */
  bool Purge(std::string_view key, DataItem &expected,
             Tidword retired_tid);

 private:
  // Keep Masstree headers out of callers.
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * @brief Advances Masstree's epoch from the storage epoch thread.
 */
void advance_epoch();

/**
 * @brief Releases this thread's Masstree epoch.
 * @details Call only after finishing with all pointers obtained in that epoch.
 * The next index operation enters an epoch again.
 */
void release_thread_epoch();

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_MASSTREE_INDEX_H
