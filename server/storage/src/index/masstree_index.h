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

#include "lineairdb/pax.h"

#include "index/data_item.h"

namespace helios::storage {
namespace index {

/**
 * @brief Maps ordered keys to DataItems.
 * @details DataItem pointers stay valid until this thread calls
 * MasstreeReleaseThreadEpoch().
 */
class MasstreeIndex final {
 public:
  MasstreeIndex();
  ~MasstreeIndex();

  /**
   * @brief Sets the PAX table for new primary entries; does not own it.
   * @details Secondary indexes leave this null and store primary-key lists.
   */
  void SetPaxTable(pax::PaxTable *store);

  DataItem *Get(std::string_view key);
  void Put(std::string_view key, DataItem &&value);

  /**
   * @brief Gets the entry for key, creating an absent DataItem if needed.
   * @details Commit must recheck the key after locking the item.
   * @pre On a primary index the caller has observed the table's PAX store
   * through Table::GetPaxTable, which orders it before the store read here.
   * @return Non-null; allocation failure throws.
   */
  DataItem *GetOrInsert(std::string_view key);

  /**
   * @brief Scans [begin, end); nullopt means no upper bound. Reverse scans down.
   * @details The key is valid only during the callback. Return true to stop.
   * Use stable reads for values; a scan is not a snapshot.
   * @return Visited key count, including the key that stopped the scan.
   */
  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view, DataItem &)> operation);
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
 * @brief Advances Masstree's epoch from the storage epoch ticker.
 */
void MasstreeAdvanceEpoch();

/**
 * @brief Releases this thread's Masstree epoch.
 * @details Call only after finishing with all pointers obtained in that epoch.
 * The next index operation enters an epoch again.
 */
void MasstreeReleaseThreadEpoch();

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_MASSTREE_INDEX_H
