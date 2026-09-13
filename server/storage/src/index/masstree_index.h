/**
 * @file server/storage/src/index/masstree_index.h
 * The ordered key to value map every index is built on, behind a pointer to
 * implementation that keeps masstree headers out of the rest of the tree.
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
 * @brief The ordered map every index is built on, behind a pointer to
 *        implementation.
 *
 * @details Masstree headers stay inside masstree_index.cc, so its templates
 * and macros do not leak into the rest of the tree or into the tests, which
 * do not have masstree on their include path.
 */
class MasstreeIndex final {
 public:
  MasstreeIndex();
  ~MasstreeIndex();

  /**
   * @brief Routes future absent primary rows through a table's PaxTable.
   *
   * @details Secondary indexes never set a PaxTable: they store index
   * metadata rather than table row payloads. The table is not owned, and a
   * null table is used for secondary-index entries without a row payload.
   */
  void SetPaxTable(pax::PaxTable *store);

  DataItem *Get(std::string_view key);
  void Put(std::string_view key, DataItem &&value);

  /**
   * @brief Returns the entry for key, creating an absent DataItem if needed.
   *
   * @details Concurrent insertions of the same key reuse the existing entry.
   * The returned pointer is protected by the caller's Masstree epoch; commit
   * must still re-resolve the key after locking the DataItem.
   * @return Non-null. Allocation failures propagate as exceptions.
   */
  DataItem *GetOrInsert(std::string_view key);

  /**
   * @brief Walks the keys in `[begin, end)`, or to the last key when end is
   *        absent; the reverse forms walk the same range downward.
   *
   * @details operation returns true to stop the walk, the opposite of
   * masstree's own visitor. ForEach walks every key.
   * @return The number of keys handed to operation, including the one that
   * stopped it.
   */
  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view)> operation);
  size_t Scan(std::string_view begin, std::string_view end,
              std::function<bool(std::string_view, DataItem &)> operation);
  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view)> operation);
  size_t ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem &)> operation);

  void ForEach(std::function<bool(std::string_view, DataItem &)> operation);

  /**
   * @brief Removes a committed tombstone structurally.
   *
   * @details Called by the deferred purge reaper only, once it has locked
   * `expected`, verified the delete transaction id and confirmed the key
   * still resolves to the same DataItem. `retired_tid` is published on the
   * removed item before it is retired to RCU.
   *
   * @param expected The locked slot that must still be stored under key.
   * @return True when the slot was removed; false when the key is gone or a
   * replacement holds it.
   */
  bool Purge(std::string_view key, DataItem &expected,
             TransactionId retired_tid);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * @brief Drives masstree's globalepoch from the epoch ticker.
 */
void MasstreeAdvanceEpoch();

/**
 * @brief Closes this thread's reclamation critical section.
 *
 * @details A thread enrols implicitly through any masstree op and calls this
 * at a boundary where no raw DataItem or leaf pointer obtained in the section
 * is used again. There is deliberately no way to advance without releasing:
 * re-stamping gc_epoch_ mid-section would let RCU reclaim pointers the caller
 * still holds. A no-op on a thread with no masstree threadinfo.
 */
void MasstreeReleaseThreadEpoch();

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_MASSTREE_INDEX_H
