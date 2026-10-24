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

#include "index/index_record.h"

namespace helios::storage {
namespace index {

// Masstree's MASSTREE_MAXKEYLEN: a scan copies each key into a buffer this
// long.
inline constexpr size_t kMaxKeyLength = 255;

/**
 * @brief Maps ordered keys to index records of one RecordType.
 * @details Record pointers stay valid until this thread calls
 * release_thread_epoch() or exits.
 */
class MasstreeIndex final {
 public:
  explicit MasstreeIndex(RecordType type = RecordType::kRow);
  ~MasstreeIndex();

  IndexRecord *Get(std::string_view key);

  /**
   * @brief Gets the entry for key, creating an absent record if needed.
   * @details The reaper may unlink the record afterwards; Commit checks the
   * record's latest bit after locking it.
   * @param expect_new A hint that the index likely has no entry for key. It
   * skips the optimistic lookup and affects cost, not the result.
   * @return Non-null; allocation failure throws.
   */
  IndexRecord *GetOrInsert(std::string_view key, bool expect_new = false);

  /**
   * @brief Walks [begin, end) upwards; nullopt for `end` means no upper bound.
   * @details The key is valid only during the callback. Return true to stop.
   * Use stable reads for values; a scan is not a snapshot.
   * @return Visited key count, including the key that stopped the scan.
   */
  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view, IndexRecord &)> operation);

  /**
   * @brief Walks the same half-open range downwards, from below `end`;
   *        nullopt for `end` starts at the largest key.
   * @details Same callback contract and same count as Scan.
   */
  size_t ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view, IndexRecord &)> operation);

  // Visits all keys unless the callback returns true to stop.
  void ForEach(std::function<bool(std::string_view, IndexRecord &)> operation);

  /**
   * @brief Removes a tombstone after the reaper locks it and checks its TID.
   * @details Publishes retired_tid before queuing the item for RCU deletion.
   * @return False if key no longer points to expected; true after removal.
   */
  bool Purge(std::string_view key, IndexRecord &expected, Tidword retired_tid);

  /**
   * @brief Unlinks key and frees its record at once, without RCU.
   *
   * Call only while no other thread can reach this index, as in recovery.
   */
  void Remove(std::string_view key);

  /**
   * @brief Frees every record of this index; its nodes stay allocated.
   *
   * @details No other thread may reach the index any more (see
   * synchronize_rcu), and the index is unusable afterwards.
   */
  void Destroy();

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
 * @brief Frees `ptr`, a std::malloc block of `size` bytes, once every thread
 *        that could have loaded it has released its epoch.
 */
void free_rcu(void *ptr, size_t size);

/**
 * @brief Releases this thread's Masstree epoch.
 * @details Call only after finishing with all pointers obtained in that epoch.
 * The next index operation enters an epoch again.
 */
void release_thread_epoch();

/**
 * @brief Enters this thread's Masstree epoch, as the next index operation
 * would.
 */
void enter_thread_epoch();

/**
 * @brief Waits until every thread that was inside its Masstree epoch at the
 * call has released it.
 * @details Releases the calling thread's epoch first. Call outside any
 * transaction and never from the epoch thread, which advances the epoch.
 */
void synchronize_rcu();

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_MASSTREE_INDEX_H
