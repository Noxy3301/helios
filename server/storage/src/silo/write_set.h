/**
 * @file server/storage/src/silo/write_set.h
 * Pending updates and locks, with one entry per record.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_WRITE_SET_H
#define HELIOS_STORAGE_SRC_SILO_WRITE_SET_H

#include <map>
#include <string>
#include <variant>
#include <vector>

#include "index/data_item.h"
#include "lineairdb/commit.h"
#include "wal/log_entry.h"

namespace helios::storage::index {
class MasstreeIndex;
class Reaper;
}  // namespace helios::storage::index

namespace helios::storage::silo {

struct Write;  // Decoded row input defined in silo/commit.h.

/**
 * @brief Pending updates and acquired locks, with one Entry per DataItem.
 *
 * AddRow and AddIndex register updates in the same map. The caller controls
 * the protocol: register, lock, validate, allocate, apply, log, then publish.
 * On failure the caller releases acquired locks with Unlock.
 * Decoded rows are borrowed and must remain valid until the commit returns.
 */
class WriteSet {
 public:
  /**
   * @brief Registers a row update, replacing that record's pending value.
   * @details If the first operation is INSERT, later updates keep its absence check.
   * @return false, with reason set, if INSERT follows a pending live row.
   */
  bool AddRow(index::MasstreeIndex &index, const Write &write,
              std::string &reason);

  /**
   * @brief Registers a primary-key addition or removal for a secondary record.
   * @details Changes to the same record share one Entry and keep input order.
   * @return false, with reason set, on repeated additions to a UNIQUE key.
   */
  bool AddIndex(index::MasstreeIndex &index, IndexConstraint constraint,
                const ExternalSecondaryIndexEntry &change, std::string &reason);

  /**
   * @brief Locks each record in pointer order and checks its index mapping.
   * @param[in,out] max_tid Raised to the largest TID observed under a lock.
   * @details Acquired locks remain held on failure; the caller must call Unlock.
   * @return false, with reason set, if a record was detached from its index.
   */
  bool Lock(Tidword &max_tid, std::string &reason);

  /** @brief Releases only the locks acquired by this WriteSet. */
  void Unlock();

  /** @brief Returns whether this WriteSet currently holds the record's lock. */
  bool OwnsLock(DataItem *item) const;

  /**
   * @brief Checks INSERT and UNIQUE constraints and prepares final SI lists.
   * @details Stored values remain unchanged.
   * @pre All records are locked.
   * @return false, with reason set, if a constraint fails.
   */
  bool Validate(std::string &reason);

  /**
   * @brief Reserves PAX slots for final row values before any values are applied.
   * @pre All records are locked.
   * @return false, with reason set, if a required slot cannot be allocated.
   */
  bool AllocateSlots(std::string &reason);

  /**
   * @brief Installs each record's final value while retaining its lock.
   * @pre Validate and AllocateSlots succeeded; all records remain locked.
   * @param commit_epoch Epoch used to label captured row before-images.
   */
  void Apply(EpochNumber commit_epoch);

  /**
   * @brief Copies final rows and ordered SI changes into owned WAL entries.
   * @pre Apply completed and all records remain locked.
   */
  wal::WriteSet BuildLog(Tidword commit_tid) const;

  /**
   * @brief Publishes TIDs, unlocks records and queues empty records for reaping.
   * @pre Values have been applied and their WAL entries copied.
   */
  void Publish(Tidword commit_tid, index::Reaper &reaper);

 private:
  /** @brief A row's final value and the INSERT condition retained from input. */
  struct RowUpdate {
    const pax::Row *value;  ///< Borrowed decoded row; unused for DELETE.
    RowOp op;
    /// The first operation on this record was INSERT and requires an absent row.
    bool check_committed_row;
  };

  /**
   * @brief Ordered changes to one secondary record and its prepared final list.
   * @details Validate builds the list; Apply installs it; BuildLog copies changes.
   */
  struct IndexUpdate {
    IndexConstraint constraint;
    // The same ordered changes produce the final list and the recovery log.
    std::vector<wal::LogEntry::SecondaryIndexDelta> changes;
    PrimaryKeyList::Ptr primary_keys;  ///< Final list prepared under the lock.
  };

  /**
   * @brief One record's index identity, lock ownership and pending update.
   * @details The DataItem pointer is held as the map key, not duplicated here.
   */
  struct Entry {
    std::string table_name;
    std::string index_name;  ///< Empty for a primary-index record.
    std::string key;
    index::MasstreeIndex *index;
    bool owns_lock;
    /// Holds either RowUpdate or IndexUpdate, depending on the record kind.
    std::variant<RowUpdate, IndexUpdate> update;
  };

  /** @brief Unique record entries, ordered by pointer for lock acquisition. */
  std::map<DataItem *, Entry> entries_;
};

}  // namespace helios::storage::silo
#endif
