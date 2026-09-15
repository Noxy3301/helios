/**
 * @file server/storage/src/silo/write_set.h
 * Pending record updates, indexed by the record to be written.
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
 * @brief Registers pending updates, with one Entry per DataItem.
 * @details Entries are visited in pointer order. The caller runs each commit
 * phase over the entries; the set manages registration and duplicate updates.
 * Decoded rows are borrowed until the commit call returns.
 */
class WriteSet {
 public:
  /**
   * @brief One record's pending update and lock ownership.
   * @details Each operation acts on the DataItem held as this entry's map key.
   * The caller determines the order of locking, preparation and publication.
   */
  class Entry {
   public:
    /**
     * @brief Locks the record and checks that its index still points to it.
     * @param[out] observed_tid The version observed when acquiring the lock.
     * @return false with reason set on detachment; Unlock must still be called.
     */
    bool Lock(DataItem &item, Tidword &observed_tid, std::string &reason);

    /** @brief Releases this entry's lock, if it holds one. */
    void Unlock(DataItem &item);

    /**
     * @brief Prepares the update, checking INSERT or UNIQUE constraints.
     * @details Builds the final SI list without changing stored values.
     * Read-set version validation is handled separately by the caller.
     * @pre The record is locked.
     * @return false with reason set if the INSERT or UNIQUE constraint fails.
     */
    bool PrepareUpdate(DataItem &item, std::string &reason);

    /**
     * @brief Allocates the final row's PAX slot; deletes and SI need no slot.
     * @pre The record is locked.
     * @return false with reason set if allocation fails.
     */
    bool AllocateSlot(DataItem &item, std::string &reason) const;

    /**
     * @brief Installs the final value while retaining the record's lock.
     * @details The caller sets the commit epoch scope for row before-images.
     * @pre Read validation, update preparation and allocation all succeeded.
     */
    void Apply(DataItem &item) const;

    /**
     * @brief Copies the final row or ordered SI changes into one WAL entry.
     * @pre Apply completed and the record remains locked.
     */
    wal::LogEntry BuildLog(DataItem &item, Tidword commit_tid) const;

    /**
     * @brief Publishes the TID, unlocks, and queues the record if it is empty.
     * @pre This record is applied and its WAL entry copied; all entries were
     * prepared and allocated before the first record was applied.
     */
    void Publish(DataItem &item, Tidword commit_tid, index::Reaper &reaper);

    /** @brief Reports a row update, including DELETE, rather than an SI change.
     */
    bool IsRow() const;

   private:
    friend class WriteSet;

    /** @brief A final row value and its initial INSERT requirement. */
    struct RowUpdate {
      const pax::Row *value;  ///< Borrowed decoded row; unused for DELETE.
      RowOp op;
      /// The first operation was INSERT and requires an absent committed row.
      bool check_committed_row;
    };

    /** @brief Ordered SI changes and the final list prepared by PrepareUpdate. */
    struct IndexUpdate {
      IndexConstraint constraint;
      std::vector<wal::LogEntry::SecondaryIndexDelta> changes;
      PrimaryKeyList::Ptr primary_keys;
    };

    Entry(std::string table_name, std::string index_name, std::string key,
          index::MasstreeIndex &index,
          std::variant<RowUpdate, IndexUpdate> update);

    std::string table_name_;
    std::string index_name_;  ///< Empty for a primary-index record.
    std::string key_;
    index::MasstreeIndex &index_;
    bool owns_lock_ = false;
    /// Holds either the row update or the secondary-index update.
    std::variant<RowUpdate, IndexUpdate> update_;
  };

  /**
   * @brief Registers a row update, replacing that record's pending value.
   * @details If the first operation is INSERT, later updates keep its absence
   * check.
   * @return false with reason set if INSERT follows a pending live row.
   */
  bool AddRow(index::MasstreeIndex &index, const Write &write,
              std::string &reason);

  /**
   * @brief Appends a primary-key addition or removal to a secondary entry.
   * @return false with reason set on repeated additions to a UNIQUE key.
   */
  bool AddIndex(index::MasstreeIndex &index, IndexConstraint constraint,
                const ExternalSecondaryIndexEntry &change, std::string &reason);

  /** @brief Returns whether this set's entry currently holds the record's lock.
   */
  bool OwnsLock(DataItem *item) const;

  /** @brief Iterates entries in the common pointer order used for locking. */
  auto begin() { return entries_.begin(); }
  auto end() { return entries_.end(); }

  /** @brief Returns the number of distinct records to update. */
  size_t size() const { return entries_.size(); }

 private:
  std::map<DataItem *, Entry> entries_;
};

}  // namespace helios::storage::silo
#endif
