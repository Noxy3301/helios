/**
 * @file server/storage/include/lineairdb/transaction.h
 * One commit attempt: the write operations and durability a request submits,
 * and the Silo protocol that validates and installs its read and write sets.
 */

#ifndef HELIOS_STORAGE_INCLUDE_LINEAIRDB_TRANSACTION_H
#define HELIOS_STORAGE_INCLUDE_LINEAIRDB_TRANSACTION_H

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "lineairdb/index.h"

#include "index/data_item.h"
#include "pax/table.h"
#include "silo/tidword.h"
#include "util/epoch.h"
#include "wal/log_record.h"

namespace helios::storage {

/**
 * @brief What a write does to the row it names.
 *
 * @details kInsert asserts that the key holds no live row at commit; if it
 * does, Commit aborts with @ref kDuplicatePrimaryKeyAbortReason. kUpdate
 * installs the value whether or not one is there.
 */
enum class RowOp {
  kUpdate = 0,
  kInsert = 1,
  kDelete = 2,
};

/**
 * @brief Abort reason Commit reports when an insert entry finds a live row.
 */
inline constexpr char kDuplicatePrimaryKeyAbortReason[] =
    "duplicate_primary_key";

/**
 * @brief Every abort reason for a refused UNIQUE secondary key starts with
 * this.
 */
inline constexpr char kDuplicateSecondaryKeyAbortPrefix[] = "unique_si_";

/**
 * @brief Whether Commit waits for its record to reach stable storage.
 * Carried per commit.
 *
 * The equivalent settings elsewhere, to keep Async from being read as a
 * faster Sync: Sync is PostgreSQL's synchronous_commit=on, SQL Server's full
 * durability, Oracle's COMMIT WAIT; Async is synchronous_commit=off, delayed
 * durability, COMMIT NOWAIT.
 */
enum class CommitDurability {
  kSync,   // Commit returns once the committer's own epoch is durable.
  kAsync,  // Commit returns at precommit; a crash can lose it.
};

class Database;
class Table;
class TableDictionary;

namespace epoch {
class Framework;
}  // namespace epoch

namespace index {
class MasstreeIndex;
class Reaper;
}  // namespace index

namespace wal {
class Logger;
}  // namespace wal

namespace silo {

// Maximum tid within one epoch; Tidword reserves 29 bits for it.
inline constexpr uint32_t kMaxTid = (1u << 29) - 1;

/**
 * @brief One commit attempt on one thread.
 *
 * @details The read phase ran on the query layer. The request feeds its
 * observations and updates, then calls Commit once. Inputs are borrowed
 * unchanged until Commit returns; the constructor arguments outlive the
 * attempt, and the last-TID slot is the calling thread's. A Write or
 * IndexWrite that returns false ends the attempt: feed nothing more and do
 * not call Commit. Records claimed by earlier writes stay absent in the
 * index, as after any abort. The thread keeps its Masstree epoch from the
 * first call until it releases it after Commit or after a refused feed.
 */
class Transaction {
 public:
  Transaction(TableDictionary &tables, epoch::Framework &epoch,
              index::Reaper &reaper, wal::Logger &logger,
              Tidword &last_commit_tid);

  /**
   * @brief Binds an attempt to the database's components and to the calling
   * worker's last-TID slot.
   */
  explicit Transaction(Database &db);
  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;

  /**
   * @brief Records a point read to revalidate: the key and the word it
   * returned.
   *
   * @details A key that had no record carries the absent word. Commit aborts
   * when the record's word moved: a new version, a row that appeared or
   * disappeared, or another committer's lock. A table that does not exist at
   * commit is accepted only with the word 0 a read of a missing table returns.
   */
  void Read(std::string_view table, std::string_view key, Tidword observed);

  /**
   * @brief Records a range read to replay at commit.
   *
   * @details The bounds, index, limit and direction describe the scan to
   * re-run over `[begin, end)`. An empty `index` names the primary index, and
   * a `limit` of 0 caps nothing. `keys`, and `primary_keys` on a secondary
   * index, are the keys that scan returned, in scan order. Commit replays the
   * scan and aborts when that ordered list differs. Row words are not part of
   * this call: each row the caller consumed is also a Read. Commit refuses a
   * range whose `end` is empty.
   */
  void RangeRead(std::string_view table, std::string_view index,
                 std::string_view begin, std::string_view end, uint64_t limit,
                 bool reverse, std::vector<std::string_view> keys,
                 std::vector<std::string_view> primary_keys);

  /**
   * @brief Decodes the row and merges it into its record's pending update.
   *
   * @details `row_bytes` holds packed bytes matching the table's installed PAX
   * schema, and is ignored for a kDelete. Later writes to one key replace the
   * value; if the record's first operation was INSERT, Commit keeps checking
   * that the row is absent.
   * @return false with reason `write_table_missing`, `pax_schema_missing`,
   * `pax_row_decode_failed`, or @ref kDuplicatePrimaryKeyAbortReason for an
   * INSERT after a pending live row. Nothing is claimed on false.
   */
  bool Write(std::string_view table_name, std::string_view key,
             std::string_view row_bytes, RowOp op, std::string &reason);

  /**
   * @brief Appends one primary-key addition or removal to a secondary record.
   *
   * @details With `remove` true, `primary_key` leaves the secondary key's
   * list; otherwise it joins it. Uniqueness belongs to the named index, not to
   * this call.
   * @return false with reason `si_table_missing`, `pax_schema_missing`, or
   * `si_index_missing`.
   */
  bool IndexWrite(std::string_view table_name, std::string_view index_name,
                  std::string_view secondary_key, std::string_view primary_key,
                  bool remove, std::string &reason);

  /**
   * @brief Runs the Silo commit protocol and installs the write set.
   *
   * @details The storage epoch is offline on entry and again on return. A
   * range with no end bound is refused before anything is locked.
   *
   * - Phase 1: lock every record in pointer order. Then join the epoch, as
   *   Silo reads it after locking.
   * - Phase 2: validate every point read by word, replay every range and
   *   compare its key list, check INSERT and UNIQUE under the locks, then
   *   choose the commit TID. Reserve every PAX slot before the first value
   *   changes.
   * - Phase 3: install each record, append its WAL write, and publish its
   *   TID, which unlocks it.
   *
   * The record is enqueued before the worker leaves the epoch. A Sync commit
   * then waits for that epoch to become durable, and only when a record was
   * logged: a read-only commit returns at once.
   *
   * @param[out] reason Cleared on entry and empty on success. On abort, a
   * short label naming the failed check.
   * @return true after publishing; false on abort, with every lock released
   * and no stored value changed.
   */
  bool Commit(CommitDurability durability, std::string &reason);

 private:
  struct ReadEntry {
    std::string_view table;
    std::string_view key;
    Tidword tid;
  };

  struct RangeEntry {
    std::string_view table;
    std::string_view index;  ///< Empty marks a primary-index range.
    std::string_view begin;
    std::string_view end;
    uint64_t limit;
    bool reverse;
    std::vector<std::string_view> keys;
    std::vector<std::string_view> primary_keys;
  };

  struct RowUpdate {
    pax::Row row;  ///< Decoded value; unused for DELETE.
    /// The table's PAX store, checked non-null at Write.
    pax::PaxTable *store;
    RowOp op;
    bool check_absent;  ///< The record's first operation was INSERT.
  };

  struct IndexUpdate {
    struct Delta {
      std::string_view primary_key;
      wal::SecondaryIndexOp op;
    };
    IndexConstraint constraint;
    std::vector<Delta> deltas;         ///< Request order, logged as is.
    PrimaryKeyList::Ptr primary_keys;  ///< Final list, built under the lock.
  };

  struct WriteEntry {
    Table *table;
    std::string_view index_name;  ///< Empty for a primary record.
    std::string_view key;
    index::MasstreeIndex *index;  ///< The tree holding the record.
    bool owns_lock = false;
    std::variant<RowUpdate, IndexUpdate> update;
  };

  TableDictionary &tables_;
  epoch::Framework &epoch_;
  index::Reaper &reaper_;
  wal::Logger &logger_;
  Tidword &last_commit_tid_;  ///< This worker's, kept across attempts.

  std::vector<ReadEntry> read_set_;
  std::vector<RangeEntry> range_set_;
  std::map<DataItem *, WriteEntry> write_set_;  ///< Locked in pointer order.

  /**
   * @brief Reports whether this attempt holds the record's lock.
   *
   * @details Read validation and both range replays allow a locked word only
   * when this attempt is the holder; any other lock aborts.
   */
  bool OwnsLock(DataItem *item) const;

  /**
   * @brief Locks the record and checks that its index still points to it.
   *
   * @param[in,out] max_tid Raised to the word observed under the lock.
   * @return false with reason set on detachment; the lock is held and
   * UnlockAll releases it.
   */
  bool Lock(DataItem &item, WriteEntry &entry, Tidword &max_tid,
            std::string &reason);

  void UnlockAll();

  // Releases the locks and the epoch and always returns false, for an abort
  // past the join.
  bool Abort();

  /**
   * @brief Checks the INSERT or UNIQUE constraint and builds the final list.
   *
   * @pre The record is locked and no stored value has changed.
   * @return false with reason set when the constraint fails.
   */
  bool Prepare(DataItem &item, WriteEntry &entry, std::string &reason);

  /**
   * @brief Checks every point word and replays every range.
   *
   * @param[in,out] max_tid Raised to the largest word validation observed.
   * @return false with reason set at the first check that fails.
   */
  bool ValidateReads(Tidword &max_tid, std::string &reason);

  bool ReplayRange(const RangeEntry &range, Tidword &max_tid);
  bool ReplayIndexRange(const RangeEntry &range, Tidword &max_tid);

  /**
   * @brief Installs the final value while the record stays locked.
   *
   * @pre Validation, preparation and slot reservation all succeeded.
   */
  static void Apply(DataItem &item, const WriteEntry &entry, EpochNumber epoch);

  /**
   * @brief Copies the installed row, or the ordered index changes, into the
   * record.
   *
   * @pre Apply completed and the record is still locked.
   */
  static void AppendLog(wal::LogRecord &record, const DataItem &item,
                        const WriteEntry &entry, Tidword commit_tid);

  /**
   * @brief Publishes the TID, which unlocks the record, and queues it when it
   * holds nothing.
   *
   * @pre Apply and AppendLog completed for this record.
   */
  void Publish(DataItem &item, WriteEntry &entry, Tidword commit_tid);
};

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_LINEAIRDB_TRANSACTION_H
