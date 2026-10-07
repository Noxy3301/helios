/**
 * @file server/storage/include/helios/transaction.h
 * One commit attempt: the write operations and durability a request submits,
 * and the Silo protocol that validates and installs its read and write sets.
 */

#ifndef HELIOS_STORAGE_INCLUDE_HELIOS_TRANSACTION_H
#define HELIOS_STORAGE_INCLUDE_HELIOS_TRANSACTION_H

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "helios/index.h"

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
  /**
   * @brief An index record a range scan visited and the TID word it read.
   *
   * @details The key is borrowed, like every RangeRead input.
   */
  struct RangeRecord {
    std::string_view key;
    Tidword tid;
  };

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
   * @brief Sizes the point-read and range sets for the counts about to be
   * fed.
   */
  void reserve(size_t reads, size_t ranges);

  /**
   * @brief Records a point read to revalidate: the key and the word it
   * returned.
   *
   * @details A key that had no record carries the absent word. Commit aborts
   * when the record's word moved: a new version, a row that appeared or
   * disappeared, or another committer's lock. A table that does not exist at
   * commit is accepted only with the word 0 a read of a missing table returns.
   *
   * A nonzero `column_mask` names the PAX fields the read used, bit
   * `min(f, 63)` for field `f`. Commit then may accept a moved word, but only
   * when all of these hold:
   * - the row exists, as it did at the read, and no committer holds its lock,
   *   so a row this attempt writes is validated whole;
   * - the PAX group's column TIDs of those fields and of the null flags do
   *   not exceed the read word.
   *
   * An install into the row after the read carries a larger TID, so the last
   * condition rules out one that inserted the row or changed or assigned
   * those fields. `observed` must come from a read in this storage run, since
   * column TIDs start at zero at each startup.
   */
  void Read(std::string_view table, std::string_view key, Tidword observed,
            uint64_t column_mask = 0);

  /**
   * @brief Records a range read to revalidate at commit.
   *
   * @details The bounds, index, limit and direction describe the scan to
   * re-run over `[begin, end)`. An empty `index` names the primary index, and
   * a `limit` of 0 caps nothing. `rows` and `visited` are the rows that scan
   * returned and the records it visited without returning them, in scan
   * order. Commit aborts unless the re-scan reaches the same records with the
   * same words, apart from tombstones the reaper purged. Each row the caller
   * consumed is also a Read. Commit refuses a range whose `end` is empty.
   */
  void RangeRead(std::string_view table, std::string_view index,
                 std::string_view begin, std::string_view end, uint64_t limit,
                 bool reverse, std::vector<RangeRecord> rows,
                 std::vector<RangeRecord> visited);

  /**
   * @brief Unpacks the row and merges it into its record's pending update.
   *
   * @details `row_bytes` holds packed bytes matching the table's installed PAX
   * schema, and is ignored for a kDelete. Later writes to one key replace the
   * value; if the record's first operation was INSERT, Commit keeps checking
   * that the row is absent. `column_mask` names the PAX fields the write
   * assigned, bit `min(f, 63)` for field `f`, and the masks of later writes
   * to one key accumulate.
   * @return false with reason `write_table_missing`, `pax_schema_missing`,
   * `pax_row_unpack_failed`, or @ref kDuplicatePrimaryKeyAbortReason for an
   * INSERT after a pending live row. Nothing is claimed on false.
   */
  bool Write(std::string_view table_name, std::string_view key,
             std::string_view row_bytes, RowOp op, std::string &reason,
             uint64_t column_mask = 0);

  /**
   * @brief Appends one primary-key addition or removal to a secondary record.
   *
   * @details With `remove` true, `primary_key` leaves the secondary key's
   * list; otherwise it joins it. Uniqueness belongs to the named index, not to
   * this call. A UNIQUE index keeps one record per secondary key; any other
   * keeps one per pair, keyed by `secondary_key` followed by `primary_key`.
   * @return false with reason `secondary_index_table_missing`,
   * `pax_schema_missing`, `secondary_index_missing`, or
   * `secondary_key_too_long` when a pair's key exceeds the index's limit.
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
   * - Phase 2: validate every point read by word, revalidate every range and
   *   compare the records it visits, check INSERT and UNIQUE under the locks,
   *   then choose the commit TID. Reserve every PAX slot before the first
   *   value changes.
   * - Phase 3: install each record, append its WAL write, and publish its
   *   TID, which unlocks it.
   *
   * The log record is enqueued before the worker leaves the epoch. A Sync
   * commit then waits for that epoch to become durable, and only when a log
   * record was enqueued: a read-only commit returns at once.
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
    uint64_t column_mask;  ///< 0 validates the whole row.
  };

  struct RangeEntry {
    std::string_view table;
    std::string_view index;  ///< Empty marks a primary-index range.
    std::string_view begin;
    std::string_view end;
    uint64_t limit;
    bool reverse;
    std::vector<RangeRecord> rows;
    std::vector<RangeRecord> visited;
  };

  struct RowUpdate {
    pax::Row row;  ///< Unpacked value; unused for DELETE.
    /// The table's PAX store, checked non-null at Write.
    pax::PaxTable *store;
    RowOp op;
    bool check_absent;  ///< The record's first operation was INSERT.
    /// The row the log records, as the request sent it, which the caller
    /// keeps alive until Commit returns. Empty for DELETE.
    std::string_view bytes;
    uint64_t column_mask;  ///< PAX fields the record's writes assigned.
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
    /// The key the log records: the primary key, or the secondary key,
    /// without the primary key a non-unique record's tree key ends with.
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
   * @details Read validation and both range revalidations allow a locked word
   * only when this attempt is the holder; any other lock aborts.
   */
  bool OwnsLock(DataItem *item) const;

  /**
   * @brief Locks the record and checks that it is still the latest.
   *
   * @details The reaper clears the latest bit when it unlinks a record, so a
   * locked record with the bit set is the one its index holds for the key.
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
   * @brief Checks every point word and revalidates every range.
   *
   * @param[in,out] max_tid Raised to the largest word validation observed.
   * @return false with reason set at the first check that fails.
   */
  bool ValidateReads(Tidword &max_tid, std::string &reason);

  bool RevalidateRange(const RangeEntry &range, Tidword &max_tid);
  bool RevalidateSecondaryRange(const RangeEntry &range, Tidword &max_tid);

  /**
   * @brief Matches a record a range re-scan reached against the next one the
   * read-phase scan recorded.
   *
   * @details A live row, primary or base, matches the next of `rows`; any
   * other record, a secondary entry included, the next of `visited`. A blank
   * record, or one the reaper unlinked (latest clear), counts as not reached.
   * Recorded tombstones the re-scan passed without reaching are skipped as
   * purged. Only this attempt's own lock may differ from the recorded word.
   *
   * @param range The range being revalidated.
   * @param[in,out] row The next of `rows`, advanced past a match.
   * @param[in,out] visited The next of `visited`, advanced past a match.
   * @param key The reached record's key.
   * @param item The reached record, checked for this attempt's lock.
   * @param tid The word the re-scan loaded from `item`.
   * @param entry The record is a secondary entry.
   * @param[in,out] max_tid Raised to the matched word.
   * @return false when another committer holds the record or it differs from
   * the next recorded one.
   */
  bool match_record(const RangeEntry &range, size_t &row, size_t &visited,
                    std::string_view key, DataItem *item, Tidword tid,
                    bool entry, Tidword &max_tid) const;

  /**
   * @brief Installs the final value while the record stays locked.
   *
   * @details A row update raises the column TIDs of its PAX group to
   * `commit_tid` for the fields it changed or assigned, the null flags
   * included. An insert raises only the column TID of the null flags.
   *
   * @pre Validation, preparation and slot reservation all succeeded.
   */
  static void Apply(DataItem &item, const WriteEntry &entry,
                    Tidword commit_tid);

  /**
   * @brief Packs the row the request sent, or the ordered index changes,
   * into the log record as LogRecord::Write elements.
   *
   * @pre Apply completed and the index record is still locked.
   */
  static void AppendLog(msgpack::packer<wal::PackedLogRecord> &pk,
                        const DataItem &item, const WriteEntry &entry,
                        Tidword commit_tid);

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

#endif  // HELIOS_STORAGE_INCLUDE_HELIOS_TRANSACTION_H
