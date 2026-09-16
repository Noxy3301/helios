/**
 * @file server/storage/tests/commit_tid_test.cc
 * Transaction-wide TIDs and worker ordering.
 */

#include <algorithm>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "lineairdb/config.h"
#include "lineairdb/database.h"

#include "db_helper.h"
#include "gtest/gtest.h"
#include "index/reaper.h"
#include "pax/epoch_image_buffer.h"
#include "silo/stable_read.h"
#include "lineairdb/transaction.h"
#include "table/table_dictionary.h"
#include "util/epoch_framework.h"
#include "wal/logger.h"
#include "wal/wal.h"

namespace {

using namespace helios::storage;
constexpr const char *kTable = "rows";

Tidword Version(EpochNumber epoch, uint32_t tid) {
  Tidword word;
  word.epoch = epoch;
  word.tid = tid;
  word.latest = true;
  return word;
}

Tidword Deleted(EpochNumber epoch, uint32_t tid) {
  Tidword word = Version(epoch, tid);
  word.absent = true;
  return word;
}

class CommitTidTest : public ::testing::Test {
 protected:
  Config config_;
  TableDictionary tables_;
  epoch::Framework epoch_;
  index::Reaper reaper_;
  std::unique_ptr<wal::Logger> logger_;
  Tidword last_tid_;
  std::string reason_;

  void SetUp() override {
    config_.work_dir = "./helios_commit_tid_test_logs";
    config_.wal_initial_capacity_bytes = 1u << 20;
    std::filesystem::remove_all(config_.work_dir);
    ASSERT_TRUE(tables_.CreateTable(kTable));
    pax::TableSchema schema;
    schema.field_max_bytes = {1, 64};
    ASSERT_TRUE(tables_.GetTable(kTable)->InstallPaxSchema(schema));
    ASSERT_TRUE(tables_.GetTable(kTable)->CreateSecondaryIndex(
        "idx", IndexConstraint::kNone));
    // Leave the ticker parked so tests control epoch boundaries exactly.
    epoch_.SetGlobalEpoch(10);
    logger_ = std::make_unique<wal::Logger>(config_);
    ASSERT_EQ(wal::Logger::RecoveryStatus::kOk, logger_->Recover().status);
    logger_->Start();
  }

  void TearDown() override {
    if (logger_) {
      logger_->RequestFlush(epoch_.GetGlobalEpoch());
      logger_.reset();
    }
    epoch_.Start();
    epoch_.Stop();
    index::MasstreeReleaseThreadEpoch();
    std::filesystem::remove_all(config_.work_dir);
  }

  DataItem *SeedRow(const std::string &key, Tidword tid) {
    auto *table = tables_.GetTable(kTable);
    auto *item = table->GetPrimaryIndex().GetOrInsert(key);
    const std::string bytes = TestHelper::Row(key);
    pax::Row row;
    EXPECT_TRUE(pax::DecodeRow(
        table->GetPaxTable()->schema(),
        reinterpret_cast<const std::byte *>(bytes.data()), bytes.size(), row));
    EXPECT_TRUE(item->AllocateSlot(*table->GetPaxTable()));
    item->InstallRow(row, tid.epoch);
    item->transaction_id.store(tid);
    return item;
  }

  bool Commit(const std::vector<TestHelper::PointRead> &reads,
              const std::vector<TestHelper::RowWrite> &writes,
              const std::vector<TestHelper::IndexOp> &index_ops = {},
              const std::vector<TestHelper::Range> &ranges = {}) {
    reason_.clear();
    // The packed rows outlive the attempt; the transaction borrows their bytes.
    std::vector<TestHelper::RowWrite> rows = writes;
    for (auto &row : rows) row.value = TestHelper::Row(row.value);
    silo::Transaction tx(tables_, epoch_, reaper_, *logger_, last_tid_);
    const bool committed =
        TestHelper::Feed(tx, reads, ranges, rows, index_ops, reason_) &&
        tx.Commit(CommitDurability::kAsync, reason_);
    index::MasstreeReleaseThreadEpoch();
    return committed;
  }
};

TEST_F(CommitTidTest, CommitReadsTheEpochAfterTheWriteSetIsFed) {
  auto *item = SeedRow("key", Version(10, 5));
  const std::string bytes = TestHelper::Row("next");

  silo::Transaction tx(tables_, epoch_, reaper_, *logger_, last_tid_);
  ASSERT_TRUE(tx.Write(kTable, "key", bytes, RowOp::kUpdate, reason_))
      << reason_;
  epoch_.SetGlobalEpoch(11);
  ASSERT_TRUE(tx.Commit(CommitDurability::kAsync, reason_)) << reason_;
  EXPECT_EQ(11u, last_tid_.epoch);
  EXPECT_EQ(epoch::Framework::kThreadOffline, epoch_.ThreadEpoch());
  EXPECT_EQ(last_tid_, item->transaction_id.load());
  EXPECT_EQ(bytes, item->CopyValue());

  // A worker TID from a later epoch cannot be ordered by this one.
  const auto published = last_tid_;
  last_tid_ = Version(12, 0);
  silo::Transaction stale(tables_, epoch_, reaper_, *logger_, last_tid_);
  ASSERT_TRUE(stale.Write(kTable, "key", bytes, RowOp::kUpdate, reason_))
      << reason_;
  EXPECT_FALSE(stale.Commit(CommitDurability::kAsync, reason_));
  EXPECT_EQ("commit_epoch_stale", reason_);
  EXPECT_EQ(Version(12, 0), last_tid_);
  EXPECT_EQ(epoch::Framework::kThreadOffline, epoch_.ThreadEpoch());
  EXPECT_EQ(published, item->transaction_id.load());
  EXPECT_FALSE(item->transaction_id.load().lock);
  index::MasstreeReleaseThreadEpoch();

  // Only the first commit reached the log; the stale attempt logged nothing.
  logger_->RequestFlush(11);
  logger_.reset();
  wal::Wal log(config_.work_dir);
  const auto scan = log.Scan();
  ASSERT_EQ(wal::WalScanResult::Status::kOk, scan.status);
  ASSERT_EQ(1u, scan.records.size());
  ASSERT_EQ(1u, scan.records.front().writes.size());
  EXPECT_EQ(published, scan.records.front().writes.front().transaction_id);
}

TEST_F(CommitTidTest, CommitJoinsTheEpochAfterItsLocksAreHeld) {
  auto *a = SeedRow("a", Version(10, 5));
  auto *b = SeedRow("b", Version(10, 5));
  const std::string bytes_a = TestHelper::Row("next_a");
  const std::string bytes_b = TestHelper::Row("next_b");
  // The write set locks in DataItem pointer order, so the committer takes
  // this one first and then waits for the other.
  DataItem *first = std::less<DataItem *>{}(a, b) ? a : b;
  DataItem *second = first == a ? b : a;

  // Another committer holds the record this attempt locks second.
  Tidword held = Version(10, 5);
  held.lock = true;
  second->transaction_id.store(held);

  bool committed = false;
  std::thread committer([&] {
    silo::Transaction tx(tables_, epoch_, reaper_, *logger_, last_tid_);
    EXPECT_TRUE(tx.Write(kTable, "a", bytes_a, RowOp::kUpdate, reason_))
        << reason_;
    EXPECT_TRUE(tx.Write(kTable, "b", bytes_b, RowOp::kUpdate, reason_))
        << reason_;
    EXPECT_EQ(epoch::Framework::kThreadOffline, epoch_.ThreadEpoch());
    committed = tx.Commit(CommitDurability::kAsync, reason_);
    EXPECT_EQ(epoch::Framework::kThreadOffline, epoch_.ThreadEpoch());
    index::MasstreeReleaseThreadEpoch();
  });

  // The committer holds the first record and is waiting on the second
  while (!first->transaction_id.load().lock) std::this_thread::yield();
  epoch_.SetGlobalEpoch(11);
  second->transaction_id.store(Version(10, 5));
  committer.join();

  ASSERT_TRUE(committed) << reason_;
  EXPECT_EQ(11u, last_tid_.epoch);
  EXPECT_EQ(last_tid_, a->transaction_id.load());
  EXPECT_EQ(last_tid_, b->transaction_id.load());
  EXPECT_FALSE(a->transaction_id.load().lock);
  EXPECT_FALSE(b->transaction_id.load().lock);
}

TEST_F(CommitTidTest, FinalRowKeepsTheFirstInsertRequirement) {
  auto *item = SeedRow("key", Version(10, 5));
  EXPECT_FALSE(Commit({}, {{kTable, "key", "new", RowOp::kInsert},
                          {kTable, "key", "", RowOp::kDelete}}));
  EXPECT_EQ(kDuplicatePrimaryKeyAbortReason, reason_);
  EXPECT_EQ(Tidword{}, last_tid_);
  EXPECT_EQ(Version(10, 5), item->transaction_id.load());
  EXPECT_EQ(TestHelper::Row("key"), item->CopyValue());

  ASSERT_TRUE(Commit({}, {{kTable, "key", "", RowOp::kDelete},
                         {kTable, "key", "final", RowOp::kInsert}}))
      << reason_;
  EXPECT_EQ(TestHelper::Row("final"), item->CopyValue());
}

TEST_F(CommitTidTest, RepeatedRowWritesLogOnlyTheFinalValue) {
  ASSERT_TRUE(Commit({}, {{kTable, "a", "first"}, {kTable, "b", "second"},
                         {kTable, "a", "", RowOp::kDelete},
                         {kTable, "a", "last", RowOp::kInsert}}))
      << reason_;
  logger_->RequestFlush(10);
  logger_.reset();
  wal::Wal log(config_.work_dir);
  const auto scan = log.Scan();
  ASSERT_EQ(wal::WalScanResult::Status::kOk, scan.status);
  ASSERT_EQ(1u, scan.records.size());
  ASSERT_EQ(2u, scan.records.front().writes.size());
  const auto &writes = scan.records.front().writes;
  for (const auto &key : {"a", "b"}) {
    const auto write =
        std::find_if(writes.begin(), writes.end(),
                     [&](const auto &entry) { return entry.key == key; });
    ASSERT_NE(writes.end(), write);
    EXPECT_EQ(TestHelper::Row(std::string(key) == "a" ? "last" : "second"),
              write->buffer);
  }
}

TEST_F(CommitTidTest, RepeatedRowWritesKeepTheEpochImageFromBeforeTheCommit) {
  auto *item = SeedRow("key", Version(10, 5));
  auto &buffer = pax::EpochImageBuffer::Global();
  const auto token = buffer.Open();
  ASSERT_TRUE(token.valid);
  const bool committed = Commit({}, {{kTable, "key", "intermediate"},
                                    {kTable, "key", "", RowOp::kDelete},
                                    {kTable, "key", "final"}});
  const auto images = buffer.SlotImages(item->pax_group(), item->pax_slot());
  buffer.Close(token);
  ASSERT_TRUE(committed) << reason_;
  ASSERT_EQ(1u, images.size());
  EXPECT_TRUE(images.front().was_visible);
  EXPECT_EQ(TestHelper::Row("key"), images.front().old_row);
  EXPECT_EQ(TestHelper::Row("final"), item->CopyValue());
}

TEST_F(CommitTidTest, FinalDeleteDoesNotAllocateAnIntermediateRow) {
  const auto *store = tables_.GetTable(kTable)->GetPaxTable();
  const auto allocated = pax::SlotsAllocated(store);
  ASSERT_TRUE(Commit({}, {{kTable, "new", "temporary"},
                         {kTable, "new", "", RowOp::kDelete}}))
      << reason_;
  EXPECT_EQ(allocated, pax::SlotsAllocated(store));
  auto *item = tables_.GetTable(kTable)->GetPrimaryIndex().Get("new");
  ASSERT_NE(nullptr, item);
  EXPECT_TRUE(item->transaction_id.load().absent);
  EXPECT_FALSE(item->transaction_id.load().lock);
}

TEST_F(CommitTidTest, OneTidCoversReadsRowsIndexesAndTheWorker) {
  auto *a = SeedRow("a", Version(10, 8));
  auto *b = SeedRow("b", Version(10, 24));
  auto *read = SeedRow("read", Version(10, 80));
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("group");
  posting->SetPrimaryKeys({"a"});
  posting->transaction_id.store(Version(10, 40));
  last_tid_ = Version(10, 60);

  ASSERT_TRUE(Commit({{kTable, "read", Version(10, 80).obj}},
                     {{kTable, "a", "first"}, {kTable, "b", "second"}},
                     {{kTable, "idx", "group", "b", false}}))
      << reason_;
  EXPECT_EQ(Version(10, 81), last_tid_);
  EXPECT_EQ(last_tid_, a->transaction_id.load());
  EXPECT_EQ(last_tid_, b->transaction_id.load());
  EXPECT_EQ(last_tid_, posting->transaction_id.load());
  EXPECT_EQ(Version(10, 80), read->transaction_id.load());

  // New records must still advance this worker's previous commit TID.
  ASSERT_TRUE(Commit({}, {{kTable, "next", "third"}})) << reason_;
  EXPECT_EQ(Version(10, 82), last_tid_);

  logger_->RequestFlush(10);
  logger_.reset();
  wal::Wal log(config_.work_dir);
  const auto scan = log.Scan();
  ASSERT_EQ(wal::WalScanResult::Status::kOk, scan.status);
  ASSERT_EQ(2u, scan.records.size());
  ASSERT_EQ(3u, scan.records[0].writes.size());
  for (const auto &write : scan.records[0].writes) {
    EXPECT_EQ(Version(10, 81), write.transaction_id);
  }
  ASSERT_EQ(1u, scan.records[1].writes.size());
  EXPECT_EQ(last_tid_, scan.records[1].writes[0].transaction_id);
}

TEST_F(CommitTidTest, WrittenRowsAndIndexesContributeTheirPreviousVersions) {
  auto *row = SeedRow("a", Version(10, 500));
  ASSERT_TRUE(Commit({}, {{kTable, "a", "value"}})) << reason_;
  EXPECT_EQ(Version(10, 501), row->transaction_id.load());

  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("group");
  posting->SetPrimaryKeys({"a"});
  posting->transaction_id.store(Version(10, 700));
  ASSERT_TRUE(Commit({}, {{kTable, "b", "value"}},
                     {{kTable, "idx", "group", "b", false}}))
      << reason_;
  EXPECT_EQ(Version(10, 701), last_tid_);
  EXPECT_EQ(last_tid_, posting->transaction_id.load());
}

TEST_F(CommitTidTest, MissingRangeEndAbortsBeforeLocking) {
  TestHelper::Range range;
  range.table_name = kTable;
  EXPECT_FALSE(Commit({}, {{kTable, "new", "value"}}, {}, {range}));
  EXPECT_EQ("range_end_key_missing", reason_);
  EXPECT_EQ(Tidword{}, last_tid_);
  EXPECT_EQ(epoch::Framework::kThreadOffline, epoch_.ThreadEpoch());

  // The write claimed its record before the range was checked; it stays
  // absent and unlocked, as after any abort.
  auto *item = tables_.GetTable(kTable)->GetPrimaryIndex().Get("new");
  ASSERT_NE(nullptr, item);
  EXPECT_EQ(Tidword::Absent(), item->transaction_id.load());
}

TEST_F(CommitTidTest, UniqueIndexRefusesASecondAdditionInOneRequest) {
  ASSERT_TRUE(tables_.GetTable(kTable)->CreateSecondaryIndex(
      "uidx", IndexConstraint::kUnique));
  silo::Transaction tx(tables_, epoch_, reaper_, *logger_, last_tid_);
  EXPECT_TRUE(tx.IndexWrite(kTable, "uidx", "s", "a", false, reason_))
      << reason_;
  EXPECT_TRUE(tx.IndexWrite(kTable, "uidx", "s", "a", true, reason_))
      << reason_;
  EXPECT_FALSE(tx.IndexWrite(kTable, "uidx", "s", "b", false, reason_));
  EXPECT_EQ(
      std::string(kDuplicateSecondaryKeyAbortPrefix) + "duplicate_in_request",
      reason_);
  index::MasstreeReleaseThreadEpoch();
}

TEST_F(CommitTidTest, PointReadFailureUsesFixedReasonForBinaryKey) {
  const std::string key("a\0\xff", 3);
  auto *item = SeedRow(key, Version(10, 20));
  EXPECT_FALSE(Commit({{kTable, key, Version(10, 19).obj}},
                      {{kTable, key, "next"}}));
  EXPECT_EQ("exact_read_tid_moved", reason_);
  EXPECT_EQ(Version(10, 20), item->transaction_id.load());
  EXPECT_EQ(TestHelper::Row(key), item->CopyValue());
}

TEST_F(CommitTidTest, SecondaryRangeFailureKeepsItsAbortReason) {
  TestHelper::Range range;
  range.table_name = kTable;
  range.index_name = "idx";
  range.end_key = "z";
  range.result_keys = {"missing"};
  range.result_primary_keys = {"key"};
  EXPECT_FALSE(Commit({}, {}, {}, {range}));
  EXPECT_EQ("secondary_range_result_changed", reason_);
  EXPECT_EQ(epoch::Framework::kThreadOffline, epoch_.ThreadEpoch());
}

TEST_F(CommitTidTest, MissingTableReadAcceptsOnlyZeroTid) {
  EXPECT_TRUE(Commit({{"missing", "key", 0}}, {})) << reason_;
  EXPECT_FALSE(Commit({{"missing", "key", Tidword::Absent().obj}}, {}));
  EXPECT_EQ(reason_, "read_table_missing");
  auto *item = SeedRow("key", Version(10, 20));
  EXPECT_FALSE(Commit({{"missing", "key", Tidword::Absent().obj}},
                      {{kTable, "key", "next"}}));
  EXPECT_EQ(reason_, "read_table_missing");
  EXPECT_EQ(Version(10, 20), item->transaction_id.load());
  EXPECT_EQ(TestHelper::Row("key"), item->CopyValue());
}

TEST_F(CommitTidTest, MissingTableReadAbortsAfterTableCreation) {
  ASSERT_TRUE(tables_.CreateTable("created"));
  EXPECT_FALSE(Commit({{"created", "key", 0}}, {}));
  EXPECT_EQ(reason_, "exact_read_tid_moved");
}

TEST_F(CommitTidTest, DuplicateReadsOfTheSameVersionValidate) {
  const auto tid = Version(10, 20);
  SeedRow("key", tid);
  EXPECT_TRUE(Commit({{kTable, "key", tid.obj}, {kTable, "key", tid.obj}}, {}))
      << reason_;
}

TEST_F(CommitTidTest, DuplicateReadsValidateThroughOwnWriteLock) {
  const auto tid = Version(10, 20);
  auto *item = SeedRow("key", tid);
  ASSERT_TRUE(Commit({{kTable, "key", tid.obj}, {kTable, "key", tid.obj}},
                     {{kTable, "key", "next"}}))
      << reason_;
  EXPECT_EQ(TestHelper::Row("next"), item->CopyValue());
  EXPECT_FALSE(item->transaction_id.load().lock);
}

TEST_F(CommitTidTest, DifferentReadVersionsAbortInEitherInputOrder) {
  const auto old_tid = Version(10, 19);
  const auto current_tid = Version(10, 20);
  auto *item = SeedRow("key", current_tid);
  for (bool reverse : {false, true}) {
    const auto first = reverse ? current_tid : old_tid;
    const auto second = reverse ? old_tid : current_tid;
    const std::vector<TestHelper::PointRead> reads = {
        {kTable, "key", first.obj}, {kTable, "key", second.obj}};
    EXPECT_FALSE(Commit(reads, {}));
    EXPECT_FALSE(Commit(reads, {{kTable, "key", "next"}}));
    EXPECT_EQ(current_tid, item->transaction_id.load());
    EXPECT_EQ(TestHelper::Row("key"), item->CopyValue());
  }
}

TEST_F(CommitTidTest, ReadOnlyCommitAdvancesWorkerOrdering) {
  SeedRow("high", Version(10, 200));
  ASSERT_TRUE(Commit({{kTable, "high", Version(10, 200).obj}}, {})) << reason_;
  EXPECT_EQ(Version(10, 201), last_tid_);
  ASSERT_TRUE(Commit({}, {{kTable, "new", "value"}})) << reason_;
  EXPECT_EQ(Version(10, 202), last_tid_);

  // The read-only commit buffered nothing; only the write reached the log.
  logger_->RequestFlush(10);
  logger_.reset();
  wal::Wal log(config_.work_dir);
  const auto scan = log.Scan();
  ASSERT_EQ(wal::WalScanResult::Status::kOk, scan.status);
  ASSERT_EQ(1u, scan.records.size());
  EXPECT_EQ(1u, scan.records.front().writes.size());
}

TEST_F(CommitTidTest, SecondaryRangeReadContributesIndexAndRowVersions) {
  auto *row = SeedRow("a", Version(10, 10));
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("group");
  posting->SetPrimaryKeys({"a"});
  posting->transaction_id.store(Version(10, 300));
  TestHelper::Range range;
  range.table_name = kTable;
  range.index_name = "idx";
  range.start_key = "group";
  range.end_key = "grouq";
  range.result_keys = {"group"};
  range.result_primary_keys = {"a"};

  ASSERT_TRUE(Commit({}, {{kTable, "unrelated", "value"}}, {}, {range}))
      << reason_;
  EXPECT_EQ(Version(10, 301), last_tid_);

  row->transaction_id.store(Version(10, 500));
  ASSERT_TRUE(Commit({}, {{kTable, "unrelated", "value"}}, {}, {range}))
      << reason_;
  EXPECT_EQ(Version(10, 501), last_tid_);
}

TEST_F(CommitTidTest, PrimaryRangeReadContributesItsVersionWithoutPointReads) {
  SeedRow("high", Version(10, 400));
  TestHelper::Range range;
  range.table_name = kTable;
  range.start_key = "high";
  range.end_key = "higi";
  range.result_keys = {"high"};
  ASSERT_TRUE(Commit({}, {{kTable, "other", "value"}}, {}, {range})) << reason_;
  EXPECT_EQ(Version(10, 401), last_tid_);
}

TEST_F(CommitTidTest, FullTidRangeAllowsPurgeAndRejectsCommitWrap) {
  constexpr uint32_t max_commit = silo::kMaxTid;
  last_tid_ = Version(10, max_commit - 1);
  auto *item = SeedRow("key", Version(10, 2));
  ASSERT_TRUE(Commit({}, {{kTable, "key", "", RowOp::kDelete}})) << reason_;
  EXPECT_EQ(Deleted(10, max_commit), item->transaction_id.load());

  EXPECT_FALSE(Commit({}, {{kTable, "blocked", "value"}}));
  EXPECT_EQ("commit_tid_exhausted", reason_);
  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  auto *blocked = tree.Get("blocked");
  ASSERT_NE(nullptr, blocked);
  EXPECT_EQ(Tidword::Absent(), blocked->transaction_id.load());

  // This thread pins the old item while another thread purges it.
  auto *retained = tree.Get("key");
  ASSERT_EQ(item, retained);
  std::thread reap([&] { reaper_.Purge(10); });
  reap.join();

  Tidword retired = Deleted(10, max_commit);
  retired.latest = false;
  EXPECT_EQ(retired, retained->transaction_id.load());
  ASSERT_FALSE(retained->transaction_id.load().lock);
  const auto read = silo::StableRead(*retained);
  EXPECT_FALSE(read.found);
  EXPECT_EQ(retired, read.tid);
  EXPECT_EQ(nullptr, tree.Get("key"));
  epoch_.SetGlobalEpoch(12);
  ASSERT_TRUE(Commit({}, {{kTable, "blocked", "value"}})) << reason_;
  EXPECT_EQ(Version(12, 0), last_tid_);
}

TEST_F(CommitTidTest, FutureEpochAbortsAndReleasesTheWriteLock) {
  auto *item = SeedRow("key", Version(11, 50));
  EXPECT_FALSE(Commit({}, {{kTable, "key", "value"}}));
  EXPECT_EQ("commit_epoch_stale", reason_);
  EXPECT_EQ(Version(11, 50), item->transaction_id.load());
  EXPECT_EQ(Tidword(), last_tid_);
  epoch_.SetGlobalEpoch(11);
  ASSERT_TRUE(Commit({}, {{kTable, "key", "value"}})) << reason_;
  EXPECT_EQ(Version(11, 51), last_tid_);
}

TEST_F(CommitTidTest, AbsentReadAcceptsABlankRecordAnotherTransactionLeft) {
  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  // An aborted attempt leaves the key materialized but still absent.
  ASSERT_NE(nullptr, tree.GetOrInsert("blank"));
  ASSERT_TRUE(Commit({{kTable, "blank", Tidword::Absent().obj}},
                     {{kTable, "other", "value"}}))
      << reason_;
}

TEST_F(CommitTidTest, AbsentReadAbortsWhileAnotherCommitterHoldsTheRecord) {
  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  DataItem *claimed = tree.GetOrInsert("claimed");
  // Another committer is midway through inserting the key.
  Tidword held = Tidword::Absent();
  held.lock = true;
  claimed->transaction_id.store(held);

  EXPECT_FALSE(Commit({{kTable, "claimed", Tidword::Absent().obj}},
                      {{kTable, "other", "value"}}));
  EXPECT_EQ(0u, reason_.rfind("exact_read_tid_moved", 0)) << reason_;
  DataItem *other = tree.Get("other");
  ASSERT_NE(nullptr, other);
  EXPECT_FALSE(other->transaction_id.load().lock);

  claimed->transaction_id.store(Tidword::Absent());
}

TEST_F(CommitTidTest, AbsentReadThenInsertOfTheSameKeyValidatesThroughOwnLock) {
  ASSERT_TRUE(Commit({{kTable, "fresh", Tidword::Absent().obj}},
                     {{kTable, "fresh", "value", RowOp::kInsert}}))
      << reason_;
  auto *item = tables_.GetTable(kTable)->GetPrimaryIndex().Get("fresh");
  ASSERT_NE(nullptr, item);
  EXPECT_EQ(last_tid_, item->transaction_id.load());
  EXPECT_FALSE(item->transaction_id.load().absent);
}

TEST_F(CommitTidTest, ReadThenWriteOfOneKeyValidatesThroughOwnLock) {
  auto *item = SeedRow("k", Version(10, 20));
  ASSERT_TRUE(
      Commit({{kTable, "k", Version(10, 20).obj}}, {{kTable, "k", "next"}}))
      << reason_;
  EXPECT_EQ(Version(10, 21), item->transaction_id.load());
}

TEST_F(CommitTidTest, RangeReplayAllowsOwnLockOnPrimaryAndSecondary) {
  SeedRow("k", Version(10, 10));
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("s");
  posting->SetPrimaryKeys({"k"});
  posting->transaction_id.store(Version(10, 12));

  TestHelper::Range rows;
  rows.table_name = kTable;
  rows.start_key = "k";
  rows.end_key = "l";
  rows.result_keys = {"k"};
  TestHelper::Range postings;
  postings.table_name = kTable;
  postings.index_name = "idx";
  postings.start_key = "s";
  postings.end_key = "t";
  postings.result_keys = {"s"};
  postings.result_primary_keys = {"k"};

  ASSERT_TRUE(Commit({}, {{kTable, "k", "next"}, {kTable, "k2", "new"}},
                     {{kTable, "idx", "s", "k2", false}}, {rows, postings}))
      << reason_;
}

TEST_F(CommitTidTest, AbsentReadAbortsWhenTheKeyWasDeletedMeanwhile) {
  // Deleting a key with no row still publishes a word on its blank record.
  ASSERT_TRUE(Commit({}, {{kTable, "gone", "", RowOp::kDelete}})) << reason_;

  EXPECT_FALSE(Commit({{kTable, "gone", Tidword::Absent().obj}},
                      {{kTable, "other", "value"}}));
  EXPECT_EQ(0u, reason_.rfind("exact_read_tid_moved", 0)) << reason_;
}

TEST_F(CommitTidTest, AbsentEvidenceAbortsAfterThePurge) {
  auto *item = SeedRow("k", Version(10, 4));
  ASSERT_TRUE(Commit({}, {{kTable, "k", "", RowOp::kDelete}})) << reason_;
  const uint64_t deleted = item->transaction_id.load().obj;

  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  reaper_.Purge(10);
  ASSERT_EQ(nullptr, tree.Get("k"));

  // The purged key reads as the absent word, not the delete's.
  EXPECT_FALSE(Commit({{kTable, "k", deleted}}, {{kTable, "other", "value"}}));
  EXPECT_EQ(0u, reason_.rfind("exact_read_tid_moved", 0)) << reason_;
}

TEST_F(CommitTidTest, ReaperRequeuesALockedRecord) {
  auto *held = SeedRow("held", Version(10, 4));
  ASSERT_TRUE(Commit({}, {{kTable, "held", "", RowOp::kDelete}})) << reason_;
  Tidword word = held->transaction_id.load();
  word.lock = true;
  held->transaction_id.store(word);

  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  reaper_.Purge(10);
  EXPECT_EQ(held, tree.Get("held"));

  word.lock = false;
  held->transaction_id.store(word);
  reaper_.Purge(10);
  EXPECT_EQ(nullptr, tree.Get("held"));

}

TEST_F(CommitTidTest, EmptyPrimaryRangeAbortsWhenAnAbsentRowIsFromALaterEpoch) {
  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  tree.GetOrInsert("x")->transaction_id.store(Deleted(11, 5));
  TestHelper::Range range;
  range.table_name = kTable;
  range.start_key = "x";
  range.end_key = "y";

  EXPECT_FALSE(Commit({}, {{kTable, "w", "value"}}, {}, {range}));
  EXPECT_EQ("commit_epoch_stale", reason_);
  auto *w = tree.Get("w");
  ASSERT_NE(nullptr, w);
  EXPECT_FALSE(w->transaction_id.load().lock);

  epoch_.SetGlobalEpoch(11);
  ASSERT_TRUE(Commit({}, {{kTable, "w", "value"}}, {}, {range})) << reason_;
}

TEST_F(CommitTidTest,
       EmptySecondaryRangeAbortsWhenAnAbsentEntryIsFromALaterEpoch) {
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  index->tree.GetOrInsert("s")->transaction_id.store(Deleted(11, 5));
  TestHelper::Range range;
  range.table_name = kTable;
  range.index_name = "idx";
  range.start_key = "s";
  range.end_key = "t";

  EXPECT_FALSE(Commit({}, {{kTable, "w", "value"}}, {}, {range}));
  EXPECT_EQ("commit_epoch_stale", reason_);
  auto *w = tables_.GetTable(kTable)->GetPrimaryIndex().Get("w");
  ASSERT_NE(nullptr, w);
  EXPECT_FALSE(w->transaction_id.load().lock);

  epoch_.SetGlobalEpoch(11);
  ASSERT_TRUE(Commit({}, {{kTable, "w", "value"}}, {}, {range})) << reason_;
}

TEST_F(CommitTidTest,
       EmptySecondaryRangeAbortsWhenAnAbsentBaseRowIsFromALaterEpoch) {
  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  tree.GetOrInsert("a")->transaction_id.store(Deleted(11, 5));
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("group");
  posting->SetPrimaryKeys({"a"});
  posting->transaction_id.store(Version(10, 30));
  TestHelper::Range range;
  range.table_name = kTable;
  range.index_name = "idx";
  range.start_key = "group";
  range.end_key = "grouq";

  EXPECT_FALSE(Commit({}, {{kTable, "unrelated", "value"}}, {}, {range}));
  EXPECT_EQ("commit_epoch_stale", reason_);
}

TEST_F(CommitTidTest,
       SecondaryDeleteThatEmptiesAnEntryPublishesAbsentAndTheReaperPurgesIt) {
  SeedRow("a", Version(10, 2));
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("s");
  posting->SetPrimaryKeys({"a"});
  posting->transaction_id.store(Version(10, 4));

  ASSERT_TRUE(Commit({}, {}, {{kTable, "idx", "s", "a", true}})) << reason_;
  EXPECT_EQ(Deleted(10, 5), posting->transaction_id.load());

  // The emptied entry replays as absent until the reaper removes it.
  TestHelper::Range range;
  range.table_name = kTable;
  range.index_name = "idx";
  range.start_key = "s";
  range.end_key = "t";
  ASSERT_TRUE(Commit({}, {{kTable, "other", "value"}}, {}, {range})) << reason_;

  reaper_.Purge(10);
  EXPECT_EQ(nullptr, index->tree.Get("s"));
}

TEST(TidWordTest, BitPositionsMatchLayout) {
  EXPECT_EQ(4u, Tidword::Absent().obj);
  EXPECT_EQ((10ull << 32) | (8u << 3) | 2u, Version(10, 8).obj);
}

TEST(CommonTidWorkerTest, LastTidBelongsToEachWorkerAndDatabase) {
  Config config;
  config.work_dir = "./helios_common_tid_worker_logs";
  config.epoch_duration_ms = 1000;
  config.wal_initial_capacity_bytes = 1u << 20;
  for (int instance = 0; instance < 2; ++instance) {
    std::filesystem::remove_all(config.work_dir);
    Database db(config);
    ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
    std::string reason;
    for (int n = 0; n < 3; ++n) {
      ASSERT_TRUE(TestHelper::CommitRows(
          db, {}, {{kTable, std::to_string(n), TestHelper::Row("v")}}, {}, {},
          reason, CommitDurability::kAsync))
          << reason;
      const auto row = db.Read(kTable, std::to_string(n));
      db.ReleaseThreadEpoch();
      if (n == 0) {
        EXPECT_EQ(0u, Tidword(row.tid).tid);
        EXPECT_TRUE(Tidword(row.tid).latest);
      }
    }
    // A different worker starts its own tid, even in the same database.
    bool worker_committed = false;
    std::thread worker([&]() {
      std::string worker_reason;
      worker_committed = TestHelper::CommitRows(
          db, {}, {{kTable, "worker", TestHelper::Row("v")}}, {}, {},
          worker_reason, CommitDurability::kAsync);
    });
    worker.join();
    ASSERT_TRUE(worker_committed);
    const auto row = db.Read(kTable, "worker");
    db.ReleaseThreadEpoch();
    EXPECT_EQ(0u, Tidword(row.tid).tid);
    EXPECT_TRUE(Tidword(row.tid).latest);
  }
  std::filesystem::remove_all(config.work_dir);
}

}  // namespace
