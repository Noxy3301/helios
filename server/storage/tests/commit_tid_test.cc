/**
 * @file server/storage/tests/commit_tid_test.cc
 * Transaction-wide TIDs and worker ordering.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"

#include "db_helper.h"
#include "gtest/gtest.h"
#include "index/reaper.h"
#include "pax/epoch_image_buffer.h"
#include "silo/stable_read.h"
#include "helios/transaction.h"
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

using pax::PaxGroup;

// Reads one slot's bit out of the imaged-slot words GroupImages fills.
bool SlotBit(const uint64_t *imaged, uint32_t slot) {
  constexpr uint32_t kWordBits = PaxGroup::kVisibilityWordBits;
  return ((imaged[slot / kWordBits] >> (slot % kWordBits)) & 1u) != 0;
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
    logger_ = std::make_unique<wal::Logger>(config_);
    ASSERT_EQ(wal::Logger::RecoveryStatus::kOk, logger_->Recover().status);
    logger_->Start();
    // Leave the epoch thread parked so tests control epoch boundaries exactly.
    SetEpoch(10);
  }

  // Moves E and lets the logger publish D = E - 2, as the epoch hook does.
  void SetEpoch(EpochNumber epoch) {
    epoch_.SetGlobalEpoch(epoch);
    logger_->RequestFlush(epoch - 2);
    ASSERT_EQ(
        wal::Logger::WaitResult::kDurable,
        logger_->WaitUntilDurable(epoch - 2, wal::Logger::Deadline::max()));
  }

  void TearDown() override {
    EXPECT_FALSE(pax::EpochImageBuffer::Global().HasOpenView());
    if (logger_) {
      logger_->RequestFlush(epoch_.GetGlobalEpoch());
      logger_.reset();
    }
    epoch_.Start();
    epoch_.Stop();
    index::release_thread_epoch();
    std::filesystem::remove_all(config_.work_dir);
  }

  DataItem *SeedRow(const std::string &key, Tidword tid) {
    auto *table = tables_.GetTable(kTable);
    auto *item = table->GetPrimaryIndex().GetOrInsert(key);
    const std::string bytes = TestHelper::Row(key);
    pax::Row row;
    EXPECT_TRUE(pax::unpack_row(
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
    index::release_thread_epoch();
    return committed;
  }
};

TEST_F(CommitTidTest, CommitReadsTheEpochAfterTheWriteSetIsFed) {
  auto *item = SeedRow("key", Version(10, 5));
  const std::string bytes = TestHelper::Row("next");

  silo::Transaction tx(tables_, epoch_, reaper_, *logger_, last_tid_);
  ASSERT_TRUE(tx.Write(kTable, "key", bytes, RowOp::kUpdate, reason_))
      << reason_;
  SetEpoch(11);
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
  index::release_thread_epoch();

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
    index::release_thread_epoch();
  });

  // The committer holds the first record and is waiting on the second
  while (!first->transaction_id.load().lock) std::this_thread::yield();
  SetEpoch(11);
  second->transaction_id.store(Version(10, 5));
  committer.join();

  ASSERT_TRUE(committed) << reason_;
  EXPECT_EQ(11u, last_tid_.epoch);
  EXPECT_EQ(last_tid_, a->transaction_id.load());
  EXPECT_EQ(last_tid_, b->transaction_id.load());
  EXPECT_FALSE(a->transaction_id.load().lock);
  EXPECT_FALSE(b->transaction_id.load().lock);
}

TEST_F(CommitTidTest, CommitWithWritesWaitsForTheDurableEpoch) {
  auto *item = SeedRow("key", Version(10, 5));
  // A raw move puts E exactly kMaxLagEpochs + 1 epochs ahead of D.
  const EpochNumber durable = logger_->GetDurableEpoch();
  epoch_.SetGlobalEpoch(durable + wal::Logger::kMaxLagEpochs + 1);

  // The sleep gives the commit time to reach the wait; the durable epoch it
  // sees on return shows it completed only once the flush closed the lag.
  std::atomic<bool> entered{false};
  std::atomic<int> result{-1};
  std::atomic<EpochNumber> durable_at_return{0};
  std::thread committer([&] {
    entered = true;
    result = Commit({}, {{kTable, "key", "next"}}) ? 1 : 0;
    durable_at_return = logger_->GetDurableEpoch();
  });
  while (!entered.load()) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  logger_->RequestFlush(durable + 1);
  committer.join();
  EXPECT_EQ(1, result.load());
  EXPECT_GE(durable_at_return.load(), durable + 1);
  EXPECT_EQ(TestHelper::Row("next"), item->CopyValue());

  // A read-only commit runs at a lag above the bound without waiting.
  epoch_.SetGlobalEpoch(logger_->GetDurableEpoch() + wal::Logger::kMaxLagEpochs +
                        1);
  EXPECT_TRUE(Commit({{kTable, "key", item->transaction_id.load().obj}}, {}))
      << reason_;
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
  auto &image_buffer = pax::EpochImageBuffer::Global();
  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(11);
  const bool committed = Commit({}, {{kTable, "key", "intermediate"},
                                    {kTable, "key", "", RowOp::kDelete},
                                    {kTable, "key", "final"}});
  const auto images =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  image_buffer.Close(se);
  ASSERT_TRUE(committed) << reason_;
  ASSERT_EQ(1u, images.size());
  EXPECT_TRUE(images.front().was_visible);
  EXPECT_EQ(TestHelper::Row("key"), images.front().old_row);
  EXPECT_EQ(TestHelper::Row("final"), item->CopyValue());
}

TEST_F(CommitTidTest, SecondInstallAfterTheSnapshotPreservesNothing) {
  auto *item = SeedRow("key", Version(10, 5));
  auto &image_buffer = pax::EpochImageBuffer::Global();
  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(12);
  const bool first = Commit({}, {{kTable, "key", "second"}});
  const auto after_first =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  const bool second = Commit({}, {{kTable, "key", "third"}});
  const auto after_second =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  SetEpoch(13);
  const bool third = Commit({}, {{kTable, "key", "fourth"}});
  const auto after_third =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  image_buffer.Close(se);

  EXPECT_EQ(10u, se);
  ASSERT_TRUE(first && second && third) << reason_;
  ASSERT_EQ(1u, after_first.size());
  EXPECT_EQ(12u, after_first.front().writer_epoch);
  EXPECT_TRUE(after_first.front().was_visible);
  EXPECT_EQ(TestHelper::Row("key"), after_first.front().old_row);
  // The slot is already at epoch 12, so no open view reads what these
  // installs replace.
  EXPECT_EQ(1u, after_second.size());
  EXPECT_EQ(1u, after_third.size());
}

TEST_F(CommitTidTest, ClosingTheOldestViewDropsItsImagesOnTheNextPreserve) {
  auto *item = SeedRow("key", Version(10, 5));
  auto &image_buffer = pax::EpochImageBuffer::Global();
  const uint64_t preserved = image_buffer.GroupPreserveCount(item->pax_group());
  const EpochNumber v1 = image_buffer.Open(epoch_);
  SetEpoch(12);
  const bool first = Commit({}, {{kTable, "key", "a"}});
  const auto after_first =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  SetEpoch(14);
  const EpochNumber v2 = image_buffer.Open(epoch_);
  SetEpoch(16);
  const bool second = Commit({}, {{kTable, "key", "b"}});
  const auto after_second =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  image_buffer.Close(v1);
  const auto after_close =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  SetEpoch(18);
  const EpochNumber v3 = image_buffer.Open(epoch_);
  SetEpoch(20);
  const bool third = Commit({}, {{kTable, "key", "c"}});
  const auto after_third =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  const uint64_t count = image_buffer.GroupPreserveCount(item->pax_group());
  image_buffer.Close(v2);
  image_buffer.Close(v3);
  const auto after_last_close =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  const uint64_t count_after_close =
      image_buffer.GroupPreserveCount(item->pax_group());

  EXPECT_EQ(10u, v1);
  EXPECT_EQ(14u, v2);
  EXPECT_EQ(18u, v3);
  ASSERT_TRUE(first && second && third) << reason_;
  ASSERT_EQ(1u, after_first.size());
  EXPECT_EQ(12u, after_first.front().writer_epoch);
  ASSERT_EQ(2u, after_second.size());
  EXPECT_EQ(12u, after_second.front().writer_epoch);
  EXPECT_EQ(16u, after_second.back().writer_epoch);
  // Closing v1 leaves its image in place; the next preserve drops it.
  ASSERT_EQ(2u, after_close.size());
  EXPECT_EQ(12u, after_close.front().writer_epoch);
  ASSERT_EQ(2u, after_third.size());
  EXPECT_EQ(16u, after_third.front().writer_epoch);
  EXPECT_EQ(20u, after_third.back().writer_epoch);
  EXPECT_TRUE(after_last_close.empty());
  EXPECT_EQ(preserved + 3, count);
  EXPECT_EQ(count, count_after_close);
}

TEST_F(CommitTidTest, TwoViewsAtOneSnapshotCloseOneAtATime) {
  auto *item = SeedRow("key", Version(10, 5));
  auto &image_buffer = pax::EpochImageBuffer::Global();
  const uint64_t preserved = image_buffer.GroupPreserveCount(item->pax_group());
  const EpochNumber a = image_buffer.Open(epoch_);
  const EpochNumber b = image_buffer.Open(epoch_);
  SetEpoch(11);
  const bool committed = Commit({}, {{kTable, "key", "next"}});
  const auto after_commit =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  image_buffer.Close(a);
  const auto after_first_close =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  const uint64_t count = image_buffer.GroupPreserveCount(item->pax_group());
  image_buffer.Close(b);
  const auto after_second_close =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  const uint64_t count_after_close =
      image_buffer.GroupPreserveCount(item->pax_group());

  EXPECT_EQ(10u, a);
  EXPECT_EQ(10u, b);
  ASSERT_TRUE(committed) << reason_;
  EXPECT_EQ(1u, after_commit.size());
  EXPECT_EQ(1u, after_first_close.size());
  EXPECT_TRUE(after_second_close.empty());
  EXPECT_EQ(preserved + 1, count);
  EXPECT_EQ(count, count_after_close);
}

TEST_F(CommitTidTest, InsertAndReinsertAfterTheSnapshotPreserveAbsence) {
  auto &index = tables_.GetTable(kTable)->GetPrimaryIndex();
  auto &image_buffer = pax::EpochImageBuffer::Global();
  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(11);
  const bool inserted = Commit({}, {{kTable, "new", "value", RowOp::kInsert}});
  auto *item = index.GetOrInsert("new");
  const auto after_insert =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  const bool deleted = Commit({}, {{kTable, "new", "", RowOp::kDelete}});
  const auto after_delete =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  SetEpoch(12);
  const EpochNumber v2 = image_buffer.Open(epoch_);
  SetEpoch(13);
  const bool reinserted =
      Commit({}, {{kTable, "new", "again", RowOp::kInsert}});
  const auto after_reinsert =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  image_buffer.Close(se);
  image_buffer.Close(v2);

  ASSERT_TRUE(inserted && deleted && reinserted) << reason_;
  ASSERT_EQ(1u, after_insert.size());
  EXPECT_EQ(11u, after_insert.front().writer_epoch);
  EXPECT_FALSE(after_insert.front().was_visible);
  // The delete replaces a version from its own epoch.
  EXPECT_EQ(1u, after_delete.size());
  ASSERT_EQ(2u, after_reinsert.size());
  EXPECT_EQ(11u, after_reinsert.front().writer_epoch);
  EXPECT_EQ(13u, after_reinsert.back().writer_epoch);
  EXPECT_FALSE(after_reinsert.back().was_visible);
}

TEST_F(CommitTidTest, ReinsertingUnderOneViewKeepsOneAbsenceImage) {
  auto &index = tables_.GetTable(kTable)->GetPrimaryIndex();
  auto &image_buffer = pax::EpochImageBuffer::Global();
  // The seed row takes a slot in the group the churned key lands in.
  auto *seed = SeedRow("seed", Version(10, 1));
  const uint64_t preserved = image_buffer.GroupPreserveCount(seed->pax_group());
  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(11);
  bool churned = true;
  for (int cycle = 0; cycle < 3; ++cycle) {
    churned = churned &&
              Commit({}, {{kTable, "churn", "value", RowOp::kInsert}}) &&
              Commit({}, {{kTable, "churn", "", RowOp::kDelete}});
  }
  auto *item = index.GetOrInsert("churn");
  const auto images =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  const uint64_t count = image_buffer.GroupPreserveCount(item->pax_group());
  image_buffer.Close(se);

  ASSERT_TRUE(churned) << reason_;
  // Every reinsert replaces the absence the first one preserved.
  ASSERT_EQ(1u, images.size());
  EXPECT_EQ(11u, images.front().writer_epoch);
  EXPECT_FALSE(images.front().was_visible);
  EXPECT_TRUE(images.front().old_row.empty());
  EXPECT_EQ(preserved + 3, count);
}

TEST_F(CommitTidTest, TheGroupPublishesItsImageStateOnTheFirstPreserve) {
  auto *item = SeedRow("key", Version(10, 5));
  auto &image_buffer = pax::EpochImageBuffer::Global();
  EXPECT_EQ(nullptr, pax::ImageState(item->pax_group()));
  EXPECT_EQ(0u, pax::PreserveCount(nullptr));

  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(12);
  const bool first = Commit({}, {{kTable, "key", "second"}});
  pax::GroupImageState *state = pax::ImageState(item->pax_group());
  const uint64_t after_first = pax::PreserveCount(state);
  SetEpoch(14);
  const EpochNumber later = image_buffer.Open(epoch_);
  SetEpoch(16);
  const bool second = Commit({}, {{kTable, "key", "third"}});
  image_buffer.Close(se);
  image_buffer.Close(later);

  ASSERT_TRUE(first && second) << reason_;
  ASSERT_NE(nullptr, state);
  // The published pointer is the one every later lookup returns, and reading
  // the counter through it matches the buffer's own lookup.
  EXPECT_EQ(state, pax::ImageState(item->pax_group()));
  EXPECT_EQ(1u, after_first);
  EXPECT_EQ(2u, pax::PreserveCount(state));
  EXPECT_EQ(image_buffer.GroupPreserveCount(item->pax_group()),
            pax::PreserveCount(state));
}

TEST_F(CommitTidTest, TheBufferAccountsForTheBytesItHolds) {
  SeedRow("key", Version(10, 5));
  auto &image_buffer = pax::EpochImageBuffer::Global();
  const pax::ImageBufferStats before = pax::ImageStats();
  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(12);
  const bool committed = Commit({}, {{kTable, "key", "second"}});
  const pax::ImageBufferStats held = pax::ImageStats();
  image_buffer.Close(se);
  const pax::ImageBufferStats after = pax::ImageStats();

  ASSERT_TRUE(committed) << reason_;
  EXPECT_EQ(0u, before.images);
  EXPECT_EQ(0u, before.bytes);
  EXPECT_EQ(1u, held.images);
  EXPECT_EQ(1u, held.open_views);
  EXPECT_GT(held.bytes, sizeof(pax::EpochImage));
  // The last close clears every image, and the counters with them.
  EXPECT_EQ(0u, after.images);
  EXPECT_EQ(0u, after.bytes);
  EXPECT_EQ(0u, after.open_views);
}

TEST_F(CommitTidTest, TheImagedSlotBitsMirrorTheImageMapKeys) {
  auto *first = SeedRow("a", Version(10, 1));
  auto *second = SeedRow("b", Version(10, 2));
  auto &image_buffer = pax::EpochImageBuffer::Global();
  uint64_t imaged[PaxGroup::kRows / PaxGroup::kVisibilityWordBits];
  uint64_t imaged_after_close[PaxGroup::kRows / PaxGroup::kVisibilityWordBits];

  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(12);
  const bool committed = Commit({}, {{kTable, "a", "later"}});
  pax::GroupImageState *state = pax::ImageState(first->pax_group());
  const auto images = pax::GroupImages(state, imaged);
  image_buffer.Close(se);
  const auto after_close = pax::GroupImages(state, imaged_after_close);

  ASSERT_TRUE(committed) << reason_;
  ASSERT_EQ(first->pax_group(), second->pax_group());
  ASSERT_EQ(1u, images.size());
  EXPECT_EQ(1u, images.count(first->pax_slot()));
  // The bits the same locked pass filled name exactly the imaged slots.
  EXPECT_TRUE(SlotBit(imaged, first->pax_slot()));
  EXPECT_FALSE(SlotBit(imaged, second->pax_slot()));
  // The last close clears the images, and with them the bits.
  EXPECT_TRUE(after_close.empty());
  EXPECT_FALSE(SlotBit(imaged_after_close, first->pax_slot()));
}

TEST_F(CommitTidTest, DeletingAMissingKeyThenInsertingPreservesAbsence) {
  auto &index = tables_.GetTable(kTable)->GetPrimaryIndex();
  auto &image_buffer = pax::EpochImageBuffer::Global();
  const EpochNumber se = image_buffer.Open(epoch_);
  SetEpoch(11);
  // The delete allocates no slot; it moves the word of a blank record.
  const bool deleted = Commit({}, {{kTable, "ghost", "", RowOp::kDelete}});
  const bool inserted =
      Commit({}, {{kTable, "ghost", "value", RowOp::kInsert}});
  auto *item = index.GetOrInsert("ghost");
  const auto images =
      image_buffer.SlotImages(item->pax_group(), item->pax_slot());
  image_buffer.Close(se);

  ASSERT_TRUE(deleted && inserted) << reason_;
  ASSERT_EQ(1u, images.size());
  EXPECT_EQ(11u, images.front().writer_epoch);
  EXPECT_FALSE(images.front().was_visible);
  EXPECT_TRUE(images.front().old_row.empty());
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
  posting->set_primary_keys({"a"});
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
  posting->set_primary_keys({"a"});
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

TEST_F(CommitTidTest, UniqueIndexTakesASecondAdditionThatFollowsARemoval) {
  ASSERT_TRUE(tables_.GetTable(kTable)->CreateSecondaryIndex(
      "uidx", IndexConstraint::kUnique));
  SeedRow("a", Version(10, 2));
  SeedRow("b", Version(10, 3));

  // The deltas apply in order under the record lock, so the removal makes
  // room for the addition that follows it.
  ASSERT_TRUE(Commit({}, {},
                     {{kTable, "uidx", "s", "a", false},
                      {kTable, "uidx", "s", "a", true},
                      {kTable, "uidx", "s", "b", false}}))
      << reason_;

  auto *posting =
      tables_.GetTable(kTable)->GetSecondaryIndex("uidx")->tree.Get("s");
  ASSERT_NE(nullptr, posting);
  const auto keys = silo::StableReadKeys(*posting);
  EXPECT_TRUE(keys.found);
  const auto view = keys.primary_keys_view();
  ASSERT_EQ(1u, view.size());
  EXPECT_EQ("b", *view.begin());
  index::release_thread_epoch();
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
  posting->set_primary_keys({"a"});
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
  SetEpoch(12);
  ASSERT_TRUE(Commit({}, {{kTable, "blocked", "value"}})) << reason_;
  EXPECT_EQ(Version(12, 0), last_tid_);
}

TEST_F(CommitTidTest, FutureEpochAbortsAndReleasesTheWriteLock) {
  auto *item = SeedRow("key", Version(11, 50));
  EXPECT_FALSE(Commit({}, {{kTable, "key", "value"}}));
  EXPECT_EQ("commit_epoch_stale", reason_);
  EXPECT_EQ(Version(11, 50), item->transaction_id.load());
  EXPECT_EQ(Tidword(), last_tid_);
  SetEpoch(11);
  ASSERT_TRUE(Commit({}, {{kTable, "key", "value"}})) << reason_;
  EXPECT_EQ(Version(11, 51), last_tid_);
}

TEST_F(CommitTidTest, AbsentReadAcceptsABlankRecordAnotherTransactionLeft) {
  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  // An aborted attempt leaves the key a blank record, still absent.
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

TEST_F(CommitTidTest, RangeRevalidationAllowsOwnLockOnPrimaryAndSecondary) {
  SeedRow("k", Version(10, 10));
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("s");
  posting->set_primary_keys({"k"});
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

TEST_F(CommitTidTest, AbsentReadAbortsAfterThePurge) {
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

  SetEpoch(11);
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

  SetEpoch(11);
  ASSERT_TRUE(Commit({}, {{kTable, "w", "value"}}, {}, {range})) << reason_;
}

TEST_F(CommitTidTest,
       EmptySecondaryRangeAbortsWhenAnAbsentBaseRowIsFromALaterEpoch) {
  auto &tree = tables_.GetTable(kTable)->GetPrimaryIndex();
  tree.GetOrInsert("a")->transaction_id.store(Deleted(11, 5));
  auto *index = tables_.GetTable(kTable)->GetSecondaryIndex("idx");
  auto *posting = index->tree.GetOrInsert("group");
  posting->set_primary_keys({"a"});
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
  posting->set_primary_keys({"a"});
  posting->transaction_id.store(Version(10, 4));

  ASSERT_TRUE(Commit({}, {}, {{kTable, "idx", "s", "a", true}})) << reason_;
  EXPECT_EQ(Deleted(10, 5), posting->transaction_id.load());

  // The emptied entry revalidates as absent until the reaper removes it.
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
