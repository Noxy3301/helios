/**
 * @file server/storage/tests/reaper_test.cc
 * Epoch cutoffs and retries in deferred physical deletion.
 */

#include <string>

#include "gtest/gtest.h"
#include "index/masstree_index.h"
#include "index/reaper.h"

namespace {

using namespace helios::storage;

class ReaperTest : public ::testing::Test {
 protected:
  index::MasstreeIndex tree_;
  index::Reaper reaper_;

  void TearDown() override { index::release_thread_epoch(); }

  DataItem *Enqueue(const std::string &key, EpochNumber epoch) {
    Tidword deleted;
    deleted.epoch = epoch;
    deleted.latest = true;
    deleted.absent = true;
    auto *item = tree_.GetOrInsert(key);
    item->transaction_id.store(deleted);
    reaper_.Enqueue(tree_, key, *item, deleted);
    return item;
  }
};

TEST_F(ReaperTest, OutOfOrderEnqueuesRespectTheInclusiveEpochCutoff) {
  Enqueue("new", 12);
  Enqueue("old", 10);
  Enqueue("middle", 11);

  reaper_.Purge(9);
  EXPECT_NE(nullptr, tree_.Get("old"));
  reaper_.Purge(10);
  EXPECT_EQ(nullptr, tree_.Get("old"));
  EXPECT_NE(nullptr, tree_.Get("middle"));
  EXPECT_NE(nullptr, tree_.Get("new"));

  reaper_.Purge(11);
  EXPECT_EQ(nullptr, tree_.Get("middle"));
  EXPECT_NE(nullptr, tree_.Get("new"));
  reaper_.Purge(12);
  EXPECT_EQ(nullptr, tree_.Get("new"));
}

TEST_F(ReaperTest, LockedCandidatesRetryWithoutBlockingOtherEligibleEpochs) {
  auto *held = Enqueue("held", 10);
  Enqueue("due", 11);
  Enqueue("future", 12);
  Tidword deleted = held->transaction_id.load();
  Tidword locked = deleted;
  locked.lock = true;
  held->transaction_id.store(locked);

  reaper_.Purge(11);
  EXPECT_EQ(held, tree_.Get("held"));
  EXPECT_EQ(nullptr, tree_.Get("due"));
  EXPECT_NE(nullptr, tree_.Get("future"));

  held->transaction_id.store(deleted);
  reaper_.Purge(11);
  EXPECT_EQ(nullptr, tree_.Get("held"));
  EXPECT_NE(nullptr, tree_.Get("future"));
}

}  // namespace
