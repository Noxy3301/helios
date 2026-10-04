/**
 * @file server/storage/tests/column_validation_test.cc
 * Which committed writes abort a point read validated by the columns it read,
 * and which it outlives.
 */

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/transaction.h"

#include "db_helper.h"
#include "gtest/gtest.h"

namespace {

constexpr const char *kTable = "column_validation_test";
constexpr const char *kKey = "k";

// PAX field 0 is the null flags, field 1 column a, field 2 column b. The mask
// leaves out field 0, which Commit checks for every masked read.
constexpr uint64_t kReadsA = 0b010;

void pack_field(std::string &out, std::string_view payload) {
  if (payload.empty()) {
    out.push_back(static_cast<char>(0xFF));
    return;
  }
  out.push_back(1);
  out.push_back(static_cast<char>(payload.size()));
  out.append(payload);
}

std::string make_row(std::string_view a, std::string_view b,
                     std::string_view null_flags = std::string_view("\0", 1)) {
  std::string row;
  pack_field(row, null_flags);
  pack_field(row, a);
  pack_field(row, b);
  return row;
}

void put(helios::storage::Database &db, const std::string &key,
         const std::string &row) {
  ASSERT_TRUE(TestHelper::WriteRow(db, kTable, key, row)) << key;
}

uint64_t observe(helios::storage::Database &db) {
  const auto result = db.Read(kTable, kKey);
  db.ReleaseThreadEpoch();
  EXPECT_TRUE(result.found);
  return result.tid;
}

// Commits a read of kKey validated by `mask`, and `write` to kKey when set.
bool commit_read(helios::storage::Database &db, uint64_t tid, uint64_t mask,
                 std::string &reason,
                 const std::optional<std::string> &write = std::nullopt) {
  helios::storage::silo::Transaction tx(db);
  tx.Read(kTable, kKey, helios::storage::Tidword(tid), mask);
  if (write && !tx.Write(kTable, kKey, *write, helios::storage::RowOp::kUpdate,
                         reason)) {
    db.ReleaseThreadEpoch();
    return false;
  }
  const bool committed =
      tx.Commit(helios::storage::CommitDurability::kSync, reason);
  db.ReleaseThreadEpoch();
  return committed;
}

class ColumnValidationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.enable_recovery = false;
    config_.work_dir = "./helios_column_validation_test_logs";
    std::filesystem::remove_all(config_.work_dir);
    db_ = std::make_unique<helios::storage::Database>(config_);
    ASSERT_TRUE(db_->CreateTable(kTable));
    ASSERT_TRUE(db_->InstallPaxSchema(kTable, {1, 16, 16}));
    db_->ReleaseThreadEpoch();
    put(*db_, kKey, make_row("a0", "b0"));
  }

  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
};

}  // namespace

TEST_F(ColumnValidationTest, AnUnreadColumnChangeCommits) {
  const uint64_t tid = observe(*db_);
  put(*db_, kKey, make_row("a0", "b1"));

  std::string reason;
  EXPECT_TRUE(commit_read(*db_, tid, kReadsA, reason)) << reason;
}

TEST_F(ColumnValidationTest, AReadColumnChangeAborts) {
  const uint64_t tid = observe(*db_);
  put(*db_, kKey, make_row("a1", "b0"));

  std::string reason;
  EXPECT_FALSE(commit_read(*db_, tid, kReadsA, reason));
  EXPECT_EQ(reason, "exact_read_tid_moved");
}

TEST_F(ColumnValidationTest, AReadColumnChangedAndRestoredAborts) {
  const uint64_t tid = observe(*db_);
  put(*db_, kKey, make_row("a1", "b0"));
  put(*db_, kKey, make_row("a0", "b0"));

  std::string reason;
  EXPECT_FALSE(commit_read(*db_, tid, kReadsA, reason));
  EXPECT_EQ(reason, "exact_read_tid_moved");
}

TEST_F(ColumnValidationTest, AReadColumnAssignedItsValueAborts) {
  // The write assigns column a the bytes it already holds.
  const uint64_t tid = observe(*db_);
  const std::string row = make_row("a0", "b0");
  std::string reason;
  helios::storage::silo::Transaction tx(*db_);
  ASSERT_TRUE(tx.Write(kTable, kKey, row, helios::storage::RowOp::kUpdate,
                       reason, kReadsA))
      << reason;
  ASSERT_TRUE(tx.Commit(helios::storage::CommitDurability::kSync, reason))
      << reason;
  db_->ReleaseThreadEpoch();

  EXPECT_FALSE(commit_read(*db_, tid, kReadsA, reason));
  EXPECT_EQ(reason, "exact_read_tid_moved");
}

TEST_F(ColumnValidationTest, ANullFlagChangeAborts) {
  const uint64_t tid = observe(*db_);
  put(*db_, kKey, make_row("a0", "", std::string_view("\2", 1)));

  std::string reason;
  EXPECT_FALSE(commit_read(*db_, tid, kReadsA, reason));
  EXPECT_EQ(reason, "exact_read_tid_moved");
}

TEST_F(ColumnValidationTest, ADeleteAborts) {
  // An async delete skips the durability wait, during which the reaper would
  // purge the record before the masked read's commit validates it.
  const uint64_t tid = observe(*db_);
  std::string reason;
  helios::storage::silo::Transaction tx(*db_);
  ASSERT_TRUE(
      tx.Write(kTable, kKey, "", helios::storage::RowOp::kDelete, reason))
      << reason;
  ASSERT_TRUE(tx.Commit(helios::storage::CommitDurability::kAsync, reason))
      << reason;
  db_->ReleaseThreadEpoch();

  EXPECT_FALSE(commit_read(*db_, tid, kReadsA, reason));
  EXPECT_EQ(reason, "exact_read_tid_moved");
}

TEST_F(ColumnValidationTest, ADeleteAndReinsertAborts) {
  const uint64_t tid = observe(*db_);
  ASSERT_TRUE(TestHelper::Delete(*db_, kTable, kKey));
  put(*db_, kKey, make_row("a0", "b0"));

  std::string reason;
  EXPECT_FALSE(commit_read(*db_, tid, kReadsA, reason));
  EXPECT_EQ(reason, "exact_read_tid_moved");
}

TEST_F(ColumnValidationTest, AWrittenRowValidatesTheWholeRow) {
  // The reader's write installs a full row built from b0, so it must see
  // the change of b even though it read only a.
  const uint64_t tid = observe(*db_);
  put(*db_, kKey, make_row("a0", "b1"));

  std::string reason;
  EXPECT_FALSE(commit_read(*db_, tid, kReadsA, reason, make_row("a1", "b0")));
  EXPECT_EQ(reason, "exact_read_tid_moved");
  EXPECT_EQ(TestHelper::ReadRow(*db_, kTable, kKey), make_row("a0", "b1"));
}

TEST_F(ColumnValidationTest, AColumnChangedBeforeTheReadCommits) {
  // a changes before the read, and only b after it.
  put(*db_, kKey, make_row("a1", "b0"));
  const uint64_t tid = observe(*db_);
  put(*db_, kKey, make_row("a1", "b1"));

  std::string reason;
  EXPECT_TRUE(commit_read(*db_, tid, kReadsA, reason)) << reason;
}

TEST_F(ColumnValidationTest, AZeroMaskValidatesTheWholeRow) {
  const uint64_t tid = observe(*db_);
  put(*db_, kKey, make_row("a0", "b1"));

  std::string reason;
  EXPECT_FALSE(commit_read(*db_, tid, 0, reason));
  EXPECT_EQ(reason, "exact_read_tid_moved");
}
