/**
 * @file server/storage/tests/range_validation_test.cc
 * Which changes inside a validated range abort the transaction that read
 * it, and which fall outside its cap.
 */

#include <filesystem>
#include <string>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/read.h"

#include "db_helper.h"
#include "gtest/gtest.h"

namespace {

constexpr const char *kTable = "range_validation_test";

helios::storage::Config MakeConfig() {
  helios::storage::Config config;
  config.enable_recovery = false;
  config.work_dir = "./helios_range_validation_test_logs";
  std::filesystem::remove_all(config.work_dir);
  return config;
}

bool CommitWrite(helios::storage::Database &db, const std::string &key,
                 const std::string &value) {
  std::string reason;
  const bool committed = TestHelper::CommitRows(
      db, {}, {{kTable, key, TestHelper::Row(value)}}, {}, {}, reason);
  EXPECT_TRUE(committed) << "write " << key << " aborted: " << reason;
  return committed;
}

bool CommitDelete(helios::storage::Database &db, const std::string &key) {
  std::string reason;
  const bool committed = TestHelper::CommitRows(
      db, {}, {{kTable, key, "", helios::storage::RowOp::kDelete}}, {}, {},
      reason);
  EXPECT_TRUE(committed) << "delete " << key << " aborted: " << reason;
  return committed;
}

// Scans the range and assembles the range read set a caller submits at commit.
TestHelper::Range ScanRange(helios::storage::Database &db,
                            const std::string &start_key,
                            const std::string &end_key, uint64_t row_limit = 0,
                            bool reverse_scan = false) {
  auto scan = db.Scan(kTable, start_key, end_key, row_limit, reverse_scan);
  db.ReleaseThreadEpoch();
  EXPECT_TRUE(scan.ok) << "scan [" << start_key << ", " << end_key << ")";

  TestHelper::Range range;
  range.table_name = kTable;
  range.start_key = start_key;
  range.end_key = end_key;
  range.row_limit = row_limit;
  range.reverse_scan = reverse_scan;
  // A refused scan has no rows for the range read set; returning the empty
  // range keeps the caller's own expectations from passing on it.
  if (!scan.ok) return range;
  for (const auto &row : scan.rows) {
    range.result_keys.emplace_back(row.key);
  }
  return range;
}

bool Revalidate(helios::storage::Database &db, const TestHelper::Range &range,
                std::string &reason) {
  return TestHelper::CommitRows(db, {}, {}, {}, {range}, reason);
}

void SeedRows(helios::storage::Database &db) {
  for (const char *key : {"k1", "k2", "k3", "k4"}) {
    ASSERT_TRUE(CommitWrite(db, key, "v"));
  }
}

// Leaves a record that never held a value. Resolving a write inserts
// the blank record before validation runs, and an aborted commit leaves it
// behind.
void LeaveBlankRecord(helios::storage::Database &db, const std::string &key) {
  std::string reason;
  const bool committed = TestHelper::CommitRows(
      db, {{kTable, "k1", 0}}, {{kTable, key, TestHelper::Row("v")}}, {}, {},
      reason);
  ASSERT_FALSE(committed) << "the write was supposed to abort";
  EXPECT_FALSE(reason.empty()) << "an abort names its reason";
}

}  // namespace

TEST(RangeValidationTest, AnUnchangedRangeCommits) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_EQ(range.result_keys,
            (std::vector<std::string>{"k1", "k2", "k3", "k4"}));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, reason)) << reason;
}

TEST(RangeValidationTest, ARowDeletedInsideTheRangeAborts) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitDelete(db, "k2"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, ARowDeletedAtTheEndOfTheRangeAborts) {
  // The re-scan is a strict prefix of the recorded keys, so nothing diverges
  // positionally and only the length check rejects it.
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitDelete(db, "k4"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, ARowInsertedInsideTheRangeAborts) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitWrite(db, "k25", "v"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, ARowInsertedAtTheEndOfTheRangeAborts) {
  // The recorded keys are a strict prefix of the re-scan, so the divergence is
  // the first live row past them.
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitWrite(db, "k45", "v"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, ALimitedRangeIgnoresChangesPastItsCap) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5", 2);
  ASSERT_EQ(range.result_keys, (std::vector<std::string>{"k1", "k2"}));
  ASSERT_TRUE(CommitWrite(db, "k45", "v"));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, reason)) << reason;
}

TEST(RangeValidationTest, ABlankRecordDoesNotConsumeTheCap) {
  // The cap counts live rows. A blank record between the first two of them must
  // leave the re-scan room to reach the second.
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);
  LeaveBlankRecord(db, "k15");

  const auto range = ScanRange(db, "k1", "k5", 2);
  ASSERT_EQ(range.result_keys, (std::vector<std::string>{"k1", "k2"}));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, reason)) << reason;
}

TEST(RangeValidationTest, AnEmptyRangeCommits) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "m1", "m9");
  ASSERT_TRUE(range.result_keys.empty());

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, reason)) << reason;
}

TEST(RangeValidationTest, ARowAppearingInAnEmptyRangeAborts) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "m1", "m9");
  ASSERT_TRUE(range.result_keys.empty());
  ASSERT_TRUE(CommitWrite(db, "m5", "v"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, EvidenceRepeatingAKeyAborts) {
  // A primary index cannot return the same key twice, so a range read set
  // that repeats one is rejected rather than matched by the positional walk.
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  auto range = ScanRange(db, "k1", "k5");
  range.result_keys.insert(range.result_keys.begin(), "k1");

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, AReverseRangeAbortsOnTheSameChange) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5", 0, true);
  ASSERT_EQ(range.result_keys,
            (std::vector<std::string>{"k4", "k3", "k2", "k1"}));
  ASSERT_TRUE(CommitDelete(db, "k2"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}
