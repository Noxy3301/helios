/**
 * @file server/storage/tests/range_validation_test.cc
 * Which changes inside a validated range abort the transaction that read
 * it, and which fall outside its row limit.
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
  range.rows = TestHelper::records(scan.rows);
  range.visited = TestHelper::records(scan.visited);
  return range;
}

std::vector<std::string> keys(const std::vector<TestHelper::Record> &records) {
  std::vector<std::string> out;
  for (const auto &record : records) out.push_back(record.key);
  return out;
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

}  // namespace

TEST(RangeValidationTest, AnUnchangedRangeCommits) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_EQ(keys(range.rows),
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
  // The re-scan reaches k4's tombstone where the scan read the live row.
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

TEST(RangeValidationTest, ALimitedRangeIgnoresChangesPastItsRowLimit) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5", 2);
  ASSERT_EQ(keys(range.rows), (std::vector<std::string>{"k1", "k2"}));
  ASSERT_TRUE(CommitWrite(db, "k45", "v"));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, reason)) << reason;
}

TEST(RangeValidationTest, ABlankRecordDoesNotConsumeTheRowLimit) {
  // The row limit counts live rows. A blank record between the first two of
  // them must leave the re-scan room to reach the second.
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  // A write whose read set is stale inserts its blank record before
  // validation runs, and the abort leaves the record behind.
  std::string blank_reason;
  ASSERT_FALSE(TestHelper::CommitRows(db, {{kTable, "k1", 0}},
                                      {{kTable, "k15", TestHelper::Row("v")}},
                                      {}, {}, blank_reason));
  EXPECT_FALSE(blank_reason.empty()) << "an abort names its reason";

  const auto range = ScanRange(db, "k1", "k5", 2);
  ASSERT_EQ(keys(range.rows), (std::vector<std::string>{"k1", "k2"}));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, reason)) << reason;
}

TEST(RangeValidationTest, AnEmptyRangeCommits) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "m1", "m9");
  ASSERT_TRUE(range.rows.empty() && range.visited.empty());

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, reason)) << reason;
}

TEST(RangeValidationTest, ARowAppearingInAnEmptyRangeAborts) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  const auto range = ScanRange(db, "m1", "m9");
  ASSERT_TRUE(range.rows.empty() && range.visited.empty());
  ASSERT_TRUE(CommitWrite(db, "m5", "v"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, ARangeReadSetRepeatingAKeyAborts) {
  // A primary index cannot return the same key twice, so a range read set
  // that repeats one is rejected rather than matched by the positional walk.
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  SeedRows(db);

  auto range = ScanRange(db, "k1", "k5");
  range.rows.insert(range.rows.begin(), range.rows.front());

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
  ASSERT_EQ(keys(range.rows),
            (std::vector<std::string>{"k4", "k3", "k2", "k1"}));
  ASSERT_TRUE(CommitDelete(db, "k2"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(RangeValidationTest, APaxScanListsTheRecordsARowScanVisits) {
  // The parallel scan must return the rows and list the tombstone a row scan
  // does.
  auto config = MakeConfig();
  // A long epoch keeps the reaper away from the tombstone.
  config.epoch_duration_ms = 1000;
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  std::string reason;
  ASSERT_TRUE(TestHelper::CommitRows(db, {},
                                     {{kTable, "k1", TestHelper::Row("v")},
                                      {kTable, "k2", TestHelper::Row("v")},
                                      {kTable, "k3", TestHelper::Row("v")}},
                                     {}, {}, reason,
                                     helios::storage::CommitDurability::kAsync))
      << reason;
  ASSERT_TRUE(TestHelper::CommitRows(
      db, {}, {{kTable, "k2", "", helios::storage::RowOp::kDelete}}, {}, {},
      reason, helios::storage::CommitDurability::kAsync))
      << reason;

  const auto scan = db.Scan(kTable, "k1", "k5", 0, false);
  db.ReleaseThreadEpoch();
  const auto pax = db.ScanPax(kTable, "k1", "k5", 0, false);
  db.ReleaseThreadEpoch();
  ASSERT_TRUE(scan.ok);
  ASSERT_TRUE(pax.ok);
  ASSERT_EQ(scan.visited.size(), 1u);
  EXPECT_EQ(scan.visited[0].key, "k2");
  EXPECT_EQ(TestHelper::records(pax.rows), TestHelper::records(scan.rows));
  EXPECT_EQ(TestHelper::records(pax.visited),
            TestHelper::records(scan.visited));
}
