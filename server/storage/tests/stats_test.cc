/**
 * @file server/storage/tests/stats_test.cc
 * The histogram the optimizer reads, and what a row written during its scan
 * does to the counts.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <functional>
#include <thread>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"

#include "db_helper.h"

namespace {

constexpr const char *kTable = "stats";

helios::storage::Config MakeConfig() {
  helios::storage::Config config;
  config.enable_recovery = false;
  config.work_dir = "./helios_stats_test_logs";
  std::filesystem::remove_all(config.work_dir);
  return config;
}

// Commits without waiting for durability: the scanning thread below holds its
// epoch, which a Sync commit would wait for.
bool WriteAsync(helios::storage::Database &db, const std::string &key,
                const std::string &value) {
  std::string commit_reason;
  return TestHelper::CommitRows(db, {}, {{kTable, key, TestHelper::Row(value)}},
                                {}, {}, commit_reason,
                                helios::storage::CommitDurability::kAsync);
}

// Writes "b" from another thread the first time the walk stands on "c".
void WriteB(helios::storage::Database &db) {
  EXPECT_TRUE(WriteAsync(db, "b", "two"));
}

struct WriteBehindTheCursor {
  helios::storage::Database &db;
  bool wrote;
  bool operator()(std::string_view key, uint32_t num_parts, size_t *ends) {
    if (num_parts != 1) return false;
    ends[0] = key.size();
    if (!wrote && key == "c") {
      wrote = true;
      std::thread writer(WriteB, std::ref(db));
      writer.join();
    }
    return true;
  }
};

}  // namespace

// The histogram walks the index twice, and the second walk can see a row the
// first one did not. The final cumulative count must be the one the second
// walk reached.
TEST(StatsTest, CountsRiseWhenARowArrivesBetweenTheWalks) {
  auto config = MakeConfig();
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  ASSERT_TRUE(WriteAsync(db, "a", "one"));
  ASSERT_TRUE(WriteAsync(db, "c", "three"));

  // One part covering the whole key. "b" is written while the first walk
  // stands on "c", behind the cursor the walk resumes from, so only the
  // second walk counts it.
  WriteBehindTheCursor parts{db, false};

  std::vector<std::string> bounds;
  std::vector<uint64_t> cum;
  ASSERT_TRUE(db.IndexHistogram(kTable, "", 1, parts, bounds, cum));
  db.ReleaseThreadEpoch();

  ASSERT_EQ(bounds.size(), cum.size());
  ASSERT_FALSE(cum.empty());
  for (size_t i = 1; i < cum.size(); ++i) EXPECT_GE(cum[i], cum[i - 1]);
  EXPECT_EQ(bounds.back(), "c");
  EXPECT_EQ(cum.back(), 3u);
}
