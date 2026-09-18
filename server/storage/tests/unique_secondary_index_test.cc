// Modified for Helios.

/**
 * @file server/storage/tests/unique_secondary_index_test.cc
 * That a unique secondary index rejects a duplicate secondary key.
 */

#include <filesystem>
#include <memory>
#include <string>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/index.h"

#include "db_helper.h"
#include "gtest/gtest.h"

namespace {

using helios::storage::IndexConstraint;

bool WriteRowAndSecondary(helios::storage::Database &db,
                          const std::string &table_name,
                          const std::string &primary_key,
                          const std::string &value,
                          const std::string &index_name,
                          const std::string &secondary_key) {
  return TestHelper::CommitWrites(
      db, {{table_name, primary_key, value}},
      {{table_name, index_name, secondary_key, primary_key, false}});
}

}  // namespace

class UniqueSecondaryIndexTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;

  void SetUp() override {
    std::filesystem::remove_all(config_.work_dir);
    config_.epoch_duration_ms = 100;
  }

  void TearDown() override { std::filesystem::remove_all(config_.work_dir); }
};

TEST_F(UniqueSecondaryIndexTest, UniqueIndexRejectsDuplicateSecondaryKey) {
  config_.work_dir = "./helios_unique_secondary_index_test_logs";
  std::filesystem::remove_all(config_.work_dir);
  config_.enable_recovery = false;

  helios::storage::Database db(config_);
  ASSERT_TRUE(TestHelper::CreateTable(db, "users"));
  ASSERT_TRUE(
      db.CreateSecondaryIndex("users", "email_idx", IndexConstraint::kUnique));

  ASSERT_TRUE(WriteRowAndSecondary(db, "users", "user1", "Alice", "email_idx",
                                   "alice@example.com"));
  EXPECT_FALSE(WriteRowAndSecondary(db, "users", "user2", "Bob", "email_idx",
                                    "alice@example.com"));
}

TEST_F(UniqueSecondaryIndexTest,
       RecoveryRestoresUniqueIndexConstraintFromLogs) {
  config_.enable_recovery = true;

  {
    helios::storage::Database db(config_);
    ASSERT_TRUE(TestHelper::CreateTable(db, "users"));
    ASSERT_TRUE(db.CreateSecondaryIndex("users", "email_idx",
                                        IndexConstraint::kUnique));

    ASSERT_TRUE(WriteRowAndSecondary(db, "users", "user1", "Alice", "email_idx",
                                     "alice@example.com"));
  }

  helios::storage::Database recovered_db(config_);

  const auto recovered = TestHelper::ReadIndex(
      recovered_db, "users", "email_idx", "alice@example.com");
  ASSERT_EQ(recovered.size(), 1u);
  EXPECT_EQ(recovered[0], "user1");

  EXPECT_FALSE(WriteRowAndSecondary(recovered_db, "users", "user2", "Bob",
                                    "email_idx", "alice@example.com"));
}
