/**
 * @file server/storage/tests/pax_recovery_test.cc
 * Column definitions survive restart and put recovered values back in PAX.
 */

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "db_helper.h"
#include "gtest/gtest.h"
#include "storage/database.h"
#include "storage/pax.h"
#include "pax/catalog.h"

namespace {

using helios::storage::Database;
using helios::storage::pax::FieldType;
namespace pax = helios::storage::pax;

const std::vector<uint32_t> kWidths{1, 4, 8};
const std::vector<FieldType> kTypes{FieldType::kUntyped, FieldType::kInt32,
                                    FieldType::kDecimal64};
const std::vector<int8_t> kScales{0, 0, 2};

std::string Row(std::string_view integer, std::string_view decimal) {
  // Null flags and two short, length-prefixed text values.
  std::string row("\1\1\0", 3);
  for (const auto field : {integer, decimal}) {
    row.push_back('\1');
    row.push_back(static_cast<char>(field.size()));
    row.append(field);
  }
  return row;
}

class PaxRecoveryTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;

  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "helios_pax_recovery_XXXXXX")
            .string();
    std::vector<char> path(pattern.begin(), pattern.end());
    path.push_back('\0');
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    config_.work_dir = path.data();
    config_.epoch_duration_ms = 1;
    config_.wal_initial_capacity_bytes = 4096;
  }

  void TearDown() override { std::filesystem::remove_all(config_.work_dir); }
};

TEST_F(PaxRecoveryTest, RestoresSchemaBeforeCheckpointAndWalValues) {
  const auto first = Row("42", "1.50");
  const auto second = Row("43", "2.50");
  {
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("t"));
    ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", first));
    ASSERT_TRUE(db.WriteCheckpoint());
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "b", second));
  }
  {
    Database db(config_);
    auto *store = db.GetPaxTable("t");
    ASSERT_NE(store, nullptr);
    EXPECT_EQ(pax::Schema(store).field_max_bytes, kWidths);
    EXPECT_EQ(pax::Schema(store).field_type, kTypes);
    EXPECT_EQ(pax::Schema(store).field_scale, kScales);
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), first);
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "b").value(), second);

    // Verify that recovery restored column locations as well as row bytes.
    const auto scan = db.ScanPax("t", "a", "z", 0, false);
    db.ReleaseThreadEpoch();
    ASSERT_TRUE(scan.ok);
    ASSERT_EQ(scan.rows.size(), 2u);
    EXPECT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
    EXPECT_FALSE(db.InstallPaxSchema("t", {1, 8, 8}, kTypes, kScales));
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", second));
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), second);
  }
}

TEST_F(PaxRecoveryTest, KeepsAllSchemasWhenRowReplayIsDisabled) {
  {
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("old"));
    ASSERT_TRUE(db.InstallPaxSchema("old", kWidths, kTypes, kScales));
    ASSERT_TRUE(TestHelper::WriteRow(db, "old", "a", Row("1", "1.00")));
  }
  {
    auto config = config_;
    config.enable_recovery = false;
    Database db(config);
    ASSERT_NE(db.GetPaxTable("old"), nullptr);
    EXPECT_FALSE(TestHelper::ReadRow(db, "old", "a").has_value());
    ASSERT_TRUE(db.CreateTable("new"));
    ASSERT_TRUE(db.InstallPaxSchema("new", {1, 3}));
  }
  {
    Database db(config_);
    EXPECT_NE(db.GetPaxTable("old"), nullptr);
    EXPECT_NE(db.GetPaxTable("new"), nullptr);
    EXPECT_TRUE(TestHelper::ReadRow(db, "old", "a").has_value());
  }
}

TEST_F(PaxRecoveryTest, FailedPublicationDoesNotEnableTheSchema) {
  Database db(config_);
  ASSERT_TRUE(db.CreateTable("first"));
  ASSERT_TRUE(db.InstallPaxSchema("first", {1, 3}));
  ASSERT_TRUE(db.CreateTable("second"));
  auto unknown_types = kTypes;
  unknown_types[1] = static_cast<FieldType>(255);
  EXPECT_FALSE(db.InstallPaxSchema("second", kWidths, unknown_types, kScales));
  EXPECT_EQ(db.GetPaxTable("second"), nullptr);
  const auto working =
      std::filesystem::path(config_.work_dir) / "pax_schema.working";
  ASSERT_TRUE(std::filesystem::create_directory(working));
  EXPECT_FALSE(db.InstallPaxSchema("second", kWidths, kTypes, kScales));
  EXPECT_EQ(db.GetPaxTable("second"), nullptr);
  const auto catalog = pax::LoadCatalog(config_.work_dir);
  ASSERT_EQ(catalog.status, pax::Catalog::Status::kOk);
  ASSERT_EQ(catalog.entries.size(), 1u);
  EXPECT_EQ(catalog.entries.begin()->first, "first");
  std::filesystem::remove(working);
  EXPECT_TRUE(db.InstallPaxSchema("second", kWidths, kTypes, kScales));
}

TEST_F(PaxRecoveryTest, RequiresSchemaBeforeTheFirstValue) {
  Database db(config_);
  ASSERT_TRUE(db.CreateTable("t"));
  EXPECT_FALSE(TestHelper::WriteRow(db, "t", "a", Row("1", "1.00")));
  EXPECT_FALSE(TestHelper::ReadRow(db, "t", "a").has_value());
  EXPECT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
  EXPECT_TRUE(TestHelper::WriteRow(db, "t", "a", Row("1", "1.00")));
}

TEST_F(PaxRecoveryTest, RejectsTheWholeWriteSetBeforeInstallingAnyValue) {
  Database db(config_);
  ASSERT_TRUE(db.CreateTable("t"));
  ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
  const auto original = Row("1", "1.00");
  ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", original));
  const auto before = db.Read("t", "a");
  db.ReleaseThreadEpoch();

  // Reject malformed rows, non-round-tripping numbers and typed overflow.
  const std::vector<std::string> unsupported{
      Row("3", "3.0"),
      Row("03", "3.00"),
      Row("2147483648", "3.00"),
      Row("3", "92233720368547758.08"),
      original.substr(0, original.size() - 1),
      original + "extra",
      ""};
  for (const auto &value : unsupported) {
    std::string commit_reason;
    EXPECT_FALSE(TestHelper::CommitRows(
        db, {}, {{"t", "a", Row("2", "2.00")}, {"t", "b", value}}, {}, {},
        commit_reason));
    const auto after = db.Read("t", "a");
    db.ReleaseThreadEpoch();
    EXPECT_EQ(after.value, original);
    EXPECT_EQ(after.tid, before.tid);
    EXPECT_FALSE(TestHelper::ReadRow(db, "t", "b").has_value());
  }
  EXPECT_TRUE(TestHelper::WriteRow(db, "t", "b", Row("3", "3.00")));
}

TEST_F(PaxRecoveryTest, RejectsBadInputBeforeConflictChecksAndSlotAllocation) {
  Database db(config_);
  ASSERT_TRUE(db.CreateTable("t"));
  ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
  const auto original = Row("1", "1.00");
  ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", original));
  const auto slots = pax::SlotsAllocated(db.GetPaxTable("t"));

  // Bad row bytes must be rejected before Silo sees the stale read evidence.
  std::string reason;
  EXPECT_FALSE(TestHelper::CommitRows(db, {{"t", "a", 0, true}},
                                      {{"t", "b", Row("2", "2.0")}}, {}, {},
                                      reason));
  EXPECT_EQ(reason, "pax_row_decode_failed");
  EXPECT_EQ(pax::SlotsAllocated(db.GetPaxTable("t")), slots);
  EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), original);
  EXPECT_FALSE(TestHelper::ReadRow(db, "t", "b").has_value());
}

TEST_F(PaxRecoveryTest, KeepsDecodedValuesSeparateAcrossWritesAndRecovery) {
  const auto first = Row("1", "1.00");
  const auto second = Row("2", "-2.50");
  const auto last = Row("3", "3.75");
  {
    std::string commit_reason;
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("t"));
    ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));

    // Each write keeps its own decoded fields, even when keys repeat.
    ASSERT_TRUE(
        TestHelper::CommitRows(db, {},
                               {{"t", "a", first},
                                {"t", "b", second},
                                {"t", "a", "", helios::storage::RowOp::kDelete},
                                {"t", "a", last}},
                               {}, {}, commit_reason));
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), last);
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "b").value(), second);
  }
  {
    Database db(config_);
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), last);
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "b").value(), second);
  }
}

TEST_F(PaxRecoveryTest, IgnoresAnInterruptedWorkingFile) {
  {
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("t"));
    ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", Row("1", "1.00")));
  }
  std::ofstream(std::filesystem::path(config_.work_dir) / "pax_schema.working")
      << "unfinished";
  Database db(config_);
  EXPECT_NE(db.GetPaxTable("t"), nullptr);
  EXPECT_TRUE(TestHelper::ReadRow(db, "t", "a").has_value());
}

TEST_F(PaxRecoveryTest, RefusesToRecoverValuesWithoutTheirSchema) {
  {
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("t"));
    ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", Row("1", "1.00")));
  }
  ASSERT_TRUE(std::filesystem::remove(std::filesystem::path(config_.work_dir) /
                                      "pax_schema.catalog"));
  EXPECT_EXIT(
      {
        Database db(config_);
        std::exit(EXIT_SUCCESS);
      },
      ::testing::ExitedWithCode(EXIT_FAILURE), "");
}

TEST_F(PaxRecoveryTest, RejectsAnUnreadablePublishedCatalog) {
  std::ofstream(std::filesystem::path(config_.work_dir) / "pax_schema.catalog")
      << "unfinished";
  EXPECT_EQ(pax::LoadCatalog(config_.work_dir).status,
            pax::Catalog::Status::kUnusable);
  EXPECT_EXIT(
      {
        Database db(config_);
        std::exit(EXIT_SUCCESS);
      },
      ::testing::ExitedWithCode(EXIT_FAILURE), "");
}

}  // namespace
