/**
 * @file server/storage/tests/pax_recovery_test.cc
 * Column definitions survive restart and put recovered values back in PAX.
 */

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "helios/database.h"
#include "helios/pax.h"

#include "db_helper.h"
#include "gtest/gtest.h"
#include "pax/catalog.h"
#include "pax/table.h"

namespace {

using helios::storage::Database;
using helios::storage::pax::FieldType;
namespace pax = helios::storage::pax;

const std::vector<uint32_t> kWidths{1, 4, 8};
const std::vector<FieldType> kTypes{FieldType::kUntyped, FieldType::kInt32,
                                    FieldType::kDecimal64};
const std::vector<int8_t> kScales{0, 0, 2};

// Row bytes of short fields: field 0 is the null flags, and an empty field is
// the empty marker.
std::string Pack(const std::vector<std::string> &fields) {
  std::string row;
  for (const auto &field : fields) {
    if (field.empty()) {
      row.push_back('\xFF');
      continue;
    }
    row.push_back('\1');
    row.push_back(static_cast<char>(field.size()));
    row.append(field);
  }
  return row;
}

// A typed column's cell: the value's little-endian bytes.
template <typename T>
std::string Cell(T value) {
  return std::string(reinterpret_cast<const char *>(&value), sizeof(value));
}

// The two payloads under no NULL flag.
std::string Fields(std::string_view integer, std::string_view decimal) {
  return Pack(
      {std::string(1, '\0'), std::string(integer), std::string(decimal)});
}

// A row of the INT column and the DECIMAL(p, 2) column holding `cents`.
std::string Row(int32_t integer, int64_t cents) {
  return Fields(Cell(integer), Cell(cents));
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
    config_.wal_segment_bytes = 1u << 20;
  }

  void TearDown() override { std::filesystem::remove_all(config_.work_dir); }
};

TEST_F(PaxRecoveryTest, RestoresSchemaBeforeCheckpointAndWalValues) {
  const auto first = Row(42, 150);
  const auto second = Row(43, 250);
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

TEST_F(PaxRecoveryTest, TableIdsSurviveCatalogOrderAndNewDeclarations) {
  {
    Database db(config_);
    ASSERT_TRUE(TestHelper::CreateTable(db, "z"));
    ASSERT_TRUE(TestHelper::CreateTable(db, "a"));
    ASSERT_TRUE(TestHelper::WriteRow(db, "z", "same", TestHelper::Row("z1")));
    ASSERT_TRUE(TestHelper::WriteRow(db, "a", "same", TestHelper::Row("a1")));
    ASSERT_TRUE(db.WriteCheckpoint());
    ASSERT_TRUE(TestHelper::WriteRow(db, "z", "same", TestHelper::Row("z2")));
  }
  {
    Database db(config_);
    EXPECT_EQ(TestHelper::ReadRow(db, "z", "same"), TestHelper::Row("z2"));
    EXPECT_EQ(TestHelper::ReadRow(db, "a", "same"), TestHelper::Row("a1"));
    ASSERT_TRUE(TestHelper::CreateTable(db, "m"));
    ASSERT_TRUE(TestHelper::WriteRow(db, "m", "same", TestHelper::Row("m1")));
    const auto catalog = pax::LoadCatalog(config_.work_dir);
    ASSERT_EQ(catalog.status, pax::Catalog::Status::kOk);
    EXPECT_EQ(catalog.entries.at("z").id, 1u);
    EXPECT_EQ(catalog.entries.at("a").id, 2u);
    EXPECT_EQ(catalog.entries.at("m").id, 3u);
  }
  {
    Database db(config_);
    EXPECT_EQ(TestHelper::ReadRow(db, "z", "same"), TestHelper::Row("z2"));
    EXPECT_EQ(TestHelper::ReadRow(db, "a", "same"), TestHelper::Row("a1"));
    EXPECT_EQ(TestHelper::ReadRow(db, "m", "same"), TestHelper::Row("m1"));
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
  EXPECT_FALSE(TestHelper::WriteRow(db, "t", "a", Row(1, 100)));
  EXPECT_FALSE(TestHelper::ReadRow(db, "t", "a").has_value());
  EXPECT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
  EXPECT_TRUE(TestHelper::WriteRow(db, "t", "a", Row(1, 100)));
}

TEST_F(PaxRecoveryTest, RejectsTheWholeWriteSetBeforeInstallingAnyValue) {
  Database db(config_);
  ASSERT_TRUE(db.CreateTable("t"));
  ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
  const auto original = Row(1, 100);
  ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", original));
  const auto before = db.Read("t", "a");
  db.ReleaseThreadEpoch();

  // Reject malformed rows and typed fields that are not their cells.
  const std::vector<std::string> unsupported{
      Fields("3", Cell<int64_t>(300)),
      Fields(Cell<int64_t>(3), Cell<int64_t>(300)),
      Fields(Cell<int32_t>(3), Cell<int32_t>(300)),
      original.substr(0, original.size() - 1),
      original + "extra",
      ""};
  for (const auto &value : unsupported) {
    std::string commit_reason;
    EXPECT_FALSE(TestHelper::CommitRows(
        db, {}, {{"t", "a", Row(2, 200)}, {"t", "b", value}}, {}, {},
        commit_reason));
    const auto after = db.Read("t", "a");
    db.ReleaseThreadEpoch();
    EXPECT_EQ(after.value, original);
    EXPECT_EQ(after.tid, before.tid);
    EXPECT_FALSE(TestHelper::ReadRow(db, "t", "b").has_value());
  }
  EXPECT_TRUE(TestHelper::WriteRow(db, "t", "b", Row(3, 300)));
}

TEST_F(PaxRecoveryTest, RejectsBadInputBeforeConflictChecksAndSlotAllocation) {
  Database db(config_);
  ASSERT_TRUE(db.CreateTable("t"));
  ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
  const auto original = Row(1, 100);
  ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", original));
  const auto slots = pax::SlotsAllocated(db.GetPaxTable("t"));

  // Bad row bytes must be rejected before Silo sees the stale observed TID.
  std::string reason;
  EXPECT_FALSE(TestHelper::CommitRows(
      db, {{"t", "a", 0}}, {{"t", "b", Fields(Cell<int32_t>(2), "2.0")}}, {},
      {}, reason));
  EXPECT_EQ(reason, "pax_row_unpack_failed");
  EXPECT_EQ(pax::SlotsAllocated(db.GetPaxTable("t")), slots);
  EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), original);
  EXPECT_FALSE(TestHelper::ReadRow(db, "t", "b").has_value());
}

TEST_F(PaxRecoveryTest, KeepsUnpackedValuesSeparateAcrossWritesAndRecovery) {
  const auto first = Row(1, 100);
  const auto second = Row(2, -250);
  const auto last = Row(3, 375);
  {
    std::string commit_reason;
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("t"));
    ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));

    // Each write keeps its own unpacked fields, even when keys repeat.
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

// `count` rows keyed from `first`, each holding `len` bytes of `fill`.
std::vector<TestHelper::RowWrite> StringRows(int first, int count, size_t len,
                                             char fill) {
  std::vector<TestHelper::RowWrite> rows;
  for (int i = first; i < first + count; i++) {
    char key[16];
    std::snprintf(key, sizeof(key), "k%06d", i);
    rows.push_back({"t", key, std::string(len, fill)});
  }
  return rows;
}

// The cell width of the group holding `key`, read through a PAX scan.
uint32_t CellWidth(Database &db, const std::string &key) {
  const auto scan = db.ScanPax("t", key, key + '\0', 0, false);
  db.ReleaseThreadEpoch();
  if (scan.rows.size() != 1) return 0;
  return static_cast<const pax::PaxGroup *>(scan.rows[0].group)
      ->layout()
      .cell_bytes[1];
}

// Replay places each recovered row in the size class of its final value,
// including the row a long update moved.
TEST_F(PaxRecoveryTest, RestoresRowsIntoTheirSizeClasses) {
  constexpr int kRows = pax::PaxGroup::kRows;
  {
    Database db(config_);
    ASSERT_TRUE(TestHelper::CreateTable(db, "t", 200));
    ASSERT_TRUE(TestHelper::CommitWrites(db, StringRows(0, kRows, 10, 'a')));
    ASSERT_TRUE(TestHelper::CommitWrites(db, StringRows(kRows, 8, 10, 'b')));
    ASSERT_TRUE(
        TestHelper::CommitWrites(db, StringRows(kRows + 3, 1, 150, 'c')));
    EXPECT_EQ(db.GetPaxTable("t")->moves(), 1u);
  }
  Database db(config_);
  auto *store = db.GetPaxTable("t");
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(pax::Group(store, 0)->layout().cell_bytes[1], 200u);
  EXPECT_EQ(TestHelper::Read(db, "t", "k000000").value(), std::string(10, 'a'));
  EXPECT_EQ(TestHelper::Read(db, "t", "k008194").value(), std::string(10, 'b'));
  EXPECT_EQ(TestHelper::Read(db, "t", "k008195").value(),
            std::string(150, 'c'));
  EXPECT_EQ(CellWidth(db, "k000000"), 16u);
  EXPECT_EQ(CellWidth(db, "k008194"), 16u);
  EXPECT_EQ(CellWidth(db, "k008195"), 160u);
}

// Typed cells and NULL read back as the bytes the commit wrote: in memory,
// from the checkpoint and from the WAL.
TEST_F(PaxRecoveryTest, TypedCellsSurviveCommitAndRecovery) {
  const std::vector<uint32_t> widths{1, 4, 8, 4, 8};
  const std::vector<FieldType> types{FieldType::kUntyped, FieldType::kInt32,
                                     FieldType::kInt64, FieldType::kDate,
                                     FieldType::kDecimal64};
  const std::vector<int8_t> scales{0, 0, 0, 0, 4};
  const std::string no_nulls(1, '\0');
  const auto low =
      Pack({no_nulls, Cell<int32_t>(INT32_MIN), Cell<int64_t>(INT64_MIN),
            Cell<int32_t>(10000101), Cell<int64_t>(-1)});
  const auto high =
      Pack({no_nulls, Cell<int32_t>(INT32_MAX), Cell<int64_t>(INT64_MAX),
            Cell<int32_t>(99991231), Cell<int64_t>(999999999999999)});
  const auto nulls = Pack({std::string(1, '\x0f'), "", "", "", ""});
  {
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("t"));
    ASSERT_TRUE(db.InstallPaxSchema("t", widths, types, scales));
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", low));
    ASSERT_TRUE(db.WriteCheckpoint());
    std::string reason;
    ASSERT_TRUE(TestHelper::CommitRows(
        db, {}, {{"t", "b", high}, {"t", "c", nulls}}, {}, {}, reason))
        << reason;
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), low);
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "b").value(), high);
    EXPECT_EQ(TestHelper::ReadRow(db, "t", "c").value(), nulls);
  }
  Database db(config_);
  EXPECT_EQ(TestHelper::ReadRow(db, "t", "a").value(), low);
  EXPECT_EQ(TestHelper::ReadRow(db, "t", "b").value(), high);
  EXPECT_EQ(TestHelper::ReadRow(db, "t", "c").value(), nulls);
}

TEST_F(PaxRecoveryTest, IgnoresAnInterruptedWorkingFile) {
  {
    Database db(config_);
    ASSERT_TRUE(db.CreateTable("t"));
    ASSERT_TRUE(db.InstallPaxSchema("t", kWidths, kTypes, kScales));
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", Row(1, 100)));
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
    ASSERT_TRUE(TestHelper::WriteRow(db, "t", "a", Row(1, 100)));
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
