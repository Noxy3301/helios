/**
 * @file server/storage/tests/wal_packing_test.cc
 * That a record the commit packs, and a frame of packed records, are byte
 * for byte msgpack's packing of the LogRecords they hold.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <msgpack.hpp>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"

#include "db_helper.h"
#include "wal/log_record.h"
#include "wal/wal.h"

namespace {

using helios::storage::EpochNumber;
using helios::storage::IndexConstraint;
using helios::storage::RowOp;
using helios::storage::Tidword;
using helios::storage::wal::LogRecord;
using helios::storage::wal::LogRecords;
using helios::storage::wal::PackedLogRecords;
using helios::storage::wal::SecondaryIndexOp;
using helios::storage::wal::Wal;
using helios::storage::wal::WalScanResult;
using Write = LogRecord::Write;

// Lengths on both sides of each msgpack str boundary: fixstr, str8, str16
// and str32.
constexpr size_t kLengths[] = {0, 1, 31, 32, 255, 256, 65535, 65536, 70 * 1024};
constexpr size_t kLengthCount = std::size(kLengths);

// Epochs and TID words across the uint encodings, with the high bits set.
constexpr EpochNumber kEpochs[] = {1,   127,   128,   255,
                                   256, 65535, 65536, UINT32_MAX};
constexpr uint64_t kTids[] = {0,
                              127,
                              128,
                              0xffffffffull,
                              0x100000000ull,
                              0x8000000000000000ull,
                              0xfedcba9876543210ull,
                              ~0ull};

std::string Packed(const LogRecords &records) {
  msgpack::sbuffer buffer;
  msgpack::pack(buffer, records);
  return std::string(buffer.data(), buffer.size());
}

std::string Filled(size_t i, char c) {
  return std::string(kLengths[i % kLengthCount], c);
}

// Primary rows, deletes, secondary deltas and a full list, first all in one
// record and then one per record.
LogRecords VariedRecords() {
  std::vector<Write> writes;
  for (size_t i = 0; i < kLengthCount; ++i) {
    Write row;
    row.key = Filled(i + 2, 'k');
    row.buffer = Filled(i, 'v');
    row.transaction_id = Tidword(kTids[i % std::size(kTids)]);
    row.table_name = Filled(i + 5, 't');
    writes.push_back(row);

    Write delta;
    delta.key = Filled(i + 1, 's');
    delta.transaction_id = Tidword(kTids[(i + 3) % std::size(kTids)]);
    delta.table_name = Filled(i + 7, 't');
    delta.index_name = Filled(i + 4, 'i');
    delta.index_type = i % 2 == 0 ? UINT32_MAX : static_cast<uint32_t>(i);
    delta.secondary_op =
        i % 2 == 0 ? SecondaryIndexOp::kInsert : SecondaryIndexOp::kDelete;
    delta.secondary_primary_key = Filled(i + 3, 'p');
    writes.push_back(delta);
  }

  Write erased;
  erased.key = "erased";
  erased.transaction_id = Tidword(kTids[6]);
  erased.transaction_id.absent = true;
  erased.table_name = "t";
  writes.push_back(erased);

  Write full;
  full.key = "full";
  full.transaction_id = Tidword(kTids[7]);
  full.table_name = "t";
  full.index_name = "idx";
  full.index_type = 1;
  full.secondary_op = SecondaryIndexOp::kFull;
  for (size_t i = 0; i < 20; ++i) full.primary_keys.push_back(Filled(i, 'f'));
  writes.push_back(full);

  LogRecords records(1);
  records[0].epoch = kEpochs[6];
  records[0].writes = writes;
  for (size_t i = 0; i < writes.size(); ++i) {
    LogRecord record;
    record.epoch = kEpochs[i % std::size(kEpochs)];
    record.writes.push_back(writes[i]);
    records.push_back(std::move(record));
  }
  return records;
}

uint32_t GetLe32(const std::string &in, size_t off) {
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i)
    value |= static_cast<uint32_t>(static_cast<uint8_t>(in[off + i]))
             << (8 * i);
  return value;
}

// The payloads of the log's frames, in file order.
std::vector<std::string> FramePayloads(const std::string &work_dir) {
  std::ifstream in(work_dir + "/wal.log", std::ios::binary);
  const std::string file((std::istreambuf_iterator<char>(in)),
                         std::istreambuf_iterator<char>());
  std::vector<std::string> payloads;
  size_t off = 0;
  while (file.size() - off >= Wal::kHeaderSize &&
         GetLe32(file, off) == Wal::kMagic) {
    const uint32_t size = GetLe32(file, off + 6);
    payloads.push_back(file.substr(off + Wal::kHeaderSize, size));
    off += Wal::kHeaderSize + size;
  }
  return payloads;
}

class WalPackingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "helios_walpack_XXXXXX")
            .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
    work_dir_ = root_ + "/logs";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  std::string root_;
  std::string work_dir_;
};

TEST_F(WalPackingTest, AFramePayloadIsTheListPacking) {
  constexpr EpochNumber kEpoch = 65536;
  LogRecords records = VariedRecords();
  PackedLogRecords packed;
  for (auto &record : records) {
    record.epoch = kEpoch;
    packed.push_back({kEpoch, {}});
    msgpack::pack(packed.back(), record);
  }

  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(),
            Wal::kNoPreallocation);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    std::map<EpochNumber, PackedLogRecords> buckets;
    buckets[kEpoch] = packed;
    ASSERT_TRUE(wal.AppendGroup(buckets, kEpoch).ok);
  }

  const auto payloads = FramePayloads(work_dir_);
  ASSERT_EQ(payloads.size(), 1u);
  EXPECT_TRUE(payloads[0] == Packed(records));

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(),
          Wal::kNoPreallocation);
  const auto scan = wal.Scan();
  ASSERT_EQ(scan.status, WalScanResult::Status::kOk) << scan.detail;
  EXPECT_TRUE(Packed(scan.records) == payloads[0]);
}

// One commit's writes and the secondary keys whose list it leaves empty.
struct Commit {
  std::vector<TestHelper::RowWrite> rows;
  std::vector<TestHelper::IndexOp> ops;
  std::set<std::string> emptied;
};

// The write set runs in record-address order; within one record the writes
// keep their feed order, which a stable sort by record preserves.
void SortByRecord(std::vector<Write> &writes) {
  std::stable_sort(writes.begin(), writes.end(),
                   [](const Write &a, const Write &b) {
                     return std::tie(a.table_name, a.index_name, a.key) <
                            std::tie(b.table_name, b.index_name, b.key);
                   });
}

TEST_F(WalPackingTest, ACommitRecordIsTheLogRecordPacking) {
  const std::string table = "t";
  const std::string wide_table(40, 'w');
  const std::string index = "idx";
  const std::string unique_index(32, 'u');
  const std::map<std::string, uint32_t> index_types = {
      {index, static_cast<uint32_t>(IndexConstraint::kNone)},
      {unique_index, static_cast<uint32_t>(IndexConstraint::kUnique)}};

  // Keys and row bytes cross the fixstr, str8 and str16 boundaries (a row of
  // value length n packs as n + 5 bytes below 256, n + 6 above); secondary
  // primary keys reach str32.
  const std::string key32(32, 'b');
  const std::string pk_str16(65535, 'p');
  const std::string pk_str32(65536, 'q');
  const std::string pk_large(70 * 1024, 'r');
  std::vector<Commit> commits(2);
  commits[0].rows = {{table, "k", ""},
                     {table, std::string(31, 'a'), std::string(26, 'x')},
                     {table, key32, std::string(27, 'x')},
                     {table, std::string(255, 'c'), std::string(250, 'x')},
                     {table, std::string(256, 'd'), std::string(251, 'x')},
                     {table, std::string(300, 'e'), std::string(4000, 'x')},
                     {wide_table, "w1", "v"}};
  commits[0].ops = {
      {table, index, "s", "k"},       {table, index, "s", std::string(31, 'a')},
      {table, index, "s", pk_str16},  {table, index, "s2", pk_str32},
      {table, index, "s2", pk_large}, {wide_table, unique_index, "u1", "w1"}};
  commits[1].rows = {{table, "k", "new"},
                     {table, key32, "", RowOp::kDelete},
                     {wide_table, "w1", "", RowOp::kDelete}};
  commits[1].ops = {{table, index, "s", "k", true},
                    {table, index, "s", "k"},
                    {table, index, "s2", pk_str32, true},
                    {table, index, "s2", pk_large, true},
                    {wide_table, unique_index, "u1", "w1", true}};
  commits[1].emptied = {"s2", "u1"};

  {
    helios::storage::Config config;
    config.epoch_duration_ms = 10;
    config.enable_recovery = false;
    config.work_dir = work_dir_;
    config.wal_initial_capacity_bytes = 1ull << 20;
    helios::storage::Database db(config);
    ASSERT_TRUE(TestHelper::CreateTable(db, table));
    ASSERT_TRUE(TestHelper::CreateTable(db, wide_table));
    ASSERT_TRUE(db.CreateSecondaryIndex(table, index, IndexConstraint::kNone));
    ASSERT_TRUE(db.CreateSecondaryIndex(wide_table, unique_index,
                                        IndexConstraint::kUnique));
    for (const auto &commit : commits) {
      std::string reason;
      ASSERT_TRUE(
          TestHelper::Commit(db, {}, commit.rows, commit.ops, {}, reason))
          << reason;
    }
  }

  // Decode each payload and pack the result again: equal bytes mean the
  // commit wrote msgpack's own packing of the fields it decodes to.
  LogRecords logged;
  for (const auto &payload : FramePayloads(work_dir_)) {
    LogRecords records;
    msgpack::unpack(payload.data(), payload.size()).get().convert(records);
    EXPECT_TRUE(Packed(records) == payload);
    logged.insert(logged.end(), records.begin(), records.end());
  }
  ASSERT_EQ(logged.size(), commits.size());

  for (size_t c = 0; c < commits.size(); ++c) {
    // The writes the commit logs, in feed order within each record.
    std::vector<Write> expected;
    for (const auto &row : commits[c].rows) {
      Write write;
      write.key = row.key;
      if (row.op != RowOp::kDelete) write.buffer = TestHelper::Row(row.value);
      write.transaction_id.absent = row.op == RowOp::kDelete;
      write.table_name = row.table;
      expected.push_back(write);
    }
    for (const auto &op : commits[c].ops) {
      Write write;
      write.key = op.secondary_key;
      write.transaction_id.absent =
          commits[c].emptied.count(op.secondary_key) != 0;
      write.table_name = op.table;
      write.index_name = op.index;
      write.index_type = index_types.at(op.index);
      write.secondary_op =
          op.remove ? SecondaryIndexOp::kDelete : SecondaryIndexOp::kInsert;
      write.secondary_primary_key = op.primary_key;
      expected.push_back(write);
    }
    auto actual = logged[c].writes;
    SortByRecord(actual);
    SortByRecord(expected);
    ASSERT_EQ(actual.size(), expected.size()) << "commit " << c;

    // Every write carries the commit's published word, unlocked.
    Tidword commit_tid = actual[0].transaction_id;
    commit_tid.absent = false;
    EXPECT_EQ(commit_tid.epoch, logged[c].epoch);
    EXPECT_FALSE(commit_tid.lock);
    EXPECT_TRUE(commit_tid.latest);
    for (size_t i = 0; i < actual.size(); ++i) {
      const Write &a = actual[i];
      const Write &e = expected[i];
      EXPECT_TRUE(
          a.key == e.key && a.buffer == e.buffer &&
          a.table_name == e.table_name && a.index_name == e.index_name &&
          a.index_type == e.index_type && a.primary_keys == e.primary_keys &&
          a.secondary_op == e.secondary_op &&
          a.secondary_primary_key == e.secondary_primary_key &&
          a.transaction_id.absent == e.transaction_id.absent)
          << "commit " << c << " write " << i;
      Tidword word = actual[i].transaction_id;
      word.absent = false;
      EXPECT_EQ(word.obj, commit_tid.obj) << "commit " << c << " write " << i;
    }
  }
}

}  // namespace
