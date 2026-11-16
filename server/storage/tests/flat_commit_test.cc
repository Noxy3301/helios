#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/rpc_flat.h"

using namespace helios::rpc;

namespace {

// Every list and field set, with empty strings and empty inner lists among
// them.
FlatCommit full_commit() {
  FlatCommit c;
  c.async_durability = true;
  c.reads.push_back({"t1", "k1", 11, 0xF0});
  c.reads.push_back({"t1", "", 0, 0});
  c.reads.push_back({"", std::string_view("\0k\0", 3),
                     std::numeric_limits<uint64_t>::max(), 1});

  FlatRange primary{"t2", "", "a", "z", 7, true, {}, {}};
  primary.rows.push_back({"pk1", 21});
  primary.rows.push_back({"", 22});
  primary.visited.push_back({"gone", 23});
  c.ranges.push_back(primary);
  FlatRange secondary{"t2", "by_name", "", "", 0, false, {}, {}};
  secondary.visited.push_back({"sk1", 24});
  c.ranges.push_back(secondary);
  c.ranges.push_back({"t3", "idx", "b", "c", 1, false, {}, {}});

  c.writes.push_back({"t1", "k2", "row", FlatRowOp::kInsert, 0x3});
  c.writes.push_back({"t1", "k3", "", FlatRowOp::kUpdate, 0});
  c.writes.push_back({"t4", "k4", "", FlatRowOp::kDelete, 0});

  c.index_ops.push_back({"t1", "by_name", "sk", "pk", false});
  c.index_ops.push_back({"t1", "by_name", "", "", true});

  c.row_deltas.push_back({"t1", 1});
  c.row_deltas.push_back({"t4", -1});
  return c;
}

void expect_records(const std::vector<FlatRecord> &got,
                    const std::vector<FlatRecord> &want) {
  ASSERT_EQ(got.size(), want.size());
  for (size_t i = 0; i < want.size(); i++) {
    EXPECT_EQ(got[i].key, want[i].key);
    EXPECT_EQ(got[i].tid, want[i].tid);
  }
}

void expect_same(const FlatCommit &got, const FlatCommit &want) {
  EXPECT_EQ(got.async_durability, want.async_durability);

  ASSERT_EQ(got.reads.size(), want.reads.size());
  for (size_t i = 0; i < want.reads.size(); i++) {
    EXPECT_EQ(got.reads[i].table, want.reads[i].table);
    EXPECT_EQ(got.reads[i].key, want.reads[i].key);
    EXPECT_EQ(got.reads[i].tid, want.reads[i].tid);
    EXPECT_EQ(got.reads[i].column_mask, want.reads[i].column_mask);
  }

  ASSERT_EQ(got.ranges.size(), want.ranges.size());
  for (size_t i = 0; i < want.ranges.size(); i++) {
    EXPECT_EQ(got.ranges[i].table, want.ranges[i].table);
    EXPECT_EQ(got.ranges[i].index, want.ranges[i].index);
    EXPECT_EQ(got.ranges[i].start_key, want.ranges[i].start_key);
    EXPECT_EQ(got.ranges[i].end_key, want.ranges[i].end_key);
    EXPECT_EQ(got.ranges[i].row_limit, want.ranges[i].row_limit);
    EXPECT_EQ(got.ranges[i].reverse_scan, want.ranges[i].reverse_scan);
    expect_records(got.ranges[i].rows, want.ranges[i].rows);
    expect_records(got.ranges[i].visited, want.ranges[i].visited);
  }

  ASSERT_EQ(got.writes.size(), want.writes.size());
  for (size_t i = 0; i < want.writes.size(); i++) {
    EXPECT_EQ(got.writes[i].table, want.writes[i].table);
    EXPECT_EQ(got.writes[i].key, want.writes[i].key);
    EXPECT_EQ(got.writes[i].value, want.writes[i].value);
    EXPECT_EQ(got.writes[i].op, want.writes[i].op);
    EXPECT_EQ(got.writes[i].column_mask, want.writes[i].column_mask);
  }

  ASSERT_EQ(got.index_ops.size(), want.index_ops.size());
  for (size_t i = 0; i < want.index_ops.size(); i++) {
    EXPECT_EQ(got.index_ops[i].table, want.index_ops[i].table);
    EXPECT_EQ(got.index_ops[i].index, want.index_ops[i].index);
    EXPECT_EQ(got.index_ops[i].secondary_key, want.index_ops[i].secondary_key);
    EXPECT_EQ(got.index_ops[i].primary_key, want.index_ops[i].primary_key);
    EXPECT_EQ(got.index_ops[i].is_delete, want.index_ops[i].is_delete);
  }

  ASSERT_EQ(got.row_deltas.size(), want.row_deltas.size());
  for (size_t i = 0; i < want.row_deltas.size(); i++) {
    EXPECT_EQ(got.row_deltas[i].table, want.row_deltas[i].table);
    EXPECT_EQ(got.row_deltas[i].delta, want.row_deltas[i].delta);
  }
}

// Unpacks from a buffer of exactly len bytes, so a sanitizer sees any overread.
bool unpack_prefix(std::string_view body, size_t len) {
  auto buf = std::make_unique<char[]>(len);
  std::copy_n(body.data(), len, buf.get());
  FlatCommit out;
  return unpack_commit(std::string_view(buf.get(), len), out);
}

}  // namespace

TEST(FlatCommitTest, EveryFieldSurvives) {
  const FlatCommit want = full_commit();
  std::string body;
  pack_commit(want, body);

  FlatCommit got;
  ASSERT_TRUE(unpack_commit(body, got));
  expect_same(got, want);

  // Unpacked strings are views over the body, not copies.
  const auto *key = got.writes[0].key.data();
  EXPECT_GE(key, body.data());
  EXPECT_LT(key, body.data() + body.size());
}

TEST(FlatCommitTest, ZeroCountsSurvive) {
  const FlatCommit want;
  std::string body;
  pack_commit(want, body);

  FlatCommit got;
  got.async_durability = true;
  ASSERT_TRUE(unpack_commit(body, got));
  expect_same(got, want);
}

TEST(FlatCommitTest, EveryTruncationIsRefused) {
  std::string body;
  pack_commit(full_commit(), body);
  for (size_t len = 0; len < body.size(); len++) {
    EXPECT_FALSE(unpack_prefix(body, len)) << "prefix of " << len << " bytes";
  }
  EXPECT_TRUE(unpack_prefix(body, body.size()));
}

TEST(FlatCommitTest, TrailingBytesAreRefused) {
  std::string body;
  pack_commit(full_commit(), body);
  body.push_back('\0');
  FlatCommit out;
  EXPECT_FALSE(unpack_commit(body, out));
}

TEST(FlatCommitTest, HugeCountAndLengthAreRefused) {
  // The reads count, right after the async byte.
  std::string body;
  put<uint8_t>(body, 0);
  put<uint32_t>(body, std::numeric_limits<uint32_t>::max());
  FlatCommit out;
  EXPECT_FALSE(unpack_commit(body, out));

  // The first read's table length.
  body.clear();
  put<uint8_t>(body, 0);
  put<uint32_t>(body, 1);
  put<uint32_t>(body, std::numeric_limits<uint32_t>::max());
  FlatCommit out2;
  EXPECT_FALSE(unpack_commit(body, out2));
}

TEST(FlatCommitTest, ReplyRoundTripAndTruncation) {
  FlatCommitReply want;
  want.committed = false;
  want.duplicate_key = true;
  want.abort_detail = "duplicate_primary_key";
  want.table_stats = {{"t1", 5}, {"", 0}, {"t2", -1}};
  std::string body;
  pack_commit_reply(want, body);

  FlatCommitReply got;
  ASSERT_TRUE(unpack_commit_reply(body, got));
  EXPECT_EQ(got.committed, want.committed);
  EXPECT_EQ(got.duplicate_key, want.duplicate_key);
  EXPECT_EQ(got.abort_detail, want.abort_detail);
  EXPECT_EQ(got.table_stats, want.table_stats);

  for (size_t len = 0; len < body.size(); len++) {
    auto buf = std::make_unique<char[]>(len);
    std::copy_n(body.data(), len, buf.get());
    FlatCommitReply out;
    EXPECT_FALSE(unpack_commit_reply(std::string_view(buf.get(), len), out))
        << "prefix of " << len << " bytes";
  }

  // A committed reply with nothing else set.
  body.clear();
  FlatCommitReply empty;
  empty.committed = true;
  pack_commit_reply(empty, body);
  FlatCommitReply got_empty;
  ASSERT_TRUE(unpack_commit_reply(body, got_empty));
  EXPECT_TRUE(got_empty.committed);
  EXPECT_FALSE(got_empty.duplicate_key);
  EXPECT_TRUE(got_empty.abort_detail.empty());
  EXPECT_TRUE(got_empty.table_stats.empty());
}
