/**
 * @file common/rpc_flat.h
 * Bodies of the RPCs that skip protobuf (see common/rpc_frame.h). Integers are
 * native-endian and a bytes field is [u32 len][bytes]. Unpacked views borrow
 * the buffer they were unpacked from.
 */
#ifndef HELIOS_COMMON_RPC_FLAT_H
#define HELIOS_COMMON_RPC_FLAT_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace helios::rpc {

/**
 * @brief The one byte a flat request has for an envelope.
 *
 * @details Protobuf cannot produce a one-byte envelope: a set arm takes a tag
 * and a length.
 * kRead:      [table][key]
 *   reply:    envelope [u8 found][u64 tid], payload the value
 * kBatchRead: [u32 n]{[table][key]}
 *   reply:    envelope [u32 n]{[u8 found][u64 tid][value]}
 * kCommit:    see pack_commit
 *   reply:    envelope, see pack_commit_reply
 */
enum class FlatOp : uint8_t {
  kRead = 1,
  kBatchRead = 2,
  kCommit = 3,
};

// A cursor that refuses to pass the end: a failed read clears ok() and gives
// zero, and every later read fails too.
class FlatReader {
 public:
  explicit FlatReader(std::string_view buf) : buf_(buf) {}

  bool ok() const { return ok_; }
  // True when the whole buffer was consumed without a failed read.
  bool done() const { return ok_ && buf_.empty(); }

  template <class T>
  T get() {
    T v{};
    if (!ok_ || buf_.size() < sizeof(T)) {
      ok_ = false;
      return v;
    }
    std::memcpy(&v, buf_.data(), sizeof(T));
    buf_.remove_prefix(sizeof(T));
    return v;
  }

  std::string_view get_bytes() {
    const uint32_t len = get<uint32_t>();
    if (!ok_ || buf_.size() < len) {
      ok_ = false;
      return {};
    }
    const std::string_view out = buf_.substr(0, len);
    buf_.remove_prefix(len);
    return out;
  }

  // Reads an element count and caps the reserve at the bytes left, since every
  // element takes at least one byte.
  template <class V>
  uint32_t get_count(V &v) {
    const uint32_t n = get<uint32_t>();
    v.reserve(std::min<size_t>(n, buf_.size()));
    return n;
  }

 private:
  std::string_view buf_;
  bool ok_ = true;
};

template <class T>
void put(std::string &out, T v) {
  out.append(reinterpret_cast<const char *>(&v), sizeof(T));
}

inline void put_bytes(std::string &out, std::string_view s) {
  put<uint32_t>(out, static_cast<uint32_t>(s.size()));
  out.append(s);
}

enum class FlatRowOp : uint8_t {
  kUpdate = 0,
  kInsert = 1,  // refused when the key holds a row
  kDelete = 2,
};

struct FlatRead {
  std::string_view table;
  std::string_view key;
  uint64_t tid;
  uint64_t column_mask;
};

struct FlatRecord {
  std::string_view key;
  uint64_t tid;
};

struct FlatRange {
  std::string_view table;
  std::string_view index;  // Empty means primary index.
  std::string_view start_key;
  std::string_view end_key;
  uint64_t row_limit;
  bool reverse_scan;
  std::vector<FlatRecord> rows;
  std::vector<FlatRecord> visited;
};

struct FlatWrite {
  std::string_view table;
  std::string_view key;
  std::string_view value;
  FlatRowOp op;
  uint64_t column_mask;
};

struct FlatIndexOp {
  std::string_view table;
  std::string_view index;
  std::string_view secondary_key;
  std::string_view primary_key;
  bool is_delete;
};

struct FlatRowDelta {
  std::string_view table;
  int64_t delta;
};

struct FlatCommit {
  bool async_durability = false;
  std::vector<FlatRead> reads;
  std::vector<FlatRange> ranges;
  std::vector<FlatWrite> writes;  // in the order the query layer issued them
  std::vector<FlatIndexOp> index_ops;
  std::vector<FlatRowDelta> row_deltas;
};

inline void put_records(std::string &out,
                        const std::vector<FlatRecord> &records) {
  put<uint32_t>(out, static_cast<uint32_t>(records.size()));
  for (const auto &record : records) {
    put_bytes(out, record.key);
    put<uint64_t>(out, record.tid);
  }
}

inline void get_records(FlatReader &in, std::vector<FlatRecord> &records) {
  const uint32_t n = in.get_count(records);
  for (uint32_t i = 0; i < n && in.ok(); i++) {
    records.push_back({in.get_bytes(), in.get<uint64_t>()});
  }
}

/**
 * @brief Packs a commit body.
 *
 * @details The layout, each list starting with [u32 n]:
 * [u8 async]
 * reads      {[table][key][u64 tid][u64 column_mask]}
 * ranges     {[table][index][start][end][u64 limit][u8 reverse]
 *             rows{[key][u64 tid]} visited{[key][u64 tid]}}
 * writes     {[table][key][value][u8 op][u64 column_mask]}
 * index ops  {[table][index][secondary key][primary key][u8 is_delete]}
 * row deltas {[table][i64 delta]}
 */
inline void pack_commit(const FlatCommit &c, std::string &out) {
  put<uint8_t>(out, c.async_durability);

  put<uint32_t>(out, static_cast<uint32_t>(c.reads.size()));
  for (const auto &read : c.reads) {
    put_bytes(out, read.table);
    put_bytes(out, read.key);
    put<uint64_t>(out, read.tid);
    put<uint64_t>(out, read.column_mask);
  }

  put<uint32_t>(out, static_cast<uint32_t>(c.ranges.size()));
  for (const auto &range : c.ranges) {
    put_bytes(out, range.table);
    put_bytes(out, range.index);
    put_bytes(out, range.start_key);
    put_bytes(out, range.end_key);
    put<uint64_t>(out, range.row_limit);
    put<uint8_t>(out, range.reverse_scan);
    put_records(out, range.rows);
    put_records(out, range.visited);
  }

  put<uint32_t>(out, static_cast<uint32_t>(c.writes.size()));
  for (const auto &write : c.writes) {
    put_bytes(out, write.table);
    put_bytes(out, write.key);
    put_bytes(out, write.value);
    put<uint8_t>(out, static_cast<uint8_t>(write.op));
    put<uint64_t>(out, write.column_mask);
  }

  put<uint32_t>(out, static_cast<uint32_t>(c.index_ops.size()));
  for (const auto &op : c.index_ops) {
    put_bytes(out, op.table);
    put_bytes(out, op.index);
    put_bytes(out, op.secondary_key);
    put_bytes(out, op.primary_key);
    put<uint8_t>(out, op.is_delete);
  }

  put<uint32_t>(out, static_cast<uint32_t>(c.row_deltas.size()));
  for (const auto &row_delta : c.row_deltas) {
    put_bytes(out, row_delta.table);
    put<int64_t>(out, row_delta.delta);
  }
}

/**
 * @brief Unpacks a whole commit body into views over it.
 *
 * @return False when an entry passes the end of the body or bytes are left
 *         over, in which case out holds no usable commit.
 */
inline bool unpack_commit(std::string_view body, FlatCommit &out) {
  FlatReader in(body);
  out.async_durability = in.get<uint8_t>() != 0;

  uint32_t n = in.get_count(out.reads);
  for (uint32_t i = 0; i < n && in.ok(); i++) {
    out.reads.push_back({in.get_bytes(), in.get_bytes(), in.get<uint64_t>(),
                         in.get<uint64_t>()});
  }

  n = in.get_count(out.ranges);
  for (uint32_t i = 0; i < n && in.ok(); i++) {
    FlatRange range{in.get_bytes(),
                    in.get_bytes(),
                    in.get_bytes(),
                    in.get_bytes(),
                    in.get<uint64_t>(),
                    in.get<uint8_t>() != 0,
                    {},
                    {}};
    get_records(in, range.rows);
    get_records(in, range.visited);
    out.ranges.push_back(std::move(range));
  }

  n = in.get_count(out.writes);
  for (uint32_t i = 0; i < n && in.ok(); i++) {
    out.writes.push_back({in.get_bytes(), in.get_bytes(), in.get_bytes(),
                          static_cast<FlatRowOp>(in.get<uint8_t>()),
                          in.get<uint64_t>()});
  }

  n = in.get_count(out.index_ops);
  for (uint32_t i = 0; i < n && in.ok(); i++) {
    out.index_ops.push_back({in.get_bytes(), in.get_bytes(), in.get_bytes(),
                             in.get_bytes(), in.get<uint8_t>() != 0});
  }

  n = in.get_count(out.row_deltas);
  for (uint32_t i = 0; i < n && in.ok(); i++) {
    out.row_deltas.push_back({in.get_bytes(), in.get<int64_t>()});
  }
  return in.done();
}

struct FlatCommitReply {
  bool committed = false;
  bool duplicate_key = false;
  std::string_view abort_detail;  // Free-form cause, for tracing only.
  std::vector<std::pair<std::string_view, int64_t>> table_stats;
};

// [u8 committed][u8 duplicate_key][abort detail][u32 n]{[table][i64 row count]}
inline void pack_commit_reply(const FlatCommitReply &r, std::string &out) {
  put<uint8_t>(out, r.committed);
  put<uint8_t>(out, r.duplicate_key);
  put_bytes(out, r.abort_detail);
  put<uint32_t>(out, static_cast<uint32_t>(r.table_stats.size()));
  for (const auto &[table, count] : r.table_stats) {
    put_bytes(out, table);
    put<int64_t>(out, count);
  }
}

inline bool unpack_commit_reply(std::string_view body, FlatCommitReply &out) {
  FlatReader in(body);
  out.committed = in.get<uint8_t>() != 0;
  out.duplicate_key = in.get<uint8_t>() != 0;
  out.abort_detail = in.get_bytes();
  const uint32_t n = in.get_count(out.table_stats);
  for (uint32_t i = 0; i < n && in.ok(); i++) {
    const std::string_view table = in.get_bytes();
    out.table_stats.emplace_back(table, in.get<int64_t>());
  }
  return in.done();
}

}  // namespace helios::rpc

#endif  // HELIOS_COMMON_RPC_FLAT_H
