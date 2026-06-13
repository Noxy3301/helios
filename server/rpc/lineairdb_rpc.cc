#include "lineairdb_rpc.hh"
#include "predicate_evaluator.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>
#include <unordered_set>
#include <cstring>
#include <algorithm>
#include <cstdlib>

#include "lineairdb.pb.h"
#include <map>
#include <set>
#include <type_traits>
#include <thread>

namespace {

// Bump the byte string to its lexicographic successor; returns empty on overflow.
std::string next_lexicographic_key(std::string key) {
    for (size_t i = key.size(); i-- > 0;) {
        auto byte = static_cast<unsigned char>(key[i]);
        if (byte != 0xFF) {
            key[i] = static_cast<char>(byte + 1);
            key.resize(i + 1);
            return key;
        }
    }
    return {};
}

// (Projection pushdown) Trim a full row VALUE to only the projected columns.
// Row format (ha_lineairdb set_write_buffer): [null_flags field][col_0]..[col_{N-1}],
// each field = [byteSize:1B][len:byteSize B][bytes], byteSize==0xFF => null (1 byte).
// `kept` = ascending unique 0-based column indices. Emits [null_flags][kept cols]
// (null_flags kept FULL). Returns false on any inconsistency; caller then ships
// the FULL value unchanged (safe fallback). num_columns = table->s->fields.
bool trim_row_value(const std::string& full,
                    const google::protobuf::RepeatedField<uint32_t>& kept,
                    uint32_t num_columns, std::string& out) {
    out.clear();
    const char* end = full.data() + full.size();
    auto read_field = [&](const char*& q, const char*& fstart,
                          size_t& flen) -> bool {
        fstart = q;
        if (q >= end) return false;
        uint8_t bs = static_cast<uint8_t>(*q);
        if (bs == 0xFF) { flen = 1; q += 1; return true; }
        if (q + 1 + bs > end) return false;
        size_t len = 0;
        for (uint8_t i = 0; i < bs; i++)
            len |= static_cast<size_t>(static_cast<uint8_t>(q[1 + i])) << (8 * i);
        if (q + 1 + bs + len > end) return false;
        flen = 1 + bs + len;
        q += flen;
        return true;
    };
    const char* q = full.data();
    const char* fs;
    size_t fl;
    if (!read_field(q, fs, fl)) return false;  // field 0 = null_flags
    out.append(fs, fl);
    int ki = 0;
    for (uint32_t c = 0; c < num_columns; c++) {  // column c is field index c+1
        const char* cs;
        size_t cl;
        if (!read_field(q, cs, cl)) return false;
        if (ki < kept.size() &&
            kept.Get(ki) == static_cast<uint32_t>(c)) {
            out.append(cs, cl);
            ki++;
        }
    }
    return ki == kept.size();  // every requested column was present
}

// Return the bytes of column `column_index` from a serialized MySQL row payload.
std::string_view extract_value_column(const std::string& row,
                                      int column_index) {
    size_t offset = 0;
    int field_index = 0;
    const int target_field = column_index + 1; // field 0 is null flags.

    while (offset < row.size()) {
        const auto byte_size = static_cast<unsigned char>(row[offset]);
        ++offset;
        if (byte_size == 0xFF) {
            if (field_index == target_field) return {};
            ++field_index;
            continue;
        }
        if (offset + byte_size > row.size()) return {};

        size_t value_length = 0;
        for (unsigned int i = 0; i < byte_size; ++i) {
            value_length |= static_cast<size_t>(
                static_cast<unsigned char>(row[offset + i])) << (8 * i);
        }
        offset += byte_size;
        if (offset + value_length > row.size()) return {};

        if (field_index == target_field) {
            return std::string_view(row.data() + offset, value_length);
        }
        offset += value_length;
        ++field_index;
    }
    return {};
}

// Phase-8 Phase B: append one LineairDBField-format field to a row buffer.
//   [byteSize:1B][valueLength: byteSize B (LE)][value]; null/empty => 0xFF.
void agg_emit_field(std::string& out, std::string_view v, bool is_null) {
    if (is_null) { out.push_back(static_cast<char>(0xFF)); return; }
    size_t len = v.size();
    size_t bs = 1;
    for (size_t t = len >> 8; t; t >>= 8) ++bs;
    out.push_back(static_cast<char>(bs));
    for (size_t i = 0; i < bs; ++i)
        out.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
    out.append(v.data(), v.size());
}

// Exact fixed-point decimal: value = mantissa * 10^-scale. +,-,* are exact
// (no rounding); used to compute aggregate-argument expressions server-side so
// SUM/AVG byte-match MySQL's my_decimal. Division is NOT done here (AVG divides
// on the proxy via my_decimal_div).
struct Dec { __int128 m = 0; int s = 0; bool null = false; };

static __int128 dec_pow10(int n) {
    __int128 r = 1;
    while (n-- > 0) r *= 10;
    return r;
}
// Parse an ASCII decimal string ("38273.13", "-5.00") to exact Dec.
static Dec dec_parse(std::string_view v) {
    Dec d;
    if (v.empty()) { d.null = true; return d; }
    size_t i = 0; bool neg = false;
    if (v[0] == '-') { neg = true; i = 1; } else if (v[0] == '+') i = 1;
    __int128 m = 0; int scale = 0; bool dot = false;
    for (; i < v.size(); ++i) {
        char c = v[i];
        if (c == '.') { dot = true; continue; }
        if (c < '0' || c > '9') { d.null = true; return d; }
        m = m * 10 + (c - '0');
        if (dot) ++scale;
    }
    d.m = neg ? -m : m; d.s = scale;
    return d;
}
static void dec_addsub(Dec& a, const Dec& b, bool sub) {
    const int s = a.s > b.s ? a.s : b.s;
    __int128 am = a.m * dec_pow10(s - a.s);
    __int128 bm = b.m * dec_pow10(s - b.s);
    a.m = sub ? am - bm : am + bm;
    a.s = s;
    a.null = a.null || b.null;
}
// Evaluate an aggregate-argument FilterExpr (COLUMN_REF / CONST_INT / +,-,*,neg)
// against a row to an exact Dec. Unsupported nodes yield null.
static Dec dec_eval(const LineairDB::Protocol::FilterExpr& e,
                    const std::string& row) {
    using FE = LineairDB::Protocol::FilterExpr;
    switch (e.op()) {
        case FE::COLUMN_REF:
            return dec_parse(extract_value_column(row, e.column_index()));
        case FE::CONST_INT:  { Dec d; d.m = e.int_val();  d.s = 0; return d; }
        case FE::CONST_UINT: { Dec d; d.m = (__int128)e.uint_val(); d.s = 0; return d; }
        case FE::OP_ADD: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            dec_addsub(a, dec_eval(e.children(1), row), false);
            return a;
        }
        case FE::OP_SUB: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            dec_addsub(a, dec_eval(e.children(1), row), true);
            return a;
        }
        case FE::OP_MUL: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            Dec b = dec_eval(e.children(1), row);
            Dec r; r.m = a.m * b.m; r.s = a.s + b.s; r.null = a.null || b.null;
            return r;
        }
        case FE::OP_NEG: {
            if (e.children_size() != 1) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row); a.m = -a.m; return a;
        }
        default: { Dec d; d.null = true; return d; }
    }
}
static std::string dec_format(const Dec& d) {
    __int128 m = d.m; bool neg = m < 0; if (neg) m = -m;
    std::string digits;
    if (m == 0) digits = "0";
    else { while (m) { digits.push_back(char('0' + int(m % 10))); m /= 10; }
           std::reverse(digits.begin(), digits.end()); }
    while (static_cast<int>(digits.size()) <= d.s) digits.insert(digits.begin(), '0');
    std::string out;
    if (neg) out.push_back('-');
    if (d.s == 0) { out += digits; return out; }
    const size_t ip = digits.size() - d.s;
    out.append(digits, 0, ip); out.push_back('.'); out.append(digits, ip, std::string::npos);
    return out;
}

struct AggGroupState {
    std::vector<std::string> key_cols;   // captured group-by column values
    std::vector<uint64_t> count;         // per agg: COUNT / non-null counter
    std::vector<Dec> sum;                // per agg: SUM/AVG accumulator
};

// Accumulate rows[begin,end) into `out` (group key -> AggGroupState). Pure over
// const spec/rows + the worker's own `out`, so it is safe to run on a worker
// thread for morsel-parallel aggregation (no shared state, no DB/RCU touch).
static void aggregate_rows_range(
    const LineairDB::Protocol::AggregateSpec& spec,
    const std::vector<LineairDB::StatelessScanRow>& rows, size_t begin,
    size_t end, std::unordered_map<std::string, AggGroupState>& out) {
  using AF = LineairDB::Protocol::AggFunc;
  const int n_agg = spec.aggs_size();
  const int n_grp = spec.group_columns_size();
  std::string keybuf;
  std::vector<std::string_view> gv(n_grp);
  for (size_t r = begin; r < end; ++r) {
    const auto& row = rows[r];
    keybuf.clear();
    for (int g = 0; g < n_grp; ++g) {
      gv[g] = extract_value_column(row.value, spec.group_columns(g));
      const uint32_t l = static_cast<uint32_t>(gv[g].size());
      keybuf.append(reinterpret_cast<const char*>(&l), sizeof(l));
      keybuf.append(gv[g].data(), gv[g].size());
    }
    auto it = out.find(keybuf);
    AggGroupState* gs;
    if (it == out.end()) {
      AggGroupState s;
      s.key_cols.resize(n_grp);
      for (int g = 0; g < n_grp; ++g) s.key_cols[g] = std::string(gv[g]);
      s.count.assign(n_agg, 0);
      s.sum.assign(n_agg, Dec{});
      gs = &out.emplace(keybuf, std::move(s)).first->second;
    } else {
      gs = &it->second;
    }
    for (int a = 0; a < n_agg; ++a) {
      const auto& af = spec.aggs(a);
      if (af.kind() == AF::AGG_COUNT) { gs->count[a] += 1; continue; }
      Dec v = dec_eval(af.arg(), row.value);
      if (!v.null) { dec_addsub(gs->sum[a], v, false); gs->count[a] += 1; }
    }
  }
}

// Merge worker-local group map `src` into `dst`. COUNT adds; SUM/AVG add via
// order-independent exact-decimal (int128 fixed-point) dec_addsub. Used by both
// the morsel-parallel aggregate-over-rows path and the parallel-scan+agg path.
static void merge_agg_groups(std::unordered_map<std::string, AggGroupState>& dst,
                             std::unordered_map<std::string, AggGroupState>& src,
                             int n_agg) {
  if (dst.empty()) { dst = std::move(src); return; }
  for (auto& kv : src) {
    auto it = dst.find(kv.first);
    if (it == dst.end()) {
      dst.emplace(kv.first, std::move(kv.second));
    } else {
      AggGroupState& d = it->second;
      AggGroupState& s = kv.second;
      for (int a = 0; a < n_agg; ++a) {
        d.count[a] += s.count[a];
        dec_addsub(d.sum[a], s.sum[a], false);
      }
    }
  }
}

// Server-side HAVING (Phase-16): exact-decimal comparison of one aggregate
// against a constant, applied at group emission. COUNT compares the row
// count; SUM compares the int128 fixed-point sum (NULL sum — zero non-null
// inputs — never passes, matching SQL three-valued logic).
static bool agg_having_passes(const LineairDB::Protocol::AggregateSpec& spec,
                              const AggGroupState& s) {
    const uint32_t op = spec.having_op();
    if (op == 0) return true;
    const int ai = static_cast<int>(spec.having_agg_index());
    if (ai < 0 || ai >= spec.aggs_size()) return false;
    Dec val;
    if (spec.aggs(ai).kind() == LineairDB::Protocol::AggFunc::AGG_COUNT) {
        val.m = static_cast<__int128>(s.count[ai]);
        val.s = 0;
    } else {  // AGG_SUM
        if (s.count[ai] == 0) return false;
        val = s.sum[ai];
    }
    const Dec c = dec_parse(spec.having_const());
    __int128 a = val.m, b = c.m;
    if (val.s < c.s) a *= dec_pow10(c.s - val.s);
    else if (c.s < val.s) b *= dec_pow10(val.s - c.s);
    switch (op) {
        case 1: return a > b;
        case 2: return a >= b;
        case 3: return a < b;
        case 4: return a <= b;
        case 5: return a == b;
        case 6: return a != b;
        default: return false;
    }
}

// Emit one synthetic group row per group into step_result->scan_values
// (format: [null_flags][group cols][per-agg value,count]). COUNT ships the row
// count; SUM/AVG ship an exact decimal sum (ASCII) + non-null count. Implicit
// grouping (no GROUP BY) emits exactly one row even over zero input.
static void emit_agg_groups(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::unordered_map<std::string, AggGroupState>& groups,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
  using AF = LineairDB::Protocol::AggFunc;
  const int n_agg = spec.aggs_size();
  const int n_grp = spec.group_columns_size();
  if (n_grp == 0 && groups.empty()) {
    AggGroupState s;
    s.count.assign(n_agg, 0);
    s.sum.assign(n_agg, Dec{});
    groups.emplace(std::string(), std::move(s));
  }
  for (auto& kv : groups) {
    AggGroupState& s = kv.second;
    if (!agg_having_passes(spec, s)) continue;
    std::string row;
    agg_emit_field(row, std::string_view(), false);  // null_flags (empty)
    for (int g = 0; g < n_grp; ++g) agg_emit_field(row, s.key_cols[g], false);
    for (int a = 0; a < n_agg; ++a) {
      const auto& af = spec.aggs(a);
      const std::string cnt = std::to_string(s.count[a]);
      if (af.kind() == AF::AGG_COUNT) {
        agg_emit_field(row, cnt, false);
        agg_emit_field(row, cnt, false);
      } else {  // SUM / AVG: (exact decimal sum | null) , non-null count
        if (s.count[a] == 0) agg_emit_field(row, std::string_view(), true);
        else agg_emit_field(row, dec_format(s.sum[a]), false);
        agg_emit_field(row, cnt, false);
      }
    }
    step_result->add_scan_keys(std::string());
    step_result->add_scan_values(std::move(row));
    step_result->add_scan_tids(0);
  }
}

// Aggregate `rows` per `spec`, emitting one synthetic group row per group into
// step_result->scan_values (format: [null_flags][group cols][per-agg value,count]).
// COUNT ships the row count; SUM/AVG ship an exact decimal sum (ASCII) + non-null
// count. Single server + single scan ⇒ groups are final.
bool server_aggregate_scan(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::vector<LineairDB::StatelessScanRow>& rows,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
    using AF = LineairDB::Protocol::AggFunc;
    const int n_agg = spec.aggs_size();
    const int n_grp = spec.group_columns_size();

    std::unordered_map<std::string, AggGroupState> groups;
    // Morsel-parallel aggregation (gated HELIOS_PARALLEL_SERVER, default OFF):
    // split rows into T chunks, each worker accumulates into its own thread-local
    // map (no shared state, no DB/RCU touch — pure over const spec/rows), then the
    // parent merges. exact-decimal merge is order-independent (Dec is fixed-point
    // int128). Emit order is irrelevant: the agg result feeds MySQL which applies
    // the query's ORDER BY. Serial path (T==1) is byte-identical to the original.
    static const bool par_on = std::getenv("HELIOS_PARALLEL_SERVER") != nullptr;
    auto env_sz = [](const char* n, size_t d) {
      const char* e = std::getenv(n); if (!e || !e[0]) return d;
      char* end = nullptr; long v = std::strtol(e, &end, 10);
      return (end && end != e && v > 0) ? (size_t)v : d;
    };
    const size_t morsel = env_sz("HELIOS_SERVER_MORSEL_ROWS", 100000);
    unsigned cap = (unsigned)env_sz("HELIOS_SERVER_THREADS",
                                    std::thread::hardware_concurrency()
                                        ? std::thread::hardware_concurrency() : 4);
    const size_t desired = morsel ? (rows.size() + morsel - 1) / morsel : 1;
    unsigned T = (par_on && desired > 1 && cap > 1)
                     ? (unsigned)std::min<size_t>(desired, cap) : 1;
    if (T <= 1) {
      aggregate_rows_range(spec, rows, 0, rows.size(), groups);
    } else {
      std::vector<std::unordered_map<std::string, AggGroupState>> locals(T);
      std::vector<std::thread> workers;
      workers.reserve(T);
      const size_t chunk = (rows.size() + T - 1) / T;
      for (unsigned t = 0; t < T; ++t) {
        const size_t b = (size_t)t * chunk;
        const size_t e = std::min(rows.size(), b + chunk);
        if (b >= e) break;
        workers.emplace_back([&, t, b, e] {
          aggregate_rows_range(spec, rows, b, e, locals[t]);
        });
      }
      for (auto& w : workers) w.join();
      for (auto& lm : locals) merge_agg_groups(groups, lm, n_agg);
    }
    emit_agg_groups(spec, groups, step_result);
    return true;
}

// Build the int-keyed primary key bytes. Mirrors the layout produced by
// ha_lineairdb::append_key_part_encoding, so server-side read-plan scan
// boundaries match what the proxy wrote into LineairDB:
//   [0x00 not-null marker | 0x10 INT type tag | 2-byte big-endian length=4
//    | 4-byte signed int with top bit flipped]
// Flipping the top bit makes byte-wise lexicographic sort agree with signed
// integer order, so Masstree can sort without knowing the column type.
// TODO: factor this and ha_lineairdb's encoder into a shared encoder so the
// two ends cannot drift.
std::string encode_int_key_part(int64_t value) {
    const auto encoded = static_cast<uint32_t>(static_cast<int32_t>(value)) ^
                         0x80000000U;
    std::string out;
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x10));
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x04));
    out.push_back(static_cast<char>((encoded >> 24) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 16) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 8) & 0xFF));
    out.push_back(static_cast<char>(encoded & 0xFF));
    return out;
}

// Inverse of encode_int_key_part: recover the leading int key-part value from a
// primary key's bytes. Returns false if the key does not begin with the
// [0x00 0x10 0x00 0x04 | 4-byte flipped int] layout (e.g. non-int leading part),
// in which case the caller must NOT range-split on it. Used to compute balanced
// scan-split boundaries from observed first/last keys (parallel scan).
bool decode_leading_int_key(std::string_view key, int64_t& out) {
    if (key.size() < 8) return false;
    if (static_cast<uint8_t>(key[0]) != 0x00 ||
        static_cast<uint8_t>(key[1]) != 0x10 ||
        static_cast<uint8_t>(key[2]) != 0x00 ||
        static_cast<uint8_t>(key[3]) != 0x04)
        return false;
    const uint32_t e = (static_cast<uint32_t>(static_cast<uint8_t>(key[4])) << 24) |
                       (static_cast<uint32_t>(static_cast<uint8_t>(key[5])) << 16) |
                       (static_cast<uint32_t>(static_cast<uint8_t>(key[6])) << 8) |
                       (static_cast<uint32_t>(static_cast<uint8_t>(key[7])));
    out = static_cast<int32_t>(e ^ 0x80000000U);
    return true;
}

// Parse a decimal-string column and encode it as int-keyed primary key bytes.
std::string encode_column_as_int_key(std::string_view column,
                                     int64_t int_delta) {
    std::string tmp(column);
    int64_t value = std::strtoll(tmp.c_str(), nullptr, 10);
    return encode_int_key_part(value + int_delta);
}

// Parallel primary range scan + filter + aggregate (gated
// HELIOS_PARALLEL_SERVER_SCAN, default OFF). Splits [start_key, end_key) into
// balanced sub-ranges on the LEADING int key part. Scan is half-open [begin,end)
// (masstree_index.cpp ScanAdapter: "key >= end -> stop"), and boundaries are
// placed at integer key-part values, so a composite key's rows never split
// across a boundary and no key is double-counted — the partition is exact.
//
// Eligible ONLY for read-only no-validate primary AGGREGATE scans. There the OCC
// metadata (range_versions/index_reads/filtered keys) is stripped downstream
// anyway, so each worker runs logical-default mode and discards validation
// output; it copies out only key/value/tid (no node pointers retained) and
// releases its own Masstree RCU section before exit. Returns false (→ caller
// falls back to the serial scan path, emitting nothing here) when the range
// can't be int-decoded/split, is too small, or any worker scan fails.
//
// First cut uses per-query std::thread workers (matches the committed
// HELIOS_PARALLEL_SERVER agg path). DB-touching short-lived threads churn
// masstree threadinfo (Codex); if that shows up in measurement, swap to a
// persistent server-local worker pool — the split/merge logic is unchanged.
static bool parallel_primary_aggregate_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
  static const bool scan_par_on =
      std::getenv("HELIOS_PARALLEL_SERVER_SCAN") != nullptr;
  if (!scan_par_on) return false;
  auto env_sz = [](const char* n, size_t d) {
    const char* e = std::getenv(n); if (!e || !e[0]) return d;
    char* end = nullptr; long v = std::strtol(e, &end, 10);
    return (end && end != e && v > 0) ? (size_t)v : d;
  };
  // Defaults follow SOTA morsel sizing (HyPer ~100k / DuckDB row group 122,880):
  // morsel 128k, and DuckDB's "need >= threads * morsel rows to go parallel"
  // gives the serial-fallback floor. threads cap min(hw, 8).
  const size_t morsel = env_sz("HELIOS_SERVER_SCAN_MORSEL_ROWS", 128000);
  const size_t min_rows = env_sz("HELIOS_SERVER_SCAN_MIN_ROWS", 500000);
  const unsigned cap = (unsigned)env_sz(
      "HELIOS_SERVER_SCAN_THREADS",
      std::min<unsigned>(std::thread::hardware_concurrency()
                             ? std::thread::hardware_concurrency() : 4u, 8u));
  if (cap <= 1) return false;

  // Probe actual first/last key (row_limit=1 forward/reverse) to bound the
  // split. These run on the handler thread inside its established RCU section.
  auto first = db->StatelessRangeScan(step.table_name(), start_key, end_key, 1, false);
  if (!first.ok || first.rows.empty()) return false;
  auto last = db->StatelessRangeScan(step.table_name(), start_key, end_key, 1, true);
  if (!last.ok || last.rows.empty()) return false;
  int64_t lo = 0, hi = 0;
  if (!decode_leading_int_key(first.rows.front().key, lo) ||
      !decode_leading_int_key(last.rows.front().key, hi)) {
    return false;
  }
  if (hi <= lo) return false;
  const uint64_t span = (uint64_t)(hi - lo) + 1;
  if (span < min_rows) return false;  // too small to amortize thread overhead
  const size_t desired = morsel ? (size_t)((span + morsel - 1) / morsel) : 1;
  const unsigned T = (unsigned)std::min<size_t>(desired, cap);
  if (T <= 1) return false;

  // Integer-aligned boundaries: sub-range i = [start_i, end_i). start_0 keeps
  // the original start_key, end_{T-1} keeps the original end_key; interior
  // bounds are encode_int_key_part(b_i) with b_i = lo + span*i/T.
  std::vector<std::string> starts(T), ends(T);
  for (unsigned i = 0; i < T; ++i) {
    const int64_t bi = lo + (int64_t)((span * i) / T);
    const int64_t bn = (i + 1 == T) ? (hi + 1)
                                    : lo + (int64_t)((span * (i + 1)) / T);
    starts[i] = (i == 0) ? start_key : encode_int_key_part(bi);
    ends[i] = (i + 1 == T) ? end_key : encode_int_key_part(bn);
  }

  const bool has_filter = step.has_filter() && step.filter().has_expr();
  const auto& spec = step.aggregate();
  const int n_agg = spec.aggs_size();
  std::vector<std::unordered_map<std::string, AggGroupState>> locals(T);
  std::vector<char> failed(T, 0);
  std::vector<std::thread> workers;
  workers.reserve(T);
  for (unsigned i = 0; i < T; ++i) {
    workers.emplace_back([&, i] {
      auto sr = db->StatelessRangeScan(step.table_name(), starts[i], ends[i], 0, false);
      if (!sr.ok) { failed[i] = 1; db->ReleaseMasstreeThreadEpoch(); return; }
      if (has_filter && !sr.rows.empty()) {
        const auto& filter_expr = step.filter().expr();
        const uint32_t num_cols = step.filter().num_columns();
        PredicateEvaluator ev;
        std::vector<LineairDB::StatelessScanRow> kept;
        kept.reserve(sr.rows.size());
        for (auto& row : sr.rows) {
          // Fail closed on parse failure (Codex P2): an unparseable row can
          // never be re-checked by MySQL once aggregated. Worker failure makes
          // the caller fall back to the serial path, which aborts loudly.
          if (!ev.parse_row(row.value.data(), row.value.size(), num_cols)) {
            failed[i] = 1;
            db->ReleaseMasstreeThreadEpoch();
            return;
          }
          if (ev.evaluate(filter_expr)) kept.push_back(std::move(row));
        }
        sr.rows = std::move(kept);
      }
      aggregate_rows_range(spec, sr.rows, 0, sr.rows.size(), locals[i]);
      db->ReleaseMasstreeThreadEpoch();
    });
  }
  for (auto& w : workers) w.join();
  for (unsigned i = 0; i < T; ++i)
    if (failed[i]) return false;  // caller re-scans serially; nothing emitted yet

  std::unordered_map<std::string, AggGroupState> groups;
  for (auto& lm : locals) merge_agg_groups(groups, lm, n_agg);
  emit_agg_groups(spec, groups, step_result);
  return true;
}

// Parallel primary range scan + filter for NON-aggregate steps (gated
// HELIOS_PARALLEL_SERVER_SCAN). q12/q14-class cost: a 6M-row FILTERED scan ran
// serially while only aggregate scans parallelized. Same integer-aligned split
// as the aggregate variant; each worker ships exactly what the serial loop
// would — passing rows (key, trimmed value, tid) and the filter-rejected KEYS
// (negative coverage). Morsels are contiguous and appended in order, so the
// emitted sequence (and thus range-replay validation and coverage semantics)
// is byte-identical to the serial scan. Any worker failure (scan error / trim
// failure) discards everything and returns false: the caller's serial path
// re-scans and reports errors with its own (fail-closed) semantics.
static bool parallel_primary_filter_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
  static const bool scan_par_on =
      std::getenv("HELIOS_PARALLEL_SERVER_SCAN") != nullptr;
  if (!scan_par_on) return false;
  auto env_sz = [](const char* n, size_t d) {
    const char* e = std::getenv(n); if (!e || !e[0]) return d;
    char* end = nullptr; long v = std::strtol(e, &end, 10);
    return (end && end != e && v > 0) ? (size_t)v : d;
  };
  const size_t morsel = env_sz("HELIOS_SERVER_SCAN_MORSEL_ROWS", 128000);
  const size_t min_rows = env_sz("HELIOS_SERVER_SCAN_MIN_ROWS", 500000);
  const unsigned cap = (unsigned)env_sz(
      "HELIOS_SERVER_SCAN_THREADS",
      std::min<unsigned>(std::thread::hardware_concurrency()
                             ? std::thread::hardware_concurrency() : 4u, 8u));
  if (cap <= 1) return false;

  auto first = db->StatelessRangeScan(step.table_name(), start_key, end_key, 1, false);
  if (!first.ok || first.rows.empty()) return false;
  auto last = db->StatelessRangeScan(step.table_name(), start_key, end_key, 1, true);
  if (!last.ok || last.rows.empty()) return false;
  int64_t lo = 0, hi = 0;
  if (!decode_leading_int_key(first.rows.front().key, lo) ||
      !decode_leading_int_key(last.rows.front().key, hi)) {
    return false;
  }
  if (hi <= lo) return false;
  const uint64_t span = (uint64_t)(hi - lo) + 1;
  if (span < min_rows) return false;
  const size_t desired = morsel ? (size_t)((span + morsel - 1) / morsel) : 1;
  const unsigned T = (unsigned)std::min<size_t>(desired, cap);
  if (T <= 1) return false;

  std::vector<std::string> starts(T), ends(T);
  for (unsigned i = 0; i < T; ++i) {
    const int64_t bi = lo + (int64_t)((span * i) / T);
    const int64_t bn = (i + 1 == T) ? (hi + 1)
                                    : lo + (int64_t)((span * (i + 1)) / T);
    starts[i] = (i == 0) ? start_key : encode_int_key_part(bi);
    ends[i] = (i + 1 == T) ? end_key : encode_int_key_part(bn);
  }

  struct WorkerOut {
    std::vector<std::string> keys, values, fkeys;
    std::vector<uint64_t> tids;
  };
  std::vector<WorkerOut> outs(T);
  std::vector<char> failed(T, 0);
  const bool has_projection = step.has_projection();
  const bool suppress_fkeys = step.suppress_filtered_keys();
  std::vector<std::thread> workers;
  workers.reserve(T);
  for (unsigned i = 0; i < T; ++i) {
    workers.emplace_back([&, i] {
      auto sr = db->StatelessRangeScan(step.table_name(), starts[i], ends[i], 0, false);
      if (!sr.ok) { failed[i] = 1; db->ReleaseMasstreeThreadEpoch(); return; }
      const auto& filter_expr = step.filter().expr();
      const uint32_t num_cols = step.filter().num_columns();
      PredicateEvaluator ev;
      WorkerOut& o = outs[i];
      o.keys.reserve(sr.rows.size());
      for (auto& row : sr.rows) {
        bool pass = true;
        if (ev.parse_row(row.value.data(), row.value.size(), num_cols)) {
          pass = ev.evaluate(filter_expr);
        }  // unparseable: ship it, MySQL re-checks (serial row_passes contract)
        if (!pass) {
          if (!suppress_fkeys) o.fkeys.push_back(std::move(row.key));
          continue;
        }
        if (has_projection && !row.value.empty()) {
          std::string trimmed;
          if (!trim_row_value(row.value, step.projection().field_indexes(),
                              step.projection().num_columns(), trimmed)) {
            failed[i] = 1;  // serial re-scan fails closed with its own error
            break;
          }
          row.value = std::move(trimmed);
        }
        o.keys.push_back(std::move(row.key));
        o.values.push_back(std::move(row.value));
        o.tids.push_back(row.tid);
      }
      db->ReleaseMasstreeThreadEpoch();
    });
  }
  for (auto& w : workers) w.join();
  for (unsigned i = 0; i < T; ++i)
    if (failed[i]) return false;
  for (unsigned i = 0; i < T; ++i) {
    WorkerOut& o = outs[i];
    for (size_t k = 0; k < o.keys.size(); ++k) {
      step_result->add_scan_keys(std::move(o.keys[k]));
      step_result->add_scan_values(std::move(o.values[k]));
      step_result->add_scan_tids(o.tids[k]);
    }
    for (auto& fk : o.fkeys) step_result->add_filtered_keys(std::move(fk));
  }
  return true;
}

// Pick the source byte string for a binding (from a step's scan key, scan value, or value).
const std::string* select_source_bytes(
    const LineairDB::Protocol::TxExecuteReadPlan::StepResult& source,
    const LineairDB::Protocol::TxExecuteReadPlan::KeyBinding& binding,
    bool from_key, int row_override) {
    if (from_key) {
        if (source.scan_keys_size() == 0) return nullptr;
        int row = row_override >= 0 ? row_override : binding.source_row();
        if (binding.use_midpoint()) row = (source.scan_keys_size() - 1) / 2;
        row = std::min(row, source.scan_keys_size() - 1);
        return &source.scan_keys(row);
    }

    if (source.scan_values_size() > 0) {
        int row = row_override >= 0 ? row_override : binding.source_row();
        if (binding.use_midpoint()) row = (source.scan_values_size() - 1) / 2;
        row = std::min(row, source.scan_values_size() - 1);
        return &source.scan_values(row);
    }
    return &source.value();
}

// Compose a read-plan key prefix plus all bindings into the actual scan key.
std::string build_plan_key(
    const std::string& prefix,
    const google::protobuf::RepeatedPtrField<
        LineairDB::Protocol::TxExecuteReadPlan::KeyBinding>& bindings,
    const std::vector<LineairDB::Protocol::TxExecuteReadPlan::StepResult*>&
        previous_results,
    int row_override = -1,
    bool *complete = nullptr) {
    if (complete != nullptr) *complete = true;
    std::string key = prefix;
    for (const auto& binding : bindings) {
        const int source_step = static_cast<int>(binding.source_step());
        if (source_step < 0 ||
            source_step >= static_cast<int>(previous_results.size())) {
            if (complete != nullptr) *complete = false;
            continue;
        }

        const auto& source = *previous_results[source_step];
        std::string scratch;
        std::string_view extracted;
        if (binding.source_column() > 0) {
            const std::string* bytes =
                select_source_bytes(source, binding, false, row_override);
            if (bytes != nullptr) {
                extracted =
                    extract_value_column(*bytes, binding.source_column() - 1);
                if (binding.column_as_int_key()) {
                    scratch =
                        encode_column_as_int_key(extracted,
                                                 binding.int_delta());
                    extracted = scratch;
                }
            } else if (complete != nullptr) {
                *complete = false;
            }
        } else {
            const std::string* bytes =
                select_source_bytes(source, binding, binding.from_key(),
                                    row_override);
            if (bytes != nullptr) {
                uint32_t offset = binding.source_offset();
                uint32_t length = binding.source_length();
                if (offset < bytes->size()) {
                    if (length == 0) length = bytes->size() - offset;
                    length = std::min<uint32_t>(
                        length, static_cast<uint32_t>(bytes->size() - offset));
                    extracted = std::string_view(bytes->data() + offset,
                                                 length);
                } else if (complete != nullptr) {
                    *complete = false;
                }
            } else if (complete != nullptr) {
                *complete = false;
            }
        }
        if (bindings.size() > 0 && extracted.empty() && complete != nullptr) {
            *complete = false;
        }
        key.append(extracted.data(), extracted.size());
    }
    return key;
}

}  // namespace

// Phase 2 server-side NDV cache (shared across per-connection rpc instances).
std::mutex LineairDBRpc::ndv_cache_mu_;
std::unordered_map<std::string, std::pair<bool, std::vector<uint64_t>>>
    LineairDBRpc::ndv_cache_;

LineairDBRpc::LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager,
                           std::shared_ptr<TableRowCounts> row_counts)
    : db_manager_(db_manager), tx_manager_(tx_manager), row_counts_(row_counts) {
}

void LineairDBRpc::handle_rpc(uint64_t sender_id, MessageType message_type,
                             const std::string& message, std::string& result) {
    LOG_DEBUG("Handling RPC: message_type=%u", static_cast<uint32_t>(message_type));

    // Close this thread's masstree RCU critical section after each
    // self-contained handler (stateless RPCs and DB_END_TRANSACTION) so
    // RCU can reclaim retired leaves; the next masstree op re-opens it.
    auto release_masstree_thread_epoch = [this]() {
        if (db_manager_) {
            auto db = db_manager_->get_database();
            if (db) db->ReleaseMasstreeThreadEpoch();
        }
    };
    switch(message_type) {
        // Transaction lifecycle
        case MessageType::TX_BEGIN_TRANSACTION:
            handleTxBeginTransaction(message, result);
            return;
        case MessageType::TX_ABORT:
            handleTxAbort(message, result);
            return;

        // Primary key operations
        case MessageType::TX_READ:
            handleTxRead(message, result);
            return;
        case MessageType::TX_BATCH_READ:
            handleTxBatchRead(message, result);
            return;
        case MessageType::TX_BATCH_WRITE:
            handleTxBatchWrite(message, result);
            return;
        case MessageType::TX_STATELESS_READ:
            handleTxStatelessRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_BATCH_READ:
            handleTxStatelessBatchRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_RANGE_SCAN:
            handleTxStatelessRangeScan(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_SECONDARY_RANGE_SCAN:
            handleTxStatelessSecondaryRangeScan(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_EXECUTE_READ_PLAN:
            handleTxExecuteReadPlan(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_GET_TABLE_STATS:
            handleTxGetTableStats(message, result);
            release_masstree_thread_epoch();  // Phase 2: NDV scans touch the index
            return;
        case MessageType::TX_VALIDATE_AND_COMMIT:
            handleTxValidateAndCommit(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_WRITE:
            handleTxWrite(message, result);
            return;
        case MessageType::TX_DELETE:
            handleTxDelete(message, result);
            return;

        // Secondary index operations
        case MessageType::TX_READ_SECONDARY_INDEX:
            handleTxReadSecondaryIndex(message, result);
            return;
        case MessageType::TX_WRITE_SECONDARY_INDEX:
            handleTxWriteSecondaryIndex(message, result);
            return;
        case MessageType::TX_DELETE_SECONDARY_INDEX:
            handleTxDeleteSecondaryIndex(message, result);
            return;
        case MessageType::TX_UPDATE_SECONDARY_INDEX:
            handleTxUpdateSecondaryIndex(message, result);
            return;

        // Primary key scan operations
        case MessageType::TX_GET_MATCHING_KEYS_IN_RANGE:
            handleTxGetMatchingKeysInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE:
            handleTxGetMatchingKeysAndValuesInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX:
            handleTxGetMatchingKeysAndValuesFromPrefix(message, result);
            return;
        case MessageType::TX_FETCH_LAST_KEY_IN_RANGE:
            handleTxFetchLastKeyInRange(message, result);
            return;
        case MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX:
            handleTxFetchFirstKeyWithPrefix(message, result);
            return;
        case MessageType::TX_FETCH_NEXT_KEY_WITH_PREFIX:
            handleTxFetchNextKeyWithPrefix(message, result);
            return;

        // Secondary index scan operations
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE:
            handleTxGetMatchingPrimaryKeysInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX:
            handleTxGetMatchingPrimaryKeysFromPrefix(message, result);
            return;
        case MessageType::TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE:
            handleTxFetchLastPrimaryKeyInSecondaryRange(message, result);
            return;
        case MessageType::TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE:
            handleTxFetchLastSecondaryEntryInRange(message, result);
            return;

        // Database operations
        case MessageType::DB_FENCE:
            handleDbFence(message, result);
            return;
        case MessageType::DB_END_TRANSACTION:
            handleDbEndTransaction(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::DB_CREATE_TABLE:
            handleDbCreateTable(message, result);
            return;
        case MessageType::DB_SET_TABLE:
            handleDbSetTable(message, result);
            return;
        case MessageType::DB_CREATE_SECONDARY_INDEX:
            handleDbCreateSecondaryIndex(message, result);
            return;

        default:
            LOG_ERROR("Unknown message type: %u", static_cast<uint32_t>(message_type));
            return;
    }
}

bool LineairDBRpc::key_prefix_is_matching(const std::string& key_prefix, const std::string& key) {
    if (key.substr(0, key_prefix.size()) != key_prefix) return false;
    return true;
}

void LineairDBRpc::handleTxBeginTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxBeginTransaction");

    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;

    request.ParseFromString(message);

    auto& tx = db_manager_->get_database()->BeginTransaction();
    int64_t tx_id = tx_manager_->generate_tx_id();
    tx_manager_->store_transaction(tx_id, &tx);

    response.set_transaction_id(tx_id);

    // Piggyback current table row counts so proxy has fresh stats.
    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    result = response.SerializeAsString();

    LOG_DEBUG("Created transaction: %ld", tx_id);
}

// Transaction-less row-count snapshot for the optimizer (no BeginTransaction,
// no tx_id, no side effects). Used by the proxy to seed accurate cardinalities
// at MySQL optimize time despite oneshot's deferred tx_begin.
void LineairDBRpc::handleTxGetTableStats(const std::string& message,
                                         std::string& result) {
    LineairDB::Protocol::GetTableStats::Request request;
    request.ParseFromString(message);
    LineairDB::Protocol::GetTableStats::Response response;
    if (row_counts_) {
        for (const auto& [name, count] : row_counts_->snapshot()) {
            auto* ts = response.add_table_stats();
            ts->set_table_name(name);
            ts->set_row_count(count);
        }
    }
    // Phase 2: optional per-index NDV (exact, live-filtered single pass) for the
    // requested table's indexes, so the proxy can set accurate rec_per_key.
    // Cached server-side (key = table\0index\0parts) so repeated requests don't
    // re-scan; ANALYZE TABLE busts the cache via ndv_force_recompute.
    if (!request.ndv_table().empty() && db_manager_) {
        auto db = db_manager_->get_database();
        for (const auto& desc : request.ndv_indexes()) {
            auto* out = response.add_index_ndv();
            out->set_index_name(desc.index_name());
            std::string ck = request.ndv_table();
            ck.push_back('\0'); ck.append(desc.index_name());
            ck.push_back('\0'); ck.append(std::to_string(desc.num_key_parts()));
            bool avail = false, found = false;
            std::vector<uint64_t> ndv;
            {
                std::lock_guard<std::mutex> g(ndv_cache_mu_);
                if (request.ndv_force_recompute()) ndv_cache_.erase(ck);
                auto it = ndv_cache_.find(ck);
                if (it != ndv_cache_.end()) {
                    found = true; avail = it->second.first; ndv = it->second.second;
                }
            }
            if (!found) {  // cache miss -> compute (outside the lock) + store
                avail = db && db->ComputeIndexNdvInt(request.ndv_table(),
                                                     desc.index_name(),
                                                     desc.num_key_parts(), ndv);
                std::lock_guard<std::mutex> g(ndv_cache_mu_);
                ndv_cache_[ck] = {avail, ndv};
            }
            out->set_available(avail);
            if (avail)
                for (uint64_t v : ndv) out->add_ndv(v);
        }
    }
    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxAbort(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxAbort");

    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        tx->Abort();
    } else {
        LOG_WARNING("Transaction not found for abort: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxRead(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxRead");

    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        auto read_result = tx->Read(request.key());
        response.set_is_aborted(tx->IsAborted());

        if (read_result.first != nullptr) {
            response.set_found(true);
            std::string value(reinterpret_cast<const char*>(read_result.first), read_result.second);
            response.set_value(value);
        } else {
            response.set_found(false);
        }

        LOG_DEBUG("Read key '%s' from transaction %ld: %s", request.key().c_str(), tx_id, (read_result.first != nullptr ? "found" : "not found"));
    } else {
        response.set_found(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for read: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxBatchRead(const std::string& message, std::string& result) {
    LineairDB::Protocol::TxBatchRead::Request request;
    LineairDB::Protocol::TxBatchRead::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        for (int i = 0; i < request.keys_size(); i++) {
            auto* read_result = response.add_results();
            auto pair = tx->Read(request.keys(i));
            if (pair.first != nullptr) {
                read_result->set_found(true);
                read_result->set_value(
                    reinterpret_cast<const char*>(pair.first), pair.second);
            } else {
                read_result->set_found(false);
            }
        }
        response.set_is_aborted(tx->IsAborted());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for batch_read: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxBatchWrite(const std::string& message, std::string& result) {
    LineairDB::Protocol::TxBatchWrite::Request request;
    LineairDB::Protocol::TxBatchWrite::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }

        if (!tx->IsAborted()) {
            for (int i = 0; i < request.ops_size(); i++) {
                const auto& op = request.ops(i);
                const std::string& op_table =
                    op.table_name().empty() ? request.table_name() : op.table_name();
                if (!op_table.empty()) {
                    tx->SetTable(op_table);
                }
                switch (op.type()) {
                    case LineairDB::Protocol::BATCH_OP_WRITE: {
                        const std::string& value_str = op.value();
                        tx->Write(op.key(),
                                  reinterpret_cast<const std::byte*>(value_str.c_str()),
                                  value_str.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_DELETE:
                        tx->Delete(op.key());
                        break;
                    case LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_WRITE: {
                        const std::string& pk = op.primary_key();
                        tx->WriteSecondaryIndex(
                            op.index_name(), op.secondary_key(),
                            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_DELETE: {
                        const std::string& pk = op.primary_key();
                        tx->DeleteSecondaryIndex(
                            op.index_name(), op.secondary_key(),
                            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_UNKNOWN:
                    default:
                        break;
                }
                if (tx->IsAborted()) break;
            }
        }

        response.set_success(!tx->IsAborted());
        response.set_is_aborted(tx->IsAborted());
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for batch_write: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessRead(const std::string& message,
                                         std::string& result) {
    LineairDB::Protocol::TxStatelessRead::Request request;
    LineairDB::Protocol::TxStatelessRead::Response response;

    request.ParseFromString(message);

    auto read_result =
        db_manager_->get_database()->StatelessRead(request.table_name(),
                                                   request.key());
    response.set_found(read_result.found);
    response.set_tid(read_result.tid);
    if (read_result.found) {
        response.set_value(std::move(read_result.value));
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessBatchRead(const std::string& message,
                                              std::string& result) {
    LineairDB::Protocol::TxStatelessBatchRead::Request request;
    LineairDB::Protocol::TxStatelessBatchRead::Response response;

    request.ParseFromString(message);

    std::vector<std::pair<std::string, std::string>> keys;
    keys.reserve(request.ops_size());
    for (const auto& op : request.ops()) {
        keys.emplace_back(op.table_name(), op.key());
    }

    auto read_results = db_manager_->get_database()->StatelessBatchRead(keys);
    for (auto& read_result : read_results) {
        auto* out = response.add_results();
        out->set_found(read_result.found);
        out->set_tid(read_result.tid);
        if (read_result.found) {
            out->set_value(std::move(read_result.value));
        }
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessRangeScan(const std::string& message,
                                              std::string& result) {
    LineairDB::Protocol::TxStatelessRangeScan::Request request;
    LineairDB::Protocol::TxStatelessRangeScan::Response response;

    request.ParseFromString(message);

    auto scan_result = db_manager_->get_database()->StatelessRangeScan(
        request.table_name(), request.start_key(), request.end_key(),
        request.row_limit(), request.reverse_scan());
    response.set_ok(scan_result.ok);
    if (!scan_result.ok) {
        result = response.SerializeAsString();
        return;
    }

    for (const auto& row : scan_result.rows) {
        auto* out = response.add_rows();
        out->set_key(row.key);
        out->set_value(row.value);
        out->set_tid(row.tid);
        out->set_found(row.found);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessSecondaryRangeScan(
    const std::string& message, std::string& result) {
    LineairDB::Protocol::TxStatelessSecondaryRangeScan::Request request;
    LineairDB::Protocol::TxStatelessSecondaryRangeScan::Response response;

    request.ParseFromString(message);

    auto scan_result = db_manager_->get_database()->StatelessSecondaryRangeScan(
        request.table_name(), request.index_name(), request.start_key(),
        request.end_key(), request.row_limit(), request.reverse_scan());
    response.set_ok(scan_result.ok);
    if (!scan_result.ok) {
        result = response.SerializeAsString();
        return;
    }

    for (const auto& row : scan_result.rows) {
        auto* out = response.add_rows();
        out->set_secondary_key(row.secondary_key);
        out->set_primary_key(row.primary_key);
        out->set_value(row.value);
        out->set_tid(row.tid);
        out->set_found(row.found);
    }

    result = response.SerializeAsString();
}

// ---- flat codec for TxExecuteReadPlan responses ---------------------------
// Protobuf's SerializeAsString has a hard ~2GB (INT_MAX) single-message limit;
// heavy TPC-H plans at SF>=1 materialize 2-3.4GB of rows in one response and
// the serialize fails (surfacing as a fake retryable abort). The response is
// therefore written as a flat length-prefixed buffer with 64-bit lengths; the
// transport frame still carries a uint32 payload size, so the ceiling moves
// from 2GB to 4GB. Same-host deployment: integers are native-endian via
// memcpy, guarded by a magic word. The proxy-side decoder
// (lineairdb_proxy.cc tx_execute_read_plan) must match this format exactly:
//
//   u64 magic 'LDBFLATP' | u8 version=1 | u8 ok
//   u64 results_count, then per StepResult:
//     u8 found | u64 tid
//     bytes value | bytes actual_key | bytes actual_start_key
//     bytes actual_end_key
//     u64 n | n x bytes scan_keys
//     u64 n | n x bytes scan_values
//     u64 n | n x u64 scan_tids
//     u64 n | n x bytes secondary_keys
//     u64 n | n x u64 group_sizes
//     u64 n | n x bytes group_start_keys
//     u64 n | n x bytes group_end_keys
//   (bytes = u64 length + raw)
namespace flat_plan {
static constexpr uint64_t kMagic = 0x5054414C46424454ull;  // "TDBFLATP" bytes
static constexpr uint8_t kVersion = 2;

template <class Sink>
inline void w_u64(Sink& o, uint64_t v) {
    char b[8];
    std::memcpy(b, &v, 8);
    o.append(b, 8);
}
template <class Sink>
inline void w_u8(Sink& o, uint8_t v) {
    char c = static_cast<char>(v);
    o.append(&c, 1);
}
template <class Sink>
inline void w_bytes(Sink& o, const std::string& s) {
    w_u64(o, s.size());
    o.append(s.data(), s.size());
}
struct CountSink {
    uint64_t n = 0;
    void append(const char*, size_t k) { n += k; }
};

template <class Sink>
static void encode_step(
    const LineairDB::Protocol::TxExecuteReadPlan::StepResult& s, Sink& out) {
    w_u8(out, s.found() ? 1 : 0);
    w_u64(out, s.tid());
    w_bytes(out, s.value());
    w_bytes(out, s.actual_key());
    w_bytes(out, s.actual_start_key());
    w_bytes(out, s.actual_end_key());
    w_u64(out, static_cast<uint64_t>(s.scan_keys_size()));
    for (const auto& k : s.scan_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.scan_values_size()));
    for (const auto& v : s.scan_values()) w_bytes(out, v);
    w_u64(out, static_cast<uint64_t>(s.scan_tids_size()));
    for (const auto t : s.scan_tids()) w_u64(out, t);
    w_u64(out, static_cast<uint64_t>(s.secondary_keys_size()));
    for (const auto& k : s.secondary_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.group_sizes_size()));
    for (const auto g : s.group_sizes()) w_u64(out, g);
    w_u64(out, static_cast<uint64_t>(s.group_start_keys_size()));
    for (const auto& k : s.group_start_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.group_end_keys_size()));
    for (const auto& k : s.group_end_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.filtered_keys_size()));
    for (const auto& k : s.filtered_keys()) w_bytes(out, k);
}

// Destructive: each step is released right after it is encoded, so the peak
// is one payload (the flat buffer plus the not-yet-encoded tail) instead of
// the full protobuf object plus the full flat buffer (two ~3.4GB copies at
// TPC-H SF=1).
static void encode_to_string(
    LineairDB::Protocol::TxExecuteReadPlan::Response& r, std::string& out) {
    CountSink c;
    c.n = 8 + 1 + 1 + 8;  // magic + version + ok + count
    for (const auto& s : r.results()) encode_step(s, c);
    out.clear();
    out.reserve(c.n);
    w_u64(out, kMagic);
    w_u8(out, kVersion);
    w_u8(out, r.ok() ? 1 : 0);
    w_u64(out, static_cast<uint64_t>(r.results_size()));
    for (int i = 0; i < r.results_size(); ++i) {
        encode_step(r.results(i), out);
        r.mutable_results(i)->Clear();
    }
}
}  // namespace flat_plan

void LineairDBRpc::handleTxExecuteReadPlan(const std::string& message,
                                           std::string& result) {
    LineairDB::Protocol::TxExecuteReadPlan::Request request;
    LineairDB::Protocol::TxExecuteReadPlan::Response response;
    request.ParseFromString(message);
    response.set_ok(true);

    std::vector<LineairDB::Protocol::TxExecuteReadPlan::StepResult*>
        previous_results;
    previous_results.reserve(request.steps_size());

    for (const auto& step : request.steps()) {
        auto* step_result = response.add_results();
        previous_results.push_back(step_result);

        bool start_complete = true;
        bool end_complete = true;
        const std::string start_key =
            build_plan_key(step.key_prefix(), step.bindings(),
                           previous_results, -1, &start_complete);
        std::string end_key =
            build_plan_key(step.end_key_prefix(), step.end_bindings(),
                           previous_results, -1, &end_complete);
        if (!start_complete || !end_complete) continue;
        if (step.is_scan() && end_key.empty()) {
            end_key = next_lexicographic_key(start_key);
        }

        // Step-level row filter (cond_push pushdown): drop non-matching rows
        // before transfer; rows the evaluator cannot parse are returned for
        // MySQL to re-evaluate (same contract as the stateless scan RPCs).
        const bool step_has_filter =
            step.has_filter() && step.filter().has_expr();
        const auto* step_filter =
            step_has_filter ? &step.filter().expr() : nullptr;
        const uint32_t step_filter_cols =
            step_has_filter ? step.filter().num_columns() : 0;
        PredicateEvaluator step_eval;
        auto row_passes = [&](const std::string& v) {
            if (step_filter == nullptr) return true;
            if (!step_eval.parse_row(v.data(), v.size(), step_filter_cols)) {
                return true;  // unparseable: ship it, MySQL re-checks
            }
            return step_eval.evaluate(*step_filter);
        };
        // Negative-coverage keys are dead weight when the planner proved no
        // other step reads this table (see PlanStep.suppress_filtered_keys).
        const bool suppress_fkeys = step.suppress_filtered_keys();
        // Anti-join existence membership (Phase-18 q22): cap each for_each probe
        // at its FIRST surviving match — NOT EXISTS only needs existence, never
        // inner rows. Collapses e.g. q22's 1.5M order rows into one marker per
        // distinct o_custkey. See PlanStep.existence_only.
        const bool existence_only = step.existence_only();

        // Projection pushdown: trim each emitted base-row VALUE to the kept
        // columns. Runs AFTER row_passes (the filter parses the FULL row).
        // Trim failure FAILS CLOSED (ok=false, the statement aborts loudly):
        // a full row slipping into a projected stream would be misread by any
        // later step whose binding source_column was remapped to the
        // projected layout (Codex P1), and a failed trim means a malformed
        // row, which must never pass silently.
        const bool step_has_projection = step.has_projection();
        bool projection_failed = false;
        auto project_value = [&](std::string&& v) -> std::string {
            if (!step_has_projection || v.empty()) return std::move(v);
            std::string out;
            if (trim_row_value(v, step.projection().field_indexes(),
                               step.projection().num_columns(), out)) {
                return out;
            }
            projection_failed = true;
            return std::move(v);
        };

        // Phase-9 semijoin membership: build each set once from the earlier
        // source step's shipped value column (optionally only from rows passing
        // source_filter), then drop probe rows whose join key is absent — they
        // cannot have a join partner, so the result is unchanged (planner
        // whitelists inner equi-joins). Built BEFORE the for_each/plain-scan
        // branch so a plain scan (Phase-17 q18 grouped-semijoin: orders reduced
        // against the aggregate step's group keys) applies it too.
        struct FeSemijoin {
            std::unordered_set<std::string> keys;
            uint32_t probe_column;
        };
        std::vector<FeSemijoin> fe_semijoins;
        {
            const int this_step_idx =
                static_cast<int>(previous_results.size()) - 1;
            for (const auto& sj : step.semijoins()) {
                const int ss = static_cast<int>(sj.source_step());
                if (ss < 0 || ss >= this_step_idx) continue;
                FeSemijoin fsj;
                fsj.probe_column = sj.probe_column();
                const bool sf_on =
                    sj.has_source_filter() && sj.source_filter().has_expr();
                const uint32_t sf_cols =
                    sf_on ? sj.source_filter().num_columns() : 0;
                for (const auto& v : previous_results[ss]->scan_values()) {
                    if (v.empty()) continue;
                    if (sf_on) {
                        PredicateEvaluator se;
                        if (se.parse_row(v.data(), v.size(), sf_cols) &&
                            !se.evaluate(sj.source_filter().expr()))
                            continue;
                    }
                    auto col = extract_value_column(v, sj.source_column());
                    if (!col.empty()) fsj.keys.emplace(col);
                }
                fe_semijoins.push_back(std::move(fsj));
            }
        }
        auto sj_reject = [&](const std::string& value) -> bool {
            for (const auto& fsj : fe_semijoins) {
                auto col = extract_value_column(value, fsj.probe_column);
                if (fsj.keys.find(std::string(col)) == fsj.keys.end())
                    return true;  // no join partner -> drop
            }
            return false;
        };

        if (step.for_each()) {
            int source_step = -1;
            if (step.bindings_size() > 0) {
                source_step = static_cast<int>(step.bindings(0).source_step());
            }
            if (source_step < 0 ||
                source_step >= static_cast<int>(previous_results.size()) - 1) {
                continue;
            }

            const auto* source = previous_results[source_step];
            const int row_count =
                std::max(source->scan_keys_size(), source->scan_values_size());
            // Dedup probes: many source rows share a join key, and the proxy
            // serves every runtime probe of one key from the single staged
            // result, so re-executing the probe only inflates the response.
            // Probe keys are RESOLVED first (cheap, serial, order-preserving)
            // so a large probe set can be executed by parallel workers below.
            std::vector<std::string> probe_keys;
            probe_keys.reserve(static_cast<size_t>(row_count));
            {
                std::unordered_set<std::string> seen_probe_keys;
                seen_probe_keys.reserve(static_cast<size_t>(row_count));
                for (int row = 0; row < row_count; ++row) {
                    bool row_complete = true;
                    const std::string row_key =
                        build_plan_key(step.key_prefix(), step.bindings(),
                                       previous_results, row, &row_complete);
                    if (!row_complete) continue;
                    if (!seen_probe_keys.insert(row_key).second) continue;
                    probe_keys.push_back(row_key);
                }
            }

            // Track-B parallel probes (gated HELIOS_PARALLEL_SERVER_SCAN):
            // q12/q14-class for_each steps execute tens of thousands of
            // independent point/prefix probes serially; chunk them across
            // workers and emit in probe order — the emitted sequence (incl.
            // group bookkeeping) is byte-identical to the serial loop. Any
            // worker failure fails the response exactly like a serial scan
            // error; projection failure fails closed the same way.
            static const bool fe_par_on =
                std::getenv("HELIOS_PARALLEL_SERVER_SCAN") != nullptr;
            const size_t kMinParProbes = 4096;
            unsigned fe_threads = std::min<unsigned>(
                std::thread::hardware_concurrency()
                    ? std::thread::hardware_concurrency() : 4u, 8u);
            if (fe_par_on && probe_keys.size() >= kMinParProbes &&
                fe_threads > 1) {
                const size_t NP = probe_keys.size();
                const unsigned T = fe_threads;
                struct ProbeOut {
                    std::vector<std::string> keys, values, sec_keys;
                    std::vector<uint64_t> tids;
                    std::vector<uint32_t> group_rows;  // per probe (scan only)
                };
                std::vector<ProbeOut> outs(T);
                std::vector<char> wfail(T, 0);
                auto* dbp = db_manager_->get_database().get();
                const bool is_scan_probe = step.is_scan();
                const bool has_proj = step.has_projection();
                std::vector<std::thread> ws;
                ws.reserve(T);
                for (unsigned wi = 0; wi < T; ++wi) {
                    ws.emplace_back([&, wi] {
                        const size_t a = NP * wi / T, b = NP * (wi + 1) / T;
                        PredicateEvaluator ev;
                        auto wp_passes = [&](const std::string& v) {
                            if (step_filter == nullptr) return true;
                            if (!ev.parse_row(v.data(), v.size(),
                                              step_filter_cols))
                                return true;  // ship it, MySQL re-checks
                            return ev.evaluate(*step_filter);
                        };
                        auto wp_project = [&](std::string&& v) -> std::string {
                            if (!has_proj || v.empty()) return std::move(v);
                            std::string out;
                            if (trim_row_value(
                                    v, step.projection().field_indexes(),
                                    step.projection().num_columns(), out))
                                return out;
                            wfail[wi] = 1;  // fail closed like project_value
                            return std::move(v);
                        };
                        ProbeOut& o = outs[wi];
                        for (size_t pi = a; pi < b && !wfail[wi]; ++pi) {
                            const std::string& row_key = probe_keys[pi];
                            if (is_scan_probe) {
                                const std::string row_end =
                                    next_lexicographic_key(row_key);
                                uint32_t group_rows = 0;
                                if (step.index_name().empty()) {
                                    auto sr = dbp->StatelessRangeScan(
                                        step.table_name(), row_key, row_end,
                                        step.scan_limit(), step.reverse_scan());
                                    if (!sr.ok) { wfail[wi] = 1; break; }
                                    for (auto& r : sr.rows) {
                                        if (!wp_passes(r.value)) continue;
                                        if (!fe_semijoins.empty() &&
                                            sj_reject(r.value))
                                            continue;
                                        o.keys.push_back(std::move(r.key));
                                        o.values.push_back(
                                            wp_project(std::move(r.value)));
                                        o.tids.push_back(r.tid);
                                        ++group_rows;
                                        if (existence_only) break;
                                    }
                                } else {
                                    auto sr = dbp->StatelessSecondaryRangeScan(
                                        step.table_name(), step.index_name(),
                                        row_key, row_end, step.scan_limit(),
                                        step.reverse_scan());
                                    if (!sr.ok) { wfail[wi] = 1; break; }
                                    for (auto& r : sr.rows) {
                                        if (!wp_passes(r.value)) continue;
                                        if (!fe_semijoins.empty() &&
                                            sj_reject(r.value))
                                            continue;
                                        o.sec_keys.push_back(
                                            std::move(r.secondary_key));
                                        o.keys.push_back(
                                            std::move(r.primary_key));
                                        o.values.push_back(
                                            wp_project(std::move(r.value)));
                                        o.tids.push_back(r.tid);
                                        ++group_rows;
                                        if (existence_only) break;
                                    }
                                }
                                o.group_rows.push_back(group_rows);
                            } else {
                                auto rr = dbp->StatelessRead(step.table_name(),
                                                             row_key);
                                o.keys.push_back(row_key);
                                o.tids.push_back(rr.tid);
                                if (rr.found &&
                                    !(!fe_semijoins.empty() &&
                                      sj_reject(rr.value))) {
                                    o.values.push_back(
                                        wp_project(std::move(rr.value)));
                                } else {
                                    o.values.push_back("");
                                }
                            }
                        }
                        dbp->ReleaseMasstreeThreadEpoch();
                    });
                }
                for (auto& w : ws) w.join();
                bool any_fail = false;
                for (unsigned wi = 0; wi < T; ++wi)
                    if (wfail[wi]) any_fail = true;
                if (any_fail) {
                    response.set_ok(false);
                    flat_plan::encode_to_string(response, result);
                    return;
                }
                size_t probe_idx = 0;
                for (unsigned wi = 0; wi < T; ++wi) {
                    ProbeOut& o = outs[wi];
                    for (size_t k = 0; k < o.keys.size(); ++k) {
                        if (!o.sec_keys.empty())
                            step_result->add_secondary_keys(
                                std::move(o.sec_keys[k]));
                        step_result->add_scan_keys(std::move(o.keys[k]));
                        step_result->add_scan_values(std::move(o.values[k]));
                        step_result->add_scan_tids(o.tids[k]);
                    }
                    if (is_scan_probe) {
                        const size_t a = NP * wi / T, b = NP * (wi + 1) / T;
                        for (size_t pi = a; pi < b; ++pi) {
                            const std::string& row_key = probe_keys[pi];
                            step_result->add_group_sizes(
                                o.group_rows[pi - a]);
                            step_result->add_group_start_keys(row_key);
                            step_result->add_group_end_keys(
                                next_lexicographic_key(row_key));
                        }
                        probe_idx = b;
                    }
                }
                (void)probe_idx;
                continue;
            }

            for (const std::string& row_key : probe_keys) {
                if (step.is_scan()) {
                    // FER/FES: per-probe prefix range [row_key, next(row_key)).
                    const std::string row_end = next_lexicographic_key(row_key);
                    int group_rows = 0;
                    if (step.index_name().empty()) {
                        auto scan_result =
                            db_manager_->get_database()->StatelessRangeScan(
                                step.table_name(), row_key, row_end,
                                step.scan_limit(), step.reverse_scan());
                        if (!scan_result.ok) {
                            response.set_ok(false);
                            flat_plan::encode_to_string(response, result);
                            return;
                        }
                        for (auto& r : scan_result.rows) {
                            if (!row_passes(r.value)) continue;
                            if (!fe_semijoins.empty() && sj_reject(r.value))
                                continue;
                            step_result->add_scan_keys(std::move(r.key));
                            step_result->add_scan_values(
                                project_value(std::move(r.value)));
                            step_result->add_scan_tids(r.tid);
                            ++group_rows;
                            if (existence_only) break;
                        }
                    } else {
                        auto scan_result =
                            db_manager_->get_database()
                                ->StatelessSecondaryRangeScan(
                                    step.table_name(), step.index_name(),
                                    row_key, row_end, step.scan_limit(),
                                    step.reverse_scan());
                        if (!scan_result.ok) {
                            response.set_ok(false);
                            flat_plan::encode_to_string(response, result);
                            return;
                        }
                        for (auto& r : scan_result.rows) {
                            if (!row_passes(r.value)) continue;
                            if (!fe_semijoins.empty() && sj_reject(r.value))
                                continue;
                            step_result->add_secondary_keys(
                                std::move(r.secondary_key));
                            step_result->add_scan_keys(std::move(r.primary_key));
                            step_result->add_scan_values(
                                project_value(std::move(r.value)));
                            step_result->add_scan_tids(r.tid);
                            ++group_rows;
                            if (existence_only) break;
                        }
                    }
                    step_result->add_group_sizes(
                        static_cast<uint32_t>(group_rows));
                    step_result->add_group_start_keys(row_key);
                    step_result->add_group_end_keys(row_end);
                    continue;
                }

                auto read_result =
                    db_manager_->get_database()->StatelessRead(
                        step.table_name(), row_key);
                step_result->add_scan_keys(row_key);
                step_result->add_scan_tids(read_result.tid);
                if (read_result.found &&
                    !(!fe_semijoins.empty() &&
                      sj_reject(read_result.value))) {
                    step_result->add_scan_values(
                        project_value(std::move(read_result.value)));
                } else {
                    // Not found, or semijoin-rejected (no join partner):
                    // serve as not-found — sound, the row could never
                    // contribute a joined result row.
                    step_result->add_scan_values("");
                }
            }
            if (projection_failed) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            continue;
        }

        if (!step.is_scan()) {
            auto read_result =
                db_manager_->get_database()->StatelessRead(
                    step.table_name(), start_key);
            step_result->set_actual_key(start_key);
            step_result->set_actual_start_key(start_key);
            step_result->set_found(read_result.found);
            step_result->set_tid(read_result.tid);
            if (read_result.found) {
                step_result->set_value(project_value(std::move(read_result.value)));
            }
            if (projection_failed) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            continue;
        }

        if (step.index_name().empty()) {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            // Track-B parallel scan: for a read-only no-validate primary
            // AGGREGATE scan (q1/q6-class), split the range across worker
            // threads, filter+aggregate per worker, merge, emit. On success
            // the group rows are already in step_result, so skip the serial
            // full scan entirely. Falls through to serial when ineligible /
            // unsplittable / a worker failed. (Aggregate steps are only
            // stamped on the ro_novalidate path on this branch, so
            // has_aggregate implies the read-only no-validate scope.)
            if (!step.for_each() &&
                step.scan_limit() == 0 && !step.reverse_scan() &&
                step.has_aggregate() && step.aggregate().aggs_size() > 0) {
                if (parallel_primary_aggregate_scan(
                        db_manager_->get_database().get(), step, start_key,
                        end_key, step_result))
                    continue;
            }
            // Track-B parallel scan for FILTERED non-aggregate primary scans.
            if (!step.for_each() && step.scan_limit() == 0 &&
                !step.reverse_scan() &&
                !(step.has_aggregate() && step.aggregate().aggs_size() > 0) &&
                step.has_filter() && step.filter().has_expr()) {
                if (parallel_primary_filter_scan(
                        db_manager_->get_database().get(), step, start_key,
                        end_key, step_result))
                    continue;
            }
            // When a predicate is attached we must disable the pushed
            // scan_limit at the LineairDB layer and re-apply it after the
            // post-filter: otherwise LineairDB returns the first N physical
            // rows and we filter from them - for `WHERE x=1 LIMIT 1` that
            // gives zero rows even when a matching row exists later.
            const bool limit_after_filter =
                step.has_filter() && step.filter().has_expr();
            const uint64_t scan_limit_for_lineairdb =
                limit_after_filter ? 0 : step.scan_limit();
            auto scan_result =
                db_manager_->get_database()->StatelessRangeScan(
                    step.table_name(), start_key, end_key,
                    scan_limit_for_lineairdb, step.reverse_scan());
            if (!scan_result.ok) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            // Aggregation pushdown: aggregate server-side and emit group rows
            // instead of raw rows. Input = the rows passing the step filter;
            // the proxy disabled projection on aggregate steps, so column refs
            // read original field indices from full rows. Falls back to
            // per-row emit when the spec is not server-aggregatable.
            bool aggregated = false;
            if (step.has_aggregate() && step.aggregate().aggs_size() > 0) {
                if (step_filter != nullptr) {
                    // Aggregate input filtering FAILS CLOSED on a parse
                    // failure: the usual ship-and-let-MySQL-recheck contract
                    // is impossible once rows are folded into group rows
                    // (Codex P2), and an unparseable row means a malformed
                    // value that must never silently join an aggregate.
                    PredicateEvaluator agg_eval;
                    bool parse_failed = false;
                    std::remove_reference_t<decltype(scan_result.rows)> filtered;
                    filtered.reserve(scan_result.rows.size());
                    for (auto& row : scan_result.rows) {
                        if (!agg_eval.parse_row(row.value.data(),
                                                row.value.size(),
                                                step_filter_cols)) {
                            parse_failed = true;
                            break;
                        }
                        if (agg_eval.evaluate(*step_filter))
                            filtered.push_back(std::move(row));
                    }
                    if (parse_failed) {
                        response.set_ok(false);
                        flat_plan::encode_to_string(response, result);
                        return;
                    }
                    scan_result.rows = std::move(filtered);
                }
                aggregated = server_aggregate_scan(step.aggregate(),
                                                   scan_result.rows, step_result);
            }
            if (!aggregated) {
                uint64_t emitted = 0;
                for (auto& row : scan_result.rows) {
                    if (!row_passes(row.value)) {
                        // Coverage for point probes into a filtered scan: ship
                        // the rejected KEY so the proxy can answer not-found
                        // locally (sound: the filter is this alias's WHERE
                        // conjunct, so MySQL would discard the row anyway).
                        if (!suppress_fkeys)
                            step_result->add_filtered_keys(std::move(row.key));
                        continue;
                    }
                    // Grouped-semijoin (Phase-17 q18): drop rows whose join key
                    // is absent from the source step's group keys. NOT shipped
                    // as filtered_keys: the IN reduction is a membership prune,
                    // not this alias's local WHERE, so a stray point probe must
                    // still miss and abort rather than read a wrong not-found.
                    if (!fe_semijoins.empty() && sj_reject(row.value)) continue;
                    step_result->add_scan_keys(std::move(row.key));
                    step_result->add_scan_values(project_value(std::move(row.value)));
                    step_result->add_scan_tids(row.tid);
                    if (step.scan_limit() > 0 &&
                        ++emitted >= step.scan_limit())
                        break;  // re-apply the pushed limit post-filter
                }
            }
        } else {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            auto scan_result =
                db_manager_->get_database()->StatelessSecondaryRangeScan(
                    step.table_name(), step.index_name(), start_key, end_key,
                    step.scan_limit(), step.reverse_scan());
            if (!scan_result.ok) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            for (auto& row : scan_result.rows) {
                if (!row_passes(row.value)) {
                    // See the primary branch: negative coverage by primary key.
                    if (!suppress_fkeys)
                        step_result->add_filtered_keys(std::move(row.primary_key));
                    continue;
                }
                step_result->add_secondary_keys(std::move(row.secondary_key));
                step_result->add_scan_keys(std::move(row.primary_key));
                step_result->add_scan_values(project_value(std::move(row.value)));
                step_result->add_scan_tids(row.tid);
            }
        }
        if (projection_failed) {
            response.set_ok(false);
            flat_plan::encode_to_string(response, result);
            return;
        }
    }

    flat_plan::encode_to_string(response, result);
}

void LineairDBRpc::handleTxValidateAndCommit(const std::string& message,
                                             std::string& result) {
    LineairDB::Protocol::TxValidateAndCommit::Request request;
    LineairDB::Protocol::TxValidateAndCommit::Response response;

    request.ParseFromString(message);

    std::vector<LineairDB::ExternalReadEntry> reads;
    reads.reserve(request.reads_size());
    for (const auto& read : request.reads()) {
        reads.push_back({read.table_name(), read.key(), read.tid(),
                         read.found()});
    }

    std::vector<LineairDB::ExternalWriteEntry> writes;
    writes.reserve(request.writes_size() + request.deletes_size());
    for (const auto& write : request.writes()) {
        writes.push_back({write.table_name(), write.key(), write.value(),
                          write.is_delete()});
    }
    for (const auto& del : request.deletes()) {
        writes.push_back({del.table_name(), del.key(), "", true});
    }

    std::vector<LineairDB::ExternalSecondaryIndexEntry> si_ops;
    si_ops.reserve(request.secondary_index_ops_size());
    for (const auto& op : request.secondary_index_ops()) {
        si_ops.push_back({op.table_name(), op.index_name(),
                          op.secondary_key(), op.primary_key(),
                          op.is_delete()});
    }

    std::vector<LineairDB::ExternalRangeReadEntry> range_reads;
    range_reads.reserve(request.range_reads_size());
    for (const auto& range : request.range_reads()) {
        LineairDB::ExternalRangeReadEntry entry;
        entry.table_name = range.table_name();
        entry.index_name = range.index_name();
        entry.start_key = range.start_key();
        entry.end_key = range.end_key();
        entry.row_limit = range.row_limit();
        entry.reverse_scan = range.reverse_scan();
        entry.result_keys.reserve(range.result_keys_size());
        for (const auto& key : range.result_keys()) {
            entry.result_keys.push_back(key);
        }
        entry.result_primary_keys.reserve(range.result_primary_keys_size());
        for (const auto& key : range.result_primary_keys()) {
            entry.result_primary_keys.push_back(key);
        }
        range_reads.push_back(std::move(entry));
    }

    std::string abort_reason;
    const bool committed =
        db_manager_->get_database()->ValidateAndCommit(reads, writes, si_ops,
                                                       range_reads,
                                                       &abort_reason);
    response.set_committed(committed);
    if (!committed && !abort_reason.empty()) {
        response.set_abort_reason(abort_reason);
    }

    if (committed && request.row_deltas_size() > 0) {
        row_counts_->apply_deltas(request.row_deltas());
    }
    if (committed && request.fence()) {
        db_manager_->get_database()->Fence();
    }

    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxWrite(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWrite");

    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& value_str = request.value();
        tx->Write(request.key(), reinterpret_cast<const std::byte*>(value_str.c_str()), value_str.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("Wrote key '%s' to transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for write: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxDelete(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxDelete");

    LineairDB::Protocol::TxDelete::Request request;
    LineairDB::Protocol::TxDelete::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        tx->Delete(request.key());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("Deleted key '%s' from transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for delete: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxReadSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxReadSecondaryIndex");

    LineairDB::Protocol::TxReadSecondaryIndex::Request request;
    LineairDB::Protocol::TxReadSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        auto results = tx->ReadSecondaryIndex(request.index_name(), request.secondary_key());
        response.set_is_aborted(tx->IsAborted());

        for (const auto& [ptr, size] : results) {
            std::string value(reinterpret_cast<const char*>(ptr), size);
            response.add_values(value);
        }
        LOG_DEBUG("ReadSecondaryIndex index='%s' key='%s' tx=%ld: %d values",
                  request.index_name().c_str(), request.secondary_key().c_str(), tx_id, response.values_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for read_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxWriteSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWriteSecondaryIndex");

    LineairDB::Protocol::TxWriteSecondaryIndex::Request request;
    LineairDB::Protocol::TxWriteSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->WriteSecondaryIndex(request.index_name(), request.secondary_key(),
                                reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for write_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxDeleteSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxDeleteSecondaryIndex");

    LineairDB::Protocol::TxDeleteSecondaryIndex::Request request;
    LineairDB::Protocol::TxDeleteSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->DeleteSecondaryIndex(request.index_name(), request.secondary_key(),
                                 reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("DeleteSecondaryIndex index='%s' key='%s' tx=%ld",
                  request.index_name().c_str(), request.secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for delete_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxUpdateSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxUpdateSecondaryIndex");

    LineairDB::Protocol::TxUpdateSecondaryIndex::Request request;
    LineairDB::Protocol::TxUpdateSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->UpdateSecondaryIndex(request.index_name(),
                                 request.old_secondary_key(), request.new_secondary_key(),
                                 reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("UpdateSecondaryIndex index='%s' old='%s' new='%s' tx=%ld",
                  request.index_name().c_str(), request.old_secondary_key().c_str(),
                  request.new_secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for update_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingKeysInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysInRange");

    LineairDB::Protocol::TxGetMatchingKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingKeysInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        auto scan_result = tx->Scan(
            start_key, end_opt, [&response](auto key, auto) {
                response.add_keys(std::string(key));
                return false;
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
        }
        LOG_DEBUG("GetMatchingKeysInRange tx=%ld: %d keys", tx_id, response.keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_keys_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingKeysAndValuesInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysAndValuesInRange");

    LineairDB::Protocol::TxGetMatchingKeysAndValuesInRange::Request request;
    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);

    // Respond with flat binary instead of protobuf to avoid per-entry overhead.
    // Format: [is_aborted:1B] [key_len:4B][key][val_len:4B][val]... [sentinel:key_len=0]
    result.clear();
    result.reserve(4096);
    result.push_back(0);   // is_aborted placeholder (updated after Scan completes)

    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();
        const uint64_t row_limit = request.row_limit();
        const bool reverse_scan = request.reverse_scan();
        // Count rows actually returned after tombstone and predicate checks.
        uint64_t returned_rows = 0;

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        // Predicate pushdown: prepare filter if present
        const bool has_filter = request.has_filter() && request.filter().has_expr();
        const auto* filter_expr = has_filter ? &request.filter().expr() : nullptr;
        uint32_t filter_num_cols = has_filter ? request.filter().num_columns() : 0;
        PredicateEvaluator evaluator;

        // Scan callback: value is pair<const void*, size_t> from LineairDB
        auto append_matching_row =
            [&result, row_limit, &returned_rows, filter_expr, filter_num_cols,
             &evaluator](auto key, auto value) {
                // Skip tombstones (deleted rows)
                if (value.first == nullptr || value.second == 0) { return false; }
                // Predicate pushdown: evaluate filter if present
                if (filter_expr) {
                    if (evaluator.parse_row(static_cast<const char*>(value.first),
                                            value.second, filter_num_cols)) {
                        if (!evaluator.evaluate(*filter_expr)) {
                            return false;  // filter rejected → skip row, continue scanning
                        }
                    } else {
                        // Return parse-failed rows for MySQL to check, but do
                        // not count them toward a pushed LIMIT.
                        uint32_t klen = static_cast<uint32_t>(key.size());
                        uint32_t vlen = static_cast<uint32_t>(value.second);
                        result.append(reinterpret_cast<const char*>(&klen), 4);
                        result.append(key.data(), key.size());
                        result.append(reinterpret_cast<const char*>(&vlen), 4);
                        result.append(static_cast<const char*>(value.first), value.second);
                        return false;
                    }
                }
                // Append key-value entry in flat binary format
                uint32_t klen = static_cast<uint32_t>(key.size());
                uint32_t vlen = static_cast<uint32_t>(value.second);
                result.append(reinterpret_cast<const char*>(&klen), 4);
                result.append(key.data(), key.size());
                result.append(reinterpret_cast<const char*>(&vlen), 4);
                result.append(static_cast<const char*>(value.first), value.second);
                returned_rows++;
                // Stop the LineairDB scan once the pushed LIMIT is satisfied.
                return row_limit > 0 && returned_rows >= row_limit;
            };

        std::optional<size_t> scan_result;
        if (reverse_scan) {
            scan_result = tx->ScanReverse(start_key, end_opt, append_matching_row);
        } else {
            scan_result = tx->Scan(start_key, end_opt, append_matching_row);
        }

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            result[0] = 1;  // update is_aborted placeholder
        } else if (tx->IsAborted()) {
            result[0] = 1;
        }
    } else {
        result[0] = 1;
        LOG_WARNING("Transaction not found for get_matching_keys_and_values_in_range: %ld", tx_id);
    }

    uint32_t sentinel = 0;
    result.append(reinterpret_cast<const char*>(&sentinel), 4);  // sentinel: key_len=0 marks end of entries
}

void LineairDBRpc::handleTxGetMatchingKeysAndValuesFromPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysAndValuesFromPrefix");

    LineairDB::Protocol::TxGetMatchingKeysAndValuesFromPrefix::Request request;
    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);

    // Same flat binary format as handleTxGetMatchingKeysAndValuesInRange
    result.clear();
    result.reserve(4096);
    result.push_back(0);   // is_aborted placeholder

    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string prefix = request.prefix();
        bool first_key_checked = false;
        bool prefix_miss = false;

        // Predicate pushdown: prepare filter if present
        const bool has_filter = request.has_filter() && request.filter().has_expr();
        const auto* filter_expr = has_filter ? &request.filter().expr() : nullptr;
        uint32_t filter_num_cols = has_filter ? request.filter().num_columns() : 0;
        PredicateEvaluator evaluator;

        // Scan callback: value is pair<const void*, size_t> from LineairDB
        auto scan_result = tx->Scan(
            prefix, std::nullopt,
            [&result, &first_key_checked, &prefix_miss, &prefix,
             filter_expr, filter_num_cols, &evaluator, this](auto key, auto value) {
                // Check if first key matches the prefix; if not, abort scan early
                if (!first_key_checked) {
                    first_key_checked = true;
                    std::string key_str(key);
                    if (!key_prefix_is_matching(prefix, key_str)) { prefix_miss = true; return true; }
                }
                // Skip tombstones (deleted rows)
                if (value.first == nullptr || value.second == 0) { return false; }
                // Predicate pushdown: evaluate filter if present
                if (filter_expr) {
                    if (evaluator.parse_row(static_cast<const char*>(value.first),
                                            value.second, filter_num_cols)) {
                        if (!evaluator.evaluate(*filter_expr)) {
                            return false;  // filter rejected → skip row, continue scanning
                        }
                    }
                    // parse_row failure → include row (safe fallback)
                }
                // Append key-value entry in flat binary format
                uint32_t klen = static_cast<uint32_t>(key.size());
                uint32_t vlen = static_cast<uint32_t>(value.second);
                result.append(reinterpret_cast<const char*>(&klen), 4);
                result.append(key.data(), key.size());
                result.append(reinterpret_cast<const char*>(&vlen), 4);
                result.append(static_cast<const char*>(value.first), value.second);
                return false;  // continue scanning
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            result[0] = 1;
        } else if (tx->IsAborted()) {
            result[0] = 1;
        }
        if (prefix_miss) {
            // No matching keys found; discard entries, keep just header + sentinel
            result.resize(1);
        }
    } else {
        result[0] = 1;
        LOG_WARNING("Transaction not found for get_matching_keys_and_values_from_prefix: %ld", tx_id);
    }

    uint32_t sentinel = 0;
    result.append(reinterpret_cast<const char*>(&sentinel), 4);  // sentinel
}

void LineairDBRpc::handleTxFetchLastKeyInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastKeyInRange");

    LineairDB::Protocol::TxFetchLastKeyInRange::Request request;
    LineairDB::Protocol::TxFetchLastKeyInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        std::optional<std::string> result;
        auto scan_result = tx->ScanReverse(
            start_key, end_opt, [&result](auto key, auto) {
                result = std::string(key);
                return true;
            });

        // Phantom detection: if ScanReverse returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchLastKeyInRange tx=%ld: found=%s", tx_id, result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_key_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchFirstKeyWithPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchFirstKeyWithPrefix");

    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string prefix = request.prefix();
        std::string prefix_end = request.prefix_end();

        std::optional<std::string_view> end_opt;
        if (!prefix_end.empty()) { end_opt = prefix_end; }

        std::optional<std::string> result;
        auto scan_result = tx->Scan(
            prefix, end_opt, [&result, &prefix_end](auto key, auto value) {
                if (!prefix_end.empty() && key == prefix_end) {
                    return true; // exclusive end
                }
                // Skip tombstones
                if (value.first == nullptr || value.second == 0) {
                    return false; // Continue scanning
                }
                result = std::string(key);
                return true; // Stop after first valid key
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchFirstKeyWithPrefix tx=%ld prefix='%s': found=%s",
                  tx_id, prefix.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_first_key_with_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchNextKeyWithPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchNextKeyWithPrefix");

    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string last_key = request.last_key();
        std::string prefix_end = request.prefix_end();
        bool skip_first = true;

        std::optional<std::string_view> end_opt;
        if (!prefix_end.empty()) { end_opt = prefix_end; }

        std::optional<std::string> result;
        auto scan_result = tx->Scan(
            last_key, end_opt,
            [&result, &skip_first, &last_key, &prefix_end](auto key, auto value) {
                // Skip the last_key itself (we want the next one)
                if (skip_first && key == last_key) {
                    skip_first = false;
                    return false; // Continue scanning
                }
                if (!prefix_end.empty() && key == prefix_end) {
                    return true; // exclusive end
                }
                // Skip tombstones
                if (value.first == nullptr || value.second == 0) {
                    return false; // Continue scanning
                }
                result = std::string(key);
                return true; // Stop after first valid key
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchNextKeyWithPrefix tx=%ld last_key='%s': found=%s",
                  tx_id, last_key.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_next_key_with_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingPrimaryKeysInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingPrimaryKeysInRange");

    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        auto scan_result = tx->ScanSecondaryIndex(
            index_name, start_key, end_opt,
            [&response]([[maybe_unused]] std::string_view secondary_key,
                        const std::vector<std::string>& primary_keys) {
                for (const auto& pk : primary_keys) { response.add_primary_keys(pk); }
                return false;
            });

        // Phantom detection: ScanSecondaryIndex returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
        }
        LOG_DEBUG("GetMatchingPrimaryKeysInRange tx=%ld index='%s': %d keys",
                  tx_id, index_name.c_str(), response.primary_keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_primary_keys_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingPrimaryKeysFromPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingPrimaryKeysFromPrefix");

    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string prefix = request.prefix();
        bool first_key_checked = false;
        bool prefix_miss = false;

        auto scan_result = tx->ScanSecondaryIndex(
            index_name, prefix, std::nullopt,
            [&response, &first_key_checked, &prefix_miss, &prefix, this]
            (std::string_view secondary_key, const std::vector<std::string>& primary_keys) {
                if (!first_key_checked) {
                    first_key_checked = true;
                    std::string key_str(secondary_key);
                    if (!key_prefix_is_matching(prefix, key_str)) { prefix_miss = true; return true; }
                }
                for (const auto& pk : primary_keys) { response.add_primary_keys(pk); }
                return false;
            });

        // Phantom detection: ScanSecondaryIndex returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (prefix_miss) { response.clear_primary_keys(); }
        }
        LOG_DEBUG("GetMatchingPrimaryKeysFromPrefix tx=%ld index='%s' prefix='%s': %d keys",
                  tx_id, index_name.c_str(), prefix.c_str(), response.primary_keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_primary_keys_from_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchLastPrimaryKeyInSecondaryRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastPrimaryKeyInSecondaryRange");

    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Request request;
    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        std::optional<std::string> result;
        auto scan_result = tx->ScanSecondaryIndexReverse(
            index_name, start_key, end_opt,
            [&result]([[maybe_unused]] std::string_view secondary_key,
                      const std::vector<std::string>& primary_keys) {
                if (primary_keys.empty()) { return false; }
                result = primary_keys.back();
                return true;
            });

        // Phantom detection: ScanSecondaryIndexReverse returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_primary_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchLastPrimaryKeyInSecondaryRange tx=%ld index='%s': found=%s",
                  tx_id, index_name.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_primary_key_in_secondary_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchLastSecondaryEntryInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastSecondaryEntryInRange");

    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Request request;
    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        bool found = false;
        auto scan_result = tx->ScanSecondaryIndexReverse(
            index_name, start_key, end_opt,
            [&response, &found](std::string_view secondary_key,
                                const std::vector<std::string>& primary_keys) {
                if (primary_keys.empty()) { return false; }
                found = true;
                auto* entry = response.mutable_entry();
                entry->set_secondary_key(std::string(secondary_key));
                for (const auto& pk : primary_keys) { entry->add_primary_keys(pk); }
                return true;
            });

        // Phantom detection: ScanSecondaryIndexReverse returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            response.set_found(found);
        }
        LOG_DEBUG("FetchLastSecondaryEntryInRange tx=%ld index='%s': found=%s",
                  tx_id, index_name.c_str(), found ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_secondary_entry_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}
void LineairDBRpc::handleDbFence(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbFence");

    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;

    request.ParseFromString(message);

    db_manager_->get_database()->Fence();
    LOG_DEBUG("Database fence completed");

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbEndTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbEndTransaction");

    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool fence = request.fence();
        bool committed = db_manager_->get_database()->EndTransaction(
            *tx, [fence, tx_id](LineairDB::TxStatus status) {
                LOG_DEBUG("Transaction %ld ended with status: %d, fence=%s", tx_id, static_cast<int>(status), fence ? "true" : "false");
            });
        bool aborted = !committed;
        response.set_is_aborted(aborted);
        tx_manager_->remove_transaction(tx_id);

        // Apply row-count deltas on successful commit
        if (committed && request.row_deltas_size() > 0) {
            row_counts_->apply_deltas(request.row_deltas());
        }

        LOG_DEBUG("Ended transaction %ld with fence=%s (committed=%s)", tx_id, fence ? "true" : "false", committed ? "true" : "false");
        // Piggyback updated table row counts for the proxy's next transaction.
        for (const auto& [name, count] : row_counts_->snapshot()) {
            auto* ts = response.add_table_stats();
            ts->set_table_name(name);
            ts->set_row_count(count);
        }
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for end: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateTable(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbCreateTable");

    LineairDB::Protocol::DbCreateTable::Request request;
    LineairDB::Protocol::DbCreateTable::Response response;

    request.ParseFromString(message);

    bool success = db_manager_->get_database()->CreateTable(request.table_name());
    response.set_success(success);
    LOG_DEBUG("CreateTable '%s': %s", request.table_name().c_str(), success ? "success" : "already exists");

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbSetTable(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbSetTable");

    LineairDB::Protocol::DbSetTable::Request request;
    LineairDB::Protocol::DbSetTable::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool success = tx->SetTable(request.table_name());
        response.set_success(success);
        LOG_DEBUG("SetTable '%s' for tx=%ld: %s", request.table_name().c_str(), tx_id, success ? "success" : "failed");
    } else {
        response.set_success(false);
        LOG_WARNING("Transaction not found for set_table: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbCreateSecondaryIndex");

    LineairDB::Protocol::DbCreateSecondaryIndex::Request request;
    LineairDB::Protocol::DbCreateSecondaryIndex::Response response;

    request.ParseFromString(message);

    bool success = db_manager_->get_database()->CreateSecondaryIndex(
        request.table_name(), request.index_name(), request.index_type());
    response.set_success(success);

    result = response.SerializeAsString();
}
