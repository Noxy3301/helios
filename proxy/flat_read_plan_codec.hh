// Flat binary codec for TX_EXECUTE_READ_PLAN responses.
//
// WHY: the prefetch response for a heavy join at scale (e.g. TPC-H Q18/Q21 at
// SF=1 materialize ~2x full lineitem) exceeds Protobuf's hard ~2GB single-
// message serialization limit (INT_MAX); SerializeToString then fails and
// returns an empty string. This codec serializes/deserializes the SAME proto
// Response object to/from a flat length-prefixed buffer using 64-bit lengths,
// so there is no 2GB ceiling. The proto message stays the schema/source of
// truth (readability); only the wire (de)serialization is flat.
//
// SCOPE / LIMITS:
//  - Only TxExecuteReadPlan::Response. All other RPCs keep Protobuf.
//  - The transport frame header still carries a uint32 payload size
//    (server/protocol/message.hh), so the encoded buffer must stay < 4GB.
//    Removing that 4GB ceiling (uint64 framing) and streaming/zero-copy are
//    follow-ups; this change removes only the immediate 2GB protobuf ceiling.
//  - Same-host deployment (proxy and server co-located): integers are written
//    in NATIVE byte order via memcpy. A magic word encodes the byte order so a
//    mismatched peer is detected rather than silently misreading. Do NOT reuse
//    this format across architectures without adding explicit LE conversion.
//
// FORMAT (all multi-byte ints are native-endian, written via memcpy):
//   u64 magic ('LDBFLAT1' as bytes) | u8 version | u8 ok
//   u64 results_count | StepResult* | u64 rv_count | RVE* | u64 ir_count | IVE*
// where each repeated section is a u64 count followed by that many records,
// and every byte string is u64 length + raw bytes (may contain NULs).

#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include "lineairdb.pb.h"

namespace helios_flat {

// 'LDBFLAT1' little bytes; if the peer reads it in a different byte order the
// value won't match and decode fails fast.
static constexpr uint64_t kMagic = 0x31544100u + (0x4C44420000000000ull);
// Version 2: appended `tx_occ_key:u64` to the Response envelope (physical-OCC,
// helios 2-RPC). Older Response had version=1 with no key. Server and proxy
// rebuild together, so a strict version check is sufficient.
static constexpr uint8_t kVersion = 2;

// ---- writer primitives ----
// Sink = anything with append(const char*, size_t). std::string works as-is;
// the server also uses a chunked-socket sink (stream, no full buffer) and a
// counting sink (compute size for the frame header). Templated so all three
// share one encoder.
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

// Counting sink: encode into it to get the exact serialized size (for the
// uint64 frame header) without allocating the buffer.
struct CountSink {
  uint64_t n = 0;
  void append(const char*, size_t k) { n += k; }
};

// ---- reader (bounds-checked cursor) ----
struct Reader {
  const char* p;
  const char* end;
  bool ok = true;
  Reader(const char* data, size_t n) : p(data), end(data + n) {}
  uint64_t u64() {
    // Compare distances (end - p), never form p+8 past the buffer (UB).
    if (!ok || static_cast<size_t>(end - p) < 8) { ok = false; return 0; }
    uint64_t v;
    std::memcpy(&v, p, 8);
    p += 8;
    return v;
  }
  uint8_t u8() {
    if (!ok || end - p < 1) { ok = false; return 0; }
    return static_cast<uint8_t>(*p++);
  }
  // Reads a length-prefixed byte string into out. Bounds-checked.
  void bytes(std::string* out) {
    uint64_t n = u64();
    if (!ok || n > static_cast<uint64_t>(end - p)) { ok = false; return; }
    out->assign(p, n);
    p += n;
  }
};

using RVE = LineairDB::Protocol::RangeValidationEntry;
using IVE = LineairDB::Protocol::IndexValidationEntry;
using SubScan = LineairDB::Protocol::TxExecuteReadPlan::SubScan;
using StepResult = LineairDB::Protocol::TxExecuteReadPlan::StepResult;
using Response = LineairDB::Protocol::TxExecuteReadPlan::Response;

// ---- encode (templated on Sink: std::string / CountSink / socket sink) ----
template <class Sink>
inline void enc_rve(Sink& o, const RVE& r) {
  w_bytes(o, r.table_name());
  w_bytes(o, r.index_name());
  w_u64(o, r.owner_ptr());
  w_u64(o, r.node_ptr());
  w_u64(o, r.version());
  w_bytes(o, r.start_key());
  w_bytes(o, r.end_key());
  w_u64(o, r.row_limit());
  w_u8(o, r.reverse_scan() ? 1 : 0);
  w_u64(o, r.result_keys_size());
  for (const auto& k : r.result_keys()) w_bytes(o, k);
  w_u64(o, r.result_primary_keys_size());
  for (const auto& k : r.result_primary_keys()) w_bytes(o, k);
}
template <class Sink>
inline void enc_ive(Sink& o, const IVE& r) {
  w_bytes(o, r.table_name());
  w_bytes(o, r.index_name());
  w_bytes(o, r.key());
  w_u64(o, r.tid());
  w_u8(o, r.found() ? 1 : 0);
}
template <class Sink>
inline void enc_subscan(Sink& o, const SubScan& s) {
  w_bytes(o, s.start_key());
  w_bytes(o, s.end_key());
  w_u64(o, s.scan_keys_size());
  for (const auto& k : s.scan_keys()) w_bytes(o, k);
  w_u64(o, s.scan_values_size());
  for (const auto& v : s.scan_values()) w_bytes(o, v);
  w_u64(o, s.scan_tids_size());
  for (uint64_t t : s.scan_tids()) w_u64(o, t);
  w_u64(o, s.secondary_keys_size());
  for (const auto& k : s.secondary_keys()) w_bytes(o, k);
  w_u64(o, s.range_versions_size());
  for (const auto& r : s.range_versions()) enc_rve(o, r);
}
template <class Sink>
inline void enc_step(Sink& o, const StepResult& s) {
  w_u8(o, s.found() ? 1 : 0);
  w_bytes(o, s.value());
  w_u64(o, s.tid());
  w_bytes(o, s.actual_key());
  w_u64(o, s.scan_keys_size());
  for (const auto& k : s.scan_keys()) w_bytes(o, k);
  w_u64(o, s.scan_values_size());
  for (const auto& v : s.scan_values()) w_bytes(o, v);
  w_u64(o, s.scan_tids_size());
  for (uint64_t t : s.scan_tids()) w_u64(o, t);
  w_u64(o, s.secondary_keys_size());
  for (const auto& k : s.secondary_keys()) w_bytes(o, k);
  w_bytes(o, s.actual_start_key());
  w_bytes(o, s.actual_end_key());
  w_u64(o, s.range_versions_size());
  for (const auto& r : s.range_versions()) enc_rve(o, r);
  w_u64(o, s.index_reads_size());
  for (const auto& r : s.index_reads()) enc_ive(o, r);
  w_u64(o, s.subscans_size());
  for (const auto& ss : s.subscans()) enc_subscan(o, ss);
  w_u64(o, s.filtered_keys_size());
  for (const auto& k : s.filtered_keys()) w_bytes(o, k);
  w_u64(o, s.filtered_tids_size());
  for (uint64_t t : s.filtered_tids()) w_u64(o, t);
}
// Templated core: streams the flat encoding into any Sink.
template <class Sink>
inline void encode_response_into(const Response& resp, Sink& o) {
  w_u64(o, kMagic);
  w_u8(o, kVersion);
  w_u8(o, resp.ok() ? 1 : 0);
  w_u64(o, resp.results_size());
  for (const auto& s : resp.results()) enc_step(o, s);
  w_u64(o, resp.range_versions_size());
  for (const auto& r : resp.range_versions()) enc_rve(o, r);
  w_u64(o, resp.index_reads_size());
  for (const auto& r : resp.index_reads()) enc_ive(o, r);
  // v2: physical-OCC tx_occ_key (0 in logical/legacy mode).
  w_u64(o, resp.tx_occ_key());
}
// Convenience: encode into a std::string (used for small/error responses).
inline void encode_response(const Response& resp, std::string* out) {
  out->clear();
  encode_response_into(resp, *out);
}
// Exact serialized size, for the uint64 frame header when streaming.
inline uint64_t flat_size(const Response& resp) {
  CountSink c;
  encode_response_into(resp, c);
  return c.n;
}

// ---- decode (mirror of encode; bounds-checked via Reader::ok) ----
inline void dec_rve(Reader& r, RVE* o) {
  r.bytes(o->mutable_table_name());
  r.bytes(o->mutable_index_name());
  o->set_owner_ptr(r.u64());
  o->set_node_ptr(r.u64());
  o->set_version(r.u64());
  r.bytes(o->mutable_start_key());
  r.bytes(o->mutable_end_key());
  o->set_row_limit(r.u64());
  o->set_reverse_scan(r.u8() != 0);
  uint64_t n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_result_keys());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_result_primary_keys());
}
inline void dec_ive(Reader& r, IVE* o) {
  r.bytes(o->mutable_table_name());
  r.bytes(o->mutable_index_name());
  r.bytes(o->mutable_key());
  o->set_tid(r.u64());
  o->set_found(r.u8() != 0);
}
inline void dec_subscan(Reader& r, SubScan* o) {
  r.bytes(o->mutable_start_key());
  r.bytes(o->mutable_end_key());
  uint64_t n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_scan_keys());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_scan_values());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) o->add_scan_tids(r.u64());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_secondary_keys());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) dec_rve(r, o->add_range_versions());
}
inline void dec_step(Reader& r, StepResult* o) {
  o->set_found(r.u8() != 0);
  r.bytes(o->mutable_value());
  o->set_tid(r.u64());
  r.bytes(o->mutable_actual_key());
  uint64_t n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_scan_keys());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_scan_values());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) o->add_scan_tids(r.u64());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_secondary_keys());
  r.bytes(o->mutable_actual_start_key());
  r.bytes(o->mutable_actual_end_key());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) dec_rve(r, o->add_range_versions());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) dec_ive(r, o->add_index_reads());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) dec_subscan(r, o->add_subscans());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) r.bytes(o->add_filtered_keys());
  n = r.u64();
  for (uint64_t i = 0; i < n && r.ok; ++i) o->add_filtered_tids(r.u64());
}
// Returns true on success. On any framing/bounds error returns false and the
// partially-built message should be discarded by the caller.
inline bool decode_response(const char* data, size_t n, Response* resp) {
  Reader r(data, n);
  if (r.u64() != kMagic) return false;
  if (r.u8() != kVersion) return false;
  resp->set_ok(r.u8() != 0);
  uint64_t cnt = r.u64();
  for (uint64_t i = 0; i < cnt && r.ok; ++i) dec_step(r, resp->add_results());
  cnt = r.u64();
  for (uint64_t i = 0; i < cnt && r.ok; ++i) dec_rve(r, resp->add_range_versions());
  cnt = r.u64();
  for (uint64_t i = 0; i < cnt && r.ok; ++i) dec_ive(r, resp->add_index_reads());
  // v2: physical-OCC tx_occ_key (0 in logical/legacy mode).
  resp->set_tx_occ_key(r.u64());
  return r.ok && r.p == r.end;  // exact consumption
}

}  // namespace helios_flat
