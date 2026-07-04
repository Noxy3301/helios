#include "flat_plan_encode.hh"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace flat_plan {
namespace {
// Native-endian bytes spell "LDBFLATP" (LineairDB flat payload).
constexpr uint64_t kMagic = 0x5054414C4642444Cull;
constexpr uint8_t kVersion = 2;

template <class Sink>
void w_u8(Sink& out, uint8_t v) {
    const char c = static_cast<char>(v);
    out.append(&c, 1);
}

template <class Sink>
void w_u64(Sink& out, uint64_t v) {
    char bytes[8];
    std::memcpy(bytes, &v, 8);
    out.append(bytes, 8);
}

template <class Sink>
void w_bytes(Sink& out, const std::string& s) {
    w_u64(out, static_cast<uint64_t>(s.size()));
    out.append(s.data(), s.size());
}

struct CountSink {
    uint64_t n = 0;
    void append(const char*, size_t k) { n += k; }
};

template <class Sink>
void encode_step(
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
}  // namespace

void encode_to_string(LineairDB::Protocol::TxExecuteReadPlan::Response& r,
                      std::string& out) {
    CountSink count;
    count.n = 8 + 1 + 1 + 8;  // magic + version + ok + result count
    for (const auto& s : r.results()) encode_step(s, count);

    out.clear();
    out.reserve(count.n);
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
