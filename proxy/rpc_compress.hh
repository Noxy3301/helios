// LZ4 wire compression for the flat TX_EXECUTE_READ_PLAN response.
//
// The response payload begins with a 1-byte CODEC tag:
//   0x00 (RAW)  : the rest is the flat buffer verbatim (helios_flat).
//   0x01 (LZ4)  : [raw_total:u64-LE] then chunk records, each
//                 [raw_len:u32-LE][enc_len:u32-LE][enc_len bytes].
//                 Chunks decompress back to exactly raw_total bytes (the flat
//                 buffer), which is then fed to decode_flat_into_readplan.
// LOSSLESS: results/OCC unchanged; on any decompress/size mismatch the proxy
// fails the RPC (never decodes partial data). LZ4 is loaded at runtime via
// dlopen("liblz4.so.1")+dlsym (no dev headers / link changes). If LZ4 is
// unavailable the server falls back to RAW (codec 0).
#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <dlfcn.h>

namespace helios_zip {

enum Codec : uint8_t { kRaw = 0, kLZ4 = 1 };
static constexpr uint32_t kChunkRaw = 1u << 20;  // 1MB uncompressed per chunk

// ---- runtime LZ4 binding (dlsym; nullptr if unavailable) ----
struct Lz4Fns {
  int (*compress_default)(const char*, char*, int, int) = nullptr;
  int (*decompress_safe)(const char*, char*, int, int) = nullptr;
  int (*compress_bound)(int) = nullptr;
};
inline const Lz4Fns& lz4() {
  static Lz4Fns fns = [] {
    Lz4Fns f;
    void* h = dlopen("liblz4.so.1", RTLD_NOW | RTLD_GLOBAL);
    if (h == nullptr) h = dlopen("liblz4.so", RTLD_NOW | RTLD_GLOBAL);
    if (h != nullptr) {
      f.compress_default =
          reinterpret_cast<int (*)(const char*, char*, int, int)>(
              dlsym(h, "LZ4_compress_default"));
      f.decompress_safe =
          reinterpret_cast<int (*)(const char*, char*, int, int)>(
              dlsym(h, "LZ4_decompress_safe"));
      f.compress_bound =
          reinterpret_cast<int (*)(int)>(dlsym(h, "LZ4_compressBound"));
    }
    return f;
  }();
  return fns;
}
inline bool lz4_available() {
  const auto& f = lz4();
  return f.compress_default && f.decompress_safe && f.compress_bound;
}

inline void put_u32(std::string& o, uint32_t v) {
  char b[4]; std::memcpy(b, &v, 4); o.append(b, 4);
}
inline void put_u64(std::string& o, uint64_t v) {
  char b[8]; std::memcpy(b, &v, 8); o.append(b, 8);
}

// Sink (codec Sink concept: append(const char*, size_t)) that LZ4-compresses the
// flat stream in <=1MB chunks into out_. Holds only the COMPRESSED bytes (plus a
// 1MB staging buffer), never the full flat buffer. raw_total_ counts input.
struct CompressSink {
  std::string out_;        // chunk records (no header)
  uint64_t raw_total_ = 0;
  bool failed = false;
  std::string staging_;
  CompressSink() { staging_.reserve(kChunkRaw); }
  void flush_chunk() {
    if (staging_.empty()) return;
    const int rl = static_cast<int>(staging_.size());
    const int bound = lz4().compress_bound(rl);
    if (bound <= 0) { failed = true; staging_.clear(); return; }
    std::string enc;
    enc.resize(static_cast<size_t>(bound));
    const int el = lz4().compress_default(staging_.data(), &enc[0], rl, bound);
    if (el <= 0) { failed = true; staging_.clear(); return; }
    put_u32(out_, static_cast<uint32_t>(rl));
    put_u32(out_, static_cast<uint32_t>(el));
    out_.append(enc.data(), static_cast<size_t>(el));
    raw_total_ += static_cast<uint64_t>(rl);
    staging_.clear();
  }
  void append(const char* p, size_t n) {
    if (failed) return;
    size_t off = 0;
    while (off < n) {
      const size_t take = std::min(n - off, kChunkRaw - staging_.size());
      staging_.append(p + off, take);
      off += take;
      if (staging_.size() >= kChunkRaw) flush_chunk();
    }
  }
  void finish() { if (!failed) flush_chunk(); }
};

// Decompress a CODEC-tagged payload into `flat`. Returns true on success
// (flat == the original flat buffer). On any inconsistency returns false.
inline bool decompress_payload(const char* data, size_t n, std::string& flat) {
  flat.clear();
  if (n < 1) return false;
  const uint8_t codec = static_cast<uint8_t>(data[0]);
  const char* p = data + 1;
  const char* end = data + n;
  if (codec == kRaw) {
    flat.assign(p, static_cast<size_t>(end - p));
    return true;
  }
  if (codec != kLZ4) return false;
  if (!lz4_available()) return false;
  // Use distance-only checks (never form an out-of-range pointer) and cap every
  // allocation before reserve/resize, so a corrupt/truncated frame returns false
  // rather than risking UB or a throwing allocation.
  auto rem = [&]() -> size_t { return static_cast<size_t>(end - p); };
  if (rem() < 8) return false;
  uint64_t raw_total = 0; std::memcpy(&raw_total, p, 8); p += 8;
  if (raw_total > flat.max_size()) return false;
  flat.reserve(static_cast<size_t>(raw_total));
  while (p < end) {
    if (rem() < 8) return false;
    uint32_t rl = 0, el = 0;
    std::memcpy(&rl, p, 4); std::memcpy(&el, p + 4, 4); p += 8;
    if (rl == 0 || rl > kChunkRaw) return false;
    if (el > rem()) return false;
    // Reconstructed size must not exceed the declared total.
    if (rl > raw_total - flat.size()) return false;
    const size_t base = flat.size();
    flat.resize(base + rl);
    const int got = lz4().decompress_safe(p, &flat[base], static_cast<int>(el),
                                          static_cast<int>(rl));
    if (got != static_cast<int>(rl)) return false;  // corrupt / size mismatch
    p += el;
  }
  return flat.size() == raw_total;  // exact reconstruction
}

}  // namespace helios_zip
