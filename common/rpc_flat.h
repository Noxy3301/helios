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
 */
enum class FlatOp : uint8_t {
  kRead = 1,
  kBatchRead = 2,
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

}  // namespace helios::rpc

#endif  // HELIOS_COMMON_RPC_FLAT_H
