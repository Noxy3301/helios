/**
 * @file server/storage/src/wal/crc32c.cc
 * The checksum every log frame carries: hardware where the CPU has it,
 * a table otherwise.
 */

#include "wal/crc32c.h"

#include <array>
#include <cstring>
#include <string_view>

#include "wal/crc32c_internal.h"

#if defined(__x86_64__)
#include <nmmintrin.h>
#endif

namespace helios::storage {
namespace wal {

namespace {

constexpr uint32_t kReflectedPolynomial = 0x82f63b78u;

constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t entry = i;
    for (int bit = 0; bit < 8; ++bit) {
      entry = (entry >> 1) ^ ((entry & 1u) ? kReflectedPolynomial : 0u);
    }
    table[i] = entry;
  }
  return table;
}

constexpr std::array<uint32_t, 256> kTable = MakeTable();

// The table loop over a literal, so the table is checked when it is built.
constexpr uint32_t Crc32cOf(std::string_view text) {
  uint32_t state = 0xffffffffu;
  for (const char c : text) {
    state = kTable[(state ^ static_cast<uint8_t>(c)) & 0xffu] ^ (state >> 8);
  }
  return state ^ 0xffffffffu;
}

static_assert(Crc32cOf("123456789") == 0xe3069283u,
              "the table does not compute CRC-32C");

}  // namespace

namespace internal {

uint32_t UpdateWithTable(uint32_t state, const void *data, size_t size) {
  const auto *bytes = static_cast<const uint8_t *>(data);
  for (size_t i = 0; i < size; ++i) {
    state = kTable[(state ^ bytes[i]) & 0xffu] ^ (state >> 8);
  }
  return state;
}

bool HasSse42() {
#if defined(__x86_64__)
  static const bool has_sse42 = __builtin_cpu_supports("sse4.2");
  return has_sse42;
#else
  return false;
#endif
}

#if defined(__x86_64__)
// Compiled via the target attribute, not -msse4.2/-march=native, so it builds
// in every configuration; HasSse42() is what keeps it off CPUs without it.
__attribute__((target("sse4.2"))) uint32_t UpdateWithSse42(uint32_t state,
                                                           const void *data,
                                                           size_t size) {
  const auto *bytes = static_cast<const uint8_t *>(data);
  uint64_t crc = state;
  while (size >= sizeof(uint64_t)) {
    uint64_t chunk;
    std::memcpy(&chunk, bytes, sizeof(chunk));
    crc = _mm_crc32_u64(crc, chunk);
    bytes += sizeof(chunk);
    size -= sizeof(chunk);
  }
  while (size > 0) {
    crc = _mm_crc32_u8(static_cast<uint32_t>(crc), *bytes);
    ++bytes;
    --size;
  }
  return static_cast<uint32_t>(crc);
}
#endif

}  // namespace internal

void Crc32c::Update(const void *data, size_t size) {
#if defined(__x86_64__)
  if (internal::HasSse42()) {
    state_ = internal::UpdateWithSse42(state_, data, size);
    return;
  }
#endif
  state_ = internal::UpdateWithTable(state_, data, size);
}

}  // namespace wal
}  // namespace helios::storage
