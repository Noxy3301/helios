#ifndef HELIOS_STORAGE_SRC_WAL_CRC32C_INTERNAL_H
#define HELIOS_STORAGE_SRC_WAL_CRC32C_INTERNAL_H

#include <cstddef>
#include <cstdint>

// The implementations behind Crc32c, for crc32c.cc and its test. `state` is
// the running CRC before the final xor; `data` may be null only when `size`
// is zero.
namespace helios::storage {
namespace wal {
namespace internal {

uint32_t UpdateWithTable(uint32_t state, const void *data, size_t size);

// False off x86 and on a CPU without SSE4.2.
bool HasSse42();

#if defined(__x86_64__)
// Callable only when HasSse42() is true.
uint32_t UpdateWithSse42(uint32_t state, const void *data, size_t size);
#endif

}  // namespace internal
}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_CRC32C_INTERNAL_H
