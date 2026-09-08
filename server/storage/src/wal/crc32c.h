/**
 * @file server/storage/src/wal/crc32c.h
 * The checksum every log frame carries.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_CRC32C_H
#define HELIOS_STORAGE_SRC_WAL_CRC32C_H

#include <cstddef>
#include <cstdint>

namespace helios::storage {
namespace wal {

/**
 * @brief CRC-32C (Castagnoli): reflected polynomial 0x82F63B78, initial
 * value 0xFFFFFFFF, final xor 0xFFFFFFFF.
 * @details The checksum the WAL frames and the checkpoint images carry. Bytes
 * are fed incrementally; Finish() may be read more than once. The check value
 * for "123456789" is 0xE3069283.
 * @note The polynomial is pinned because "crc32" names two different
 * algorithms: zlib's crc32() computes IEEE CRC-32 and will not validate
 * these frames.
 */
class Crc32c {
 public:
  /**
   * @brief Extends the checksum so that successive calls equal one call on
   *        the concatenation.
   *
   * @param data Null only when size is 0.
   */
  void Update(const void *data, size_t size);
  uint32_t Finish() const { return state_ ^ 0xffffffffu; }

 private:
  uint32_t state_{0xffffffffu};
};

}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_CRC32C_H
