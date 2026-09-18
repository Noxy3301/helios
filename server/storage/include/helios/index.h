// Modified for Helios.

/**
 * @file server/storage/include/helios/index.h
 * What a secondary index promises about its keys.
 */

#ifndef HELIOS_STORAGE_INCLUDE_HELIOS_INDEX_H
#define HELIOS_STORAGE_INCLUDE_HELIOS_INDEX_H

#include <cstdint>

namespace helios::storage {

/**
 * @brief What a secondary index promises about its keys.
 *
 * @details The values cross the wire, so they are fixed. A value that is
 * neither of these is refused rather than read as one of them.
 */
enum class IndexConstraint : uint32_t {
  kNone = 0,
  kUnique = 1,
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_HELIOS_INDEX_H
