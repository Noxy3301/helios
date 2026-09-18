// Modified for Helios.

/**
 * @file server/storage/src/index/secondary_index.h
 * A secondary index: the secondary key to primary-key-list map, and the
 * uniqueness it was declared with.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H

#include "helios/index.h"

#include "index/masstree_index.h"

namespace helios::storage {
namespace index {

struct SecondaryIndex {
  explicit SecondaryIndex(IndexConstraint index_type = IndexConstraint::kNone)
      : constraint(index_type) {}

  // Tree operations are shared with the primary index; uniqueness is metadata.
  MasstreeIndex tree;
  const IndexConstraint constraint;
};
}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
