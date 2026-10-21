// Modified for Helios.

/**
 * @file server/storage/src/index/secondary_index.h
 * A secondary index: the secondary key to primary-key-list map, and the
 * uniqueness it was declared with.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H

#include <string_view>
#include <utility>

#include "helios/index.h"

#include "index/masstree_index.h"

namespace helios::storage {
namespace index {

struct SecondaryIndex {
  explicit SecondaryIndex(IndexConstraint index_type = IndexConstraint::kNone)
      : tree(index_type == IndexConstraint::kUnique ? RecordType::kUnique
                                                    : RecordType::kNonUnique),
        constraint(index_type) {}

  // Tree operations are shared with the primary index; the constraint picks
  // the record type.
  MasstreeIndex tree;
  const IndexConstraint constraint;
};

/**
 * @brief Splits a record of `index` into its secondary key and the primary
 *        key it points at.
 *
 * @details A non-unique index record's key ends with its primary key; a
 * UNIQUE record's key is the secondary key alone. The caller reads the record
 * under one live version of its TID word.
 *
 * @param index  The index holding the record.
 * @param key    The record's index key.
 * @param record The record.
 * @return The secondary key and the primary key, as views into `key` or into
 * the UNIQUE record's block.
 */
inline std::pair<std::string_view, std::string_view> split_entry(
    const SecondaryIndex &index, std::string_view key,
    const IndexRecord &record) {
  if (index.constraint == IndexConstraint::kUnique) {
    return {key, static_cast<const UniqueRecord &>(record).primary_key()};
  }
  const size_t pk_len = static_cast<const NonUniqueRecord &>(record).pk_len();
  return {key.substr(0, key.size() - pk_len), key.substr(key.size() - pk_len)};
}
}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
