// Modified for Helios.

/**
 * @file server/storage/src/index/secondary_index.h
 * A secondary index: the secondary key to primary-key-list map, and the
 * uniqueness it was declared with.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "helios/index.h"

#include "index/masstree_index.h"

namespace helios::storage {
namespace index {

struct SecondaryIndex {
  explicit SecondaryIndex(IndexConstraint index_type = IndexConstraint::kNone,
                          std::vector<uint32_t> slices = {})
      : tree(index_type == IndexConstraint::kUnique ? RecordType::kUnique
                                                    : RecordType::kNonUnique),
        constraint(index_type),
        pk_slices(std::move(slices)) {}

  // Tree operations are shared with the primary index; the constraint picks
  // the record type.
  MasstreeIndex tree;
  const IndexConstraint constraint;
  // (offset, length) of each primary-key part in a non-unique record's key,
  // in primary-key order, when the secondary key holds primary-key columns.
  // Empty when the key ends with the whole primary key.
  const std::vector<uint32_t> pk_slices;
};

/**
 * @brief Builds the key of a non-unique index record.
 *
 * @return The secondary key followed by the primary-key parts that no slice
 * of `index` finds inside it, in primary-key order; without slices, followed
 * by the whole primary key.
 */
inline std::string join_entry(const SecondaryIndex &index,
                              std::string_view secondary_key,
                              std::string_view primary_key) {
  std::string key(secondary_key);
  if (index.pk_slices.empty()) return key.append(primary_key);
  size_t off = 0;
  for (size_t i = 0; i < index.pk_slices.size(); i += 2) {
    if (index.pk_slices[i] >= secondary_key.size())
      key.append(primary_key.substr(off, index.pk_slices[i + 1]));
    off += index.pk_slices[i + 1];
  }
  return key;
}

/**
 * @brief Splits a record of `index` into its secondary key and the primary
 *        key it points at.
 *
 * @details A non-unique index record's key adds pk_len bytes past its
 * secondary key: the primary key, or with pk_slices the primary-key parts the
 * secondary key lacks. A UNIQUE record's key is the secondary key alone. The
 * caller reads the record under one live version of its TID word.
 *
 * @param index  The index holding the record.
 * @param key    The record's index key.
 * @param record The record.
 * @return The secondary key, as a view into `key`, and the primary key.
 */
inline std::pair<std::string_view, std::string> split_entry(
    const SecondaryIndex &index, std::string_view key,
    const IndexRecord &record) {
  if (index.constraint == IndexConstraint::kUnique) {
    return {key, std::string(
                     static_cast<const UniqueRecord &>(record).primary_key())};
  }
  const size_t pk_len = static_cast<const NonUniqueRecord &>(record).pk_len();
  const std::string_view secondary_key = key.substr(0, key.size() - pk_len);
  if (index.pk_slices.empty())
    return {secondary_key, std::string(key.substr(secondary_key.size()))};
  std::string primary_key;
  for (size_t i = 0; i < index.pk_slices.size(); i += 2)
    primary_key.append(key.substr(index.pk_slices[i], index.pk_slices[i + 1]));
  return {secondary_key, std::move(primary_key)};
}
}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
