// Modified for Helios.

/**
 * @file server/storage/src/table/table.h
 * One table: its primary index, its secondary indexes, and the lock that
 * keeps a definition change apart from the reads.
 */

#ifndef HELIOS_STORAGE_SRC_TABLE_TABLE_H
#define HELIOS_STORAGE_SRC_TABLE_TABLE_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "lineairdb/pax.h"

#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "pax/table.h"

namespace helios::storage {

/**
 * @brief One table: its primary index, its named secondary indexes, and its
 *        optional PAX store.
 *
 * @details table_lock_ covers the secondary-index map and the PAX install
 * only; the primary index is reached without it. A secondary index is never
 * removed, so a pointer to one stays valid for the table's lifetime.
 */
class Table {
 public:
  explicit Table(std::string_view table_name);

  /**
   * @brief Declares a secondary index on this table.
   *
   * @return false when an index of that name already exists.
   */
  bool CreateSecondaryIndex(const std::string_view index_name,
                            const IndexConstraint index_type);

  /**
   * @brief Installs PAX storage metadata for rows created after the call.
   *
   * @details The schema records per-field maximum cell widths and must be
   * installed before values are written. A table accepts only one PAX schema.
   *
   * @return true when the schema is installed for this table.
   * @return false when a schema has already been installed.
   */
  bool InstallPaxSchema(pax::TableSchema schema);

  /**
   * @brief Returns the table's PAX table, or nullptr when no PAX schema has
   * been installed on it.
   */
  pax::PaxTable *GetPaxTable() const;

  const std::string &Name() const;

  index::MasstreeIndex &GetPrimaryIndex();

  /**
   * @brief Returns the index of that name, or nullptr.
   */
  index::SecondaryIndex *GetSecondaryIndex(const std::string_view index_name);

  /**
   * @brief Calls `f(name, index)` for each secondary index of this table.
   *
   * @details Runs under the table lock, so `f` must not call back into a
   * method that changes the table's definition.
   */
  template <typename Func>
  void ForEachSecondaryIndex(Func &&f) {
    std::shared_lock<std::shared_mutex> lk(table_lock_);
    for (auto &[index_name, index_ptr] : secondary_indices_) {
      f(index_name, *index_ptr);
    }
  }

  /**
   * @brief Returns the index of that name, creating it when there is none.
   *
   * @return The index, or nullptr when an index of that name is declared with
   * a different constraint.
   */
  index::SecondaryIndex *GetOrCreateSecondaryIndex(
      const std::string_view index_name, const IndexConstraint index_type);

 private:
  index::MasstreeIndex primary_index_;
  std::unique_ptr<pax::PaxTable> pax_table_;
  mutable std::shared_mutex table_lock_;
  std::unordered_map<std::string, std::unique_ptr<index::SecondaryIndex>>
      secondary_indices_;
  std::string table_name_;
};
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_TABLE_TABLE_H
