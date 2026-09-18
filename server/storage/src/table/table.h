// Modified for Helios.

/**
 * @file server/storage/src/table/table.h
 * One table: its primary index, its secondary indexes and its PAX store,
 * published for lock-free lookup.
 */

#ifndef HELIOS_STORAGE_SRC_TABLE_TABLE_H
#define HELIOS_STORAGE_SRC_TABLE_TABLE_H

#include <atomic>
#include <string>
#include <vector>

#include "helios/pax.h"

#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "pax/catalog.h"
#include "pax/table.h"

namespace helios::storage {

/**
 * @brief One table: its primary index, its named secondary indexes, and its
 *        optional PAX store.
 *
 * @details A definition change publishes with a release store and a lookup
 * takes no lock; the caller serializes definition changes (Database's DDL
 * mutex, or recovery on one thread). An index is never removed, and a
 * pointer to one stays valid for the table's lifetime.
 */
class Table {
 public:
  explicit Table(std::string_view table_name);
  ~Table();

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
   */
  template <typename Func>
  void ForEachSecondaryIndex(Func &&f) {
    for (IndexNode *node = indexes_.load(std::memory_order_acquire);
         node != nullptr; node = node->next) {
      f(node->name, node->index);
    }
  }

  /**
   * @brief Returns the name and constraint of every secondary index, as the
   * catalog stores them.
   */
  std::vector<pax::IndexDefinition> IndexDefinitions();

  /**
   * @brief Returns the index of that name, creating it when there is none.
   *
   * @return The index, or nullptr when an index of that name is declared with
   * a different constraint.
   */
  index::SecondaryIndex *GetOrCreateSecondaryIndex(
      const std::string_view index_name, const IndexConstraint index_type);

 private:
  struct IndexNode {
    std::string name;
    index::SecondaryIndex index;
    IndexNode *next;

    IndexNode(std::string_view index_name, IndexConstraint index_type,
              IndexNode *next)
        : name(index_name), index(index_type), next(next) {}
  };

  index::MasstreeIndex primary_index_;
  // Set once by InstallPaxSchema; the destructor deletes it.
  std::atomic<pax::PaxTable *> pax_table_{nullptr};
  // Append-only chain, newest first.
  std::atomic<IndexNode *> indexes_{nullptr};
  std::string table_name_;

  IndexNode *FindIndex(std::string_view index_name) const;
};
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_TABLE_TABLE_H
