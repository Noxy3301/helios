// Modified for Helios.

/**
 * @file server/storage/src/table/table.h
 * One table: its primary index, its secondary indexes and its PAX store,
 * published for lock-free lookup.
 */

#ifndef HELIOS_STORAGE_SRC_TABLE_TABLE_H
#define HELIOS_STORAGE_SRC_TABLE_TABLE_H

#include <atomic>
#include <cstdint>
#include <memory>
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
 * mutex, or recovery on one thread). A lookup by name enters the calling
 * thread's Masstree epoch, and an unlinked index stays allocated until every
 * thread that found it has released that epoch.
 */
class Table {
 public:
  // One secondary index and its link in the table's chain.
  struct IndexNode {
    std::string name;
    index::SecondaryIndex index;
    std::atomic<IndexNode *> next;

    IndexNode(std::string_view index_name, IndexConstraint index_type,
              uint32_t index_id, std::vector<uint32_t> pk_slices,
              IndexNode *next)
        : name(index_name),
          index(index_type, index_id, std::move(pk_slices)),
          next(next) {}
  };

  explicit Table(std::string_view table_name);
  ~Table();

  /**
   * @brief Declares a secondary index on this table.
   *
   * @param index_id The id the catalog assigned to the index.
   * @param pk_slices SecondaryIndex::pk_slices.
   * @return false when an index of that name already exists.
   */
  bool CreateSecondaryIndex(const std::string_view index_name,
                            const IndexConstraint index_type, uint32_t index_id,
                            std::vector<uint32_t> pk_slices = {});

  /**
   * @brief Removes the index of that name from lookups.
   *
   * @details A thread that found the index earlier may use it until it
   * releases its Masstree epoch, so the caller frees the node only after
   * index::synchronize_rcu().
   * @return The unlinked node, or nullptr when the table holds no such index.
   */
  std::unique_ptr<IndexNode> UnlinkSecondaryIndex(std::string_view index_name);

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
   * @details The caller serializes with definition changes or is inside its
   * Masstree epoch, or an index dropped meanwhile may be freed under `f`.
   */
  template <typename Func>
  void ForEachSecondaryIndex(Func &&f) {
    for (IndexNode *node = indexes_.load(std::memory_order_acquire);
         node != nullptr; node = node->next.load(std::memory_order_acquire)) {
      f(node->name, node->index);
    }
  }

  /**
   * @brief Returns the name, constraint, id and primary-key slices of every
   * secondary index, as the catalog stores them.
   */
  std::vector<pax::IndexDefinition> IndexDefinitions();

 private:
  index::MasstreeIndex primary_index_;
  // Set once by InstallPaxSchema; the destructor deletes it.
  std::atomic<pax::PaxTable *> pax_table_{nullptr};
  // Chain of the live indexes, newest first.
  std::atomic<IndexNode *> indexes_{nullptr};
  std::string table_name_;

  IndexNode *FindIndex(std::string_view index_name) const;
};
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_TABLE_TABLE_H
