/**
 * @file server/storage/src/pax/catalog.h
 * PAX column and secondary index definitions saved alongside the WAL for
 * recovery.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_CATALOG_H
#define HELIOS_STORAGE_SRC_PAX_CATALOG_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "helios/index.h"
#include "helios/pax.h"

namespace helios::storage::pax {

// One secondary index, by the name, constraint and primary-key slices it was
// declared with and the id the catalog assigned it.
struct IndexDefinition {
  std::string name;
  IndexConstraint constraint = IndexConstraint::kNone;
  uint32_t id = 0;
  std::vector<uint32_t> pk_slices;
};

// Everything one table declares: its row format and its secondary indexes.
struct TableDefinition {
  TableSchema schema;
  std::vector<IndexDefinition> indexes;
};

using CatalogEntries = std::map<std::string, TableDefinition>;

struct Catalog {
  enum class Status { kOk, kAbsent, kUnusable };
  Status status = Status::kAbsent;
  CatalogEntries entries;
  // The id the next index declaration takes; no earlier index ever had it.
  uint32_t next_index_id = 1;
  std::string detail;
};

/**
 * @brief Loads PAX definitions and secondary index definitions before
 * recovery installs any values.
 * @return kAbsent for a missing file, kUnusable for a read or format error.
 */
Catalog LoadCatalog(const std::string &work_dir);

/**
 * @brief Saves all PAX definitions and secondary index definitions before a
 * new schema becomes writable.
 * @details Writes and syncs a temporary file, renames it, then syncs the
 * directory. An interrupted temporary file is ignored by LoadCatalog.
 * @return False on an I/O failure. If the directory sync fails, the renamed
 * file may already be visible, but its durability is not confirmed.
 */
bool StoreCatalog(const std::string &work_dir, const CatalogEntries &entries,
                  uint32_t next_index_id);

}  // namespace helios::storage::pax

#endif  // HELIOS_STORAGE_SRC_PAX_CATALOG_H
