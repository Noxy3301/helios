/**
 * @file server/storage/src/pax/catalog.h
 * PAX column and secondary index definitions saved alongside the WAL for
 * recovery.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_CATALOG_H
#define HELIOS_STORAGE_SRC_PAX_CATALOG_H

#include <map>
#include <string>
#include <vector>

#include "lineairdb/index.h"
#include "lineairdb/pax.h"

namespace helios::storage::pax {

// One secondary index, by the name and constraint it was declared with.
struct IndexDefinition {
  std::string name;
  IndexConstraint constraint = IndexConstraint::kNone;
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
bool StoreCatalog(const std::string &work_dir, const CatalogEntries &entries);

}  // namespace helios::storage::pax

#endif  // HELIOS_STORAGE_SRC_PAX_CATALOG_H
