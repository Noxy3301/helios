// Modified for Helios.

/**
 * @file server/storage/src/table/table.cc
 * Table construction, and the definition changes and lookups the table lock
 * covers.
 */

#include "table/table.h"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>

#include "index/masstree_index.h"

namespace helios::storage {
Table::Table(std::string_view table_name) : table_name_(table_name) {}

bool Table::CreateSecondaryIndex(const std::string_view index_name,
                                 const IndexConstraint index_type) {
  if (GetSecondaryIndex(index_name) != nullptr) return false;
  return GetOrCreateSecondaryIndex(index_name, index_type) != nullptr;
}

bool Table::InstallPaxSchema(pax::TableSchema schema) {
  std::unique_lock<std::shared_mutex> lk(table_lock_);
  if (pax_table_ != nullptr) return false;
  pax_table_ = std::make_unique<pax::PaxTable>(std::move(schema));
  primary_index_.SetPaxTable(pax_table_.get());
  return true;
}

pax::PaxTable *Table::GetPaxTable() const {
  std::shared_lock<std::shared_mutex> lk(table_lock_);
  return pax_table_.get();
}

// The lock covers the lookup only: an index is never removed, so the pointer
// stays valid after it is released.
index::SecondaryIndex *Table::GetSecondaryIndex(
    const std::string_view index_name) {
  std::shared_lock<std::shared_mutex> lk(table_lock_);
  auto it = secondary_indices_.find(std::string(index_name));
  if (it == secondary_indices_.end()) {
    return nullptr;
  }
  return it->second.get();
}

index::SecondaryIndex *Table::GetOrCreateSecondaryIndex(
    const std::string_view index_name, const IndexConstraint index_type) {
  const std::string name(index_name);
  std::unique_lock<std::shared_mutex> lk(table_lock_);
  auto it = secondary_indices_.find(name);
  if (it != secondary_indices_.end()) {
    const bool same = it->second->constraint == index_type;
    return same ? it->second.get() : nullptr;
  }
  auto new_index = std::make_unique<index::SecondaryIndex>(index_type);
  auto *created = new_index.get();
  secondary_indices_[name] = std::move(new_index);
  return created;
}

const std::string &Table::Name() const { return table_name_; }
index::MasstreeIndex &Table::GetPrimaryIndex() { return primary_index_; }

}  // namespace helios::storage
