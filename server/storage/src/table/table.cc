// Modified for Helios.

/**
 * @file server/storage/src/table/table.cc
 * Table construction, definition changes and the lock-free lookups.
 */

#include "table/table.h"

#include <string>
#include <string_view>
#include <utility>

#include "index/masstree_index.h"

namespace helios::storage {
Table::Table(std::string_view table_name) : table_name_(table_name) {}

Table::~Table() {
  delete pax_table_.load(std::memory_order_acquire);
  IndexNode *node = indexes_.load(std::memory_order_acquire);
  while (node != nullptr) {
    IndexNode *next = node->next;
    delete node;
    node = next;
  }
}

bool Table::CreateSecondaryIndex(const std::string_view index_name,
                                 const IndexConstraint index_type) {
  if (GetSecondaryIndex(index_name) != nullptr) return false;
  return GetOrCreateSecondaryIndex(index_name, index_type) != nullptr;
}

bool Table::InstallPaxSchema(pax::TableSchema schema) {
  if (pax_table_.load(std::memory_order_relaxed) != nullptr) return false;
  pax_table_.store(new pax::PaxTable(std::move(schema)),
                   std::memory_order_release);
  return true;
}

pax::PaxTable *Table::GetPaxTable() const {
  return pax_table_.load(std::memory_order_acquire);
}

Table::IndexNode *Table::FindIndex(const std::string_view index_name) const {
  for (IndexNode *node = indexes_.load(std::memory_order_acquire);
       node != nullptr; node = node->next) {
    if (node->name == index_name) return node;
  }
  return nullptr;
}

index::SecondaryIndex *Table::GetSecondaryIndex(
    const std::string_view index_name) {
  IndexNode *node = FindIndex(index_name);
  return node == nullptr ? nullptr : &node->index;
}

index::SecondaryIndex *Table::GetOrCreateSecondaryIndex(
    const std::string_view index_name, const IndexConstraint index_type) {
  if (IndexNode *node = FindIndex(index_name)) {
    return node->index.constraint == index_type ? &node->index : nullptr;
  }
  auto *node = new IndexNode(index_name, index_type,
                             indexes_.load(std::memory_order_relaxed));
  indexes_.store(node, std::memory_order_release);
  return &node->index;
}

const std::string &Table::Name() const { return table_name_; }
index::MasstreeIndex &Table::GetPrimaryIndex() { return primary_index_; }

}  // namespace helios::storage
