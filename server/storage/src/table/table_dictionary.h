// Modified for Helios.

/**
 * @file server/storage/src/table/table_dictionary.h
 * The tables of one database, published by data definition and read
 * without a lock by every request.
 */

#ifndef HELIOS_STORAGE_SRC_TABLE_TABLE_DICTIONARY_H
#define HELIOS_STORAGE_SRC_TABLE_TABLE_DICTIONARY_H

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>

#include "table/table.h"

namespace helios::storage {

/**
 * @brief The tables of one database: published by DDL, read by every request.
 *
 * @details Append-only, since a table is never removed while the database
 * lives. Creation serializes on a mutex and publishes the new node with a
 * release store; a lookup walks the chain with an acquire load and takes no
 * lock of its own. Tables are few and created only by DDL.
 */
class TableDictionary {
 public:
  TableDictionary() = default;
  TableDictionary(const TableDictionary &) = delete;
  TableDictionary &operator=(const TableDictionary &) = delete;

  ~TableDictionary() {
    Node *node = head_.load(std::memory_order_acquire);
    while (node != nullptr) {
      Node *next = node->next;
      delete node;
      node = next;
    }
  }

  /**
   * @brief Publishes a new table.
   *
   * @return false when a table of that name already exists.
   */
  bool CreateTable(std::string_view table_name) {
    std::lock_guard<std::mutex> lk(create_mutex_);
    if (Find(table_name) != nullptr) return false;
    auto *node = new Node(table_name, head_.load(std::memory_order_relaxed));
    head_.store(node, std::memory_order_release);
    return true;
  }

  /**
   * @brief Returns the table of that name, or nullptr.
   */
  Table *GetTable(std::string_view table_name) const {
    Node *node = Find(table_name);
    if (node == nullptr) return nullptr;
    return &node->table;
  }

  void ForEachTable(const std::function<void(Table &)> &f) const {
    for (Node *node = head_.load(std::memory_order_acquire); node != nullptr;
         node = node->next) {
      f(node->table);
    }
  }

 private:
  struct Node {
    std::string name;
    Table table;
    Node *next;

    Node(std::string_view table_name, Node *next)
        : name(table_name), table(table_name), next(next) {}
  };

  Node *Find(std::string_view table_name) const {
    for (Node *node = head_.load(std::memory_order_acquire); node != nullptr;
         node = node->next) {
      if (node->name == table_name) return node;
    }
    return nullptr;
  }

  std::atomic<Node *> head_{nullptr};
  std::mutex create_mutex_;
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_TABLE_TABLE_DICTIONARY_H
