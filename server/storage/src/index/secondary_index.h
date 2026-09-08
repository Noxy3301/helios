/**
 * @file server/storage/src/index/secondary_index.h
 * A secondary index: the secondary key to primary-key-list map, and the
 * uniqueness it was declared with.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H

#include <cassert>
#include <string_view>
#include <utility>

#include "index/masstree_index.h"
#include "storage/index.h"

namespace helios::storage {
namespace index {

class SecondaryIndex {
 public:
  explicit SecondaryIndex(IndexConstraint index_type = IndexConstraint::kNone)
      : index_type_(index_type) {}

  DataItem *Get(std::string_view key) { return index_.Get(key); }

  DataItem *GetOrInsert(std::string_view key) {
    auto *item = index_.Get(key);
    if (item == nullptr) {
      index_.PutBlank(key);
      item = index_.Get(key);
      assert(item != nullptr);
    }
    return item;
  }

  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view)> operation) {
    return index_.Scan(begin, end, operation);
  }

  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view)> operation) {
    return index_.ScanReverse(begin, end, operation);
  }

  bool Purge(std::string_view key, DataItem *expected,
             TransactionId retired_tid = {}) {
    return index_.Purge(key, expected, retired_tid);
  }

  void ForEach(std::function<bool(std::string_view, DataItem &)> f) {
    index_.ForEach(f);
  }

  void Put(std::string_view key, DataItem &&value) {
    index_.Put(key, std::move(value));
  }

  bool IsUnique() const { return index_type_ == IndexConstraint::kUnique; }

  IndexConstraint GetIndexType() const { return index_type_; }

 private:
  IndexConstraint index_type_;
  MasstreeIndex index_;
};
}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
