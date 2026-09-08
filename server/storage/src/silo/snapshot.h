/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/**
 * @file server/storage/src/silo/snapshot.h
 * One entry of a transaction's write set, and what it does to the
 * secondary indexes.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_SNAPSHOT_H
#define HELIOS_STORAGE_SRC_SILO_SNAPSHOT_H

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "index/data_item.h"
#include "storage/index.h"
#include "util/epoch.h"

namespace helios::storage {

enum class SecondaryIndexOp : uint8_t {
  kNone = 0,    // a primary row, which names no secondary index
  kAdd = 1,     // one primary key joins the key's list
  kRemove = 2,  // one primary key leaves it
  kFull = 3,    // the whole list, as a checkpoint image carries it
};

/**
 * @brief One entry of a write set: the row this transaction wrote, and the
 *        index it belongs to.
 *
 * @details An empty `index_name` marks a primary row; otherwise the entry
 * belongs to that secondary index and `secondary_index_deltas` says what it
 * does to it. `data_item_copy` is owned. `item` is the live slot the commit
 * path resolved, borrowed until the commit publishes its TIDs, and null on
 * the recovery path, which has no slots yet.
 */
struct Snapshot {
  std::string key;
  DataItem data_item_copy;
  DataItem *item;
  std::string table_name;
  std::string index_name;
  IndexConstraint index_type;
  struct SecondaryIndexDelta {
    std::string primary_key;
    SecondaryIndexOp op;
  };
  std::vector<SecondaryIndexDelta> secondary_index_deltas;

  Snapshot(const std::string_view key, const std::byte row[], const size_t len,
           DataItem *const item, std::string_view table_name,
           std::string_view index_name, const TransactionId tid = {},
           IndexConstraint index_type = IndexConstraint::kNone)
      : key(key),
        item(item),
        table_name(table_name),
        index_name(index_name),
        index_type(index_type) {
    if (row != nullptr) data_item_copy.Reset(row, len, tid);
  }
  Snapshot(const Snapshot &) = default;
  Snapshot &operator=(const Snapshot &) = default;
  // Declaring copy ctor/assign above suppresses implicit move generation;
  // an emplace_back(std::move(snapshot)) would otherwise fall back to the
  // deep copy path.
  Snapshot(Snapshot &&) = default;
  Snapshot &operator=(Snapshot &&) = default;

  // A later call for the same primary key replaces op; Add and Remove do not
  // compose.
  void RecordSecondaryDelta(const std::string_view primary_key,
                            SecondaryIndexOp op) {
    for (auto &delta : secondary_index_deltas) {
      if (delta.primary_key == primary_key) {
        delta.op = op;
        return;
      }
    }
    secondary_index_deltas.push_back({std::string(primary_key), op});
  }
};

using WriteSetType = std::vector<Snapshot>;

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_SNAPSHOT_H
