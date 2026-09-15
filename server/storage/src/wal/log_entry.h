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

// Modified for Helios.

/**
 * @file server/storage/src/wal/log_entry.h
 * One entry of the write set a commit hands to the log: the row it wrote,
 * and what it does to the secondary indexes.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_LOG_ENTRY_H
#define HELIOS_STORAGE_SRC_WAL_LOG_ENTRY_H

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "lineairdb/index.h"

#include "index/data_item.h"
#include "util/epoch.h"

namespace helios::storage {
namespace wal {

enum class SecondaryIndexOp : uint8_t {
  kNone = 0,    // a primary row, which names no secondary index
  kInsert = 1,  // one primary key joins the key's list
  kDelete = 2,  // one primary key leaves it
  kFull = 3,    // the whole list, as a checkpoint carries it
};

/**
 * @brief One entry of a write set: the row this transaction wrote, and the
 *        index it belongs to.
 *
 * @details An empty `index_name` marks a primary row; otherwise the entry
 * belongs to that secondary index and `secondary_index_deltas` says what it
 * does to it. The value and primary-key list are owned. `item` is the live
 * slot the commit path resolved, borrowed until it publishes its TIDs, and null
 * on the recovery path, which has no slots yet.
 */
struct LogEntry {
  std::string key;
  std::string value;
  Tidword tid{};
  std::vector<std::string> primary_keys;
  DataItem *item;
  std::string table_name;
  std::string index_name;
  IndexConstraint index_type;
  struct SecondaryIndexDelta {
    std::string primary_key;
    SecondaryIndexOp op;
  };
  std::vector<SecondaryIndexDelta> secondary_index_deltas;

  LogEntry(const std::string_view key, const std::byte row[], const size_t len,
           DataItem *const item, std::string_view table_name,
           std::string_view index_name, const Tidword tid = {},
           IndexConstraint index_type = IndexConstraint::kNone)
      : key(key),
        tid(tid),
        item(item),
        table_name(table_name),
        index_name(index_name),
        index_type(index_type) {
    if (row != nullptr) value.assign(reinterpret_cast<const char *>(row), len);
  }
  LogEntry(const LogEntry &) = default;
  LogEntry &operator=(const LogEntry &) = default;
  // Declaring copy ctor/assign above suppresses implicit move generation;
  // an emplace_back(std::move(entry)) would otherwise fall back to the
  // deep copy path.
  LogEntry(LogEntry &&) = default;
  LogEntry &operator=(LogEntry &&) = default;

  // A later call for the same primary key replaces op; Insert and Delete do
  // not compose.
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

using WriteSet = std::vector<LogEntry>;

}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_LOG_ENTRY_H
