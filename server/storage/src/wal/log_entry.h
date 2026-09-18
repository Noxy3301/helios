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
 * The row values and secondary-index lists that recovery replays.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_LOG_ENTRY_H
#define HELIOS_STORAGE_SRC_WAL_LOG_ENTRY_H

#include <cstdint>
#include <string>
#include <vector>

#include "helios/index.h"

#include "silo/tidword.h"

namespace helios::storage {
namespace wal {

enum class SecondaryIndexOp : uint8_t {
  kNone = 0,    // a primary row, which names no secondary index
  kInsert = 1,  // one primary key joins the key's list
  kDelete = 2,  // one primary key leaves it
  kFull = 3,    // the whole list, as a checkpoint carries it
};

/**
 * @brief One row value or secondary-index entry to replay.
 *
 * @details An empty `index_name` marks a primary row, whose payload is
 * `value`; otherwise the entry belongs to that secondary index and carries
 * the final `primary_keys` list recovery produced. Values and lists are
 * owned.
 */
struct LogEntry {
  std::string key;
  std::string value;
  Tidword tid{};
  std::vector<std::string> primary_keys;
  std::string table_name;
  std::string index_name;
  IndexConstraint index_type = IndexConstraint::kNone;
};

using LogEntries = std::vector<LogEntry>;

}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_LOG_ENTRY_H
