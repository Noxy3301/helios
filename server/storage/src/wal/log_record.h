// Modified for Helios.

/**
 * @file server/storage/src/wal/log_record.h
 * The epoch-tagged group of writes persisted on the log and inside a
 * checkpoint.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_LOG_RECORD_H
#define HELIOS_STORAGE_SRC_WAL_LOG_RECORD_H

#include <cstdint>
#include <msgpack.hpp>
#include <string>
#include <vector>

#include "silo/transaction_id.h"
#include "util/epoch.h"
#include "wal/log_entry.h"

namespace helios::storage {
namespace wal {

/**
 * @brief An epoch-tagged group of writes, as it is persisted.
 *
 * @details The logger emits one per committed transaction whose write set is
 * non-empty, tagged with its commit epoch; the checkpoint writer packs one
 * record per table.
 *
 * @note Lives at namespace scope rather than inside Logger so that
 * persistence code can name it without depending on the logger interface.
 */
struct LogRecord {
  /**
   * @brief One recorded write, primary or secondary.
   *
   * @details An empty `index_name` marks a primary row, whose payload is
   * `buffer`. Otherwise the write belongs to that secondary index, and
   * `secondary_op` says how to read it: kFull carries the whole primary-key
   * list in `primary_keys`, kInsert and kDelete carry one key in
   * `secondary_primary_key`.
   */
  struct Write {
    std::string key;
    std::string buffer;
    TransactionId transaction_id;
    std::string table_name;
    std::string index_name;
    uint32_t index_type = 0;  // IndexConstraint, as a msgpack-stable integer.
    std::vector<std::string> primary_keys;
    SecondaryIndexOp secondary_op = SecondaryIndexOp::kNone;
    std::string secondary_primary_key;
    MSGPACK_DEFINE(key, buffer, transaction_id, table_name, index_name,
                   index_type, primary_keys, secondary_op,
                   secondary_primary_key);
  };

  EpochNumber epoch = 0;
  std::vector<Write> writes;
  MSGPACK_DEFINE(epoch, writes);
};

using LogRecords = std::vector<LogRecord>;

}  // namespace wal
}  // namespace helios::storage

MSGPACK_ADD_ENUM(helios::storage::wal::SecondaryIndexOp);

#endif  // HELIOS_STORAGE_SRC_WAL_LOG_RECORD_H
