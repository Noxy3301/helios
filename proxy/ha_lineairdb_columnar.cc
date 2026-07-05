#include "ha_lineairdb_columnar.hh"

#include <cassert>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>

#include "my_alloc.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/handler.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "thr_lock.h"

namespace lineairdb_columnar {

namespace {

class LoadedTables {
 public:
  void add(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    tables_.emplace(std::piecewise_construct,
                    std::forward_as_tuple(std::make_pair(db, table)),
                    std::forward_as_tuple());
  }

  bool contains(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    return tables_.count(std::make_pair(db, table)) > 0;
  }

  THR_LOCK *lock(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    auto it = tables_.find(std::make_pair(db, table));
    return it == tables_.end() ? nullptr : &it->second.lock;
  }

  void erase(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    tables_.erase(std::make_pair(db, table));
  }

 private:
  struct TableState {
    THR_LOCK lock;

    TableState() { thr_lock_init(&lock); }
    ~TableState() { thr_lock_delete(&lock); }

    TableState(const TableState &) = delete;
    TableState &operator=(const TableState &) = delete;
  };

  std::mutex mutex_;
  std::map<std::pair<std::string, std::string>, TableState> tables_;
};

LoadedTables *loaded_tables = nullptr;

bool RejectSecondaryExecution(THD *, LEX *) {
  my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
           "LINEAIRDB_COLUMNAR executor is not available yet");
  return true;
}

handler *CreateColumnarHandler(handlerton *hton, TABLE_SHARE *table_share,
                               bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_lineairdb_columnar(hton, table_share);
}

}  // namespace

ha_lineairdb_columnar::ha_lineairdb_columnar(handlerton *hton,
                                             TABLE_SHARE *table_share_arg)
    : handler(hton, table_share_arg) {}

int ha_lineairdb_columnar::open(const char *, int, unsigned int,
                                const dd::Table *) {
  THR_LOCK *lock =
      loaded_tables->lock(table_share->db.str, table_share->table_name.str);
  if (lock == nullptr) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "Table has not been loaded");
    return HA_ERR_GENERIC;
  }

  thr_lock_data_init(lock, &lock_data_, nullptr);
  return 0;
}

int ha_lineairdb_columnar::info(unsigned int flags) {
  handler *primary = ha_get_primary_handler();
  if (primary == nullptr) return HA_ERR_GENERIC;

  const int error = primary->info(flags);
  if (error == 0) stats.records = primary->stats.records;
  return error;
}

ha_rows ha_lineairdb_columnar::records_in_range(unsigned int index,
                                                key_range *min_key,
                                                key_range *max_key) {
  handler *primary = ha_get_primary_handler();
  return primary == nullptr ? HA_POS_ERROR
                            : primary->records_in_range(index, min_key,
                                                        max_key);
}

unsigned long ha_lineairdb_columnar::index_flags(unsigned int index,
                                                 unsigned int part,
                                                 bool all_parts) const {
  const handler *primary = ha_get_primary_handler();
  const unsigned long primary_flags =
      primary == nullptr ? 0 : primary->index_flags(index, part, all_parts);

  // Indexes are available only to let the optimizer estimate primary ranges.
  return (HA_READ_RANGE | HA_KEY_SCAN_NOT_ROR) & primary_flags;
}

THR_LOCK_DATA **ha_lineairdb_columnar::store_lock(THD *, THR_LOCK_DATA **to,
                                                  thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && lock_data_.type == TL_UNLOCK)
    lock_data_.type = lock_type;
  *to++ = &lock_data_;
  return to;
}

int ha_lineairdb_columnar::load_table(const TABLE &table) {
  assert(table.file != nullptr);
  loaded_tables->add(table.s->db.str, table.s->table_name.str);
  return 0;
}

int ha_lineairdb_columnar::unload_table(const char *db_name,
                                        const char *table_name,
                                        bool error_if_not_loaded) {
  if (error_if_not_loaded &&
      !loaded_tables->contains(db_name, table_name)) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Table is not loaded on a secondary engine");
    return 1;
  }

  loaded_tables->erase(db_name, table_name);
  return 0;
}

}  // namespace lineairdb_columnar

struct st_mysql_storage_engine lineairdb_columnar_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

int lineairdb_columnar_init(void *p) {
  lineairdb_columnar::loaded_tables = new lineairdb_columnar::LoadedTables();

  handlerton *hton = static_cast<handlerton *>(p);
  hton->create = lineairdb_columnar::CreateColumnarHandler;
  hton->state = SHOW_OPTION_YES;
  hton->flags = HTON_IS_SECONDARY_ENGINE;
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->prepare_secondary_engine = lineairdb_columnar::RejectSecondaryExecution;
  hton->secondary_engine_flags =
      MakeSecondaryEngineFlags(SecondaryEngineFlag::USE_EXTERNAL_EXECUTOR);
  return 0;
}

int lineairdb_columnar_deinit(void *) {
  delete lineairdb_columnar::loaded_tables;
  lineairdb_columnar::loaded_tables = nullptr;
  return 0;
}
