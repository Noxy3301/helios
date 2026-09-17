/* Copyright (c) 2004, 2021, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
 * @file ha_lineairdb.cc
 *
 * The LINEAIRDB storage engine handler: plugin registration, session and
 * transaction lifecycle, and the handler calls that do not belong to one of
 * the scan, DML, DDL or statistics units.
 */

#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "lineairdb_field_types.h"
#include "lineairdb_keyenc.hh"
#include "lineairdb_prefetch.hh"
#include "lineairdb.pb.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "typelib.h"

#define BLOB_MEMROOT_ALLOC_SIZE (8192)

// LineairDB server connection target (GLOBAL sysvars backing storage)
static char *srv_server_host = nullptr;
static ulong srv_server_port = 9999;
ulong srv_read_path = kReadPathPlan;
bool srv_stats_drift_refresh = false;
handlerton *lineairdb_hton;

// THD-scoped context
struct LineairDBThdCtx {
  std::shared_ptr<LineairDBProxy> proxy;
  LineairDBTransaction *tx{nullptr};
};

/**
 * @brief Return the THD-local LineairDB context slot.
 */
static LineairDBThdCtx *&lineairdb_thd_ctx(THD *thd, handlerton *hton) {
  return *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, hton));
}

/**
 * @brief Create the THD context and RPC proxy when this thread has none.
 */
static void ensure_lineairdb_proxy(LineairDBThdCtx *&ctx) {
  if (ctx == nullptr)
    ctx = new LineairDBThdCtx();
  if (!ctx->proxy) {
    std::string host =
        srv_server_host ? srv_server_host : std::string("127.0.0.1");
    int port = static_cast<int>(srv_server_port);
    ctx->proxy = std::make_shared<LineairDBProxy>(host, port);
  }
}

namespace lineairdb {

std::shared_ptr<LineairDBProxy> acquire_shared_proxy(THD *thd) {
  if (thd == nullptr || lineairdb_hton == nullptr) return nullptr;
  LineairDBThdCtx *&ctx = lineairdb_thd_ctx(thd, lineairdb_hton);
  ensure_lineairdb_proxy(ctx);
  return ctx->proxy;
}

}  // namespace lineairdb

static int lineairdb_commit(handlerton *hton, THD *thd, bool shouldCommit);
static int lineairdb_abort(handlerton *hton, THD *thd, bool);

static int lineairdb_close_connection(handlerton *hton, THD *thd);

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_handler_tablename ha_lineairdb_system_tables[] = {
    {(const char *)nullptr, (const char *)nullptr}};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @retval true   Given db.table_name is supported system table.
  @retval false  Given db.table_name is not a supported system table.
*/
static bool
lineairdb_is_supported_system_table(const char *db, const char *table_name,
                                    bool is_sql_layer_system_table) {
  st_handler_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab = ha_lineairdb_system_tables;
  while (systab && systab->db) {
    if (systab->db == db && strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

static handler *lineairdb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool partitioned, MEM_ROOT *mem_root);

/* Interface to mysqld, to check system tables supported by SE */
static bool lineairdb_is_supported_system_table(const char *db,
                                                const char *table_name,
                                                bool is_sql_layer_system_table);

static handler *lineairdb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_lineairdb(hton, table);
}

static int lineairdb_init_func(void *p) {
  DBUG_TRACE;

  lineairdb_hton = (handlerton *)p;
  lineairdb_hton->state = SHOW_OPTION_YES;
  lineairdb_hton->create = lineairdb_create_handler;
  lineairdb_hton->flags =
      HTON_CAN_RECREATE | HTON_SUPPORTS_SECONDARY_ENGINE;
  // SECONDARY_LOAD/UNLOAD cleanup calls the primary engine's post_ddl hook.
  // LineairDB has no post-DDL storage work here, but the hook must be present.
  lineairdb_hton->post_ddl = [](THD *) {};
  lineairdb_hton->is_supported_system_table =
      lineairdb_is_supported_system_table;
  lineairdb_hton->db_type = DB_TYPE_UNKNOWN;
  lineairdb_hton->commit = lineairdb_commit;
  lineairdb_hton->rollback = lineairdb_abort;
  lineairdb_hton->close_connection = lineairdb_close_connection;

  return 0;
}

LineairDB_share::LineairDB_share() { thr_lock_init(&lock); }

LineairDB_share *ha_lineairdb::get_share() {
  LineairDB_share *tmp_share;

  DBUG_TRACE;

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<LineairDB_share *>(get_ha_share_ptr()))) {
    tmp_share = new LineairDB_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  return tmp_share;
}

LineairDBProxy *ha_lineairdb::get_proxy() {
  // thd_ha_data provides a single void* slot per THD per storage engine.
  // We need LineairDBThdCtx to hold both the RPC proxy and the transaction.
  LineairDBThdCtx *&ctx = lineairdb_thd_ctx(userThread, lineairdb_hton);
  ensure_lineairdb_proxy(ctx);
  return ctx->proxy.get();
}

std::string ha_lineairdb::server_connection_host() {
  return srv_server_host ? srv_server_host : std::string("127.0.0.1");
}

int ha_lineairdb::server_connection_port() {
  return static_cast<int>(srv_server_port);
}

static PSI_memory_key lineairdb_key_memory_blobroot;

ha_lineairdb::ha_lineairdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg), m_ds_mrr(this), buffer_position_(0),
      scan_exhausted_(false),
      blobroot(lineairdb_key_memory_blobroot, BLOB_MEMROOT_ALLOC_SIZE) {}

int ha_lineairdb::extra(enum ha_extra_function operation) {
  DBUG_TRACE;
  switch (operation) {
    case HA_EXTRA_WRITE_CAN_REPLACE:
      insert_can_replace_ = true;
      break;
    case HA_EXTRA_WRITE_CANNOT_REPLACE:
      insert_can_replace_ = false;
      break;
    // Both mean the statement resolves the duplicate itself, from the row.
    case HA_EXTRA_IGNORE_DUP_KEY:
    case HA_EXTRA_INSERT_WITH_UPDATE:
      insert_peeks_duplicates_ = true;
      break;
    case HA_EXTRA_NO_IGNORE_DUP_KEY:
      insert_peeks_duplicates_ = false;
      break;
    default:
      break;
  }
  return 0;
}

int ha_lineairdb::reset() {
  DBUG_TRACE;
  insert_can_replace_ = false;
  insert_peeks_duplicates_ = false;
  duplicate_key_index_ = MAX_KEY;
  // Not every statement shape reaches end_bulk_insert, and a stale estimate
  // would size the next statement's reservation.
  bulk_insert_rows_ = 0;
  bulk_insert_generated_ = 0;
  bulk_insert_active_ = false;
  insert_probe_keys_.clear();
  return 0;
}

void ha_lineairdb::start_bulk_insert(ha_rows rows) {
  DBUG_TRACE;
  bulk_insert_rows_ = rows;
  bulk_insert_generated_ = 0;
  bulk_insert_active_ = true;
  insert_probe_keys_.clear();
}

int ha_lineairdb::end_bulk_insert() {
  DBUG_TRACE;
  bulk_insert_rows_ = 0;
  bulk_insert_generated_ = 0;
  bulk_insert_active_ = false;

  auto *tx = active_transaction(ha_thd());
  if (tx == nullptr) {
    insert_probe_keys_.clear();
    return 0;
  }

  // ER_DUP_ENTRY is the statement's to report, and this is its last chance.
  if (!tx->is_aborted()) {
    if (const int error = flush_insert_probe(tx); error != 0) {
      set_my_errno(error);
      return error;
    }
  }
  insert_probe_keys_.clear();
  if (!tx->is_aborted()) return 0;

  const int error = abort_errno(tx);
  // Sql_cmd_load_table::execute_inner reads my_errno() rather than this return
  // value, so a stale errno would be reported in place of the real one.
  set_my_errno(error);
  return error;
}

int ha_lineairdb::delete_all_rows() {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

int ha_lineairdb::external_lock(THD *thd, int lock_type) {
  DBUG_TRACE;

  userThread = thd;

  const bool tx_is_ready_to_commit = lock_type == F_UNLCK;
  if (tx_is_ready_to_commit) return 0;

  // get_transaction() will automatically start the transaction if needed
  LineairDBTransaction *tx = get_transaction(thd);
  if (tx != nullptr) {
    const LEX_CSTRING &q = thd->query();
    if (q.str != nullptr && q.length > 0) {
      tx->on_stmt_boundary(std::string(q.str, q.length));
    }
  }

  // The server count is authoritative over the local shards, which count
  // only this proxy's committed deltas.
  seed_row_count_from_cache(get_proxy());

  return 0;
}

int ha_lineairdb::start_stmt(THD *thd, thr_lock_type lock_type) {
  assert(lock_type > 0);
  userThread = thd;
  return external_lock(thd, lock_type);
}

LineairDBTransaction *ha_lineairdb::active_transaction(THD *thd) const {
  if (thd == nullptr) return nullptr;
  LineairDBThdCtx *ctx =
      *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, lineairdb_hton));
  return (ctx != nullptr) ? ctx->tx : nullptr;
}

LineairDBTransaction *ha_lineairdb::new_transaction(THD *thd) {
  if (thd == nullptr) return nullptr;
  userThread = thd;
  return new LineairDBTransaction(thd, get_proxy(), lineairdb_hton);
}

LineairDBTransaction *&ha_lineairdb::get_transaction(THD *thd) {
  LineairDBThdCtx *&ctx = lineairdb_thd_ctx(thd, lineairdb_hton);
  ensure_lineairdb_proxy(ctx);
  if (ctx->tx == nullptr) {
    ctx->tx = new LineairDBTransaction(thd, ctx->proxy.get(), lineairdb_hton);
  }
  if (ctx->tx->is_not_started()) {
    ctx->tx->begin_transaction();
    maybe_prefetch_for_transaction(thd, ctx->tx);
  }
  return ctx->tx;
}

int ha_lineairdb::abort_errno(LineairDBTransaction *tx,
                              bool duplicate_is_conflict) {
  if (tx != nullptr && tx->has_transport_error()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_NO_CONNECTION;
  }
  // A refused key is permanent and ends the transaction; info(HA_STATUS_ERRKEY)
  // names it. A hidden key is not user-addressable, so its refusal falls
  // through as contention below.
  if (tx != nullptr && tx->duplicate_key_abort() && !duplicate_is_conflict &&
      is_primary_key_exists()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    duplicate_key_index_ = table_share->primary_key;
    return HA_ERR_FOUND_DUPP_KEY;
  }
  // Default: a genuine OCC/server abort is retryable contention.
  thd_mark_transaction_to_rollback(ha_thd(), 1);
  return HA_ERR_LOCK_DEADLOCK;
}

/**
 * implementation of commit for lineairdb_hton
 */
static int lineairdb_commit(handlerton *hton, THD *thd, bool all) {
  LineairDBThdCtx *&ctx =
      *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, hton));

  // Nothing to commit when this engine took no part in the transaction.
  if (ctx == nullptr || ctx->tx == nullptr)
    return 0;

  const bool should_terminate_now =
      (all == true) || ctx->tx->is_a_single_statement();
  if (!should_terminate_now)
    return 0;

  bool transport_error = false;
  bool duplicate_key = false;
  const bool committed =
      ctx->tx->end_transaction(&transport_error, &duplicate_key);
  ctx->tx = nullptr;

  if (!committed) {
    thd_mark_transaction_to_rollback(thd, true);
    if (transport_error) return HA_ERR_NO_CONNECTION;
    // No handler is in scope here, so MySQL wraps this in ER_ERROR_DURING_COMMIT
    // rather than naming the key. The code still separates a duplicate key from
    // contention, which is the difference a client has to act on.
    if (duplicate_key) return HA_ERR_FOUND_DUPP_KEY;
    return HA_ERR_LOCK_DEADLOCK;
  }
  return 0;
}

/**
 * implementation of rollback for lineairdb_hton
 */
static int lineairdb_abort(handlerton *hton, THD *thd, bool) {
  LineairDBThdCtx *&ctx =
      *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, hton));

  // Nothing to roll back when this engine took no part in the transaction.
  if (ctx == nullptr || ctx->tx == nullptr)
    return 0;

  ctx->tx->set_status_to_abort();
  (void)ctx->tx->end_transaction();
  ctx->tx = nullptr;
  return 0;
}

static int lineairdb_close_connection(handlerton *hton, THD *thd) {
  LineairDBThdCtx **ctx_slot =
      reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, hton));
  if (ctx_slot == nullptr)
    return 0;

  LineairDBThdCtx *ctx = *ctx_slot;
  if (ctx == nullptr)
    return 0;

  LOG_INFO("lineairdb_close_connection: thd=%p ctx=%p proxy=%p",
           static_cast<void *>(thd), static_cast<void *>(ctx),
           ctx->proxy.get());

  if (ctx->tx != nullptr) {
    LOG_INFO("lineairdb_close_connection: aborting pending tx=%p", ctx->tx);
    ctx->tx->set_status_to_abort();
    (void)ctx->tx->end_transaction();
    ctx->tx = nullptr;
  }

  if (ctx->proxy) {
    LOG_INFO("lineairdb_close_connection: releasing proxy=%p",
             ctx->proxy.get());
  }
  ctx->proxy.reset();
  delete ctx;
  *ctx_slot = nullptr;
  return 0;
}

THR_LOCK_DATA **ha_lineairdb::store_lock(THD *, THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type) {
  DBUG_TRACE;
  /*
    LineairDB uses its own transaction-level locking, so we don't take part
    in the server's THR_LOCK table locking. lock_count() advertises this by
    returning 0; keep store_lock() consistent by leaving the lock array
    untouched.
  */
  return to;
}

/**
 * @brief Whether this statement's reads are staged through a read plan.
 *
 * MRR cost estimation must stay side-effect-free, so it cannot call
 * get_transaction() (which allocates and may emit RPCs). Read the session
 * instead.
 */
bool ha_lineairdb::statement_uses_read_plan(THD *thd) {
  return srv_read_path == kReadPathPlan && thd_can_use_prefetch(thd);
}

struct st_mysql_storage_engine lineairdb_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

// LineairDB server connection target sysvars
static MYSQL_SYSVAR_STR(server_host, srv_server_host,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "LineairDB server hostname or IP address.", nullptr,
                        nullptr, "127.0.0.1");
static MYSQL_SYSVAR_ULONG(server_port, srv_server_port, PLUGIN_VAR_RQCMDARG,
                          "LineairDB server TCP port.", nullptr, nullptr, 9999,
                          1, 65535, 0);
static const char *read_path_names[] = {"row", "plan", NullS};
static TYPELIB read_path_typelib = {array_elements(read_path_names) - 1,
                                    "read_path_typelib", read_path_names,
                                    nullptr};
static MYSQL_SYSVAR_ENUM(read_path, srv_read_path, PLUGIN_VAR_RQCMDARG,
                         "Where a statement's reads come from: row sends one "
                         "request per handler read, plan stages what it can in "
                         "one request and sends the rest as they happen.",
                         nullptr, nullptr, kReadPathPlan, &read_path_typelib);
static MYSQL_SYSVAR_BOOL(stats_drift_refresh, srv_stats_drift_refresh,
                         PLUGIN_VAR_OPCMDARG,
                         "Automatically refresh index statistics before SELECT "
                         "when the row-count difference exceeds 20% of the "
                         "larger count. Disabled by default because the "
                         "synchronous refresh scans every requested index on "
                         "the server.",
                         nullptr, nullptr, false);
static SYS_VAR *lineairdb_system_variables[] = {
    MYSQL_SYSVAR(server_host),
    MYSQL_SYSVAR(server_port),
    MYSQL_SYSVAR(read_path),
    MYSQL_SYSVAR(stats_drift_refresh),
    nullptr};

extern struct st_mysql_storage_engine lineairdb_columnar_storage_engine;
extern int lineairdb_columnar_init(void *p);
extern int lineairdb_columnar_deinit(void *p);

mysql_declare_plugin(lineairdb){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &lineairdb_storage_engine,
    "LINEAIRDB",
    PLUGIN_AUTHOR_ORACLE,
    "LineairDB storage engine",
    PLUGIN_LICENSE_GPL,
    lineairdb_init_func, /* Plugin Init */
    nullptr,             /* Plugin check uninstall */
    nullptr,             /* Plugin Deinit */
    0x0001 /* 0.1 */,
    nullptr,                    /* status variables */
    lineairdb_system_variables, /* system variables */
    nullptr,                    /* config options */
    0,                          /* flags */
},
{
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &lineairdb_columnar_storage_engine,
    "LINEAIRDB_COLUMNAR",
    PLUGIN_AUTHOR_ORACLE,
    "LineairDB columnar secondary engine",
    PLUGIN_LICENSE_GPL,
    lineairdb_columnar_init,   /* Plugin Init */
    nullptr,                   /* Plugin check uninstall */
    lineairdb_columnar_deinit, /* Plugin Deinit */
    0x0001 /* 0.1 */,
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
} mysql_declare_plugin_end;
