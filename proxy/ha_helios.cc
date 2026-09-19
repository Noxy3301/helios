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
 * @file ha_helios.cc
 *
 * The HELIOS storage engine handler: plugin registration, session and
 * transaction lifecycle, and the handler calls that do not belong to one of
 * the scan, DML, DDL or statistics units.
 */

#include "storage/helios/ha_helios.hh"
#include "helios_log.hh"

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

#include "helios_field_types.h"
#include "key_pack.hh"
#include "helios_prefetch.hh"
#include "helios.pb.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
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

// Helios server connection target (GLOBAL sysvars backing storage)
static char *srv_server_host = nullptr;
static ulong srv_server_port = 9999;
ulong srv_read_path = kReadPathPlan;
enum CommitDurability { kCommitDurabilityAsync = 0, kCommitDurabilitySync = 1 };
// The mode this node last set on the storage server. The server starts
// under the contract helios.cnf gives it, which no query node observes.
static ulong srv_commit_durability = kCommitDurabilitySync;
bool srv_stats_drift_refresh = false;
bool srv_rpc_trace = false;
char *srv_rpc_trace_path = nullptr;
handlerton *helios_hton;

// Error log service, acquired for the life of the plugin.
static SERVICE_TYPE(registry) *reg_srv = nullptr;
SERVICE_TYPE(log_builtins) *log_bi = nullptr;
SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;

// THD-scoped context
struct HeliosThdCtx {
  std::shared_ptr<HeliosProxy> proxy;
  HeliosTransaction *tx{nullptr};
};

/**
 * @brief Return the THD-local Helios context slot.
 */
static HeliosThdCtx *&helios_thd_ctx(THD *thd, handlerton *hton) {
  return *reinterpret_cast<HeliosThdCtx **>(thd_ha_data(thd, hton));
}

/**
 * @brief Create the THD context and RPC proxy when this thread has none.
 */
static void ensure_helios_proxy(HeliosThdCtx *&ctx) {
  if (ctx == nullptr)
    ctx = new HeliosThdCtx();
  if (!ctx->proxy) {
    std::string host =
        srv_server_host ? srv_server_host : std::string("127.0.0.1");
    int port = static_cast<int>(srv_server_port);
    ctx->proxy = std::make_shared<HeliosProxy>(host, port);
  }
}

namespace helios {

std::shared_ptr<HeliosProxy> acquire_shared_proxy(THD *thd) {
  if (thd == nullptr || helios_hton == nullptr) return nullptr;
  HeliosThdCtx *&ctx = helios_thd_ctx(thd, helios_hton);
  ensure_helios_proxy(ctx);
  return ctx->proxy;
}

}  // namespace helios

static int helios_commit(handlerton *hton, THD *thd, bool shouldCommit);
static int helios_abort(handlerton *hton, THD *thd, bool);

static int helios_close_connection(handlerton *hton, THD *thd);

static handler *helios_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool partitioned, MEM_ROOT *mem_root);

static handler *helios_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_helios(hton, table);
}

static int helios_init_func(void *p) {
  DBUG_TRACE;

  if (init_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs)) return 1;

  helios_hton = (handlerton *)p;
  helios_hton->state = SHOW_OPTION_YES;
  helios_hton->create = helios_create_handler;
  helios_hton->flags =
      HTON_CAN_RECREATE | HTON_SUPPORTS_SECONDARY_ENGINE;
  // SECONDARY_LOAD/UNLOAD cleanup calls the primary engine's post_ddl hook.
  // Helios has no post-DDL storage work here, but the hook must be present.
  helios_hton->post_ddl = [](THD *) {};
  helios_hton->db_type = DB_TYPE_UNKNOWN;
  helios_hton->commit = helios_commit;
  helios_hton->rollback = helios_abort;
  helios_hton->close_connection = helios_close_connection;

  return 0;
}

static int helios_deinit_func(void *) {
  deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
  return 0;
}

Helios_share::Helios_share() { thr_lock_init(&lock); }

Helios_share *ha_helios::get_share() {
  Helios_share *tmp_share;

  DBUG_TRACE;

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<Helios_share *>(get_ha_share_ptr()))) {
    tmp_share = new Helios_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  return tmp_share;
}

HeliosProxy *ha_helios::get_proxy() {
  // thd_ha_data provides a single void* slot per THD per storage engine.
  // We need HeliosThdCtx to hold both the RPC proxy and the transaction.
  HeliosThdCtx *&ctx = helios_thd_ctx(userThread, helios_hton);
  ensure_helios_proxy(ctx);
  return ctx->proxy.get();
}

std::string ha_helios::server_connection_host() {
  return srv_server_host ? srv_server_host : std::string("127.0.0.1");
}

int ha_helios::server_connection_port() {
  return static_cast<int>(srv_server_port);
}

static PSI_memory_key helios_key_memory_blobroot;

ha_helios::ha_helios(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg), m_ds_mrr(this), buffer_position_(0),
      scan_exhausted_(false),
      blobroot(helios_key_memory_blobroot, BLOB_MEMROOT_ALLOC_SIZE) {}

int ha_helios::extra(enum ha_extra_function operation) {
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

int ha_helios::reset() {
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

void ha_helios::start_bulk_insert(ha_rows rows) {
  DBUG_TRACE;
  bulk_insert_rows_ = rows;
  bulk_insert_generated_ = 0;
  bulk_insert_active_ = true;
  insert_probe_keys_.clear();
}

int ha_helios::end_bulk_insert() {
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

int ha_helios::delete_all_rows() {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

int ha_helios::external_lock(THD *thd, int lock_type) {
  DBUG_TRACE;

  userThread = thd;

  const bool tx_is_ready_to_commit = lock_type == F_UNLCK;
  if (tx_is_ready_to_commit) return 0;

  // get_transaction() will automatically start the transaction if needed
  HeliosTransaction *tx = get_transaction(thd);
  if (tx != nullptr) {
    const LEX_CSTRING &q = thd->query();
    if (q.str != nullptr && q.length > 0) {
      tx->on_stmt_boundary(std::string(q.str, q.length));
    }
  }

  // The server count is authoritative over the local shards, which count
  // only this query node's committed deltas.
  seed_row_count_from_cache(get_proxy());

  return 0;
}

int ha_helios::start_stmt(THD *thd, thr_lock_type lock_type) {
  assert(lock_type > 0);
  userThread = thd;
  return external_lock(thd, lock_type);
}

HeliosTransaction *ha_helios::active_transaction(THD *thd) const {
  if (thd == nullptr) return nullptr;
  HeliosThdCtx *ctx =
      *reinterpret_cast<HeliosThdCtx **>(thd_ha_data(thd, helios_hton));
  return (ctx != nullptr) ? ctx->tx : nullptr;
}

HeliosTransaction *ha_helios::new_transaction(THD *thd) {
  if (thd == nullptr) return nullptr;
  userThread = thd;
  return new HeliosTransaction(thd, get_proxy(), helios_hton);
}

HeliosTransaction *&ha_helios::get_transaction(THD *thd) {
  HeliosThdCtx *&ctx = helios_thd_ctx(thd, helios_hton);
  ensure_helios_proxy(ctx);
  if (ctx->tx == nullptr) {
    ctx->tx = new HeliosTransaction(thd, ctx->proxy.get(), helios_hton);
  }
  if (ctx->tx->is_not_started()) {
    ctx->tx->begin_transaction();
    maybe_prefetch_for_transaction(thd, ctx->tx);
  }
  return ctx->tx;
}

int ha_helios::abort_errno(HeliosTransaction *tx,
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
 * implementation of commit for helios_hton
 */
static int helios_commit(handlerton *hton, THD *thd, bool all) {
  HeliosThdCtx *&ctx =
      *reinterpret_cast<HeliosThdCtx **>(thd_ha_data(thd, hton));

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
 * implementation of rollback for helios_hton
 */
static int helios_abort(handlerton *hton, THD *thd, bool) {
  HeliosThdCtx *&ctx =
      *reinterpret_cast<HeliosThdCtx **>(thd_ha_data(thd, hton));

  // Nothing to roll back when this engine took no part in the transaction.
  if (ctx == nullptr || ctx->tx == nullptr)
    return 0;

  ctx->tx->set_status_to_abort();
  (void)ctx->tx->end_transaction();
  ctx->tx = nullptr;
  return 0;
}

static int helios_close_connection(handlerton *hton, THD *thd) {
  HeliosThdCtx **ctx_slot =
      reinterpret_cast<HeliosThdCtx **>(thd_ha_data(thd, hton));
  if (ctx_slot == nullptr)
    return 0;

  HeliosThdCtx *ctx = *ctx_slot;
  if (ctx == nullptr)
    return 0;

  LOG_INFO("helios_close_connection: thd=%p ctx=%p proxy=%p",
           static_cast<void *>(thd), static_cast<void *>(ctx),
           ctx->proxy.get());

  if (ctx->tx != nullptr) {
    LOG_INFO("helios_close_connection: aborting pending tx=%p", ctx->tx);
    ctx->tx->set_status_to_abort();
    (void)ctx->tx->end_transaction();
    ctx->tx = nullptr;
  }

  if (ctx->proxy) {
    LOG_INFO("helios_close_connection: releasing proxy=%p",
             ctx->proxy.get());
  }
  ctx->proxy.reset();
  delete ctx;
  *ctx_slot = nullptr;
  return 0;
}

THR_LOCK_DATA **ha_helios::store_lock(THD *, THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type) {
  DBUG_TRACE;
  /*
    Helios uses its own transaction-level locking, so we don't take part
    in the server's THR_LOCK table locking. lock_count() advertises this by
    returning 0; keep store_lock() consistent by leaving the lock array
    untouched.
  */
  return to;
}

/**
 * @brief Whether this statement's reads come from a read plan.
 *
 * MRR cost estimation must stay side-effect-free, so it cannot call
 * get_transaction() (which allocates and may emit RPCs). Read the session
 * instead.
 */
bool ha_helios::statement_uses_read_plan(THD *thd) {
  return srv_read_path == kReadPathPlan && thd_can_use_prefetch(thd);
}

struct st_mysql_storage_engine helios_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

// Helios server connection target sysvars
static MYSQL_SYSVAR_STR(server_host, srv_server_host,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Helios server hostname or IP address.", nullptr,
                        nullptr, "127.0.0.1");
static MYSQL_SYSVAR_ULONG(server_port, srv_server_port, PLUGIN_VAR_RQCMDARG,
                          "Helios server TCP port.", nullptr, nullptr, 9999,
                          1, 65535, 0);
static const char *read_path_names[] = {"row", "plan", NullS};
static TYPELIB read_path_typelib = {array_elements(read_path_names) - 1,
                                    "read_path_typelib", read_path_names,
                                    nullptr};
static MYSQL_SYSVAR_ENUM(read_path, srv_read_path, PLUGIN_VAR_RQCMDARG,
                         "Where a statement's reads come from: row sends one "
                         "request per handler read, plan caches what it can in "
                         "one request and sends the rest as they happen.",
                         nullptr, nullptr, kReadPathPlan, &read_path_typelib);
static const char *commit_durability_names[] = {"async", "sync", NullS};
static TYPELIB commit_durability_typelib = {
    array_elements(commit_durability_names) - 1, "commit_durability_typelib",
    commit_durability_names, nullptr};

// Publishes the mode on the storage server; a refused switch fails the SET
// GLOBAL with the server's reason and leaves the variable as it was.
static void update_commit_durability(THD *, SYS_VAR *, void *var_ptr,
                                     const void *save) {
  const ulong mode = *static_cast<const ulong *>(save);
  const std::string host =
      srv_server_host ? srv_server_host : std::string("127.0.0.1");
  HeliosProxy proxy(host, static_cast<int>(srv_server_port));
  std::string error;
  if (!proxy.db_set_commit_durability(
          mode == kCommitDurabilitySync
              ? Helios::Protocol::DbSetCommitDurability::SYNC
              : Helios::Protocol::DbSetCommitDurability::ASYNC,
          &error)) {
    my_printf_error(ER_WRONG_VALUE_FOR_VAR, "helios_commit_durability: %s",
                    MYF(0), error.c_str());
    return;
  }
  *static_cast<ulong *>(var_ptr) = mode;
}

static MYSQL_SYSVAR_ENUM(commit_durability, srv_commit_durability,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_NOCMDOPT |
                             PLUGIN_VAR_NOPERSIST,
                         "Commit acknowledgement contract of the storage "
                         "server: async returns before the log is on disk, "
                         "sync waits for it. Setting it switches the running "
                         "server.",
                         nullptr, update_commit_durability,
                         kCommitDurabilitySync, &commit_durability_typelib);
static MYSQL_SYSVAR_BOOL(stats_drift_refresh, srv_stats_drift_refresh,
                         PLUGIN_VAR_OPCMDARG,
                         "Automatically refresh index statistics before SELECT "
                         "when the row-count difference exceeds 20% of the "
                         "larger count. Disabled by default because the "
                         "synchronous refresh scans every requested index on "
                         "the server.",
                         nullptr, nullptr, false);
// The trace file is opened once, on the first RPC of the process.
static MYSQL_SYSVAR_BOOL(rpc_trace, srv_rpc_trace, PLUGIN_VAR_READONLY,
                         "Write a JSONL trace of every RPC, statement and "
                         "transaction.",
                         nullptr, nullptr, false);
static MYSQL_SYSVAR_STR(rpc_trace_path, srv_rpc_trace_path,
                        PLUGIN_VAR_READONLY | PLUGIN_VAR_MEMALLOC,
                        "Where the RPC trace is written; unset writes "
                        "/tmp/helios_rpc_trace_<pid>.jsonl.",
                        nullptr, nullptr, nullptr);
static SYS_VAR *helios_system_variables[] = {
    MYSQL_SYSVAR(server_host),
    MYSQL_SYSVAR(server_port),
    MYSQL_SYSVAR(read_path),
    MYSQL_SYSVAR(commit_durability),
    MYSQL_SYSVAR(stats_drift_refresh),
    MYSQL_SYSVAR(rpc_trace),
    MYSQL_SYSVAR(rpc_trace_path),
    nullptr};

extern struct st_mysql_storage_engine helios_columnar_storage_engine;
extern int helios_columnar_init(void *p);
extern int helios_columnar_deinit(void *p);

mysql_declare_plugin(helios){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &helios_storage_engine,
    "HELIOS",
    PLUGIN_AUTHOR_ORACLE,
    "Helios storage engine",
    PLUGIN_LICENSE_GPL,
    helios_init_func, /* Plugin Init */
    nullptr,             /* Plugin check uninstall */
    helios_deinit_func,  /* Plugin Deinit */
    0x0001 /* 0.1 */,
    nullptr,                    /* status variables */
    helios_system_variables, /* system variables */
    nullptr,                    /* config options */
    0,                          /* flags */
},
{
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &helios_columnar_storage_engine,
    "HELIOS_COLUMNAR",
    PLUGIN_AUTHOR_ORACLE,
    "Helios columnar secondary engine",
    PLUGIN_LICENSE_GPL,
    helios_columnar_init,   /* Plugin Init */
    nullptr,                   /* Plugin check uninstall */
    helios_columnar_deinit, /* Plugin Deinit */
    0x0001 /* 0.1 */,
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
} mysql_declare_plugin_end;
