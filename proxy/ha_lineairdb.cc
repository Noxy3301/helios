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
  @file ha_lineairdb.cc

  @brief
  The ha_lineairdb engine is a stubbed storage engine for lineairdb purposes
  only; it does nothing at this point. Its purpose is to provide a source code
  illustration of how to begin writing new storage engines; see also
  /storage/lineairdb/ha_lineairdb.h.

  @details
  ha_lineairdb will let you create/open/delete tables, but
  nothing further (for lineairdb, indexes are not supported nor can data
  be stored in the table). Use this lineairdb as a template for
  implementing the same functionality in your own storage engine. You
  can enable the lineairdb storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-lineairdb-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE \<table name\> (...) ENGINE=LINEAIRDB;

  The lineairdb storage engine is set up to use table locks. It
  implements an lineairdb "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  lineairdb handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_lineairdb.h before reading the rest
  of this file.

  @note
  When you create an LINEAIRDB table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an lineairdb select that would do a scan of an entire
  table:

  @code
  ha_lineairdb::store_lock
  ha_lineairdb::external_lock
  ha_lineairdb::info
  ha_lineairdb::rnd_init
  ha_lineairdb::extra
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::extra
  ha_lineairdb::external_lock
  ha_lineairdb::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the lineairdb storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_lineairdb::open() would also have been necessary. Calls to
  ha_lineairdb::extra() are hints as to what will be occurring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "lineairdb_field_types.h"
#include "lineairdb_keyenc.hh"
#include "lineairdb_prefetch.hh"
#include "lineairdb_pushdown.hh"
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
#include "sql/sql_optimizer.h"             // JOIN
#include "sql/join_optimizer/access_path.h"  // AccessPath (QEP tree)
#include "sql/join_optimizer/walk_access_paths.h"
#include "sql/join_optimizer/materialize_path_parameters.h"  // MATERIALIZE param
#include "sql/query_result.h"                 // Query_result
#include "sql/item_sum.h"                     // Item_sum
#include "sql/visible_fields.h"               // VisibleFields
#include "sql/my_decimal.h"                   // my_decimal
#include <map>
#include "typelib.h"

#define BLOB_MEMROOT_ALLOC_SIZE (8192)
#define FENCE false

// LineairDB SecondaryIndexOption::Constraint wire bit for UNIQUE.
static constexpr uint LDB_INDEX_UNIQUE = 1u;

// Rows per backfill commit; bounds each OCC write-set.
static constexpr uint64_t kBackfillWriteChunkRows = 2000;  // FIXME: make configurable
// Worker threads that commit a non-unique index backfill in parallel.
static constexpr size_t kBackfillParallelWorkers = 16;  // FIXME: make configurable

// LineairDB server connection target (GLOBAL sysvars backing storage)
static char *srv_server_host = nullptr;
static ulong srv_server_port = 9999;
static bool srv_prefetch_execution = false;
// Non-static: read by LineairDBTransaction at begin (see lineairdb_transaction.cc)
bool srv_prefetch_ro_novalidate = false;

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

/**
 * @brief Shared RPC connection for the LINEAIRDB_COLUMNAR secondary engine.
 *
 * Both engines talk to the same LineairDB server through the THD-scoped
 * context owned by this (primary) engine.
 */
std::shared_ptr<LineairDBProxy> lineairdb_acquire_shared_proxy(THD *thd) {
  if (thd == nullptr || lineairdb_hton == nullptr) return nullptr;
  LineairDBThdCtx *&ctx = lineairdb_thd_ctx(thd, lineairdb_hton);
  ensure_lineairdb_proxy(ctx);
  return ctx->proxy;
}

/**
 * @brief List the indexes whose key-prefix NDV the server should measure.
 */
static std::vector<std::pair<std::string, uint32_t>> index_ndv_descriptors(
    TABLE *table) {
  std::vector<std::pair<std::string, uint32_t>> descs;
  if (table == nullptr || table->s == nullptr)
    return descs;

  descs.reserve(table->s->keys);
  for (uint i = 0; i < table->s->keys; ++i) {
    KEY *key = table->key_info + i;
    const bool is_primary = (i == table->s->primary_key);
    descs.emplace_back(is_primary ? std::string()
                                  : std::string(key->name ? key->name : ""),
                       key->user_defined_key_parts);
  }
  return descs;
}

static int lineairdb_commit(handlerton *hton, THD *thd, bool shouldCommit);
static int lineairdb_abort(handlerton *hton, THD *thd, bool);

static MYSQL_THDVAR_STR(last_create_thdvar, PLUGIN_VAR_MEMALLOC, nullptr,
                        nullptr, nullptr, nullptr);

static MYSQL_THDVAR_UINT(create_count_thdvar, 0, nullptr, nullptr, nullptr, 0,
                         0, 1000, 0);

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

struct lineairdb_vars_t {
  ulong var1;
  double var2;
  char var3[64];
  bool var4;
  bool var5;
  ulong var6;
};

static handler *lineairdb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool partitioned, MEM_ROOT *mem_root);

handlerton *lineairdb_hton;

/* Interface to mysqld, to check system tables supported by SE */
static bool lineairdb_is_supported_system_table(const char *db,
                                                const char *table_name,
                                                bool is_sql_layer_system_table);

static handler *lineairdb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_lineairdb(hton, table);
}

/**
 * Aggregate pushdown uses MySQL's engine-pushdown hook to replace execution
 * only for query shapes this file can fully evaluate.
 */

namespace {
enum HeliosAggKind { HK_PASS = 0, HK_COUNT, HK_SUM, HK_AVG };

// One visible SELECT output column in the aggregate executor plan.
struct HeliosOut {
  Item *orig = nullptr;                 // SELECT output item
  HeliosAggKind kind = HK_PASS;
  Item *arg = nullptr;                  // aggregate argument expr (nullptr=COUNT*)
  Item_result rtype = STRING_RESULT;    // result/accumulation type
};

// Per-group accumulator used by both server-result finalization and fallback.
struct HeliosAccum {
  longlong cnt = 0;        // COUNT, and non-null counter for SUM/AVG
  my_decimal dec;          // SUM/AVG decimal accumulator
  bool dec_init = false;
  double dbl = 0;          // SUM/AVG real accumulator
  bool p_null = false;     // passthrough captured value
  longlong p_int = 0;
  double p_dbl = 0;
  my_decimal p_dec;
  String p_str;            // owns a copy
};

struct HeliosHavingDesc {
  Item_sum *sum = nullptr;
  HeliosAggKind kind = HK_COUNT;
  Item *arg = nullptr;
  Item_result rtype = INT_RESULT;
  Item_func::Functype op = Item_func::EQ_FUNC;
  Item *cnst = nullptr;
};

static constexpr uint kHeliosAggregateDecimalPrecisionMax = 18;

/**
 * @brief Return true if a field is safe for server-side decimal aggregation.
 *
 * The server evaluates aggregate arguments from raw Field::val_str() bytes with
 * dec_parse(), so the text must be decimal syntax with the same numeric meaning
 * SQL gives the field. Temporal strings such as YYYY-MM-DD are not safe even
 * though MySQL can SUM them numerically. The server also accumulates mantissas
 * in signed __int128; DECIMAL precision 18 keeps each value below 10^18,
 * leaving more than 20 decimal digits of headroom under 2^127 for realistic
 * per-group accumulation while still accepting TPC-H q18's DECIMAL(15,2).
 */
static bool helios_safe_decimal_aggregate_field(const Field *field) {
  if (field == nullptr || field->is_array()) return false;
  switch (field->real_type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      return field->result_type() == INT_RESULT;
    case MYSQL_TYPE_NEWDECIMAL:
      return field->result_type() == DECIMAL_RESULT &&
             down_cast<const Field_new_decimal *>(field)->precision <=
                 kHeliosAggregateDecimalPrecisionMax;
    default:
      return false;
  }
}

static Item_field *helios_safe_bare_decimal_aggregate_arg(Item *arg) {
  if (arg == nullptr) return nullptr;
  Item *real = arg->real_item();
  if (real == nullptr || real->type() != Item::FIELD_ITEM) return nullptr;
  Item_field *field_arg = down_cast<Item_field *>(real);
  return helios_safe_decimal_aggregate_field(field_arg->field) ? field_arg
                                                              : nullptr;
}
}  // namespace

/**
 * @brief Build the visible SELECT-output plan for aggregate execution.
 *
 * Uses Query_block::fields because JOIN::fields can be rebound to an
 * aggregation temp table after optimization.
 */
static bool helios_plan_outputs(JOIN *join, std::vector<HeliosOut> *out) {
  if (join->query_block == nullptr) return false;
  for (Item *it : VisibleFields(join->query_block->fields)) {
    HeliosOut o;
    o.orig = it;
    if (it->type() == Item::SUM_FUNC_ITEM) {
      // Only bare aggregate items are supported here.
      Item_sum *s = down_cast<Item_sum *>(it);
      if (s->has_wf() || s->has_subquery()) return false;
      switch (s->sum_func()) {
        case Item_sum::COUNT_FUNC:
          o.kind = HK_COUNT;
          // COUNT(*) and COUNT(non-null const) count every input row.
          if (s->argument_count() > 0) {
            Item *a0 = s->arguments()[0];
            if (!a0->const_item() || a0->is_nullable() || a0->is_null())
              return false;
          }
          break;
        case Item_sum::SUM_FUNC: o.kind = HK_SUM; break;
        case Item_sum::AVG_FUNC: o.kind = HK_AVG; break;
        default: return false;
      }
      o.rtype = s->result_type();
      if (o.kind != HK_COUNT && o.rtype != DECIMAL_RESULT &&
          o.rtype != REAL_RESULT && o.rtype != INT_RESULT)
        return false;
      o.arg = (s->argument_count() > 0) ? s->arguments()[0] : nullptr;
      if (o.arg != nullptr &&
          (o.arg->has_subquery() || o.arg->has_aggregation() ||
           o.arg->is_non_deterministic()))
        return false;
    } else {
      // Passthrough columns must be plain group fields the cache can store.
      if (it->type() != Item::FIELD_ITEM) return false;
      if (it->has_aggregation() || it->has_wf() || it->has_subquery() ||
          it->is_non_deterministic())
        return false;
      if (it->is_temporal() || it->data_type() == MYSQL_TYPE_JSON ||
          it->data_type() == MYSQL_TYPE_BIT ||
          it->data_type() == MYSQL_TYPE_GEOMETRY)
        return false;
      o.kind = HK_PASS;
      o.rtype = it->result_type();
      if (o.rtype != STRING_RESULT && o.rtype != INT_RESULT &&
          o.rtype != REAL_RESULT && o.rtype != DECIMAL_RESULT)
        return false;
    }
    out->push_back(o);
  }
  return !out->empty();
}

/**
 * @brief Return true for query shapes the aggregate override can execute.
 *
 * This whitelist is intentionally narrow: after the override is installed, the
 * row stream is owned by helios_override_executor.
 */
static bool helios_offloadable_shape(THD *thd, JOIN *join) {
  if (thd == nullptr || join == nullptr || thd->lex == nullptr) return false;
  // Only plain SELECT is eligible.
  if (thd->lex->sql_command != SQLCOM_SELECT) return false;
  // EXPLAIN ANALYZE executes the query, so skip all EXPLAIN variants.
  if (thd->lex->is_explain()) return false;
  Query_block *qb = join->query_block;
  if (qb == nullptr) return false;
  // Only the statement's outermost query block is safe to replace.
  if (qb->outer_query_block() != nullptr) return false;
  // No UNION or other set operations.
  Query_expression *qe = qb->master_query_expression();
  if (qe == nullptr || !qe->is_simple()) return false;
  if (qb->leaf_table_count != 1) return false;     // exactly one base table
  if (!qb->is_grouped()) return false;             // GROUP BY or aggregate funcs
  if (qb->is_distinct()) return false;             // DISTINCT not handled
  if (qb->having_cond() != nullptr) return false;  // HAVING not handled
  if (qb->has_limit()) return false;               // LIMIT/OFFSET handled later
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) return false;  // no ROLLUP
  if (qb->has_windows()) return false;                  // no window functions

  // Group keys are plain string fields; the executor orders them by strnxfrm.
  std::vector<Item *> gitems;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next) {
    Item *gi = *g->item;
    if (gi->type() != Item::FIELD_ITEM) return false;
    if (gi->result_type() != STRING_RESULT) return false;
    if (gi->is_temporal() || gi->data_type() == MYSQL_TYPE_JSON) return false;
    if (gi->has_aggregation() || gi->has_subquery()) return false;
    gitems.push_back(gi);
  }

  // Output columns must be executable by this aggregate path.
  std::vector<HeliosOut> probe;
  if (!helios_plan_outputs(join, &probe)) return false;

  // Without GROUP BY, passthrough columns have no group row to bind to.
  if (gitems.empty())
    for (const HeliosOut &o : probe)
      if (o.kind == HK_PASS) return false;

  // The executor emits ascending group-key order only.
  if (qb->order_list.elements != 0) {
    if (qb->order_list.elements != gitems.size()) return false;
    size_t i = 0;
    for (ORDER *ord = qb->order_list.first; ord != nullptr;
         ord = ord->next, ++i) {
      if (ord->direction == ORDER_DESC) return false;
      Item *oi = (*ord->item)->real_item();
      Item *gi = gitems[i]->real_item();
      if (oi->type() != Item::FIELD_ITEM || gi->type() != Item::FIELD_ITEM)
        return false;
      if (down_cast<Item_field *>(oi)->field !=
          down_cast<Item_field *>(gi)->field)
        return false;
    }
  }
  return true;
}

/**
 * @brief Allocate writable Item_cache cells for the already-described output.
 */
static bool helios_make_output_caches(JOIN *join,
                                      mem_root_deque<Item *> *row) {
  for (Item *orig : VisibleFields(join->query_block->fields)) {
    Item_cache *cache = Item_cache::get_cache(orig);
    if (cache == nullptr) return true;
    cache->setup(orig);
    cache->set_nullable(orig->is_nullable());
    cache->hidden = false;
    row->push_back(cache);
  }
  return false;
}

/**
 * @brief Serialize an aggregate argument for server-side decimal evaluation.
 */
// Non-static: also used by the LINEAIRDB_COLUMNAR secondary engine.
bool helios_serialize_arith(const Item *it,
                            LineairDB::Protocol::FilterExpr *out) {
  switch (it->type()) {
    case Item::FIELD_ITEM: {
      const Item_field *f = down_cast<const Item_field *>(it);
      out->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      out->set_column_index(f->field->field_index());
      return true;
    }
    case Item::INT_ITEM: {
      out->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
      out->set_int_val(const_cast<Item *>(it)->val_int());
      return true;
    }
    case Item::FUNC_ITEM: {
      const Item_func *fn = down_cast<const Item_func *>(it);
      using FE = LineairDB::Protocol::FilterExpr;
      FE::Op op;
      switch (fn->functype()) {
        case Item_func::PLUS_FUNC:  op = FE::OP_ADD; break;
        case Item_func::MINUS_FUNC: op = FE::OP_SUB; break;
        case Item_func::MUL_FUNC:   op = FE::OP_MUL; break;
        case Item_func::NEG_FUNC:   op = FE::OP_NEG; break;
        default: return false;
      }
      out->set_op(op);
      if (op == FE::OP_NEG) {
        if (fn->argument_count() != 1) return false;
        return helios_serialize_arith(fn->arguments()[0], out->add_children());
      }
      if (fn->argument_count() != 2) return false;
      return helios_serialize_arith(fn->arguments()[0], out->add_children()) &&
             helios_serialize_arith(fn->arguments()[1], out->add_children());
    }
    default:
      return false;
  }
}

static bool helios_q18_env_enabled(const char *name) {
  const char *v = std::getenv(name);
  return v != nullptr && std::strcmp(v, "1") == 0;
}

static constexpr uint32_t kHeliosAggregateHavingFilterColumns =
    std::numeric_limits<uint32_t>::max();

static bool helios_classify_having_sum(Item_sum *s, HeliosHavingDesc *out) {
  if (s == nullptr || out == nullptr) return false;
  if (s->has_wf() || s->has_subquery()) return false;
  switch (s->sum_func()) {
    case Item_sum::COUNT_FUNC:
      out->kind = HK_COUNT;
      if (s->argument_count() > 0) {
        Item *a0 = s->arguments()[0];
        if (!a0->const_item() || a0->is_nullable() || a0->is_null())
          return false;
      }
      break;
    case Item_sum::SUM_FUNC:
      out->kind = HK_SUM;
      break;
    case Item_sum::AVG_FUNC:
      out->kind = HK_AVG;
      break;
    default:
      return false;
  }
  out->rtype = s->result_type();
  if (out->kind != HK_COUNT && out->rtype != DECIMAL_RESULT &&
      out->rtype != INT_RESULT)
    return false;
  out->arg = (s->argument_count() > 0) ? s->arguments()[0] : nullptr;
  if (out->arg != nullptr &&
      (out->arg->has_subquery() || out->arg->has_aggregation() ||
       out->arg->is_non_deterministic()))
    return false;
  if (out->kind != HK_COUNT) {
    Item_field *field_arg = helios_safe_bare_decimal_aggregate_arg(out->arg);
    if (field_arg == nullptr) return false;
    out->arg = field_arg;
  }
  out->sum = s;
  return true;
}

static bool helios_parse_having(Query_block *qb, HeliosHavingDesc *out) {
  if (qb == nullptr || out == nullptr) return false;
  *out = HeliosHavingDesc();
  Item *h = qb->having_cond();
  if (h == nullptr) return true;

  if (h->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(h)->functype() == Item_func::COND_AND_FUNC) {
    Item *user_having = nullptr;
    for (Item &c : *down_cast<Item_cond *>(h)->argument_list()) {
      if (c.created_by_in2exists()) continue;
      if (user_having != nullptr) return false;
      user_having = &c;
    }
    if (user_having == nullptr) return true;
    h = user_having;
  } else if (h->created_by_in2exists()) {
    return true;
  }

  if (h->type() != Item::FUNC_ITEM) return false;
  Item_func *f = down_cast<Item_func *>(h);
  Item_func::Functype op = f->functype();
  switch (op) {
    case Item_func::EQ_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GT_FUNC:
    case Item_func::GE_FUNC:
      break;
    default:
      return false;
  }
  if (f->argument_count() != 2) return false;

  Item *a = f->arguments()[0]->real_item();
  Item *b = f->arguments()[1]->real_item();
  Item *sum_side = nullptr;
  Item *const_side = nullptr;
  bool sum_on_left = false;
  if (a->type() == Item::SUM_FUNC_ITEM && b->const_item()) {
    sum_side = a;
    const_side = b;
    sum_on_left = true;
  } else if (b->type() == Item::SUM_FUNC_ITEM && a->const_item()) {
    sum_side = b;
    const_side = a;
    sum_on_left = false;
  } else {
    return false;
  }
  if (const_side->has_subquery() || const_side->is_non_deterministic())
    return false;
  const Item_result crt = const_side->result_type();
  if (crt != INT_RESULT && crt != DECIMAL_RESULT) return false;
  /*
    DECIMAL constants need the same precision bound as DECIMAL aggregate
    arguments: both operands are parsed into signed __int128 mantissas before
    scale alignment, so a wider literal can overflow even when SUM/AVG input is
    safe. INT_RESULT is exempt because LLONG_MAX is already inside the same
    __int128 headroom. This is the literal mantissa precision reported by
    decimal_precision(), not the byte count of any post-scale-align product.
  */
  if (crt == DECIMAL_RESULT &&
      const_side->decimal_precision() > kHeliosAggregateDecimalPrecisionMax)
    return false;
  if (!helios_classify_having_sum(down_cast<Item_sum *>(sum_side), out))
    return false;

  if (sum_on_left) {
    out->op = op;
  } else {
    switch (op) {
      case Item_func::LT_FUNC:
        out->op = Item_func::GT_FUNC;
        break;
      case Item_func::LE_FUNC:
        out->op = Item_func::GE_FUNC;
        break;
      case Item_func::GT_FUNC:
        out->op = Item_func::LT_FUNC;
        break;
      case Item_func::GE_FUNC:
        out->op = Item_func::LE_FUNC;
        break;
      default:
        out->op = op;
        break;
    }
  }
  out->cnst = const_side;
  return true;
}

static bool helios_build_phase_b_spec(
    TABLE *t, bool ro_novalidate, const std::vector<HeliosOut> &outs,
    const std::vector<Item *> &gitems, const HeliosHavingDesc &having,
    std::vector<int> *out_agg, std::vector<int> *out_grp, int *h_agg_pos,
    std::string *spec_ser) {
  if (t == nullptr || out_agg == nullptr || out_grp == nullptr ||
      h_agg_pos == nullptr || spec_ser == nullptr)
    return false;
  const size_t n = outs.size();
  out_agg->assign(n, -1);
  out_grp->assign(n, -1);
  *h_agg_pos = -1;
  spec_ser->clear();
  if (!ro_novalidate) return false;
  for (Item *gi : gitems)
    if (gi == nullptr || gi->is_nullable()) return false;

  int agg_pos = 0;
  for (size_t c = 0; c < n; ++c) {
    if (outs[c].kind == HK_PASS) {
      Field *of = down_cast<Item_field *>(outs[c].orig)->field;
      for (size_t g = 0; g < gitems.size(); ++g) {
        if (down_cast<Item_field *>(gitems[g])->field == of) {
          (*out_grp)[c] = static_cast<int>(g);
          break;
        }
      }
      if ((*out_grp)[c] < 0) return false;
    } else {
      (*out_agg)[c] = agg_pos++;
    }
  }

  LineairDB::Protocol::AggregateSpec spec;
  spec.set_num_columns(t->s->fields);
  for (Item *gi : gitems) {
    spec.add_group_columns(down_cast<Item_field *>(gi)->field->field_index());
  }
  for (size_t c = 0; c < n; ++c) {
    if (outs[c].kind == HK_PASS) continue;
    auto *af = spec.add_aggs();
    if (outs[c].kind == HK_COUNT) {
      af->set_kind(LineairDB::Protocol::AggFunc::AGG_COUNT);
    } else {
      af->set_kind(outs[c].kind == HK_SUM
                       ? LineairDB::Protocol::AggFunc::AGG_SUM
                       : LineairDB::Protocol::AggFunc::AGG_AVG);
      if (outs[c].rtype != DECIMAL_RESULT || outs[c].arg == nullptr ||
          !helios_serialize_arith(outs[c].arg, af->mutable_arg()))
        return false;
    }
    af->set_result_scale(0);
  }
  if (having.sum != nullptr) {
    auto *af = spec.add_aggs();
    if (having.kind == HK_COUNT) {
      af->set_kind(LineairDB::Protocol::AggFunc::AGG_COUNT);
    } else {
      af->set_kind(having.kind == HK_SUM
                       ? LineairDB::Protocol::AggFunc::AGG_SUM
                       : LineairDB::Protocol::AggFunc::AGG_AVG);
      if (having.rtype != DECIMAL_RESULT || having.arg == nullptr ||
          !helios_serialize_arith(having.arg, af->mutable_arg()))
        return false;
    }
    af->set_result_scale(0);
    *h_agg_pos = spec.aggs_size() - 1;
  }
  spec.SerializeToString(spec_ser);
  return true;
}

static bool helios_build_aggregate_having_filter(
    const HeliosHavingDesc &having, uint32_t group_row_value_col,
    std::string *out) {
  if (out == nullptr || having.sum == nullptr || having.cnst == nullptr)
    return false;
  out->clear();
  if (having.kind == HK_AVG) return false;

  LineairDB::Protocol::FilterExpr::Op op;
  switch (having.op) {
    case Item_func::GT_FUNC:
      op = LineairDB::Protocol::FilterExpr::OP_GT;
      break;
    case Item_func::GE_FUNC:
      op = LineairDB::Protocol::FilterExpr::OP_GE;
      break;
    case Item_func::LT_FUNC:
      op = LineairDB::Protocol::FilterExpr::OP_LT;
      break;
    case Item_func::LE_FUNC:
      op = LineairDB::Protocol::FilterExpr::OP_LE;
      break;
    case Item_func::EQ_FUNC:
      op = LineairDB::Protocol::FilterExpr::OP_EQ;
      break;
    case Item_func::NE_FUNC:
      op = LineairDB::Protocol::FilterExpr::OP_NE;
      break;
    default:
      return false;
  }

  my_decimal cval;
  my_decimal *cp = having.cnst->val_decimal(&cval);
  if (cp == nullptr || having.cnst->null_value) return false;
  String cstr;
  if (my_decimal2string(E_DEC_FATAL_ERROR, cp, &cstr) != E_DEC_OK)
    return false;

  LineairDB::Protocol::PushedPredicate pred;
  pred.set_num_columns(kHeliosAggregateHavingFilterColumns);
  auto *expr = pred.mutable_expr();
  expr->set_op(op);
  auto *lhs = expr->add_children();
  lhs->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
  lhs->set_column_index(group_row_value_col);
  auto *rhs = expr->add_children();
  rhs->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
  rhs->set_string_val(std::string(cstr.ptr(), cstr.length()));
  pred.SerializeToString(out);
  return true;
}

static std::string helios_phys_table_key(const TABLE *t) {
  if (t == nullptr || t->s == nullptr) return std::string();
  const TABLE_SHARE *s = t->s;
  std::string key = "./";
  if (s->db.str != nullptr && s->db.length > 0)
    key.append(s->db.str, s->db.length);
  key.push_back('/');
  if (s->table_name.str != nullptr && s->table_name.length > 0)
    key.append(s->table_name.str, s->table_name.length);
  return key;
}

static Item_in_subselect *helios_as_where_in_subselect(Item *item) {
  if (item == nullptr) return nullptr;
  Item *cur = item->real_item();
  if (cur == nullptr) return nullptr;
  if (cur->type() == Item::CACHE_ITEM) {
    cur = down_cast<Item_cache *>(cur)->get_example();
    if (cur == nullptr) return nullptr;
    cur = cur->real_item();
    if (cur == nullptr) return nullptr;
  }
  if (cur->type() == Item::SUBSELECT_ITEM) {
    Item_subselect *subselect = down_cast<Item_subselect *>(cur);
    if (subselect->substype() == Item_subselect::IN_SUBS)
      return down_cast<Item_in_subselect *>(subselect);
  }

  if (cur->type() != Item::FUNC_ITEM) return nullptr;
  Item_func *func = down_cast<Item_func *>(cur);
  if (func->argument_count() != 1 ||
      std::strcmp(func->func_name(), "<in_optimizer>") != 0)
    return nullptr;
  Item *arg = func->get_arg(0);
  if (arg == nullptr) return nullptr;
  arg = arg->real_item();
  if (arg == nullptr || arg->type() != Item::SUBSELECT_ITEM)
    return nullptr;
  Item_subselect *subselect = down_cast<Item_subselect *>(arg);
  if (subselect->substype() != Item_subselect::IN_SUBS) return nullptr;
  return down_cast<Item_in_subselect *>(subselect);
}

static bool helios_is_outer_where_top_level_in(Item *wc,
                                               const Item_in_subselect *insub) {
  if (wc == nullptr || insub == nullptr) return false;
  if (helios_as_where_in_subselect(wc) == insub) return true;
  if (wc->type() != Item::COND_ITEM ||
      down_cast<Item_cond *>(wc)->functype() != Item_func::COND_AND_FUNC)
    return false;
  for (Item &arg : *down_cast<Item_cond *>(wc)->argument_list()) {
    if (helios_as_where_in_subselect(&arg) == insub) return true;
  }
  return false;
}

static bool helios_table_belongs_to_query_block(const Query_block *qb,
                                                const TABLE *table) {
  if (qb == nullptr || table == nullptr) return false;
  for (const Table_ref *tr = qb->leaf_tables; tr != nullptr;
       tr = tr->next_leaf) {
    if (tr->table == table) return true;
  }
  return false;
}

// The grouped semijoin is bound to the outer probe scan at injection time by
// matching the physical table key against a read-plan step (lineairdb_autogen
// .cc), taking the first match. The autogen path stages the whole statement --
// the outer block, materialized derived blocks, and item-held inner units -- in
// one step vector and folds byte-identical scans, so a probe table that also
// appears in another query block can leave an earlier or folded same-key step
// that the membership filter binds to instead, silently dropping rows for an
// alias the IN never constrained (and the same drop pass that removes the inner
// base scan only re-serves it through the bound step). Requiring the probe's
// physical table to be unique across the whole statement makes that bind
// unambiguous, so there is exactly one candidate scan and the inner-scan drop
// and the membership injection stay paired.
static bool helios_phys_table_unique_in_statement(THD *thd,
                                                  const TABLE *table) {
  if (thd == nullptr || thd->lex == nullptr || table == nullptr) return false;
  const std::string key = helios_phys_table_key(table);
  if (key.empty()) return false;
  int count = 0;
  for (const Table_ref *tr = thd->lex->query_tables; tr != nullptr;
       tr = tr->next_global) {
    if (tr->table != nullptr && helios_phys_table_key(tr->table) == key) {
      if (++count > 1) return false;
    }
  }
  return count == 1;
}

static bool helios_grouped_semijoin_integer_key_type(const Field *f) {
  if (f == nullptr || f->result_type() != INT_RESULT) return false;
  switch (f->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      return true;
    default:
      return false;
  }
}

static bool helios_grouped_semijoin_key_bytes_match_sql_equality(
    const Field *gf, const Field *of) {
  if (!helios_grouped_semijoin_integer_key_type(gf) ||
      !helios_grouped_semijoin_integer_key_type(of))
    return false;
  if (gf->is_unsigned() != of->is_unsigned()) return false;
  // The server groups and probes membership on raw Field::val_str() bytes, which
  // is canonical decimal text for integers EXCEPT under ZEROFILL: val_str() then
  // left-pads to the declared display width (Field_num::prepend_zeros), so two
  // SQL-equal values in columns of different display width serialize to distinct
  // bytes and a surviving outer row is silently dropped. The integer-type check
  // above guarantees a Field_num, so read the same flag val_str() consults.
  if (down_cast<const Field_num *>(gf)->zerofill ||
      down_cast<const Field_num *>(of)->zerofill)
    return false;
  return true;
}

static void helios_try_register_grouped_semijoin(THD *thd, JOIN *join) {
  const bool semijoin_gate = helios_q18_env_enabled("HELIOS_Q18_SEMIJOIN");
  const bool gs_gate = helios_q18_env_enabled("HELIOS_Q18_GS");
  if (!semijoin_gate && !gs_gate) return;
  if (thd == nullptr || join == nullptr || thd->lex == nullptr) return;
  if (thd->lex->sql_command != SQLCOM_SELECT || thd->lex->is_explain()) return;

  Query_block *qb = join->query_block;
  if (qb == nullptr || qb->outer_query_block() == nullptr) return;
  Query_block *oqb = qb->outer_query_block();
  Query_expression *qe = qb->master_query_expression();
  if (qe == nullptr || !qe->is_simple() || qe->uncacheable != 0) return;
  if (qe->item == nullptr ||
      qe->item->substype() != Item_subselect::IN_SUBS)
    return;
  Item_in_subselect *insub = down_cast<Item_in_subselect *>(qe->item);
  if (insub->left_expr == nullptr) return;
  // The outer scan can be reduced only when the IN predicate itself is a
  // positive top-level conjunct of the outer block's WHERE. MySQL may store
  // that conjunct as the Item_in_subselect directly or as Item_in_optimizer
  // with the subselect in args[0]; anything nested below OR/NOT/comparison is
  // not proof that surviving outer rows are present in the grouped key set.
  Item *outer_where = oqb->where_cond();
  if (!helios_is_outer_where_top_level_in(outer_where, insub)) return;
  // Reject negated / IS-FALSE forms. NOT IN and the IS-[NOT]-FALSE family share
  // substype()==IN_SUBS with a positive IN, distinguished only by value_transform;
  // they flip membership, so a positive grouped-semijoin would return the
  // complement (silent wrong rows). Only the positive transforms are safe: plain
  // IN is BOOL_IDENTITY, and a WHERE-context IN usually becomes BOOL_IS_TRUE.
  if (insub->value_transform != Item::BOOL_IDENTITY &&
      insub->value_transform != Item::BOOL_IS_TRUE)
    return;

  if (qb->leaf_table_count != 1 || !qb->is_grouped()) return;
  if (qb->is_distinct() || qb->has_limit() ||
      qb->olap != UNSPECIFIED_OLAP_TYPE || qb->has_windows() ||
      qb->group_list.elements != 1 || qb->having_cond() == nullptr)
    return;
  if (qb->where_cond() != nullptr) return;

  TABLE *t = qb->leaf_tables != nullptr ? qb->leaf_tables->table : nullptr;
  if (t == nullptr || t->file == nullptr || t->file->ht != lineairdb_hton)
    return;
  ha_lineairdb *hl = down_cast<ha_lineairdb *>(t->file);
  if (!hl->tx_ro_novalidate()) return;

  Item *gi = *qb->group_list.first->item;
  if (gi->type() != Item::FIELD_ITEM || gi->is_nullable()) return;
  Field *gf = down_cast<Item_field *>(gi)->field;
  if (gf == nullptr || gf->is_nullable()) return;

  std::vector<HeliosOut> outs;
  if (!helios_plan_outputs(join, &outs)) return;
  if (outs.size() != 1 || outs[0].kind != HK_PASS) return;
  if (down_cast<Item_field *>(outs[0].orig)->field != gf) return;

  HeliosHavingDesc having;
  if (!helios_parse_having(qb, &having) || having.sum == nullptr) return;

  std::vector<Item *> gitems{gi};
  std::vector<int> out_agg, out_grp;
  int h_agg_pos = -1;
  std::string spec_ser;
  if (!helios_build_phase_b_spec(t, hl->tx_ro_novalidate(), outs, gitems,
                                 having, &out_agg, &out_grp, &h_agg_pos,
                                 &spec_ser))
    return;
  if (h_agg_pos < 0) return;
  std::string having_filter;
  const uint32_t having_value_col =
      static_cast<uint32_t>(gitems.size() + 2 * h_agg_pos);
  if (!helios_build_aggregate_having_filter(having, having_value_col,
                                            &having_filter))
    return;

  Item *lhs = insub->left_expr->real_item();
  if (lhs->type() != Item::FIELD_ITEM) return;
  Field *of = down_cast<Item_field *>(lhs)->field;
  if (of == nullptr || of->table == nullptr || of->table->file == nullptr ||
      of->table->file->ht != lineairdb_hton)
    return;
  TABLE *ot = of->table;
  if (ot == t) return;
  if (!helios_table_belongs_to_query_block(oqb, ot)) return;
  // The injection binds the membership filter to the first read-plan step whose
  // physical table key matches, and the autogen path stages the whole statement
  // (outer block, derived blocks, inner units) into one folded step vector.
  // Require the probe's physical table to be unique across the statement so that
  // bind is unambiguous: a same-key scan from another block or a folded
  // multi-alias step would otherwise take the filter and drop unconstrained rows.
  if (!helios_phys_table_unique_in_statement(thd, ot)) return;

  int probe_col = -1;
  for (uint i = 0; i < ot->s->fields; ++i) {
    if (ot->field[i] == of) {
      probe_col = static_cast<int>(i);
      break;
    }
  }
  if (probe_col < 0) return;

  const Table_ref *otr = ot->pos_in_table_list;
  if (otr == nullptr || otr->is_inner_table_of_outer_join()) return;
  for (const Table_ref *emb = otr->embedding; emb != nullptr;
       emb = emb->embedding) {
    if (emb->is_sj_or_aj_nest()) return;
  }
  if (otr->query_block == nullptr ||
      otr->query_block->outer_query_block() != nullptr)
    return;
  if (of->is_nullable()) return;
  if (gf->type() != of->type() || gf->pack_length() != of->pack_length())
    return;
  if (gf->result_type() == STRING_RESULT && gf->charset() != of->charset())
    return;
  if (!helios_grouped_semijoin_key_bytes_match_sql_equality(gf, of)) return;

  const std::string inner_key = helios_phys_table_key(t);
  const std::string outer_key = helios_phys_table_key(ot);
  if (inner_key.empty() || outer_key.empty()) return;

  // At most one grouped semijoin per statement. The inner aggregate is served
  // from a per-statement cache keyed by physical inner-table name (gs_regs_ /
  // grouped_semijoin_groups_), so a second registration over the same inner
  // table -- e.g. a second top-level IN conjunct on the same probe with a
  // different HAVING -- would reuse the first aggregate's group set and silently
  // answer the second IN with the wrong membership. q18 has a single grouped IN;
  // reject any further candidate and let it run through normal execution.
  if (hl->tx_has_grouped_semijoin()) return;

  if (semijoin_gate) {
    LineairDBTransaction::GroupedSemijoin gs;
    gs.inner_table_key = inner_key;
    gs.agg_spec = spec_ser;
    gs.having_filter = having_filter;
    gs.outer_table_key = outer_key;
    gs.outer_probe_column = static_cast<uint32_t>(probe_col);
    hl->tx_register_grouped_semijoin(std::move(gs));
  }

  if (!gs_gate) return;
  if (having.kind != HK_SUM || having.arg == nullptr) return;
  Item *sarg = having.arg->real_item();
  if (sarg->type() != Item::FIELD_ITEM || sarg->is_nullable()) return;
  Field *sumf = down_cast<Item_field *>(sarg)->field;
  if (sumf == nullptr || sumf->table != t || sumf == gf ||
      sumf->is_nullable())
    return;

  int sum_col = -1;
  int grp_col = -1;
  for (uint i = 0; i < t->s->fields; ++i) {
    if (t->field[i] == sumf) sum_col = static_cast<int>(i);
    if (t->field[i] == gf) grp_col = static_cast<int>(i);
  }
  if (sum_col < 0 || grp_col < 0) return;
  for (uint i = 0; i < t->s->fields; ++i) {
    if (!bitmap_is_set(t->read_set, i)) continue;
    if (static_cast<int>(i) != sum_col && static_cast<int>(i) != grp_col)
      return;
  }

  LineairDBTransaction::GsRegistration reg;
  reg.spec = std::move(spec_ser);
  reg.filter = std::move(having_filter);
  reg.template_cols.assign(t->s->fields, "0");
  reg.group_col = static_cast<uint32_t>(grp_col);
  reg.col_a = static_cast<uint32_t>(sum_col);
  reg.col_b = static_cast<uint32_t>(sum_col);
  reg.single_sum = true;
  hl->tx_register_gs(std::move(reg));
}

/**
 * @brief Execute a supported single-table aggregate SELECT.
 *
 * The caller owns result metadata and EOF. This function only sends data rows.
 * It first tries server-side aggregation, then falls back to local aggregation.
 */
static bool helios_override_executor(JOIN *join, Query_result *query_result) {
  THD *thd = join->thd;
  Query_block *qb = join->query_block;
  TABLE *t = qb->leaf_tables->table;
  join->send_records = 0;

  std::vector<HeliosOut> outs;
  if (!helios_plan_outputs(join, &outs)) return true;  // whitelist already checked
  const size_t n = outs.size();

  std::vector<Item *> gitems;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next)
    gitems.push_back(*g->item);
  const bool implicit = gitems.empty();

  // Try server-side aggregation first.
  {
    bool server_b = true;
    for (const HeliosOut &o : outs)
      if (o.kind != HK_PASS && o.kind != HK_COUNT &&
          o.kind != HK_SUM && o.kind != HK_AVG) server_b = false;
    // Group rows do not carry a per-base-row validation footprint.
    if (!down_cast<ha_lineairdb *>(t->file)->tx_ro_novalidate()) server_b = false;
    // Server group-key decoding does not represent NULL separately yet.
    for (Item *gi : gitems)
      if (gi->is_nullable()) server_b = false;
    std::vector<int> out_agg(n, -1), out_grp(n, -1);
    if (server_b) {
      int agg_pos = 0;
      for (size_t c = 0; c < n && server_b; ++c) {
        if (outs[c].kind == HK_PASS) {
          Field *of = down_cast<Item_field *>(outs[c].orig)->field;
          for (size_t g = 0; g < gitems.size(); ++g)
            if (down_cast<Item_field *>(gitems[g])->field == of) { out_grp[c] = (int)g; break; }
          if (out_grp[c] < 0) server_b = false;  // passthrough not a group col
        } else {
          out_agg[c] = agg_pos++;
        }
      }
    }
    if (server_b) {
      LineairDB::Protocol::AggregateSpec spec;
      spec.set_num_columns(t->s->fields);
      for (Item *gi : gitems)
        spec.add_group_columns(down_cast<Item_field *>(gi)->field->field_index());
      for (size_t c = 0; c < n && server_b; ++c) {
        if (outs[c].kind == HK_PASS) continue;
        auto *af = spec.add_aggs();
        if (outs[c].kind == HK_COUNT) {
          af->set_kind(LineairDB::Protocol::AggFunc::AGG_COUNT);
        } else {  // HK_SUM / HK_AVG: serialize the exact-decimal arg expression
          af->set_kind(outs[c].kind == HK_SUM
                           ? LineairDB::Protocol::AggFunc::AGG_SUM
                           : LineairDB::Protocol::AggFunc::AGG_AVG);
          // Non-decimal SUM/AVG stay on the local fallback path.
          if (outs[c].rtype != DECIMAL_RESULT || outs[c].arg == nullptr ||
              !helios_serialize_arith(outs[c].arg, af->mutable_arg())) {
            server_b = false;
            break;
          }
        }
        af->set_result_scale(0);
      }
      if (!server_b) {
        goto local_aggregate_fallback;
      }
      std::string spec_ser;
      spec.SerializeToString(&spec_ser);
      ha_lineairdb *hl = down_cast<ha_lineairdb *>(t->file);
      if (!hl->tx_set_pushed_aggregate(spec_ser)) {
        goto local_aggregate_fallback;
      }

      int err = t->file->ha_rnd_init(true);
      if (err) {
        hl->tx_clear_pushed_aggregate();
        t->file->print_error(err, MYF(0));
        return true;
      }
      const size_t ng = gitems.size();
      const int n_aggs = spec.aggs_size();

      auto parse_fields = [](std::string_view row, std::vector<std::string_view> *fv,
                             std::vector<bool> *nul) -> bool {
        size_t off = 0;
        while (off < row.size()) {
          uint8_t bs = static_cast<uint8_t>(row[off++]);
          if (bs == 0xFF) { fv->emplace_back(); nul->push_back(true); continue; }
          if (off + bs > row.size()) return false;
          size_t len = 0;
          for (uint8_t i = 0; i < bs; ++i)
            len |= static_cast<size_t>(static_cast<uint8_t>(row[off + i])) << (8 * i);
          off += bs;
          if (off + len > row.size()) return false;
          fv->push_back(std::string_view(row.data() + off, len));
          nul->push_back(false);
          off += len;
        }
        return true;
      };

      std::map<std::string, std::vector<HeliosAccum>> groups;
      std::string_view raw;
      while (hl->agg_next_raw(&raw)) {
        std::vector<std::string_view> fv;
        std::vector<bool> nul;
        // Malformed group rows cannot be rechecked from base rows here.
        if (!parse_fields(raw, &fv, &nul) ||
            fv.size() != 1 + ng + 2 * static_cast<size_t>(n_aggs)) {
          t->file->ha_rnd_end();
          hl->tx_clear_pushed_aggregate();
          my_error(ER_INTERNAL_ERROR, MYF(0), "helios agg group row malformed");
          return true;
        }
        std::string key;
        for (size_t g = 0; g < ng; ++g) {
          std::string_view gvsv = fv[1 + g];
          if (nul[1 + g]) { key.push_back('\0'); continue; }
          const CHARSET_INFO *cs = gitems[g]->collation.collation;
          const uint nweights = gitems[g]->max_char_length();
          size_t cap = cs->coll->strnxfrmlen(
              cs, static_cast<size_t>(gitems[g]->max_length) + cs->mbmaxlen);
          if (cap < 1) cap = 1;
          std::string w(cap, '\0');
          const size_t wn = cs->coll->strnxfrm(
              cs, reinterpret_cast<uchar *>(&w[0]), w.size(), nweights,
              reinterpret_cast<const uchar *>(gvsv.data()), gvsv.size(),
              MY_STRXFRM_PAD_TO_MAXLEN);
          w.resize(wn);
          key.push_back('\1');
          key.append(w);
        }
        auto it = groups.find(key);
        std::vector<HeliosAccum> *grp;
        if (it == groups.end()) {
          auto &v = groups[key];
          v.resize(n);
          for (size_t c = 0; c < n; ++c)
            if (outs[c].kind == HK_PASS) {
              std::string_view gv = fv[1 + out_grp[c]];
              if (nul[1 + out_grp[c]]) v[c].p_null = true;
              else v[c].p_str.copy(gv.data(), gv.size(),
                                   gitems[out_grp[c]]->collation.collation);
            }
          grp = &v;
        } else {
          grp = &it->second;
        }
        for (size_t c = 0; c < n; ++c) {
          const HeliosOut &o = outs[c];
          if (o.kind == HK_PASS) continue;
          const size_t vi = 1 + ng + 2 * out_agg[c];  // value column in fv
          const size_t ci = vi + 1;                    // count column in fv
          HeliosAccum &a = (*grp)[c];
          if (o.kind == HK_COUNT) {
            if (vi < fv.size() && !nul[vi]) {
              std::string s(fv[vi]);
              a.cnt += std::strtoll(s.c_str(), nullptr, 10);
            }
          } else {  // HK_SUM / HK_AVG: exact decimal sum + non-null count
            if (vi < fv.size() && !nul[vi]) {
              my_decimal d;
              str2my_decimal(E_DEC_FATAL_ERROR, fv[vi].data(), fv[vi].size(),
                             &my_charset_bin, &d);
              if (!a.dec_init) { a.dec = d; a.dec_init = true; }
              else {
                my_decimal tmp;
                my_decimal_add(E_DEC_FATAL_ERROR, &tmp, &a.dec, &d);
                a.dec = tmp;
              }
            }
            if (ci < fv.size() && !nul[ci]) {
              std::string s(fv[ci]);
              a.cnt += std::strtoll(s.c_str(), nullptr, 10);
            }
          }
        }
      }
      t->file->ha_rnd_end();
      hl->tx_clear_pushed_aggregate();
      // Staging abort must not look like a clean empty aggregate.
      if (hl->tx_is_aborted()) {
        my_error(ER_LOCK_DEADLOCK, MYF(0));
        return true;
      }

      mem_root_deque<Item *> row(thd->mem_root);
      if (helios_make_output_caches(join, &row)) return true;
      const int div_inc = static_cast<int>(thd->variables.div_precincrement);
      for (auto &kv : groups) {
        std::vector<HeliosAccum> &g = kv.second;
        for (size_t c = 0; c < n; ++c) {
          Item_cache *cache = down_cast<Item_cache *>(row[c]);
          const HeliosOut &o = outs[c];
          HeliosAccum &a = g[c];
          if (o.kind == HK_PASS) {
            if (a.p_null) { cache->store_null(); continue; }
            cache->null_value = false;
            down_cast<Item_cache_str *>(cache)->store_value(cache, a.p_str);
          } else if (o.kind == HK_COUNT) {
            cache->null_value = false;
            down_cast<Item_cache_int *>(cache)->store_value(cache, a.cnt);
          } else if (o.kind == HK_SUM) {
            if (a.cnt == 0) { cache->store_null(); continue; }
            cache->null_value = false;
            down_cast<Item_cache_decimal *>(cache)->store_value(cache, &a.dec);
          } else {  // HK_AVG
            if (a.cnt == 0) { cache->store_null(); continue; }
            cache->null_value = false;
            my_decimal cnt_dec, res;
            int2my_decimal(E_DEC_FATAL_ERROR, a.cnt, false, &cnt_dec);
            my_decimal_div(E_DEC_FATAL_ERROR, &res, &a.dec, &cnt_dec, div_inc);
            down_cast<Item_cache_decimal *>(cache)->store_value(cache, &res);
          }
        }
        if (query_result->send_data(thd, row)) return true;
        ++join->send_records;
      }
      return false;
    }
  }
local_aggregate_fallback:;

  Item *where = join->where_cond != nullptr ? join->where_cond : qb->where_cond();

  std::map<std::string, std::vector<HeliosAccum>> groups;
  std::vector<HeliosAccum> *implicit_grp = nullptr;
  if (implicit) {  // implicit grouping emits exactly one row even over 0 input rows
    auto &v = groups[std::string()];
    v.resize(n);
    implicit_grp = &v;
  }

  my_decimal dec_buf;
  String str_buf;

  int err = t->file->ha_rnd_init(true);
  if (err) { t->file->print_error(err, MYF(0)); return true; }
  while ((err = t->file->ha_rnd_next(t->record[0])) == 0) {
    if (thd->killed) { t->file->ha_rnd_end(); thd->send_kill_message(); return true; }
    if (where != nullptr) {
      const longlong pass = where->val_int();
      if (thd->is_error()) { t->file->ha_rnd_end(); return true; }
      if (where->null_value || pass == 0) continue;
    }

    std::vector<HeliosAccum> *grp;
    if (implicit) {
      grp = implicit_grp;
    } else {
      std::string key;
      for (Item *gi : gitems) {
        String *s = gi->val_str(&str_buf);
        if (gi->null_value || s == nullptr) { key.push_back('\0'); continue; }
        // strnxfrm produces a collation-sortable, fixed-width group-key part.
        const CHARSET_INFO *cs = s->charset();
        const uint nweights = gi->max_char_length();
        size_t cap = cs->coll->strnxfrmlen(
            cs, (static_cast<size_t>(gi->max_length) + cs->mbmaxlen));
        if (cap < 1) cap = 1;
        std::string w(cap, '\0');
        const size_t wn = cs->coll->strnxfrm(
            cs, reinterpret_cast<uchar *>(&w[0]), w.size(), nweights,
            reinterpret_cast<const uchar *>(s->ptr()), s->length(),
            MY_STRXFRM_PAD_TO_MAXLEN);
        w.resize(wn);
        key.push_back('\1');
        key.append(w);
      }
      auto it = groups.find(key);
      if (it != groups.end()) {
        grp = &it->second;
      } else {
        auto &v = groups[key];
        v.resize(n);
        // Passthrough values are group fields, so the first row is enough.
        for (size_t c = 0; c < n; ++c) {
          if (outs[c].kind != HK_PASS) continue;
          Item *oi = outs[c].orig;
          HeliosAccum &a = v[c];
          switch (outs[c].rtype) {
            case STRING_RESULT: {
              String *sv = oi->val_str(&str_buf);
              if (oi->null_value || sv == nullptr) a.p_null = true;
              else a.p_str.copy(sv->ptr(), sv->length(), sv->charset());
              break; }
            case DECIMAL_RESULT: {
              my_decimal *d = oi->val_decimal(&dec_buf);
              if (oi->null_value || d == nullptr) a.p_null = true; else a.p_dec = *d;
              break; }
            case REAL_RESULT:
              a.p_dbl = oi->val_real(); a.p_null = oi->null_value; break;
            default:
              a.p_int = oi->val_int(); a.p_null = oi->null_value; break;
          }
        }
        grp = &v;
      }
    }

    for (size_t c = 0; c < n; ++c) {
      const HeliosOut &o = outs[c];
      if (o.kind == HK_PASS) continue;
      HeliosAccum &a = (*grp)[c];
      if (o.kind == HK_COUNT) { ++a.cnt; continue; }
      if (o.rtype == REAL_RESULT) {
        const double v = o.arg->val_real();
        if (!o.arg->null_value) { a.dbl += v; ++a.cnt; }
      } else {  // DECIMAL/INT accumulate as decimal
        my_decimal *d = o.arg->val_decimal(&dec_buf);
        if (!o.arg->null_value && d != nullptr) {
          if (!a.dec_init) { a.dec = *d; a.dec_init = true; }
          else {
            // my_decimal_add does not support aliasing output with input.
            my_decimal tmp;
            my_decimal_add(E_DEC_FATAL_ERROR, &tmp, &a.dec, d);
            a.dec = tmp;
          }
          ++a.cnt;
        }
      }
    }
  }
  const int end_err = t->file->ha_rnd_end();
  if (err != HA_ERR_END_OF_FILE) { t->file->print_error(err, MYF(0)); return true; }
  if (end_err) { t->file->print_error(end_err, MYF(0)); return true; }

  mem_root_deque<Item *> row(thd->mem_root);
  if (helios_make_output_caches(join, &row)) return true;
  const int div_inc = static_cast<int>(thd->variables.div_precincrement);

  for (auto &kv : groups) {  // std::map => ascending group-key order
    std::vector<HeliosAccum> &g = kv.second;
    for (size_t c = 0; c < n; ++c) {
      Item_cache *cache = down_cast<Item_cache *>(row[c]);
      const HeliosOut &o = outs[c];
      HeliosAccum &a = g[c];
      if (o.kind == HK_PASS) {
        if (a.p_null) { cache->store_null(); continue; }
        cache->null_value = false;
        switch (o.rtype) {
          case STRING_RESULT:
            down_cast<Item_cache_str *>(cache)->store_value(cache, a.p_str); break;
          case DECIMAL_RESULT:
            down_cast<Item_cache_decimal *>(cache)->store_value(cache, &a.p_dec); break;
          case REAL_RESULT:
            down_cast<Item_cache_real *>(cache)->store_value(cache, a.p_dbl); break;
          default:
            down_cast<Item_cache_int *>(cache)->store_value(cache, a.p_int); break;
        }
      } else if (o.kind == HK_COUNT) {
        cache->null_value = false;
        down_cast<Item_cache_int *>(cache)->store_value(cache, a.cnt);
      } else if (o.kind == HK_SUM) {
        // SUM over zero non-NULL inputs is NULL in SQL (not 0).
        if (a.cnt == 0) { cache->store_null(); continue; }
        cache->null_value = false;
        if (o.rtype == REAL_RESULT)
          down_cast<Item_cache_real *>(cache)->store_value(cache, a.dbl);
        else
          down_cast<Item_cache_decimal *>(cache)->store_value(cache, &a.dec);
      } else {  // HK_AVG
        if (a.cnt == 0) { cache->store_null(); continue; }
        cache->null_value = false;
        if (o.rtype == REAL_RESULT) {
          down_cast<Item_cache_real *>(cache)->store_value(cache, a.dbl / static_cast<double>(a.cnt));
        } else {
          my_decimal cnt_dec, res;
          int2my_decimal(E_DEC_FATAL_ERROR, a.cnt, false, &cnt_dec);
          if (!a.dec_init) my_decimal_set_zero(&a.dec);
          my_decimal_div(E_DEC_FATAL_ERROR, &res, &a.dec, &cnt_dec, div_inc);
          down_cast<Item_cache_decimal *>(cache)->store_value(cache, &res);
        }
      }
    }
    if (query_result->send_data(thd, row)) return true;
    ++join->send_records;
  }
  return false;
}

// Row count at or above which a grouped aggregate is served by the override
// (a primary full scan) rather than a selective secondary index.
static constexpr double kSmallAggregateInputRows = 1000.0;  // FIXME: make configurable

/**
 * @brief Return true when the chosen leaf is a full PRIMARY scan.
 *
 * @details The override reads the table by a primary full scan, and autogen
 * stages that primary range. Install it only for such a leaf; a secondary or
 * bounded leaf would be staged as a scan the override never reads, so prefetch
 * would miss and abort. Other leaves run on MySQL's normal executor.
 */
static bool helios_leaf_is_full_primary_scan(AccessPath *root_path,
                                             JOIN *join) {
  if (root_path == nullptr || join == nullptr || join->query_block == nullptr ||
      join->query_block->leaf_tables == nullptr) {
    return false;
  }

  AccessPath *leaf = nullptr;
  WalkAccessPaths(root_path, join, WalkAccessPathPolicy::ENTIRE_TREE,
                  [&leaf](AccessPath *path, const JOIN *) -> bool {
                    const TABLE *t = GetBasicTable(path);
                    // Skip MySQL's internal aggregate temp table (autogen skips
                    // it too); the override reads the base table, not the temp.
                    if (t == nullptr ||
                        (t->s != nullptr && t->s->tmp_table != NO_TMP_TABLE))
                      return false;
                    leaf = path;
                    return true;
                  });
  if (leaf == nullptr)
    return false;

  const TABLE *leaf_table = GetBasicTable(leaf);
  if (leaf_table != join->query_block->leaf_tables->table)
    return false;

  // A plain full table scan is the primary range the override reads.
  if (leaf->type == AccessPath::TABLE_SCAN)
    return true;
  // A full index scan on the primary key is staged identically (no secondary
  // index name), so the override consumes it too.
  if (leaf->type == AccessPath::INDEX_SCAN && leaf_table->s != nullptr &&
      leaf_table->s->primary_key != MAX_KEY &&
      leaf->index_scan().idx == static_cast<int>(leaf_table->s->primary_key))
    return true;
  return false;
}

/**
 * @brief Install the aggregate executor override for supported query shapes.
 *
 * Unsupported queries leave `override_executor_func` unset and continue through
 * MySQL's normal iterator executor.
 */
static int lineairdb_push_to_engine(THD *thd, AccessPath *root_path,
                                    JOIN *join) {
  helios_try_register_grouped_semijoin(thd, join);
  if (!helios_offloadable_shape(thd, join)) return 0;
  // The override reads a PRIMARY full scan; install it only when the plan chose
  // that scan, else its read misses the staged secondary and prefetch aborts.
  if (!helios_leaf_is_full_primary_scan(root_path, join)) return 0;
  join->override_executor_func = &helios_override_executor;
  return 0;
}

/**
 * @brief Expose the engine-pushdown hook to MySQL's optimizer.
 */
const handlerton *ha_lineairdb::hton_supporting_engine_pushdown() {
  return lineairdb_hton;
}

static int lineairdb_init_func(void *p) {
  DBUG_TRACE;

  lineairdb_hton = (handlerton *)p;
  lineairdb_hton->state = SHOW_OPTION_YES;
  lineairdb_hton->create = lineairdb_create_handler;
  lineairdb_hton->flags =
      HTON_CAN_RECREATE | HTON_SUPPORTS_SECONDARY_ENGINE;
  // ALTER TABLE ... SECONDARY_LOAD/UNLOAD unconditionally invokes the
  // primary engine's post_ddl hook (sql_table.cc, cleanup lambda); LineairDB
  // DDL needs no post-commit work, but the pointer must be non-null.
  lineairdb_hton->post_ddl = [](THD *) {};
  lineairdb_hton->is_supported_system_table =
      lineairdb_is_supported_system_table;
  lineairdb_hton->db_type = DB_TYPE_UNKNOWN;
  lineairdb_hton->commit = lineairdb_commit;
  lineairdb_hton->rollback = lineairdb_abort;
  lineairdb_hton->close_connection = lineairdb_close_connection;
  lineairdb_hton->push_to_engine = lineairdb_push_to_engine;

  return 0;
}

LineairDB_share::LineairDB_share() {
  thr_lock_init(&lock);
  next_hidden_pk.store(0);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each lineairdb handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

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

static PSI_memory_key csv_key_memory_blobroot;

ha_lineairdb::ha_lineairdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg), m_ds_mrr(this), current_position_(0),
      buffer_position_(0), last_batch_key_(), scan_exhausted_(false),
      blobroot(csv_key_memory_blobroot, BLOB_MEMROOT_ALLOC_SIZE) {}

void ha_lineairdb::set_key_and_key_part_info(const TABLE *const table) {
  key_info = table->key_info;
  uint pk_index = table->s->primary_key;

  if (pk_index != MAX_KEY) {
    primary_key_type = static_cast<ha_base_keytype>(
        table->key_info[pk_index].key_part[0].type);

    key_part = table->key_info[pk_index].key_part;
    indexed_key_part = key_part[0];
    num_key_parts = table->key_info[pk_index].user_defined_key_parts;
  } else {
    primary_key_type = HA_KEYTYPE_END;
    key_part = nullptr;
    num_key_parts = 0;
  }
}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_lineairdb::open(const char *table_name, int, uint, const dd::Table *) {
  DBUG_TRACE;
  if (!(share = get_share()))
    return 1;
  thr_lock_data_init(&share->lock, &lock, nullptr);

  db_table_name = std::string(table_name);

  if ((num_keys = table->s->keys))
    set_key_and_key_part_info(table);

  if (table->s->primary_key != MAX_KEY) {
    // Calculate LineairDBField-encoded PK size. Each key part is encoded as:
    //   1 (null marker) + 1 (type tag) + 2 (length field) + payload
    // For STRING types, an extra terminator byte is added (+5 total overhead).
    // MySQL's key_length only counts raw column bytes, which is smaller.
    uint pk_index = table->s->primary_key;
    KEY *pk = &table->key_info[pk_index];
    size_t encoded_pk_size = 0;
    for (uint i = 0; i < pk->user_defined_key_parts; i++) {
      KEY_PART_INFO *part = &pk->key_part[i];
      Field *field = part->field;
      LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(field->type());
      if (ldb_type == LineairDBFieldType::LINEAIRDB_STRING) {
        // STRING: marker(1) + type(1) + payload + terminator(1) + length(2)
        encoded_pk_size += 5 + part->length;
      } else {
        // INT/DATETIME/OTHER: marker(1) + type(1) + length(2) + payload
        encoded_pk_size += 4 + field->pack_length();
      }
    }
    ref_length = sizeof(uint16_t) + encoded_pk_size;
  } else {
    ref_length = sizeof(uint16_t) + serialize_hidden_primary_key(0).size();
  }

  return 0;
}

/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_lineairdb::close(void) {
  DBUG_TRACE;
  return 0;
}

int ha_lineairdb::change_active_index(uint keynr) {
  DBUG_TRACE;
  active_index = keynr;

  if (table && table->s && keynr < table->s->keys) {
    current_index_name = std::string(table->key_info[keynr].name);
  } else {
    current_index_name.clear();
  }

  return 0;
}

int ha_lineairdb::index_init(uint idx, bool sorted [[maybe_unused]]) {
  DBUG_TRACE;
  reset_index_search_buffers();
  last_fetched_primary_key_.clear();

  return change_active_index(idx);
}

int ha_lineairdb::index_end() {
  DBUG_TRACE;
  active_index = MAX_KEY;
  mrr_use_batch_ = false;
  mrr_buffer_.clear();
  mrr_buffer_pos_ = 0;
  return 0;
}

int ha_lineairdb::index_read(uchar *buf, const uchar *key, uint key_len,
                             enum ha_rkey_function find_flag) {
  DBUG_TRACE;
  return index_read_map(buf, key, HA_WHOLE_KEY, find_flag);
}

int ha_lineairdb::index_read_last(uchar *buf, const uchar *key, uint key_len) {
  DBUG_TRACE;

  if (key == nullptr || key_len == 0) {
    return index_last(buf);
  }

  KEY *key_info = &table->key_info[active_index];
  uint total_len = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    total_len += key_info->key_part[i].store_length;
  }

  if (key_len >= total_len) {
    return index_read_map(buf, key, HA_WHOLE_KEY, HA_READ_PREFIX_LAST);
  }

  key_part_map keypart_map = 0;
  uint consumed = 0;
  bool aligned = false;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    const uint part_len = key_info->key_part[i].store_length;
    if (consumed + part_len > key_len) {
      break;
    }
    consumed += part_len;
    keypart_map |= (static_cast<key_part_map>(1) << i);
    if (consumed == key_len) {
      aligned = true;
      break;
    }
  }

  if (!aligned) {
    return HA_ERR_WRONG_COMMAND;
  }

  return index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST);
}

/**
  @brief
  write_row() inserts a row.
  No extra() hint is given currently if a bulk load is happening.
  @param buf is a byte array of data.
*/
int ha_lineairdb::write_row(uchar *buf) {
  DBUG_TRACE;

  set_write_buffer(buf);
  auto key = extract_key(buf);

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  // buffer_write appends to a local buffer (no RPC yet), so no error check needed.
  // The actual RPC is sent at flush time (buffer full, table change, or commit).
  tx->buffer_write(db_table_name, key, write_buffer_);

  // Write secondary index entries.
  // Normal transactions check UNIQUE indexes immediately.
  // Prefetch sends UNIQUE index writes to validate-and-commit with row writes.
  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    if (i == table->s->primary_key) continue;

    std::string secondary_key = build_secondary_key_from_row(buf, key_info);

    if (key_info.flags & HA_NOSAME) {
      if (tx->is_prefetch_mode()) {
        tx->buffer_write_secondary_index(db_table_name, key_info.name,
                                         secondary_key, key);
      } else {
        tx->flush_write_buffer();
        tx->choose_table(db_table_name);
        bool ok = tx->write_secondary_index(key_info.name, secondary_key, key);
        if (!ok || tx->is_aborted()) {
          return abort_errno(tx);
        }
      }
    } else {
      tx->buffer_write_secondary_index(db_table_name, key_info.name,
                                        secondary_key, key);
    }
  }

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->add_rowcount_delta(share, db_table_name, +1);

  return 0;
}

int ha_lineairdb::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  auto key = extract_key_from_mysql(old_data);
  const auto new_key = extract_key_from_mysql(new_data);

  // FIXME: reject a PK-changing UPDATE. update_row overwrites in place at the old
  // key and cannot move a row, so executing one would store new_data under the
  // old key with nothing at the new key -- silent corruption. A real move (delete
  // old + insert new + secondary-index rewrite) is not implemented.
  if (key != new_key) {
    return prefetch_reject_unsupported(ha_thd(), tx,
                                       "primary-key-changing UPDATE");
  }

  if (key.empty()) {
    key = last_fetched_primary_key_;
  }

  if (key.empty()) {
    key = extract_primary_key_from_ref(ref);
  }

  last_fetched_primary_key_ = key;

  set_write_buffer(new_data);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  // Buffer the base-row update; read/scan paths and commit flush it later.
  tx->buffer_write(db_table_name, key, write_buffer_);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];

    if (i == table->s->primary_key) {
      continue;
    }

    std::string old_secondary_key =
        build_secondary_key_from_row(old_data, key_info);
    std::string new_secondary_key =
        build_secondary_key_from_row(new_data, key_info);

    if (old_secondary_key == new_secondary_key) {
      continue;
    }

    tx->update_secondary_index(key_info.name, old_secondary_key,
                               new_secondary_key, key);

    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
  }

  return 0;
}

int ha_lineairdb::delete_row(const uchar *buf) {
  DBUG_TRACE;

  auto key = extract_key_from_mysql(buf);

  if (key.empty()) {
    key = last_fetched_primary_key_;
  }

  if (key.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  last_fetched_primary_key_ = key;

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  // Buffer the base-row delete; read/scan paths and commit flush it later.
  tx->buffer_delete(db_table_name, key);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    if (i != table->s->primary_key) {
      std::string secondary_key = build_secondary_key_from_row(buf, key_info);

      tx->buffer_delete_secondary_index(db_table_name, key_info.name,
                                        secondary_key, key);
    }
  }

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->add_rowcount_delta(share, db_table_name, -1);

  return 0;
}

int ha_lineairdb::index_read_map(uchar *buf, const uchar *key,
                                 key_part_map keypart_map,
                                 enum ha_rkey_function find_flag) {
  DBUG_TRACE;

  stats.records = 0;
  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);
  if (!pushed_filter_serialized_.empty()) {
    tx->set_pushed_filter(pushed_filter_serialized_);
  } else if (!tx->has_pushed_aggregate()) {
    // See rnd_init: a pending aggregation pushdown owns the tx filter.
    tx->clear_pushed_filter();
  }

  KEY *key_info = &table->key_info[active_index];

  // MySQL runs single-table UPDATE/DELETE through the old executor
  // (sql_update.cc/sql_delete.cc), which has no JOIN/access path. Derive its
  // autogen plan from the optimizer-selected handler access instead.
  if (prefetch_needs_legacy_dml_handler(ha_thd(), tx)) {
    build_search_plan(key, keypart_map, find_flag, key_info);
    if (int err = maybe_prefetch_for_legacy_dml_handler(
            ha_thd(), tx, table, active_index, current_plan_)) {
      return err;
    }
    return execute_plan(buf, tx);
  }

  // A legacy single-table DML staged its plan on the first handler access; a
  // second handler access here means the statement spans multiple index ranges
  // (e.g. index merge over different indexes), which the single staged plan
  // cannot cover. Reject loudly (no-fallback) rather than let the read miss the
  // cache and surface as a retryable deadlock, which would livelock on retry.
  if (tx->is_prefetch_mode() && !tx->tx_plan_used() &&
      tx->is_autogen_stmt_handler_deferred()) {
    return prefetch_reject_unsupported(
        ha_thd(), tx, "legacy DML multi-index access (index merge)");
  }

  // The optimizer has run, so the SELECT/generic-DML QEP is available.
  if (int err = maybe_prefetch_for_statement(ha_thd(), tx, table)) return err;

  if (tx->gs_skipped(table) && key == nullptr) {
    reset_index_search_buffers();
    last_fetched_primary_key_.clear();
    if (int err = gs_fill_buffers(tx)) return err;
    if (secondary_index_results_.empty()) return HA_ERR_END_OF_FILE;
    return fetch_and_set_current_result(buf, tx);
  }

  build_search_plan(key, keypart_map, find_flag, key_info);

  return execute_plan(buf, tx);
}

/**
 * @brief index_next: The next row after the current cursor position
 */
int ha_lineairdb::index_next(uchar *buf) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  // materialize mode
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
    if (materialized_scan_truncated_ && !secondary_index_results_.empty()) {
      tx->fallback_to_normal_transaction("read past limit-staged scan");
      return abort_errno(tx);
    }
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_lineairdb::index_next_same(uchar *buf, const uchar *key [[maybe_unused]],
                                  uint key_len [[maybe_unused]]) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  // materialize mode
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
    if (materialized_scan_truncated_ && !secondary_index_results_.empty()) {
      tx->fallback_to_normal_transaction("read past limit-staged scan");
      return abort_errno(tx);
    }
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_lineairdb::index_prev(uchar *buf) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  // materialize mode
  if (secondary_index_results_.empty() || current_position_in_index_ < 2) {
    return HA_ERR_END_OF_FILE;
  }

  current_position_in_index_ -= 2;
  return fetch_and_set_current_result(buf, tx);
}

/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_lineairdb::index_first(uchar *buf) {
  DBUG_TRACE;
  int error = index_read(buf, nullptr, 0, HA_READ_AFTER_KEY);

  /* MySQL does not seem to allow this to return HA_ERR_KEY_NOT_FOUND */

  if (error == HA_ERR_KEY_NOT_FOUND) {
    error = HA_ERR_END_OF_FILE;
  }

  return error;
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_lineairdb::index_last(uchar *buf) {
  DBUG_TRACE;

  reset_index_search_buffers();
  last_fetched_primary_key_.clear();

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);

  // index_last seeks the index tail with no search key: it does not route
  // through index_read_map and carries no range to build an autogen plan from,
  // and a statement-scoped plan keyed by the QEP's range cannot cover it.
  // Reject loudly under no-fallback rather than silently miss the local view.
  // Range-driven reverse scans (ORDER BY ... DESC LIMIT) are supported; only
  // this key-less tail seek is not.
  // TODO: support index_last under prefetch (autogen reverse tail-scan).
  if (tx->is_prefetch_mode()) {
    return prefetch_reject_unsupported(ha_thd(), tx, "index_last access");
  }

  if (active_index == table->s->primary_key) {
    auto key_values = tx->get_matching_keys_and_values_in_range("", "");
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ =
        tx->get_matching_primary_keys_in_range(current_index_name, "", "");
    batch_fetch_secondary_payloads(tx);
  }

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  current_position_in_index_ =
      static_cast<uint>(secondary_index_results_.size() - 1);
  int error = fetch_and_set_current_result(buf, tx);
  if (error == HA_ERR_KEY_NOT_FOUND) {
    error = HA_ERR_END_OF_FILE;
  }
  return error;
}

const Item *ha_lineairdb::cond_push(const Item *cond) {
  DBUG_TRACE;
  pushed_filter_serialized_.clear();
  has_unpushed_filter_ = false;
  if (!cond || !table) return cond;

  LineairDB::Protocol::PushedPredicate predicate;
  predicate.set_num_columns(table->s->fields);
  if (!serialize_item(cond, predicate.mutable_expr())) {
    // Serialization failed → no PP, MySQL evaluates everything
    has_unpushed_filter_ = true;
    return cond;
  }
  predicate.SerializeToString(&pushed_filter_serialized_);
  return cond;  // Always return cond: MySQL re-evaluates (safety net)
}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the lineairdb in the introduction at the top of this file to see
  when rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_lineairdb::rnd_init(bool) {
  DBUG_ENTER("ha_lineairdb::rnd_init");
  scanned_keys_.clear();
  scanned_values_.clear();
  scan_cache_.clear();
  buffer_position_ = 0;
  last_batch_key_.clear();
  scan_exhausted_ = false;
  last_fetched_primary_key_.clear();
  current_position_ = 0;
  stats.records = 0;

  change_active_index(table->s->primary_key);

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    DBUG_RETURN(abort_errno(tx));
  }

  tx->choose_table(db_table_name);

  // Predicate pushdown: propagate filter serialized by cond_push() to the
  // transaction. When an aggregation pushdown is pending, the tx filter was
  // prepared by tx_set_pushed_aggregate from the statement WHERE — clearing
  // it here (cond_push is empty in practice) would make the server aggregate
  // UNFILTERED rows.
  if (!pushed_filter_serialized_.empty()) {
    tx->set_pushed_filter(pushed_filter_serialized_);
  } else if (!tx->has_pushed_aggregate()) {
    tx->clear_pushed_filter();
  }

  if (prefetch_needs_legacy_dml_handler(ha_thd(), tx)) {
    DBUG_RETURN(prefetch_reject_unsupported(
        ha_thd(), tx, "legacy DML full/reverse table scan"));
  }

  // The optimizer has run, so the QEP is available.
  // Statement-scoped autogen stages and executes the prefetch plan once per
  // statement; an unsupported QEP fails here.
  if (int err = maybe_prefetch_for_statement(ha_thd(), tx, table)) {
    DBUG_RETURN(err);
  }

  if (tx->gs_skipped(table)) {
    DBUG_RETURN(gs_fill_buffers(tx));
  }

  DBUG_RETURN(0);
}

int ha_lineairdb::rnd_end() {
  DBUG_TRACE;
  // NOTE: Do NOT clear scan_cache_ / scanned_values_ here.
  // MySQL calls rnd_end() after scanning, then rnd_pos() to re-read rows in
  // sorted order. In InnoDB the re-read hits the Buffer Pool, but we need our
  // own cache (scan_cache_) since re-reads would otherwise require RPCs.
  // Cleared at the start of the next rnd_init() instead.
  buffer_position_ = 0;
  last_batch_key_.clear();
  scan_exhausted_ = false;
  blobroot.Clear();
  return 0;
}

/**
 * @brief Fetch next batch of rows for full table scan.
 *
 * Proxy adaptation: uses get_matching_keys_and_values_from_prefix("")
 * instead of SE's Scan() callback.
 */
bool ha_lineairdb::fetch_next_batch() {
  DBUG_ENTER("ha_lineairdb::fetch_next_batch");

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    DBUG_RETURN(false);
  }

  tx->choose_table(db_table_name);

  scanned_keys_.clear();
  scanned_values_.clear();
  scan_cache_.clear();
  buffer_position_ = 0;

  // Proxy: fetch all rows via RPC in one call (no batched Scan callback)
  auto key_value_pairs = tx->get_matching_keys_and_values_from_prefix("");

  for (auto &kv : key_value_pairs) {
    // skip tombstones
    if (kv.second.empty()) continue;

    // Store row data and build scan_cache_ so rnd_pos() can reuse it later.
    size_t idx = scanned_keys_.size();
    scanned_keys_.push_back(kv.first);
    const auto &val = kv.second;
    scanned_values_.emplace_back(
        reinterpret_cast<const std::byte *>(val.data()),
        reinterpret_cast<const std::byte *>(val.data()) + val.size());
    scan_cache_[kv.first] = idx;
  }

  // Check if tx was aborted during RPC
  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    DBUG_RETURN(false);
  }

  if (scanned_keys_.empty()) {
    DBUG_RETURN(false);
  }

  // Mark as exhausted since we fetched everything in one call
  scan_exhausted_ = true;

  DBUG_RETURN(true);
}

void ha_lineairdb::reset_index_search_buffers() {
  secondary_index_results_.clear();
  secondary_index_payloads_.clear();
  current_position_in_index_ = 0;
  materialized_scan_truncated_ = false;
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/

// assumption: takes 1 row
int ha_lineairdb::rnd_next(uchar *buf) {
  DBUG_ENTER("ha_lineairdb::rnd_next");
  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  if (buffer_position_ >= scanned_keys_.size()) {
    if (scan_exhausted_) {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    if (!fetch_next_batch()) {
      auto tx = get_transaction(ha_thd());
      if (tx->is_aborted()) {
        DBUG_RETURN(abort_errno(tx));
      }
      scan_exhausted_ = true;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }

  auto &key = scanned_keys_[buffer_position_];
  auto &value = scanned_values_[buffer_position_];
  buffer_position_++;

  int error = set_fields_from_lineairdb(buf, value.data(), value.size());
  if (error == 0) {
    last_fetched_primary_key_ = key;
  }
  current_position_++;
  DBUG_RETURN(error);
}

/**
 * @brief Attach an aggregate spec and its server-side WHERE filter to the tx.
 *
 * Server aggregation folds base rows into group rows, so MySQL cannot recheck
 * the original WHERE afterward. If the WHERE cannot be serialized, return
 * false and let the caller aggregate locally.
 */
bool ha_lineairdb::tx_set_pushed_aggregate(const std::string &s) {
  auto tx = get_transaction(ha_thd());
  // agg_next_raw bypasses the usual read path that selects the table.
  tx->choose_table(db_table_name);
  // Aggregation needs only WHERE serialization; LIMIT safety is irrelevant here.
  prepare_select_filter_for_tx(ha_thd(), table, tx, nullptr);
  const Item *agg_where = nullptr;
  if (ha_thd()->lex != nullptr && ha_thd()->lex->unit != nullptr) {
    Query_block *qb = ha_thd()->lex->unit->global_parameters();
    if (qb != nullptr) agg_where = qb->where_cond();
  }
  if (agg_where != nullptr && tx->get_pushed_filter().empty()) {
    return false;  // WHERE exists but could not be fully serialized
  }
  tx->set_pushed_aggregate(s);
  return true;
}

/**
 * @brief Return true when the current transaction has aborted during staging.
 */
bool ha_lineairdb::tx_is_aborted() {
  auto tx = get_transaction(ha_thd());
  return tx == nullptr || tx->is_aborted();
}

void ha_lineairdb::tx_register_gs(LineairDBTransaction::GsRegistration reg) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return;
  THD *thd = ha_thd();
  const uint64_t query_id =
      thd != nullptr ? static_cast<uint64_t>(thd->query_id) : 0;
  if (tx->autogen_query_id() != query_id)
    tx->reset_autogen_for_statement(query_id);
  tx->register_gs(table, std::move(reg));
}

void ha_lineairdb::tx_register_grouped_semijoin(
    LineairDBTransaction::GroupedSemijoin gs) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return;
  THD *thd = ha_thd();
  const uint64_t query_id =
      thd != nullptr ? static_cast<uint64_t>(thd->query_id) : 0;
  if (tx->autogen_query_id() != query_id)
    tx->reset_autogen_for_statement(query_id);
  tx->register_grouped_semijoin(std::move(gs));
}

bool ha_lineairdb::tx_has_grouped_semijoin() {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return false;
  THD *thd = ha_thd();
  const uint64_t query_id =
      thd != nullptr ? static_cast<uint64_t>(thd->query_id) : 0;
  // Registrations carry over in the transaction until the next register resets
  // them for a new statement (reset_autogen_for_statement). A stale query id
  // means any held registration belongs to an earlier statement, so it does not
  // count against the current one.
  if (tx->autogen_query_id() != query_id) return false;
  return tx->has_gs_registrations() || !tx->grouped_semijoins().empty();
}

LineairDBTransaction *ha_lineairdb::tx_for_autogen() {
  return get_transaction(ha_thd());
}

/**
 * @brief Return whether server aggregation may use read-only no-validation.
 */
bool ha_lineairdb::tx_ro_novalidate() {
  // Server aggregation consumes staged group rows, so require prefetch mode.
  return srv_prefetch_execution && srv_prefetch_ro_novalidate;
}

int ha_lineairdb::gs_fill_buffers(LineairDBTransaction *tx) {
  const LineairDBTransaction::GsRegistration *reg = tx->gs_registration(table);
  if (reg == nullptr) {
    my_error(ER_INTERNAL_ERROR, MYF(0), "helios gs skipped but unregistered");
    return HA_ERR_INTERNAL_ERROR;
  }

  std::vector<std::string> groups;
  if (const std::vector<std::string> *cached =
          tx->grouped_semijoin_groups(db_table_name)) {
    groups = *cached;
  } else {
    LineairDBProxy::ReadPlanStep step;
    step.table_name = db_table_name;
    step.is_scan = true;
    step.end_key_prefix = lineairdb_keyenc::scan_end_sentinel();
    step.serialized_filter = reg->filter;
    step.aggregate_serialized = reg->spec;
    if (!tx->execute_read_plan_raw({step}, &groups)) {
      my_error(ER_LOCK_DEADLOCK, MYF(0));
      return HA_ERR_LOCK_DEADLOCK;
    }
  }

  auto parse_fields = [](std::string_view row,
                         std::vector<std::string_view> *fv,
                         std::vector<bool> *nul) -> bool {
    size_t off = 0;
    while (off < row.size()) {
      uint8_t bs = static_cast<uint8_t>(row[off++]);
      if (bs == 0xFF) {
        fv->emplace_back();
        nul->push_back(true);
        continue;
      }
      if (off + bs > row.size()) return false;
      size_t len = 0;
      for (uint8_t i = 0; i < bs; ++i) {
        len |= static_cast<size_t>(static_cast<uint8_t>(row[off + i]))
               << (8 * i);
      }
      off += bs;
      if (off + len > row.size()) return false;
      fv->push_back(std::string_view(row.data() + off, len));
      nul->push_back(false);
      off += len;
    }
    return true;
  };

  const uint nf = table->s->fields;
  std::vector<uchar> zero_nulls(table->s->null_bytes, 0);
  const auto emit_row = [&](const std::string &gkey, const std::string &a_str,
                            const std::string &b_str) {
    LineairDBField ldb;
    ldb.set_null_field(zero_nulls.data(), zero_nulls.size());
    std::string value = ldb.get_null_field();
    for (uint i = 0; i < nf; ++i) {
      const std::string *src = &reg->template_cols[i];
      if (i == reg->group_col)
        src = &gkey;
      else if (i == reg->col_a)
        src = &a_str;
      else if (i == reg->col_b)
        src = &b_str;
      ldb.set_lineairdb_field(src->c_str(), src->size());
      value += ldb.get_lineairdb_field();
    }
    std::string skey = "\x01gs:" + gkey + ":" +
                       std::to_string(scanned_keys_.size());
    const size_t idx = scanned_keys_.size();
    scanned_keys_.push_back(skey);
    secondary_index_results_.push_back(skey);
    secondary_index_payloads_.push_back(value);
    scan_cache_[std::move(skey)] = idx;
    scanned_values_.emplace_back(
        reinterpret_cast<const std::byte *>(value.data()),
        reinterpret_cast<const std::byte *>(value.data()) + value.size());
  };

  for (const std::string &grow : groups) {
    std::vector<std::string_view> fv;
    std::vector<bool> nul;
    if (!parse_fields(grow, &fv, &nul) || fv.size() != 4 || nul[1] ||
        nul[2]) {
      my_error(ER_INTERNAL_ERROR, MYF(0), "helios gs group row malformed");
      return HA_ERR_INTERNAL_ERROR;
    }
    const std::string gkey(fv[1]);
    if (!reg->single_sum) {
      my_error(ER_INTERNAL_ERROR, MYF(0), "helios gs unsupported mode");
      return HA_ERR_INTERNAL_ERROR;
    }

    Field *sf = table->field[reg->col_a];
    size_t digits = 0;
    for (char ch : fv[2]) {
      if (ch == '.') break;
      if (ch >= '0' && ch <= '9') ++digits;
    }
    const uint max_int_digits =
        sf->field_length >= (sf->decimals() + 2)
            ? static_cast<uint>(sf->field_length) - sf->decimals() - 2
            : 0;
    if (digits > max_int_digits) {
      my_error(ER_INTERNAL_ERROR, MYF(0), "helios gs sum overflow");
      return HA_ERR_INTERNAL_ERROR;
    }
    emit_row(gkey, std::string(fv[2]), std::string());
  }

  scan_exhausted_ = true;
  return 0;
}

/**
 * @brief Clear the aggregate spec from the current transaction.
 */
void ha_lineairdb::tx_clear_pushed_aggregate() {
  get_transaction(ha_thd())->clear_pushed_aggregate();
}

/**
 * @brief Return the next raw group row from the staged scan cache.
 *
 * This is the aggregate equivalent of rnd_next(): it advances the same cursor
 * but returns server-produced group-row bytes instead of unpacking a base row
 * into MySQL's record buffer.
 */
bool ha_lineairdb::agg_next_raw(std::string_view *out_value) {
  // Surface staging aborts instead of treating them as clean EOF.
  {
    auto tx = get_transaction(ha_thd());
    if (tx != nullptr && tx->is_aborted()) return false;
  }
  if (buffer_position_ >= scanned_keys_.size()) {
    if (scan_exhausted_) return false;
    if (!fetch_next_batch()) { scan_exhausted_ = true; return false; }
  }
  if (buffer_position_ >= scanned_values_.size()) return false;
  const auto &value = scanned_values_[buffer_position_];
  buffer_position_++;
  *out_value = std::string_view(
      reinterpret_cast<const char *>(value.data()), value.size());
  current_position_++;
  return true;
}

/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_lineairdb::position(const uchar *) {
  DBUG_TRACE;

  if (last_fetched_primary_key_.empty()) {
    return;
  }

  store_primary_key_in_ref(last_fetched_primary_key_);
}

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and
  sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_lineairdb::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_TRACE;

  std::string primary_key = extract_primary_key_from_ref(pos);

  if (primary_key.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  // Return from scan_cache_ if available (equivalent of hitting InnoDB's Buffer
  // Pool). Without this, every re-read would be a separate RPC to LineairDB.
  auto cache_it = scan_cache_.find(primary_key);
  if (cache_it != scan_cache_.end()) {
    auto &value = scanned_values_[cache_it->second];
    if (set_fields_from_lineairdb(buf, value.data(), value.size())) {
      return HA_ERR_OUT_OF_MEM;
    }
    last_fetched_primary_key_ = primary_key;
    return 0;
  }

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);
  auto result = tx->read(primary_key);

  // read() aborts the tx on a prefetch cache miss; catch it before the empty
  // result below reads as a missing key.
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  if (result.first == nullptr || result.second == 0) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  if (set_fields_from_lineairdb(buf, result.first, result.second)) {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }

  last_fetched_primary_key_ = primary_key;

  return 0;
}

/**
 * @brief Seed this table's row-count baseline from the proxy stats cache.
 */
bool ha_lineairdb::seed_row_count_from_cache(LineairDBProxy *proxy) {
  if (proxy == nullptr || share == nullptr || db_table_name.empty())
    return false;

  const auto &stats_cache = proxy->cached_table_stats();
  auto it = stats_cache.find(db_table_name);
  if (it == stats_cache.end() || it->second <= 0)
    return false;

  share->stats_base_records.store(static_cast<uint64_t>(it->second),
                                  std::memory_order_relaxed);
  for (auto &shard : share->rowcount_shards)
    shard.delta.store(0, std::memory_order_relaxed);
  return true;
}

/**
 * @brief Copy the proxy's latest per-index optimizer stats into shared table
 * metadata.
 */
void ha_lineairdb::load_index_stats_from_cache(LineairDBProxy *proxy) {
  if (proxy == nullptr || share == nullptr)
    return;

  const uint64_t records =
      share->stats_base_records.load(std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock(share->index_ndv_mu_);
  share->index_ndv_.clear();
  share->index_hist_.clear();
  for (const auto &entry : proxy->last_index_ndv()) {
    if (entry.second.available)
      share->index_ndv_[entry.first] = entry.second.values;
  }
  for (const auto &entry : proxy->last_index_hist()) {
    const auto &hist = entry.second;
    if (!hist.available || hist.bounds.empty() ||
        hist.bounds.size() != hist.cum.size())
      continue;

    bool monotone = true;
    for (size_t i = 1; i < hist.cum.size(); ++i) {
      if (hist.cum[i] < hist.cum[i - 1] ||
          hist.bounds[i] < hist.bounds[i - 1]) {
        monotone = false;
        break;
      }
    }
    if (!monotone)
      continue;

    LineairDB_share::RangeHist range_hist;
    range_hist.bounds = hist.bounds;
    range_hist.cum = hist.cum;
    share->index_hist_[entry.first] = std::move(range_hist);
  }
  share->index_ndv_records_.store(records, std::memory_order_relaxed);
  share->index_ndv_loaded_.store(true, std::memory_order_relaxed);
}

/**
 * @brief Mark cached NDV stale when a SELECT sees more than 20% row-count
 * drift.
 */
void ha_lineairdb::mark_stale_index_ndv_for_select() {
  if (share == nullptr ||
      !share->index_ndv_loaded_.load(std::memory_order_relaxed))
    return;

  THD *thd = ha_thd();
  const bool is_select = thd != nullptr && thd->lex != nullptr &&
                         thd->lex->sql_command == SQLCOM_SELECT;
  if (!is_select)
    return;

  const uint64_t at_fetch =
      share->index_ndv_records_.load(std::memory_order_relaxed);
  const uint64_t now =
      share->stats_base_records.load(std::memory_order_relaxed);
  const uint64_t hi = std::max(at_fetch, now);
  const uint64_t lo = std::min(at_fetch, now);
  // Refetch when the gap is more than 20% of the larger row count.
  if (hi == 0 || (hi - lo) * 5 <= hi)
    return;

  share->index_ndv_force_refresh_.store(true, std::memory_order_relaxed);
  share->index_ndv_loaded_.store(false, std::memory_order_relaxed);
}

/**
 * @brief Fetch row-count and per-index NDV stats before optimizer planning.
 */
void ha_lineairdb::seed_optimizer_stats() {
  if (share == nullptr || db_table_name.empty())
    return;

  const bool need_rowcount =
      share->stats_base_records.load(std::memory_order_relaxed) == 0;
  mark_stale_index_ndv_for_select();
  const bool need_ndv =
      !share->index_ndv_loaded_.load(std::memory_order_relaxed);
  const bool force_ndv =
      share->index_ndv_force_refresh_.load(std::memory_order_relaxed);
  if (!need_rowcount && !need_ndv && !force_ndv)
    return;

  THD *thd = ha_thd();
  if (thd == nullptr)
    return;

  LineairDBThdCtx *&ctx = lineairdb_thd_ctx(thd, lineairdb_hton);
  ensure_lineairdb_proxy(ctx);
  if (ctx == nullptr || !ctx->proxy)
    return;

  // Existing path: use BEGIN/END piggyback stats when available.
  const bool seeded = seed_row_count_from_cache(ctx->proxy.get());

  // Cold optimizer paths can reach info() before tx_begin.
  if (!seeded || need_ndv || force_ndv) {
    bool fetched = false;
    bool requested_ndv = false;
    if (need_ndv || force_ndv) {
      const auto descs = index_ndv_descriptors(table);
      // Consume ANALYZE's force flag only when issuing the NDV RPC.
      const bool force = share->index_ndv_force_refresh_.exchange(
          false, std::memory_order_relaxed);
      fetched = ctx->proxy->fetch_table_stats(db_table_name, descs, force);
      requested_ndv = true;
    } else {
      fetched = ctx->proxy->fetch_table_stats();
    }
    if (fetched) {
      seed_row_count_from_cache(ctx->proxy.get());
      if (requested_ndv)
        load_index_stats_from_cache(ctx->proxy.get());
    }
  }
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really
  needed. SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc and sql_update.cc
*/
int ha_lineairdb::info(uint flag) {
  DBUG_TRACE;

  if (table == nullptr || table->s == nullptr) {
    if (stats.records < 2)
      stats.records = 2;
    return 0;
  }

  if (flag & (HA_STATUS_VARIABLE | HA_STATUS_CONST)) {
    seed_optimizer_stats();

    int64_t delta_sum = 0;
    for (const auto &shard : share->rowcount_shards) {
      delta_sum += shard.delta.load(std::memory_order_relaxed);
    }

    const int64_t base = static_cast<int64_t>(
        share->stats_base_records.load(std::memory_order_relaxed));
    int64_t total = base + delta_sum;
    if (total < 0)
      total = 0;

    stats.records = static_cast<ha_rows>(total);

    // Check for uncommitted row-count delta in the active transaction
    THD *thd = ha_thd();
    if (thd != nullptr) {
      LineairDBThdCtx *ctx =
          *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, lineairdb_hton));
      LineairDBTransaction *active_tx = (ctx != nullptr) ? ctx->tx : nullptr;
      if (active_tx != nullptr && !active_tx->is_not_started()) {
        if (active_tx->is_aborted()) {
          return abort_errno(active_tx);
        }

        const int64_t local_delta = active_tx->peek_rowcount_delta(share);
        if (local_delta != 0) {
          int64_t local_total =
              static_cast<int64_t>(stats.records) + local_delta;
          if (local_total < 0)
            local_total = 0;
          stats.records = static_cast<ha_rows>(local_total);
        }
      }
    }

    if (stats.records < 2)
      stats.records = 2;

    stats.mean_rec_length = table->s->reclength > 0 ? table->s->reclength : 100;
    stats.data_file_length = stats.records * stats.mean_rec_length;
    stats.index_file_length = stats.data_file_length / 2;
  }
  if ((flag & (HA_STATUS_CONST | HA_STATUS_VARIABLE)) && table != nullptr &&
      table->s != nullptr) {
    for (uint i = 0; i < table->s->keys; i++) {
      KEY *key = table->key_info + i;
      if (key == nullptr)
        continue;
      bool is_primary = (i == table->s->primary_key);
      set_generic_rec_per_key(key, key->user_defined_key_parts, is_primary);
    }
  }

  return 0;
}

int ha_lineairdb::analyze(THD *, HA_CHECK_OPT *) {
  DBUG_TRACE;

  if (share != nullptr) {
    share->stats_base_records.store(0, std::memory_order_relaxed);
    for (auto &shard : share->rowcount_shards)
      shard.delta.store(0, std::memory_order_relaxed);
    share->index_ndv_loaded_.store(false, std::memory_order_relaxed);
    share->index_ndv_force_refresh_.store(true, std::memory_order_relaxed);
  }

  info(HA_STATUS_VARIABLE | HA_STATUS_CONST);
  return HA_ADMIN_OK;
}

/**
 * @brief Estimate rec_per_key, the average rows matching each key prefix.
 *
 * @details Prefer server-measured NDV and compute rows / NDV(prefix). If NDV
 * is unavailable, keep the old uniform-distribution heuristic as a fallback.
 *
 * The fallback assumes N rows are split evenly across K key parts, so each
 * additional key part narrows the estimate by N^(1/K).
 *
 * Example with 1,000,000 rows and a 4-part unique key:
 * @verbatim
 * per_part = 1000000^(1/4) ~= 31.6
 * 1 part: 1000000 / 31.6   = 31,623 rows
 * 2 parts: 1000000 / 31.6^2 = 1,000 rows
 * 3 parts: 1000000 / 31.6^3 = 32 rows
 * 4 parts: full unique key  = 1 row
 * @endverbatim
 *
 * Full primary or unique keys are always costed as one row.
 *
 * See also: NDB's ndb_index_stat_set_rpk (ha_ndb_index_stat.cc:2529).
 */
void ha_lineairdb::set_generic_rec_per_key(KEY *key, uint key_parts,
                                           bool is_primary) {
  bool is_unique = (key->flags & HA_NOSAME);

  std::vector<uint64_t> ndv;
  if (share != nullptr &&
      share->index_ndv_loaded_.load(std::memory_order_relaxed)) {
    const std::string index_name =
        is_primary ? std::string() : std::string(key->name ? key->name : "");
    std::lock_guard<std::mutex> lock(share->index_ndv_mu_);
    auto it = share->index_ndv_.find(index_name);
    if (it != share->index_ndv_.end() && it->second.size() >= key_parts)
      ndv = it->second;
  }
  const bool has_ndv = ndv.size() >= key_parts;

  // How much each additional key part narrows the result set (fallback path).
  double per_part = std::max(
      2.0, std::pow(static_cast<double>(stats.records), 1.0 / key_parts));

  for (uint j = 0; j < key_parts; j++) {
    ulong rpk; // records per key
    if ((is_primary || is_unique) && j == key_parts - 1) {
      // All parts specified on a UNIQUE/PK -> exactly 1 row
      rpk = 1;
    } else if (has_ndv && ndv[j] > 0) {
      // Real stats: average rows per distinct key-prefix value.
      const uint64_t records = static_cast<uint64_t>(stats.records);
      const uint64_t distinct = ndv[j];
      rpk = static_cast<ulong>(
          std::max<uint64_t>(1, (records + distinct - 1) / distinct));
    } else {
      // per_part^(j+1) = total divisor for j+1 key parts
      double selectivity = std::pow(per_part, static_cast<double>(j + 1));
      rpk = static_cast<ulong>(
          std::max(1.0, static_cast<double>(stats.records) / selectivity));
    }
    key->rec_per_key[j] = rpk;
    key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk));
  }
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_lineairdb::extra(enum ha_extra_function) {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases
  where the optimizer realizes that all rows will be removed as a result of an
  SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_query_block_query_expression::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_query_block_query_expression::exec() in sql_union.cc.
*/
int ha_lineairdb::delete_all_rows() {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to
  understand this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_lineairdb::external_lock(THD *thd, int lock_type) {
  DBUG_TRACE;

  userThread = thd;

  const bool tx_is_ready_to_commit = lock_type == F_UNLCK;
  if (tx_is_ready_to_commit) {
    // Drop the predicate pushed by cond_push() so the next statement starts clean.
    pushed_filter_serialized_.clear();
    has_unpushed_filter_ = false;
    LineairDBThdCtx **ctx_slot = reinterpret_cast<LineairDBThdCtx **>(
        thd_ha_data(thd, lineairdb_hton));
    if (ctx_slot != nullptr && *ctx_slot != nullptr &&
        (*ctx_slot)->tx != nullptr) {
      (*ctx_slot)->tx->clear_pushed_filter();
    }
    return 0;
  }

  // get_transaction() will automatically start the transaction if needed
  LineairDBTransaction *tx = get_transaction(thd);
  if (tx != nullptr) {
    const LEX_CSTRING &q = thd->query();
    if (q.str != nullptr && q.length > 0) {
      tx->on_stmt_boundary(std::string(q.str, q.length));
    }
  }

  // Stats sync: apply cached table row counts from the server (received
  // in BEGIN/END responses) so the optimizer sees correct cardinalities.
  // The server count is authoritative (includes all committed deltas from
  // all proxies), so we reset local counters to avoid double-counting.
  if (share != nullptr && !db_table_name.empty()) {
    auto *proxy = get_proxy();
    if (proxy != nullptr) {
      const auto &stats = proxy->cached_table_stats();
      auto it = stats.find(db_table_name);
      if (it != stats.end() && it->second > 0) {
        share->stats_base_records.store(
            static_cast<uint64_t>(it->second), std::memory_order_relaxed);
        // Reset local counters: server count already includes our committed deltas.
        for (auto &shard : share->rowcount_shards)
          shard.delta.store(0, std::memory_order_relaxed);
      }
    }
  }

  return 0;
}

int ha_lineairdb::start_stmt(THD *thd, thr_lock_type lock_type) {
  assert(lock_type > 0);
  userThread = thd;
  return external_lock(thd, lock_type);
}

/**
 * @brief Gets transaction from MySQL allocated memory
 *
 * This function follows the InnoDB pattern of "lazy transaction start".
 * The transaction is automatically started when first accessed, rather than
 * relying solely on external_lock() to start it.
 *
 * This is necessary because MySQL's query optimizer may call handler methods
 * (like index_read_map) before external_lock() in certain scenarios:
 * - Semi-join optimization
 * - Subquery materialization
 * - Complex JOIN operations
 *
 * Without this lazy start, accessing a transaction before external_lock()
 * would result in a nullptr dereference or assertion failure.
 */
LineairDBTransaction *&ha_lineairdb::get_transaction(THD *thd) {
  LineairDBThdCtx *&ctx = lineairdb_thd_ctx(thd, lineairdb_hton);
  ensure_lineairdb_proxy(ctx);
  if (ctx->tx == nullptr) {
    ctx->tx =
        new LineairDBTransaction(thd, ctx->proxy.get(), lineairdb_hton, FENCE);
    // Prefetch protocol is fixed for the transaction: enabled whenever the
    // sysvar is on and the first statement is prefetch-eligible. Whether a plan
    // is actually staged is decided per statement: an injected @_tx_plan
    // at begin (tx-scoped), else statement-scoped autogen at
    // rnd_init / index_read.
    const bool can_use_prefetch =
        (srv_prefetch_execution && thd_can_use_prefetch(thd));
    ctx->tx->set_prefetch_mode(can_use_prefetch);
  }
  if (ctx->tx->is_not_started()) {
    ctx->tx->begin_transaction();
    maybe_prefetch_for_transaction(thd, ctx->tx);
  }
  return ctx->tx;
}

int ha_lineairdb::abort_errno(LineairDBTransaction *tx) {
  // A prefetch cache miss is an unsupported access shape, not contention, so
  // reject it non-retryably -- retrying the same read cannot make it hit.
  if (tx != nullptr && tx->aborted_by_cache_miss()) {
    return prefetch_reject_unsupported(ha_thd(), tx, "prefetch cache miss");
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

  // 参加していない（このエンジンのトランザクションが無い）場合は noop
  if (ctx == nullptr || ctx->tx == nullptr)
    return 0;

  const bool should_terminate_now =
      (all == true) || ctx->tx->is_a_single_statement();
  if (!should_terminate_now)
    return 0;

  const bool committed = ctx->tx->end_transaction();
  ctx->tx = nullptr;

  if (!committed) {
    thd_mark_transaction_to_rollback(thd, true);
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

  // 参加していない場合は noop
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

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for lineairdb, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
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
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_lineairdb::delete_table(const char *, const dd::Table *) {
  DBUG_TRACE;
  /* This is not implemented but we want someone to be able that it works. */
  return 0;
}

/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_lineairdb::rename_table(const char *, const char *, const dd::Table *,
                               dd::Table *) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
 * @brief Decode an integer key part from a MySQL range endpoint.
 *
 * @details Uses key_restore() into table->record[1] instead of reading raw key
 * bytes, so MySQL owns signedness and key-format decoding. Unsupported shapes
 * return false and the caller falls back to the old coarse estimate.
 */
static bool helios_decode_keypart_int(TABLE *table, KEY *key, uint part,
                                      const key_range *range,
                                      longlong *out_value) {
  if (table == nullptr || key == nullptr || range == nullptr ||
      range->key == nullptr || out_value == nullptr) {
    return false;
  }
  if (part >= key->user_defined_key_parts) return false;

  uint required = 0;
  for (uint i = 0; i <= part; ++i) {
    required += key->key_part[i].store_length;
  }
  if (range->length < required) return false;

  const KEY_PART_INFO &kp = key->key_part[part];
  Field *field = kp.field;
  if (field == nullptr || field->result_type() != INT_RESULT) return false;
  if (field->is_nullable()) return false;
  if (kp.key_part_flag & HA_REVERSE_SORT) return false;
  if (table->record[0] == nullptr || table->record[1] == nullptr) return false;

  uchar *scratch = table->record[1];
  key_restore(scratch, range->key, key, range->length);
  const ptrdiff_t delta =
      static_cast<ptrdiff_t>(scratch - table->record[0]);
  field->move_field_offset(delta);
  *out_value = field->val_int();
  field->move_field_offset(-delta);
  return true;
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @note
  MySQL passes WHERE conditions as min_key / max_key:
    - Equality (WHERE col=1):        min_key == max_key (same bytes)
    - Range (WHERE col BETWEEN 1,5): min_key != max_key

  We compare min_key and max_key byte-by-byte per key part to detect
  how many leading columns are equality conditions, then use rec_per_key
  for that depth. See NDB's records_in_range (ha_ndbcluster.cc:12861).

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_lineairdb::records_in_range(uint inx, key_range *min_key,
                                       key_range *max_key) {
  DBUG_TRACE;

  // Guard: table metadata not available
  if (table == nullptr || table->s == nullptr) {
    return 10;
  }

  // Get the index definition (inx = which index the optimizer is asking about)
  KEY *key = table->key_info + inx;
  if (key == nullptr) {
    return 10;
  }

  // Total rows in this table (floor of 2 to avoid zero-cost estimates)
  ha_rows total_records = stats.records;
  if (total_records < 2)
    total_records = 2;

  // No bounds at all -> full table scan
  if (min_key == nullptr && max_key == nullptr) {
    return total_records;
  }

  // Count how many key parts (columns) the query specifies
  uint key_parts_used = 0;
  if (min_key != nullptr) {
    key_parts_used = calculate_key_parts_from_length(key, min_key->length);
  } else if (max_key != nullptr) {
    key_parts_used = calculate_key_parts_from_length(key, max_key->length);
  }

  // All columns of a UNIQUE/PK specified -> exactly 1 row
  if ((key->flags & HA_NOSAME) &&
      key_parts_used == key->user_defined_key_parts) {
    return 1;
  }

  // No columns matched -> full scan
  if (key_parts_used == 0) {
    return total_records;
  }

  // --- Equality prefix detection ---
  // Compare min_key and max_key per column to find how many leading
  // columns have equal values (= condition vs BETWEEN/range).
  //
  // Example: WHERE w=1 AND d=1 AND o_id BETWEEN 100 AND 200
  //   min = [1][1][100], max = [1][1][200]
  //     col 0: 1==1     -> eq
  //     col 1: 1==1     -> eq
  //     col 2: 100!=200 -> neq
  //   -> eq_parts = 2
  uint eq_parts = 0;
  if (min_key != nullptr && max_key != nullptr) {
    uint cmp_len = std::min(min_key->length, max_key->length);
    uint consumed = 0; // byte offset into key buffer
    for (uint p = 0; p < key->user_defined_key_parts && consumed < cmp_len; p++) {
      uint part_len = key->key_part[p].store_length;
      if (consumed + part_len > cmp_len)
        break;
      if (memcmp(min_key->key + consumed, max_key->key + consumed, part_len) == 0) {
        eq_parts++;
        consumed += part_len;
      } else {
        break; // This column differs -> range condition from here
      }
    }
  } else if (min_key != nullptr) {
    // One-sided: only min_key given. If exact match flag, treat as equality.
    if (min_key->flag == HA_READ_KEY_EXACT ||
        min_key->flag == HA_READ_KEY_OR_NEXT) {
      eq_parts = key_parts_used;
    }
  }

  // --- Estimate row count ---
  ha_rows estimate;
  if (eq_parts > 0) {
    // Use rec_per_key at the equality depth
    uint rpk_idx = eq_parts - 1; // 0-based array index
    if (rpk_idx < key->user_defined_key_parts) {
      estimate = static_cast<ha_rows>(key->rec_per_key[rpk_idx]);
    } else {
      estimate = 1;
    }
    // Range after an equality prefix: estimate how many distinct trailing
    // values fall inside the range, then multiply by rows per trailing value.
    // Unsupported key shapes fall back to the old /2 heuristic.
    if (eq_parts < key_parts_used) {
      longlong lo = 0;
      longlong hi = 0;
      if (eq_parts < key->user_defined_key_parts &&
          key->rec_per_key[eq_parts] > 0 &&
          helios_decode_keypart_int(table, key, eq_parts, min_key, &lo) &&
          helios_decode_keypart_int(table, key, eq_parts, max_key, &hi) &&
          hi >= lo) {
        const double range_vals =
            (hi > lo) ? static_cast<double>(hi - lo) : 1.0;
        double est =
            range_vals * static_cast<double>(key->rec_per_key[eq_parts]);

        const double cap = static_cast<double>(key->rec_per_key[rpk_idx]);
        if (cap >= 1.0 && est > cap) est = cap;
        if (est < 1.0) est = 1.0;
        estimate = static_cast<ha_rows>(est);
      } else {
        estimate = std::max(static_cast<ha_rows>(1), estimate / 2);
      }
    }
  } else {
    // No equality at all -> pure range scan.
    // Heuristic: two-sided range ~5%, one-sided ~10% (NDB fallback).
    if (min_key != nullptr && max_key != nullptr) {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 20);
    } else {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 10);
    }

    // Leading-key range histogram: use the server-built cumulative counts as
    // a floor over the heuristic. Missing or malformed stats keep the heuristic.
    const bool is_primary = table->s != nullptr && inx == table->s->primary_key;
    const std::string index_name =
        is_primary ? std::string() : std::string(key->name ? key->name : "");
    if (share != nullptr) {
      std::lock_guard<std::mutex> lock(share->index_ndv_mu_);
      auto hist_it = share->index_hist_.find(index_name);
      if (hist_it != share->index_hist_.end()) {
        const LineairDB_share::RangeHist &hist = hist_it->second;
        if (!hist.bounds.empty() && hist.bounds.size() == hist.cum.size()) {
          auto rank_le = [&](const std::string &encoded_key) -> double {
            if (encoded_key >= hist.bounds.back())
              return static_cast<double>(hist.cum.back());
            auto it =
                std::upper_bound(hist.bounds.begin(), hist.bounds.end(),
                                 encoded_key);
            if (it == hist.bounds.begin())
              return 0.0;
            const size_t pos =
                static_cast<size_t>((it - hist.bounds.begin()) - 1);
            return static_cast<double>(hist.cum[pos]);
          };

          constexpr key_part_map kLeadingPart = 1;
          bool enc_ok = true;
          double lo = 0.0;
          double hi = static_cast<double>(hist.cum.back());
          if (min_key != nullptr) {
            std::string encoded = lineairdb_keyenc::convert_key_to_ldbformat(
                table, inx, min_key->key, kLeadingPart);
            if (encoded.empty())
              enc_ok = false;
            else
              lo = rank_le(encoded);
          }
          if (enc_ok && max_key != nullptr) {
            std::string encoded = lineairdb_keyenc::convert_key_to_ldbformat(
                table, inx, max_key->key, kLeadingPart);
            if (encoded.empty())
              enc_ok = false;
            else
              hi = rank_le(encoded);
          }
          const double hist_est = enc_ok ? (hi - lo) : -1.0;
          if (hist_est >= 1.0 &&
              static_cast<ha_rows>(hist_est) > estimate) {
            estimate = static_cast<ha_rows>(hist_est);
          }
        }
      }
    }
  }

  // Floor: always return at least 1 row to avoid zero-cost estimates
  if (estimate < 1)
    estimate = 1;

  return estimate;
}

/**
  @brief
  create() is called to create a database. The variable name will have the
  name of the table.
  @see
  ha_create_table() in handle.cc
*/

// PAX typed-cell kinds (mirror LineairDB::Pax::FieldKind in pax_store.h; the
// proxy is PAX-oblivious so the values are duplicated by design, kept in sync).
namespace pax_kind {
constexpr uint32_t UNTYPED = 0;
constexpr uint32_t INT32 = 1;   // 4-byte LE signed int
constexpr uint32_t INT64 = 2;   // 8-byte LE signed int
constexpr uint32_t DATE = 3;    // 4-byte LE YYYYMMDD
[[maybe_unused]] constexpr uint32_t DEC64 = 4;  // 8-byte LE scaled int (M2b)
}  // namespace pax_kind

// PAX single-copy storage: per-field cell widths shipped at CREATE TABLE.
// For UNTYPED fields each width is a safe upper bound on the bytes
// Field::val_str() can render (the row format stores val_str strings). For a
// typed field (M2) the width is the fixed binary payload width (4/8) and the
// kind tells the engine to parse val_str once at scatter and reformat it at
// gather. Widths are only a hint — a row that outgrows / fails to parse its
// cell falls back to the server's heap path — but a table with any field wider
// than kMaxCellBytes skips PAX entirely (empty result) to avoid pathological
// padding. `kinds`/`scales` are filled in lockstep with the returned widths.
static std::vector<uint32_t> compute_pax_field_widths(
    TABLE *table, std::vector<uint32_t> *kinds = nullptr,
    std::vector<int32_t> *scales = nullptr) {
  constexpr uint32_t kMaxCellBytes = 2048;
  std::vector<uint32_t> widths;
  widths.reserve(table->s->fields + 1);
  if (kinds) {
    kinds->clear();
    kinds->reserve(table->s->fields + 1);
    kinds->push_back(pax_kind::UNTYPED);  // field #0: null flags, verbatim
  }
  if (scales) {
    scales->clear();
    scales->reserve(table->s->fields + 1);
    scales->push_back(0);
  }
  widths.push_back(table->s->null_bytes);  // field #0: null flags, verbatim
  for (uint i = 0; i < table->s->fields; i++) {
    Field *f = table->field[i];
    uint32_t w = f->field_length;
    uint32_t kind = pax_kind::UNTYPED;
    int32_t scale = 0;
    // A ZEROFILL integer left-pads val_str to its display width, so the value
    // alone cannot reproduce the exact bytes — keep such columns UNTYPED.
    // MYSQL_TYPE_YEAR renders zero-filled to its display width ("0000" for 0)
    // REGARDLESS of the Field_num::zerofill member (which is observed false at
    // runtime), so relying on that flag mis-types YEAR and gathers "0" != "0000"
    // (caught by the YEAR round-trip regression) — force YEAR UNTYPED explicitly.
    const bool zerofill =
        (f->type() == MYSQL_TYPE_YEAR)
            ? true
            : ((f->type() == MYSQL_TYPE_TINY || f->type() == MYSQL_TYPE_SHORT ||
                f->type() == MYSQL_TYPE_INT24 || f->type() == MYSQL_TYPE_LONG ||
                f->type() == MYSQL_TYPE_LONGLONG ||
                f->type() == MYSQL_TYPE_DECIMAL ||
                f->type() == MYSQL_TYPE_NEWDECIMAL)
                   ? down_cast<const Field_num *>(f)->zerofill
                   : false);
    switch (f->type()) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_YEAR:
        // field_length is a display width; val_str can render up to 20
        // digits + sign regardless.
        w = std::max<uint32_t>(w, 21);
        if (!zerofill) {
          const bool is_ll = f->type() == MYSQL_TYPE_LONGLONG;
          const bool is_long = f->type() == MYSQL_TYPE_LONG;
          if (is_ll && f->is_unsigned()) {
            // BIGINT UNSIGNED can exceed int64 range: keep ASCII.
          } else if (is_ll || (is_long && f->is_unsigned())) {
            kind = pax_kind::INT64;  // 8-byte range
            w = 8;
          } else {
            kind = pax_kind::INT32;  // TINY/SHORT/INT24/LONG(signed)/YEAR
            w = 4;
          }
        }
        break;
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
        w = std::max<uint32_t>(w, 40);
        break;
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        w += 2;  // sign + decimal point slack (UNTYPED bound)
        // [M2b] A fixed-scale DECIMAL(p,s) whose value fits a scaled int64
        // becomes an 8-byte FK_DEC64 cell here; DISABLED in M2a (DECIMAL stays
        // ASCII UNTYPED) — enabled in the M2b stage together with the server's
        // typed-decimal filter/agg path (strtod-double boundary semantics).
        break;
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
        // DATE val_str is always "YYYY-MM-DD" -> YYYYMMDD int (fits int32).
        kind = pax_kind::DATE;
        w = 4;
        break;
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIME2:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_TIMESTAMP2:
        w = std::max<uint32_t>(w, 32);
        break;
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
        // field_length is the charset OCTET length: utf8mb4 reserves 4 bytes
        // per declared character, so a VARCHAR(44) utf8mb4 cell pads to 176 B
        // even though pure-ASCII data (all of TPC-H/TPC-C) needs at most 44.
        // Size the cell to the declared CHARACTER length instead
        // (char_length() == field_length / mbmaxlen; a no-op for latin1/binary
        // where mbmaxlen == 1). A genuine multibyte row whose encoded bytes
        // exceed char_length() overflows to the EXISTING per-row heap fallback
        // (ScatterRow -> false -> BumpOverflow): never wrong, but it disables
        // strip-direct scans for that table (loud/slow degradation), so this
        // trades a rare-row scan slowdown for a ~4x string-strip memory cut.
        w = f->char_length();
        break;
      default:
        break;  // other types: field_length already bounds val_str bytes
    }
    if (w > kMaxCellBytes) return {};  // e.g. TEXT/BLOB: keep row-store
    widths.push_back(w);
    if (kinds) kinds->push_back(kind);
    if (scales) scales->push_back(scale);
  }
  return widths;
}

int ha_lineairdb::create(const char *table_name, TABLE *table, HA_CREATE_INFO *,
                         dd::Table *) {
  DBUG_TRACE;
  db_table_name = std::string(table_name);

  // create() is called without external_lock/start_stmt, so userThread may not
  // be set yet. Use ha_thd() to ensure get_proxy() can find the THD context.
  userThread = ha_thd();

  // Create table via RPC.
  // In a disaggregated setup, multiple MySQL nodes share the same LineairDB
  // storage. The table may already exist from another node's CREATE TABLE.
  // Ignore "already exists" — MySQL-side metadata still needs to be created.
  auto proxy = get_proxy();
  std::vector<uint32_t> pax_kinds;
  std::vector<int32_t> pax_scales;
  std::vector<uint32_t> pax_widths =
      compute_pax_field_widths(table, &pax_kinds, &pax_scales);
  if (pax_widths.empty()) {  // table skips PAX: send nothing typed
    pax_kinds.clear();
    pax_scales.clear();
  }
  proxy->db_create_table(db_table_name, pax_widths, pax_kinds, pax_scales);

  // Create secondary indexes (also ignore "already exists")
  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    uint index_type = (key_info.flags & HA_NOSAME) ? LDB_INDEX_UNIQUE : 0;
    if (i != table->s->primary_key) {
      proxy->db_create_secondary_index(
          db_table_name, std::string(key_info.name), index_type);
    }
  }
  return 0;
}

/**
  Check if inplace alter is supported for the given operation.
  Currently supports ADD_INDEX and ADD_UNIQUE_INDEX.
*/
enum_alter_inplace_result ha_lineairdb::check_if_supported_inplace_alter(
    TABLE *altered_table [[maybe_unused]], Alter_inplace_info *ha_alter_info) {
  DBUG_TRACE;

  // Support ADD/DROP INDEX operations.
  // DROP_INDEX is a no-op placeholder: the index data remains in LineairDB.
  // It must be accepted because MySQL sends ADD_INDEX | DROP_INDEX together
  // when replacing a foreign key auto-index with an explicit CREATE INDEX.
  Alter_inplace_info::HA_ALTER_FLAGS dominated_flags =
      Alter_inplace_info::ADD_INDEX | Alter_inplace_info::DROP_INDEX |
      Alter_inplace_info::ADD_UNIQUE_INDEX |
      Alter_inplace_info::DROP_UNIQUE_INDEX;

  // ALTER TABLE ... SECONDARY_ENGINE = x|NULL is a pure metadata change
  // (the DD option string); no storage work is needed.
  if (ha_alter_info->handler_flags == Alter_inplace_info::CHANGE_CREATE_OPTION &&
      ha_alter_info->create_info != nullptr &&
      (ha_alter_info->create_info->used_fields &
       HA_CREATE_USED_SECONDARY_ENGINE) &&
      !(ha_alter_info->create_info->used_fields &
        ~HA_CREATE_USED_SECONDARY_ENGINE)) {
    return HA_ALTER_INPLACE_INSTANT;
  }

  if (ha_alter_info->handler_flags & ~dominated_flags) {
    // Unsupported operation requested
    return HA_ALTER_INPLACE_NOT_SUPPORTED;
  }

  return HA_ALTER_INPLACE_EXCLUSIVE_LOCK;
}

bool ha_lineairdb::backfill_commit_chunk(
    std::vector<LineairDBProxy::BatchOp> &ops) {
  if (ops.empty()) return true;

  auto *chunk_tx =
      new LineairDBTransaction(ha_thd(), get_proxy(), lineairdb_hton, FENCE);
  chunk_tx->set_prefetch_mode(false);
  chunk_tx->begin_transaction();
  chunk_tx->choose_table(db_table_name);

  // One checked batch write per chunk so a server-side abort is observed.
  const bool wrote = chunk_tx->batch_write(db_table_name, ops);
  ops.clear();
  if (!wrote || chunk_tx->is_aborted()) {
    chunk_tx->set_status_to_abort();
    chunk_tx->end_transaction();
    return false;
  }
  return chunk_tx->end_transaction();
}

bool ha_lineairdb::backfill_indexes_parallel(
    std::vector<std::pair<std::string, std::string>> &rows,
    const std::vector<std::pair<std::string, const KEY *>> &specs) {
  // Phase A: decode each row once, build one write per index, and bucket it by
  // secondary-key hash. Single-threaded -- decode uses the shared record buffer.
  std::vector<std::vector<LineairDBProxy::BatchOp>> partition(
      kBackfillParallelWorkers);
  // Reserve each bucket to its expected hash share so the per-row push_back
  // below does not repeatedly reallocate the per-worker write buffers.
  const size_t total_ops = rows.size() * specs.size();
  const size_t reserve_per_bucket =
      total_ops == 0 ? 0
                     : total_ops / kBackfillParallelWorkers +
                           total_ops / (kBackfillParallelWorkers * 8) + 1;
  if (reserve_per_bucket != 0) {
    for (auto &bucket : partition) bucket.reserve(reserve_per_bucket);
  }
  std::hash<std::string> hasher;
  bool decode_failed = false;
  for (auto &row : rows) {
    if (row.second.empty()) continue;
    const auto *value = reinterpret_cast<const std::byte *>(row.second.data());
    if (set_fields_from_lineairdb(table->record[0], value, row.second.size())) {
      decode_failed = true;
      break;
    }
    for (const auto &spec : specs) {
      LineairDBProxy::BatchOp op;
      op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexWrite;
      op.table_name = db_table_name;
      op.index_name = spec.first;
      op.primary_key = row.first;
      op.secondary_key =
          build_secondary_key_from_row(table->record[0], *spec.second);
      partition[hasher(op.secondary_key) % kBackfillParallelWorkers].push_back(
          std::move(op));
    }
  }
  blobroot.Clear();
  if (decode_failed) return false;

  // Phase B: one worker per partition, each on its own connection. The hash
  // partition commits every op for a secondary key on one worker, so no two
  // workers mutate the same index DataItem; distinct keys are distinct DataItems
  // committed through the normal concurrent path LineairDB serves for multiple
  // query layers. Workers touch no MySQL state; a failure sets the shared flag
  // for the caller to report.
  std::atomic<bool> failed{false};
  const std::string host =
      srv_server_host ? srv_server_host : std::string("127.0.0.1");
  const int port = static_cast<int>(srv_server_port);
  std::vector<std::thread> workers;
  workers.reserve(kBackfillParallelWorkers);
  for (size_t w = 0; w < kBackfillParallelWorkers; ++w) {
    if (partition[w].empty()) continue;
    workers.emplace_back([&, w]() {
      LineairDBProxy conn(host, port);
      std::vector<LineairDBProxy::BatchOp> chunk;
      chunk.reserve(kBackfillWriteChunkRows);
      // Ship the buffered writes as one stateless commit (no reads to validate).
      auto commit_chunk = [&]() -> bool {
        if (chunk.empty()) return true;
        std::string reason;
        const bool ok = conn.tx_validate_and_commit({}, {}, {}, {}, chunk, {},
                                                     FENCE, &reason);
        chunk.clear();
        return ok;
      };
      for (auto &op : partition[w]) {
        if (failed.load(std::memory_order_relaxed)) return;
        chunk.push_back(std::move(op));
        if (chunk.size() >= kBackfillWriteChunkRows && !commit_chunk()) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
      if (!commit_chunk()) failed.store(true, std::memory_order_relaxed);
    });
  }
  for (auto &t : workers) t.join();
  return !failed.load(std::memory_order_relaxed);
}

bool ha_lineairdb::backfill_unique_serial(const std::string &index_name,
                                          const KEY &runtime_key) {
  // A unique index scans and commits serially through the staging path, which
  // keeps the in-write duplicate check. Its cost is small (no unique index is on
  // the large fact table); the parallel scan-once path is for the non-unique set.
  auto *scan_tx = get_transaction(ha_thd());
  if (scan_tx == nullptr || scan_tx->is_aborted()) return false;
  scan_tx->choose_table(db_table_name);
  auto rows = scan_tx->get_matching_keys_and_values_from_prefix(std::string());
  if (scan_tx->is_aborted()) return false;

  std::vector<LineairDBProxy::BatchOp> write_chunk;
  write_chunk.reserve(kBackfillWriteChunkRows);
  bool failed = false;
  for (auto &row : rows) {
    if (row.second.empty()) continue;
    const auto *value = reinterpret_cast<const std::byte *>(row.second.data());
    if (set_fields_from_lineairdb(table->record[0], value, row.second.size())) {
      failed = true;
      break;
    }
    LineairDBProxy::BatchOp op;
    op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexWrite;
    op.table_name = db_table_name;
    op.index_name = index_name;
    op.primary_key = std::move(row.first);
    op.secondary_key = build_secondary_key_from_row(table->record[0], runtime_key);
    write_chunk.push_back(std::move(op));
    if (write_chunk.size() >= kBackfillWriteChunkRows &&
        !backfill_commit_chunk(write_chunk)) {
      failed = true;
      break;
    }
  }
  blobroot.Clear();
  if (failed || scan_tx->is_aborted() ||
      !backfill_commit_chunk(write_chunk)) {
    return false;
  }
  return true;
}

bool ha_lineairdb::inplace_alter_table(TABLE *altered_table,
                                       Alter_inplace_info *ha_alter_info,
                                       const dd::Table *old_table_def
                                       [[maybe_unused]],
                                       dd::Table *new_table_def
                                       [[maybe_unused]]) {
  DBUG_TRACE;

  // Fill each new secondary index from existing rows. The EXCLUSIVE metadata
  // lock is node-local, so this covers single-node ADD INDEX only; cross-node
  // DDL coordination belongs to the ddl-sync work.
  userThread = ha_thd();
  auto proxy = get_proxy();

  if (altered_table == nullptr || altered_table->s == nullptr) return true;

  // Non-unique indexes are collected and backfilled together below so a single
  // scan and decode pass feeds them all. A unique index keeps the staging commit
  // path (its in-write duplicate check) and is backfilled serially.
  std::vector<std::pair<std::string, const KEY *>> nu_specs;
  for (uint i = 0; i < ha_alter_info->index_add_count; i++) {
    const uint key_idx = ha_alter_info->index_add_buffer[i];
    const KEY *key_info = &ha_alter_info->key_info_buffer[key_idx];
    const std::string index_name(key_info->name ? key_info->name : "");
    if (index_name.empty()) return true;  // fail closed: unnamed index

    const uint index_type = (key_info->flags & HA_NOSAME) ? LDB_INDEX_UNIQUE : 0;

    // key_info_buffer and TABLE::key_info use different field-number bases;
    // resolve the runtime KEY by name or the encoder reads the wrong column.
    const KEY *runtime_key = nullptr;
    for (uint k = 0; k < altered_table->s->keys; ++k) {
      const KEY *candidate = &altered_table->key_info[k];
      if (candidate->name != nullptr && index_name == candidate->name) {
        runtime_key = candidate;
        break;
      }
    }
    if (runtime_key == nullptr) return true;

    // LineairDB treats an encoded NULL key as a duplicate, but SQL allows many
    // NULLs in a UNIQUE index; reject nullable UNIQUE backfill instead.
    if (key_info->flags & HA_NOSAME) {
      for (uint p = 0; p < runtime_key->user_defined_key_parts; ++p) {
        if (runtime_key->key_part[p].null_bit != 0) return true;
      }
    }

    // Register the index; fail closed on error. On a multi-index ALTER a later
    // failure leaves earlier backfilled indexes on the server (the DD rollback
    // hides them); purging them is the ddl-sync DROP work.
    if (!proxy->db_create_secondary_index(db_table_name, index_name,
                                          index_type)) {
      return true;
    }

    if (index_type == LDB_INDEX_UNIQUE) {
      if (!backfill_unique_serial(index_name, *runtime_key)) return true;
    } else {
      nu_specs.emplace_back(index_name, runtime_key);
    }
  }

  // Non-unique indexes share one scan and one decode pass, then commit in
  // parallel. A multi-index ALTER on the fact table makes this the common path.
  if (!nu_specs.empty()) {
    auto *scan_tx = get_transaction(ha_thd());
    if (scan_tx == nullptr || scan_tx->is_aborted()) return true;
    scan_tx->choose_table(db_table_name);

    auto rows =
        scan_tx->get_matching_keys_and_values_from_prefix(std::string());
    if (scan_tx->is_aborted()) return true;  // aborted scan: emit no writes
    if (!backfill_indexes_parallel(rows, nu_specs)) return true;
  }

  return false;
}

/**
 * @brief True only for MySQL's standard forward index-range sequence.
 *
 * The range source is identified by its seq->init function: quick_range_seq_init
 * is the forward scan, quick_range_rev_seq_init is the reverse one, and BKA
 * supplies its own callback. Only the forward range matches the forward-staged
 * cache, so the others are rejected.
 */
static bool lineairdb_is_forward_index_range_sequence(RANGE_SEQ_IF *seq) {
  extern range_seq_t quick_range_seq_init(void *, uint, uint);
  return seq != nullptr && seq->init == quick_range_seq_init;
}

/**
 * @brief Predict prefetch mode without starting a transaction.
 *
 * MRR cost estimation must stay side-effect-free, so it cannot call
 * get_transaction() (which allocates and may emit RPCs). Reuse an existing
 * transaction's fixed mode, else predict from the session as get_transaction will.
 */
static bool lineairdb_predict_prefetch_mode(THD *thd) {
  auto *ctx =
      *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, lineairdb_hton));
  if (ctx != nullptr && ctx->tx != nullptr) return ctx->tx->is_prefetch_mode();
  return srv_prefetch_execution && thd_can_use_prefetch(thd);
}

/**
 * @brief Return whether read_cost() should charge per-row materialization.
 *
 * @details Non-covering secondary ref/range reads usually fetch each base row
 * by PK after the index probe, so they should pay the materialization charge.
 * Primary-key reads already produce the base row, and grouped single-table
 * SELECTs served by bulk prefetch do not fetch rows one by one by PK.
 */
bool ha_lineairdb::helios_charge_materialise(uint index, double rows) const {
  const TABLE *t = table;
  if (t == nullptr || t->in_use == nullptr) return true;

  if (t->s != nullptr && t->s->primary_key != MAX_KEY &&
      index == t->s->primary_key) {
    return false;
  }

  THD *thd = t->in_use;
  if (thd->lex == nullptr) return true;

  // Only plain read-only SELECTs can use the bulk grouped path.
  if (thd->lex->sql_command != SQLCOM_SELECT || thd->lex->is_explain())
    return true;
  if (t->reginfo.lock_type > TL_READ) return true;
  if (!lineairdb_predict_prefetch_mode(thd)) return true;

  // The grouped bulk path is recognizable before the final QEP exists.
  Table_ref *tr = t->pos_in_table_list;
  if (tr == nullptr || tr->query_block == nullptr) return true;
  Query_block *qb = tr->query_block;
  if (qb->leaf_table_count != 1 || !qb->is_grouped()) return true;
  if (qb->is_distinct() || qb->olap != UNSPECIFIED_OLAP_TYPE ||
      qb->has_windows()) {
    return true;
  }

  // A large grouped aggregate is served by the override via a primary full scan.
  // Above the threshold, charge so the optimizer prefers that primary scan;
  // below it, keep the secondary cheap so a small aggregate still uses the index.
  return rows >= kSmallAggregateInputRows;
}

/**
 * @brief Advertise custom batch MRR for primary-key point lookups.
 *
 * In non-prefetch ("batched") mode, clear HA_MRR_USE_DEFAULT_IMPL for PK lookups
 * so multi_range_read_init() takes the custom batch path that sends all keys in
 * one RPC. Under prefetch the advertisement is suppressed: reads are served from
 * the staged cache through default MRR (read_range_first -> index_read_map).
 */
ha_rows ha_lineairdb::multi_range_read_info_const(
    uint keyno, RANGE_SEQ_IF *seq, void *seq_init_param, uint n_ranges,
    uint *bufsz, uint *flags, bool *force_default_mrr, Cost_estimate *cost) {
  ha_rows rows = handler::multi_range_read_info_const(
      keyno, seq, seq_init_param, n_ranges, bufsz, flags, force_default_mrr,
      cost);
  if (rows == HA_POS_ERROR) return rows;

  // Custom batch MRR only in batched mode; prefetch serves reads from the cache.
  if (!lineairdb_predict_prefetch_mode(ha_thd()) &&
      keyno == table->s->primary_key) {
    *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
    *bufsz = 0;
    if (cost) {
      cost->reset();
      cost->add_io(1.0);
    }
  }
  return rows;
}

ha_rows ha_lineairdb::multi_range_read_info(uint keyno, uint n_ranges,
                                            uint keys, uint *bufsz,
                                            uint *flags,
                                            Cost_estimate *cost) {
  ha_rows rows = handler::multi_range_read_info(keyno, n_ranges, keys, bufsz,
                                                flags, cost);
  // Custom batch MRR only in batched mode; prefetch serves reads from the cache.
  if (!lineairdb_predict_prefetch_mode(ha_thd()) &&
      keyno == table->s->primary_key) {
    *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
    *bufsz = 0;
    if (cost) {
      cost->reset();
      cost->add_io(1.0);
    }
  }
  return rows;
}

/**
 * @brief Initialize custom MRR: validate ranges and batch-read all keys.
 *
 * Iterates the range sequence to verify every range is a full-key point lookup
 * (EQ_RANGE with all PK columns specified). If any range is a partial-key match
 * or an inequality scan, falls back to MySQL's default MRR — because batch_read
 * only supports exact full-key lookups against LineairDB's KV store.
 *
 * On success, all keys are sent in a single batch_read RPC and results are
 * buffered in mrr_buffer_ for retrieval by multi_range_read_next().
 */
int ha_lineairdb::multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                                        uint n_ranges, uint mode,
                                        HANDLER_BUFFER *buf) {
  auto tx = get_transaction(ha_thd());
  if (!tx || tx->is_aborted()) {
    return abort_errno(tx);
  }

  // Prefetch never uses the custom batch path: the staging RPC already holds the
  // rows, so default MRR (read_range_first -> index_read_map) consumes the cache.
  if (tx->is_prefetch_mode()) {
    if (!(mode & HA_MRR_USE_DEFAULT_IMPL)) {
      // Custom MRR is not advertised under prefetch, so native MRR reaching here
      // is a shape the staged cache cannot serve.
      return prefetch_reject_unsupported(ha_thd(), tx,
                                         "native MRR under prefetch");
    }
    const bool legacy_dml =
        prefetch_needs_legacy_dml_handler(ha_thd(), tx);
    // Statement-scoped autogen stages a single forward range per statement.
    if (!tx->tx_plan_used()) {
      if (n_ranges != 1) {
        return prefetch_reject_unsupported(ha_thd(), tx, "MRR multi-range scan");
      }
      if (!lineairdb_is_forward_index_range_sequence(seq)) {
        return prefetch_reject_unsupported(ha_thd(), tx,
                                           "MRR reverse or non-standard range");
      }
    }
    // Legacy single-table DML has no QEP plan. Default DS-MRR reaches
    // read_range_first()->index_read_map(), where the complete bounds exist.
    if (!legacy_dml) {
      if (int err = maybe_prefetch_for_statement(ha_thd(), tx, table)) return err;
    }
    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
    mrr_use_batch_ = false;
    mrr_buffer_.clear();
    mrr_buffer_pos_ = 0;
    m_ds_mrr.init(table);
    return m_ds_mrr.dsmrr_init(seq, seq_init_param, n_ranges,
                               mode | HA_MRR_USE_DEFAULT_IMPL, buf);
  }

  if (mode & HA_MRR_USE_DEFAULT_IMPL) {
    mrr_use_batch_ = false;
    m_ds_mrr.init(table);
    return m_ds_mrr.dsmrr_init(seq, seq_init_param, n_ranges, mode, buf);
  }

  // Collect all lookup keys from the range sequence
  range_seq_t seq_ctx = seq->init(seq_init_param, n_ranges, mode);
  KEY_MULTI_RANGE range;
  std::vector<std::string> batch_keys;
  std::vector<char *> range_infos;

  // Determine the full-key keypart_map for the active index
  const uint pk_parts = table->key_info[active_index].user_defined_key_parts;
  const key_part_map full_key_map =
      (pk_parts < sizeof(key_part_map) * 8)
          ? ((static_cast<key_part_map>(1) << pk_parts) - 1)
          : ~static_cast<key_part_map>(0);

  while (seq->next(seq_ctx, &range) == 0) {
    // Only batch full-key point lookups (EQ_RANGE with all PK parts).
    // Partial-key ranges (e.g. 3 of 4 PK cols) or range scans (e.g. id > 15)
    // cannot be converted to individual key lookups — fall back to default.
    if (!(range.range_flag & EQ_RANGE) ||
        (range.start_key.keypart_map & full_key_map) != full_key_map) {
      mrr_use_batch_ = false;
      m_ds_mrr.init(table);
      return m_ds_mrr.dsmrr_init(seq, seq_init_param, n_ranges,
                                 mode | HA_MRR_USE_DEFAULT_IMPL, buf);
    }
    std::string ldb_key =
        convert_key_to_ldbformat(range.start_key.key, range.start_key.keypart_map);
    batch_keys.push_back(ldb_key);
    range_infos.push_back(range.ptr);
  }

  mrr_use_batch_ = true;
  mrr_buffer_.clear();
  mrr_buffer_pos_ = 0;

  tx->choose_table(db_table_name);

  if (batch_keys.empty()) return 0;

  // Send all keys in a single batch RPC
  auto results = tx->batch_read(batch_keys);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  // Buffer results for multi_range_read_next()
  for (size_t i = 0; i < results.size(); i++) {
    if (results[i].first) {
      mrr_buffer_.push_back({std::move(results[i].second), range_infos[i]});
    }
  }

  return 0;
}

int ha_lineairdb::multi_range_read_next(char **range_info) {
  if (!mrr_use_batch_) {
    return m_ds_mrr.dsmrr_next(range_info);
  }

  if (mrr_buffer_pos_ >= mrr_buffer_.size()) {
    return HA_ERR_END_OF_FILE;
  }

  auto &row = mrr_buffer_[mrr_buffer_pos_++];

  const std::byte *ptr = reinterpret_cast<const std::byte *>(row.value.data());
  if (set_fields_from_lineairdb(table->record[0], ptr, row.value.size())) {
    return HA_ERR_OUT_OF_MEM;
  }

  *range_info = row.range_info;
  return 0;
}

int ha_lineairdb::read_range_first(const key_range *start_key,
                                   const key_range *end_key, bool eq_range_arg,
                                   bool sorted) {
  return handler::read_range_first(start_key, end_key, eq_range_arg, sorted);
}

int ha_lineairdb::read_range_next() { return handler::read_range_next(); }


/**
 * @brief Format and set the requested row into `write_buffer_`.
 */
void ha_lineairdb::set_write_buffer(uchar *buf) {
  ldbField.set_null_field(buf, table->s->null_bytes);
  write_buffer_ = ldbField.get_null_field();

  String attribute;
  attribute.set_charset(&my_charset_bin);

  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);
  for (Field **field = table->field; *field; field++) {
    if ((*field)->is_nullable() && (*field)->is_null()) {
      ldbField.set_lineairdb_field("", 0);
    } else {
      attribute.length(0);
      (*field)->val_str(&attribute, &attribute);
      ldbField.set_lineairdb_field(attribute.c_ptr(), attribute.length());
    }
    write_buffer_ += ldbField.get_lineairdb_field();
  }
  tmp_restore_column_map(table->read_set, org_bitmap);
}

bool ha_lineairdb::is_primary_key_exists() {
  return table->s->primary_key != MAX_KEY;
}

bool ha_lineairdb::store_blob_to_field(Field **field) {
  if ((*field)->is_flag_set(BLOB_FLAG)) {
    Field_blob *blob_field = down_cast<Field_blob *>(*field);
    size_t length = blob_field->get_length();
    if (length > 0) {
      unsigned char *new_blob = new (&blobroot) unsigned char[length];
      if (new_blob == nullptr)
        return true;
      memcpy(new_blob, blob_field->get_blob_data(), length);
      blob_field->set_ptr(length, new_blob);
    }
  }
  return false;
}

int ha_lineairdb::set_fields_from_lineairdb(uchar *buf,
                                            const std::byte *const read_buf,
                                            const size_t read_buf_size) {
  // Clear BLOB data from the previous row.
  blobroot.ClearForReuse();
  ldbField.make_mysql_table_row(read_buf, read_buf_size);
  /**
   * for each 8 potentially null columns, buf holds 1 byte flag at the front
   * the number of null flag bytes in buf is shown in table->s->null_bytes
   * the flag is originally set to 0xff, or b11111111
   * if you want to make the first potentially null column to show a non-null
   * value, store 0xfe, or b11111110, in buf
   */
  const auto nullFlags = ldbField.get_null_flags();
  for (size_t i = 0; i < nullFlags.size(); i++) {
    buf[i] = static_cast<uchar>(nullFlags[i]);
  }

  /* Avoid asserts in ::store() for columns that are not going to be updated
   */
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  THD *const thd_for_serve = ha_thd();
  const uint64_t serve_query_id =
      thd_for_serve != nullptr
          ? static_cast<uint64_t>(thd_for_serve->query_id)
          : 0;

  // Refresh once per statement, and again if statement-root staging registers
  // projection after an earlier unit serve populated the memo.
  const uint64_t proj_epoch =
      LineairDBTransaction::load_projection_global_epoch();
  if (serve_memo_query_id_ != serve_query_id ||
      serve_memo_proj_epoch_ != proj_epoch) {
    serve_memo_query_id_ = serve_query_id;
    serve_memo_proj_epoch_ = proj_epoch;
    auto tx = get_transaction(thd_for_serve);
    serve_memo_projection_ =
        tx != nullptr ? tx->load_table_projection(db_table_name) : nullptr;

    // DML may rebuild rows and secondary keys from this buffer, so only a
    // plain SELECT can leave unread fields untouched.
    serve_memo_can_skip_unread_fields_ =
        thd_for_serve != nullptr && thd_for_serve->lex != nullptr &&
        thd_for_serve->lex->sql_command == SQLCOM_SELECT;
  }

  const bool can_skip_unread_fields = serve_memo_can_skip_unread_fields_;

  {
    // Projected rows contain kept columns only; null flags remain full-width.
    const std::vector<uint32_t> *kept = serve_memo_projection_;

    // Unit-only prefetch can still serve full rows for a projected table.
    if (kept != nullptr && ldbField.get_row_size() != kept->size()) {
      kept = nullptr;
    }

    if (kept != nullptr) {
      for (size_t projected_col = 0; projected_col < kept->size();
           ++projected_col) {
        const uint32_t field_index = (*kept)[projected_col];
        if (field_index >= table->s->fields) break;
        if (can_skip_unread_fields &&
            !bitmap_is_set(table->read_set, field_index)) {
          continue;
        }
        Field *f = table->field[field_index];
        const auto mysqlFieldValue =
            ldbField.get_column_of_row(projected_col);
        if (f->is_nullable() && f->is_null_in_record(buf)) {
          f->set_null();
        } else {
          f->store(mysqlFieldValue.data(), mysqlFieldValue.size(),
                   &my_charset_bin, CHECK_FIELD_WARN);
          Field *arr[2] = {f, nullptr};
          if (store_blob_to_field(arr)) {
            dbug_tmp_restore_column_map(table->write_set, org_bitmap);
            return HA_ERR_OUT_OF_MEM;
          }
        }
      }
      dbug_tmp_restore_column_map(table->write_set, org_bitmap);
      return 0;
    }
  }

  // Full rows map parsed column index directly to TABLE::field[].
  size_t columnIndex = 0;
  for (Field **field = table->field; *field; field++) {
    if (columnIndex >= ldbField.get_row_size()) break;  // short/trimmed value
    const auto mysqlFieldValue = ldbField.get_column_of_row(columnIndex++);
    if (can_skip_unread_fields &&
        !bitmap_is_set(table->read_set, (*field)->field_index())) {
      continue;  // pure SELECT: column not read this statement
    }
    if ((*field)->is_nullable() && (*field)->is_null_in_record(buf)) {
      (*field)->set_null();
    } else {
      (*field)->store(mysqlFieldValue.data(), mysqlFieldValue.size(),
                      &my_charset_bin, CHECK_FIELD_WARN);
      if (store_blob_to_field(field))
        return HA_ERR_OUT_OF_MEM;
    }
  }
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  return 0;
}

struct st_mysql_storage_engine lineairdb_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

static ulong srv_enum_var = 0;
static ulong srv_ulong_var = 0;
static double srv_double_var = 0;
static int srv_signed_int_var = 0;
static long srv_signed_long_var = 0;
static longlong srv_signed_longlong_var = 0;

const char *enum_var_names[] = {"e1", "e2", NullS};

TYPELIB enum_var_typelib = {array_elements(enum_var_names) - 1,
                            "enum_var_typelib", enum_var_names, nullptr};

static MYSQL_SYSVAR_ENUM(enum_var,                       // name
                         srv_enum_var,                   // varname
                         PLUGIN_VAR_RQCMDARG,            // opt
                         "Sample ENUM system variable.", // comment
                         nullptr,                        // check
                         nullptr,                        // update
                         0,                              // def
                         &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(ulong_var, srv_ulong_var, PLUGIN_VAR_RQCMDARG,
                          "0..1000", nullptr, nullptr, 8, 0, 1000, 0);

static MYSQL_SYSVAR_DOUBLE(double_var, srv_double_var, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5,
                           0); // reserved always 0

static MYSQL_THDVAR_DOUBLE(double_thdvar, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5, 0);

static MYSQL_SYSVAR_INT(signed_int_var, srv_signed_int_var, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_THDVAR_INT(signed_int_thdvar, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_SYSVAR_LONG(signed_long_var, srv_signed_long_var,
                         PLUGIN_VAR_RQCMDARG, "LONG_MIN..LONG_MAX", nullptr,
                         nullptr, -10, LONG_MIN, LONG_MAX, 0);

static MYSQL_THDVAR_LONG(signed_long_thdvar, PLUGIN_VAR_RQCMDARG,
                         "LONG_MIN..LONG_MAX", nullptr, nullptr, -10, LONG_MIN,
                         LONG_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(signed_longlong_var, srv_signed_longlong_var,
                             PLUGIN_VAR_RQCMDARG, "LLONG_MIN..LLONG_MAX",
                             nullptr, nullptr, -10, LLONG_MIN, LLONG_MAX, 0);

static MYSQL_THDVAR_LONGLONG(signed_longlong_thdvar, PLUGIN_VAR_RQCMDARG,
                             "LLONG_MIN..LLONG_MAX", nullptr, nullptr, -10,
                             LLONG_MIN, LLONG_MAX, 0);

// LineairDB server connection target sysvars
static MYSQL_SYSVAR_STR(server_host, srv_server_host,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "LineairDB server hostname or IP address.", nullptr,
                        nullptr, "127.0.0.1");
static MYSQL_SYSVAR_ULONG(server_port, srv_server_port, PLUGIN_VAR_RQCMDARG,
                          "LineairDB server TCP port.", nullptr, nullptr, 9999,
                          1, 65535, 0);
static MYSQL_SYSVAR_BOOL(prefetch_execution, srv_prefetch_execution,
                         PLUGIN_VAR_OPCMDARG,
                         "Enable experimental prefetch execution.", nullptr,
                         nullptr, false);
static MYSQL_SYSVAR_BOOL(
    prefetch_ro_novalidate, srv_prefetch_ro_novalidate, PLUGIN_VAR_OPCMDARG,
    "Skip commit-time read validation for autocommit read-only SELECT under "
    "prefetch execution. Sound only without concurrent writers (e.g. "
    "analytical read-only workloads); the commit RPC is omitted entirely.",
    nullptr, nullptr, false);

static SYS_VAR *lineairdb_system_variables[] = {
    MYSQL_SYSVAR(server_host),
    MYSQL_SYSVAR(server_port),
    MYSQL_SYSVAR(prefetch_execution),
    MYSQL_SYSVAR(prefetch_ro_novalidate),
    MYSQL_SYSVAR(enum_var),
    MYSQL_SYSVAR(ulong_var),
    MYSQL_SYSVAR(double_var),
    MYSQL_SYSVAR(double_thdvar),
    MYSQL_SYSVAR(last_create_thdvar),
    MYSQL_SYSVAR(create_count_thdvar),
    MYSQL_SYSVAR(signed_int_var),
    MYSQL_SYSVAR(signed_int_thdvar),
    MYSQL_SYSVAR(signed_long_var),
    MYSQL_SYSVAR(signed_long_thdvar),
    MYSQL_SYSVAR(signed_longlong_var),
    MYSQL_SYSVAR(signed_longlong_thdvar),
    nullptr};

// this is an lineairdb of SHOW_FUNC
static int show_func_lineairdb(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_CHAR;
  var->value = buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
           "enum_var is %lu, ulong_var is %lu, "
           "double_var is %f, signed_int_var is %d, "
           "signed_long_var is %ld, signed_longlong_var is %lld",
           srv_enum_var, srv_ulong_var, srv_double_var, srv_signed_int_var,
           srv_signed_long_var, srv_signed_longlong_var);
  return 0;
}

lineairdb_vars_t lineairdb_vars = {100,  20.01, "three hundred",
                                   true, false, 8250};

static SHOW_VAR show_status_lineairdb[] = {
    {"var1", (char *)&lineairdb_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"var2", (char *)&lineairdb_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF,
     SHOW_SCOPE_UNDEF} // null terminator required
};

static SHOW_VAR show_array_lineairdb[] = {
    {"array", (char *)show_status_lineairdb, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
    {"var3", (char *)&lineairdb_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
    {"var4", (char *)&lineairdb_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

static SHOW_VAR func_status[] = {
    {"lineairdb_func_lineairdb", (char *)show_func_lineairdb, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {"lineairdb_status_var5", (char *)&lineairdb_vars.var5, SHOW_BOOL,
     SHOW_SCOPE_GLOBAL},
    {"lineairdb_status_var6", (char *)&lineairdb_vars.var6, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"lineairdb_status", (char *)show_array_lineairdb, SHOW_ARRAY,
     SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

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
    func_status,                /* status variables */
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
