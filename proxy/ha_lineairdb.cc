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
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <unordered_set>
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "lineairdb_field_types.h"
#include "lineairdb.pb.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "sql/sql_optimizer.h"             // JOIN
#include "sql/join_optimizer/access_path.h"  // AccessPath (QEP tree)
#include "sql/join_optimizer/materialize_path_parameters.h"  // MATERIALIZE param
#include "sql/query_result.h"                 // Query_result (Phase-8 override)
#include "sql/item_sum.h"                     // Item_sum (Phase-8 aggregation)
#include "sql/visible_fields.h"               // VisibleFields (Phase-8 output)
#include "sql/my_decimal.h"                   // my_decimal (Phase-8 SUM/AVG)
#include <map>                                // Phase-8 group accumulators
#include "typelib.h"

#define BLOB_MEMROOT_ALLOC_SIZE (8192)
#define FENCE false

// LineairDB SecondaryIndexOption::Constraint wire bit for UNIQUE.
static constexpr uint LDB_INDEX_UNIQUE = 1u;

namespace {
constexpr unsigned char kKeyMarkerNotNull = 0x00;
constexpr unsigned char kKeyMarkerNull = 0x01;

constexpr unsigned char kKeyTypeInt = 0x10;
constexpr unsigned char kKeyTypeString = 0x20;
constexpr unsigned char kKeyTypeDatetime = 0x30;
constexpr unsigned char kKeyTypeOther = 0xF0;

} // namespace

// LIMIT and direction to pass to a range scan
struct RangeScanLimit {
  ha_rows row_limit{0};
  bool reverse_scan{false};
};

// True when one ORDER BY item is exactly the requested keypart and direction
static bool order_item_matches_key_part(ORDER *order, const KEY *key,
                                        uint key_part_idx,
                                        enum_order direction) {
  // Validate the ORDER BY node shape before comparing fields
  if (order == nullptr) return false;                // Missing ORDER node
  if (key == nullptr) return false;                  // Missing key metadata
  if (order->direction != direction) return false;   // Wrong direction

  const bool key_part_exists = (key_part_idx < key->user_defined_key_parts);
  if (!key_part_exists) return false;                // ORDER exceeds key

  const bool has_order_item =
      (order->item != nullptr && *(order->item) != nullptr);
  if (!has_order_item) return false;                 // Missing ORDER item

  // Resolve ORDER BY expression to a table field
  Item *item = (*order->item)->real_item();
  const bool is_field_item = (item->type() == Item::FIELD_ITEM);
  if (!is_field_item) return false;                  // Not a bare field

  // Compare the ORDER BY field with the expected keypart
  Item_field *field_item = down_cast<Item_field *>(item);
  const Field *order_field = field_item->field;
  const Field *key_field = key->key_part[key_part_idx].field;
  if (order_field == nullptr) return false;          // Missing ORDER field
  if (key_field == nullptr) return false;            // Missing key field

  return order_field->field_index() == key_field->field_index();
}

// True when ORDER BY is a prefix of the remaining key suffix in one direction
static bool order_by_matches_key_suffix(Query_block *qb, const KEY *key,
                                        uint matched_prefix,
                                        enum_order direction) {
  // LIMIT needs an explicit ORDER BY that matches scan order
  if (qb == nullptr) return false;                   // Missing query block
  if (key == nullptr) return false;                  // Missing key metadata
  if (qb->order_list.elements == 0) return false;    // Unordered LIMIT

  // If all keyparts are fixed, the scan can return at most one key
  const uint key_parts = key->user_defined_key_parts;
  if (matched_prefix >= key_parts) return true;      // Full key fixed

  // ORDER BY must follow the unfixed key suffix in one direction
  uint order_pos = 0;
  for (ORDER *order = qb->order_list.first; order != nullptr;
       order = order->next, ++order_pos) {
    const uint key_part_idx = matched_prefix + order_pos;
    if (!order_item_matches_key_part(order, key, key_part_idx, direction)) {
      return false;
    }
  }

  return true;
}

// Returns LIMIT and scan direction to push, or row_limit=0 to keep it local
static RangeScanLimit range_scan_limit_for_order(
    THD *thd, const KEY *key, uint matched_prefix, bool has_mysql_only_filter) {
  RangeScanLimit scan_limit;

  // Keep scans unlimited when MySQL must evaluate a filter the server lacks
  if (has_mysql_only_filter) return scan_limit;        // Server may miss rows

  // Validate this is a simple SELECT with a query expression to inspect
  if (thd == nullptr) return scan_limit;               // Missing session
  if (thd->lex == nullptr) return scan_limit;          // Missing SQL state
  if (thd->lex->unit == nullptr) return scan_limit;    // Missing query unit
  const bool is_select = (thd->lex->sql_command == SQLCOM_SELECT);
  if (!is_select) return scan_limit;                   // Not SELECT

  // LIMIT is only safe for one-table, non-aggregate reads
  Query_expression *unit = thd->lex->unit;
  Query_block *qb = unit->global_parameters();
  if (qb == nullptr) return scan_limit;                // Missing query block
  if (qb->leaf_table_count != 1) return scan_limit;    // Join needs root LIMIT
  if (qb->is_explicitly_grouped()) return scan_limit;  // GROUP BY changes rows
  if (qb->is_implicitly_grouped()) return scan_limit;  // Aggregate changes rows

  // Extract LIMIT N and reject shapes the storage scan cannot represent
  if (unit->offset_limit_cnt != 0) return scan_limit;  // OFFSET not supported
  if (unit->select_limit_cnt == 0) return scan_limit;  // Empty LIMIT
  const bool has_limit = (unit->select_limit_cnt != HA_POS_ERROR);
  if (!has_limit) return scan_limit;                   // No LIMIT clause

  // A full-key point lookup returns at most one row, so direction is irrelevant
  const uint key_parts = key->user_defined_key_parts;
  if (matched_prefix >= key_parts) {
    scan_limit.row_limit = unit->select_limit_cnt;
    return scan_limit;
  }

  // ASC uses the natural key order; DESC uses LineairDB ScanReverse
  if (order_by_matches_key_suffix(qb, key, matched_prefix, ORDER_ASC)) {
    scan_limit.row_limit = unit->select_limit_cnt;
    return scan_limit;
  }
  if (order_by_matches_key_suffix(qb, key, matched_prefix, ORDER_DESC)) {
    scan_limit.row_limit = unit->select_limit_cnt;
    scan_limit.reverse_scan = true;
    return scan_limit;
  }

  return scan_limit;
}

// LineairDB server connection target (GLOBAL sysvars backing storage)
static char *srv_server_host = nullptr;
static ulong srv_server_port = 9999;
static bool srv_oneshot_execution = false;

// THD-scoped context
struct LineairDBThdCtx {
  std::shared_ptr<LineairDBProxy> proxy;
  LineairDBTransaction *tx{nullptr};
};

// Phase-5 handler-entry timing (Codex attribution). One file-scope env read so
// the disabled path is a single bool branch — no get_transaction() cost on the
// 6M-call rnd_next hot path when HELIOS_HANDLER_TIMEPROF is unset. When enabled,
// HTP_SCOPE(field) obtains the tx and starts an RAII timer that accumulates into
// tx->htp_.<field>_ns / _n; the [HTIMEPROF] line at commit dumps the totals.
static const bool g_htp_on = std::getenv("HELIOS_HANDLER_TIMEPROF") != nullptr;
#define HTP_SCOPE(field)                                                       \
  LineairDBTransaction *_htp_tx = g_htp_on ? get_transaction(ha_thd()) : nullptr; \
  LineairDBTransaction::HtimeprofScope _htp_scope(                             \
      _htp_tx, _htp_tx ? &_htp_tx->htp_.field##_ns : nullptr,                  \
      _htp_tx ? &_htp_tx->htp_.field##_n : nullptr)

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

// ---------------------------------------------------------------------------
// Phase-8 (gated, default OFF): full aggregation pushdown via the optimizer's
// engine-pushdown hook + JOIN::override_executor_func.
//
// MySQL 8.0.43 calls JOIN::push_to_engines() unconditionally in the optimize
// path (sql_optimizer.cc); for every leaf table whose
// handler::hton_supporting_engine_pushdown() is non-null it invokes
// handlerton::push_to_engine(thd, root_path, join). Inside that hook we may set
// join->override_executor_func, which Query_expression::ExecuteIteratorQuery()
// (sql_union.cc:1711) then calls INSTEAD OF running the iterator pipeline —
// ungated by secondary-engine status. This lets a primary storage engine take
// over execution of a recognized query shape and emit final rows directly.
//
// NOTE: override_executor_func is an internal, version-pinned MySQL hook (the
// same handlerton::push_to_engine entry NDB uses for pushed joins, but the
// override path itself has no in-tree user). Treated as a research PoC, not a
// supported storage-engine API. All of this is dormant unless HELIOS_AGG_PUSHDOWN
// is set, so the default 22-query path is byte-for-byte unchanged.
//
// STAGE: skeleton. The override emits ZERO rows (send_records=0) to prove the
// bypass fires end-to-end: with the gate ON a recognized grouped query returns
// an empty result set instead of its normal rows, with no crash and correct
// protocol (metadata + EOF are sent by the caller around the override).
static bool helios_agg_pushdown_enabled() {
  static const bool on = std::getenv("HELIOS_AGG_PUSHDOWN") != nullptr;
  return on;
}

// Per-output-column plan for the proxy aggregator.
namespace {
enum HeliosAggKind { HK_PASS = 0, HK_COUNT, HK_SUM, HK_AVG };
struct HeliosOut {
  Item *orig = nullptr;                 // SELECT output item
  HeliosAggKind kind = HK_PASS;
  Item *arg = nullptr;                  // aggregate argument expr (nullptr=COUNT*)
  Item_result rtype = STRING_RESULT;    // result/accumulation type
};
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
}  // namespace

// Classify visible SELECT output columns. Returns false (=> do NOT offload) if
// any column uses something the proxy aggregator cannot execute correctly.
// Supported: passthrough (grouping) columns + COUNT(*) / SUM / AVG over
// INT/REAL/DECIMAL. Rejected: DISTINCT/MIN/MAX/STD/..., COUNT(non-const-expr),
// STRING-result aggregates.
// NOTE: we classify over Query_block::fields, NOT JOIN::fields. After the
// optimizer builds an aggregation temp table (any GROUP BY query), JOIN::fields
// is rebound to temp-table Item_fields (no Item_sum left), whereas
// query_block->fields still holds the original Item_sum aggregates and the
// base-table Item_fields whose args read record[0] during our scan.
static bool helios_plan_outputs(JOIN *join, std::vector<HeliosOut> *out) {
  if (join->query_block == nullptr) return false;
  for (Item *it : VisibleFields(join->query_block->fields)) {
    HeliosOut o;
    o.orig = it;
    if (it->type() == Item::SUM_FUNC_ITEM) {
      // A BARE aggregate only. `SUM(x)+1`, `COALESCE(SUM(x),0)`,
      // `ROUND(AVG(x),2)` are Item_func (not SUM_FUNC_ITEM) and fall to the
      // else-branch where has_aggregation() rejects them — we never re-aggregate
      // inside a wrapper expression.
      Item_sum *s = down_cast<Item_sum *>(it);
      if (s->has_wf() || s->has_subquery()) return false;
      switch (s->sum_func()) {
        case Item_sum::COUNT_FUNC:
          o.kind = HK_COUNT;
          // COUNT(*) / COUNT(non-null const) only — counts every row. Reject
          // COUNT(col) (needs null filtering) and COUNT(NULL).
          if (s->argument_count() > 0) {
            Item *a0 = s->arguments()[0];
            if (!a0->const_item() || a0->is_nullable() || a0->is_null())
              return false;
          }
          break;
        case Item_sum::SUM_FUNC: o.kind = HK_SUM; break;
        case Item_sum::AVG_FUNC: o.kind = HK_AVG; break;
        default: return false;  // DISTINCT / MIN / MAX / STD / BIT / ...
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
      // Passthrough (grouping) column. Must be a plain base-table field of a
      // value type that Item_cache::get_cache materialises as a plain
      // int/real/decimal/string cache — reject temporal (Item_cache_datetime)
      // and JSON (Item_cache_json) which our store path does not handle, and
      // reject anything carrying an aggregate/subquery/window/non-determinism.
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

// Conservative offload whitelist. Anything outside it leaves the normal path
// untouched (override_executor_func stays null). MUST only return true for
// shapes helios_override_executor can emit correct final rows for — once the
// override is installed there is no fallback (metadata is already sent).
static bool helios_offloadable_shape(THD *thd, JOIN *join) {
  if (thd == nullptr || join == nullptr || thd->lex == nullptr) return false;
  // Read-only scope only (Phase-8 PoC): a plain SELECT. OLTP DML (TPC-C) is not
  // SQLCOM_SELECT and is never hijacked; this also keeps the OCC read-footprint
  // story simple — the override still scans every base row so the read set is
  // recorded as usual, and the handlerton commit runs normally afterwards.
  if (thd->lex->sql_command != SQLCOM_SELECT) return false;
  // Plain EXPLAIN never reaches the override, but EXPLAIN ANALYZE executes —
  // skip all EXPLAIN so we never hijack an explain.
  if (thd->lex->is_explain()) return false;
  Query_block *qb = join->query_block;
  if (qb == nullptr) return false;
  // Top-level query block ONLY. override_executor_func is honored solely by the
  // top-level Query_expression::ExecuteIteratorQuery (sql_union.cc:1711); a
  // subquery's or derived table's JOIN must never be hijacked (e.g. TPC-H q17/
  // q20 carry a single-table aggregate correlated subquery that otherwise
  // matches this whitelist and would be mis-executed). outer_query_block()==null
  // means this is the statement's outermost block.
  if (qb->outer_query_block() != nullptr) return false;
  // No UNION / set operations: the enclosing query expression must be a plain
  // single query block (is_simple lives on Query_expression, not Query_block).
  Query_expression *qe = qb->master_query_expression();
  if (qe == nullptr || !qe->is_simple()) return false;
  if (qb->leaf_table_count != 1) return false;     // exactly one base table
  if (!qb->is_grouped()) return false;             // GROUP BY or aggregate funcs
  if (qb->is_distinct()) return false;             // DISTINCT not handled
  if (qb->having_cond() != nullptr) return false;  // HAVING not handled
  if (qb->has_limit()) return false;               // LIMIT/OFFSET handled later
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) return false;  // no ROLLUP
  if (qb->has_windows()) return false;                  // no window functions

  // GROUP BY items: restrict to plain string base columns (q1's
  // l_returnflag/l_linestatus). String keys are encoded collation-correctly via
  // strnxfrm in the executor. Numeric/temporal grouping would need an
  // order-preserving typed key encoding and is deferred.
  std::vector<Item *> gitems;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next) {
    Item *gi = *g->item;
    if (gi->type() != Item::FIELD_ITEM) return false;
    if (gi->result_type() != STRING_RESULT) return false;
    if (gi->is_temporal() || gi->data_type() == MYSQL_TYPE_JSON) return false;
    if (gi->has_aggregation() || gi->has_subquery()) return false;
    gitems.push_back(gi);
  }

  // Every output column must be executable by the proxy aggregator. Non-agg
  // (passthrough) columns are captured from the first row of each group; under
  // the default ONLY_FULL_GROUP_BY sql_mode they are functionally dependent on
  // the GROUP BY, hence constant within a group, so that is correct.
  std::vector<HeliosOut> probe;
  if (!helios_plan_outputs(join, &probe)) return false;

  // Implicit grouping (no GROUP BY) must have ONLY aggregate outputs — a stray
  // passthrough column is undefined when ONLY_FULL_GROUP_BY is off.
  if (gitems.empty())
    for (const HeliosOut &o : probe)
      if (o.kind == HK_PASS) return false;

  // ORDER BY must be absent, or exactly the GROUP BY columns in non-descending
  // order: the executor emits rows in std::map group-key (ascending collation)
  // order, so only that ordering is correct.
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

// Build a mem-root list of writable Item_cache cells matching the SELECT output
// columns (same count/order/type as the already-sent metadata).
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

// Phase-8 Phase B: serialize an aggregate-argument Item into the FilterExpr
// arithmetic IR the server evaluates in exact (mantissa,scale) integer form.
// Supports COLUMN_REF (base field), integer constants, and +,-,* (the ops in
// TPC-H q1's SUM args). Returns false for anything else (floats, /, funcs) so
// the caller falls back to proxy-side Phase A aggregation.
static bool helios_serialize_arith(const Item *it,
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
      return false;  // decimal/real literals, casts, etc. — stay exact via fallback
  }
}

// Override executor. Contract (sql_union.cc:1697-1729): the caller has already
// sent result-set metadata and will send EOF; here we only emit data rows via
// query_result->send_data(thd, items) and set join->send_records. PS re-exec
// reuses this JOIN, so all state is rebuilt locally every call.
//
// Phase A: execute a single-table grouped aggregate on the proxy. Drive the
// base-table scan ourselves (which triggers helios's normal prefetch + records
// the OCC read footprint), evaluate WHERE (FILTER iterator is bypassed), bucket
// rows by GROUP BY key, accumulate COUNT/SUM/AVG (DECIMAL via my_decimal for
// exact results), then emit one final row per group. std::map keeps groups in
// ascending key order (matches TPC-H q1's `ORDER BY 1,2`).
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

  // ---- Phase-8 Phase B: server-side aggregation ----------------------------
  // When every aggregate in this query is one the server can compute (B0:
  // COUNT(*) only), push GROUP BY + the aggregates to the server: it returns one
  // group row per group (via the step's scan_values) and we just format + emit
  // them here. Single server + single scan ⇒ groups are final; we still merge by
  // collation key (cheap, robust to any batching) and emit in sorted order.
  {
    bool server_b = true;
    for (const HeliosOut &o : outs)
      if (o.kind != HK_PASS && o.kind != HK_COUNT &&
          o.kind != HK_SUM && o.kind != HK_AVG) server_b = false;
    // OCC soundness (Codex P1): the server returns only group rows (synthetic
    // key/tid), so range_versions/per-row TID validation cannot catch a
    // concurrent UPDATE of an aggregated column. Restrict server-side
    // aggregation to the read-only no-validate scope (no concurrent writers,
    // the TPC-H measurement mode); otherwise fall back to Phase A, which scans
    // every base row and records the read footprint normally.
    if (!down_cast<ha_lineairdb *>(t->file)->tx_ro_novalidate()) server_b = false;
    // Nullable GROUP BY columns (Codex P1): the server's extract_value_column
    // cannot distinguish NULL from empty, so reject nullable group keys here
    // (Phase A handles them correctly with its own NULL marker).
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
          // Only DECIMAL-result SUM/AVG go server-side (exact int128 path);
          // REAL aggregates stay on Phase A.
          if (outs[c].rtype != DECIMAL_RESULT || outs[c].arg == nullptr ||
              !helios_serialize_arith(outs[c].arg, af->mutable_arg())) {
            server_b = false;
            break;
          }
        }
        af->set_result_scale(0);
      }
      if (!server_b) {
        // Spec not fully server-aggregatable — fall through to Phase A. (No tx
        // state was set yet.)
        goto phase_a_fallthrough;
      }
      std::string spec_ser;
      spec.SerializeToString(&spec_ser);
      if (std::getenv("HELIOS_FE_DEBUG"))
        std::fprintf(stderr, "[AGGSRV] spec: groups=%d aggs=%d ser_bytes=%zu n_out=%zu\n",
                     spec.group_columns_size(), spec.aggs_size(), spec_ser.size(), n);
      ha_lineairdb *hl = down_cast<ha_lineairdb *>(t->file);
      hl->tx_set_pushed_aggregate(spec_ser);

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
        // A well-formed group row has exactly [null_flags][ng group cols]
        // [2 per agg]. Anything else is a protocol bug; abort rather than emit
        // a silently wrong result (Codex P2).
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
      if (std::getenv("HELIOS_FE_DEBUG"))
        std::fprintf(stderr, "[AGGSRV] phase-B groups=%zu sent=%llu\n",
                     groups.size(), (unsigned long long)join->send_records);
      return false;
    }
  }
  // ---- end Phase B; fall through to Phase A (proxy aggregation) -------------
phase_a_fallthrough:;

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
  const bool aggdbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
  long scanned = 0, passed = 0;

  int err = t->file->ha_rnd_init(true);
  if (err) { t->file->print_error(err, MYF(0)); return true; }
  while ((err = t->file->ha_rnd_next(t->record[0])) == 0) {
    ++scanned;
    if (thd->killed) { t->file->ha_rnd_end(); thd->send_kill_message(); return true; }
    if (where != nullptr) {
      const longlong pass = where->val_int();
      if (thd->is_error()) { t->file->ha_rnd_end(); return true; }
      if (where->null_value || pass == 0) continue;
    }
    ++passed;

    std::vector<HeliosAccum> *grp;
    if (implicit) {
      grp = implicit_grp;
    } else {
      std::string key;
      for (Item *gi : gitems) {
        String *s = gi->val_str(&str_buf);
        if (gi->null_value || s == nullptr) { key.push_back('\0'); continue; }
        // Collation-correct group key: strnxfrm weights compare under memcmp in
        // the column's collation order AND are equal for collation-equal inputs
        // (handles case/accent-insensitive collations). PAD_TO_MAXLEN with a
        // per-column fixed weight count makes every value of a column the same
        // width, so the concatenated multi-column key stays unambiguous and
        // ordered. The '\1' non-null marker sorts after the '\0' null marker
        // (NULL-first ascending, matching MySQL).
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
        // Capture passthrough columns once, from this first row of the group.
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
            // decimal_add does not support output aliasing an input; with the
            // running sum aliased (&a.dec,&a.dec,d) inter-word carries are lost
            // once the sum spans >1 base-1e9 word. Accumulate via a temp.
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

  if (aggdbg) {
    std::fprintf(stderr,
        "[AGGEXEC] scanned=%ld passed=%ld groups=%zu n_out=%zu gitems=%zu implicit=%d\n",
        scanned, passed, groups.size(), n, gitems.size(), (int)implicit);
    size_t gi = 0;
    for (auto &kv : groups) {
      if (gi++ >= 4) break;
      std::fprintf(stderr, "[AGGEXEC]  group keylen=%zu cnt0=%lld\n",
                   kv.first.size(), (long long)kv.second[ n>0 ? n-1 : 0 ].cnt);
    }
  }

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

static int lineairdb_push_to_engine(THD *thd, AccessPath * /*root_path*/,
                                    JOIN *join) {
  if (!helios_agg_pushdown_enabled()) return 0;
  if (!helios_offloadable_shape(thd, join)) return 0;
  if (std::getenv("HELIOS_FE_DEBUG"))
    std::fprintf(stderr, "[AGGPD] installing override_executor_func\n");
  join->override_executor_func = &helios_override_executor;
  return 0;
}

const handlerton *ha_lineairdb::hton_supporting_engine_pushdown() {
  return helios_agg_pushdown_enabled() ? lineairdb_hton : nullptr;
}

static int lineairdb_init_func(void *p) {
  DBUG_TRACE;

  lineairdb_hton = (handlerton *)p;
  lineairdb_hton->state = SHOW_OPTION_YES;
  lineairdb_hton->create = lineairdb_create_handler;
  lineairdb_hton->flags = HTON_CAN_RECREATE;
  lineairdb_hton->is_supported_system_table =
      lineairdb_is_supported_system_table;
  lineairdb_hton->db_type = DB_TYPE_UNKNOWN;
  lineairdb_hton->commit = lineairdb_commit;
  lineairdb_hton->rollback = lineairdb_abort;
  lineairdb_hton->close_connection = lineairdb_close_connection;
  lineairdb_hton->push_to_engine = lineairdb_push_to_engine;  // Phase-8 (gated)

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
  LineairDBThdCtx *&ctx =
      *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(userThread, lineairdb_hton));
  if (ctx == nullptr)
    ctx = new LineairDBThdCtx();
  if (!ctx->proxy) {
    // Construct RPC proxy using GLOBAL sysvars
    std::string host =
        srv_server_host ? srv_server_host : std::string("127.0.0.1");
    int port = static_cast<int>(srv_server_port);
    ctx->proxy = std::make_shared<LineairDBProxy>(host, port);
  }
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

// Phase-3e: defined later; auto-generates the QEP-based prefetch plan on the
// first data access (when the optimizer's join plan is available).
static void maybe_auto_stage_oneshot_plan(THD *thd, LineairDBTransaction *tx);

int ha_lineairdb::index_init(uint idx, bool sorted [[maybe_unused]]) {
  DBUG_TRACE;
  HTP_SCOPE(index_init);
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
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  // buffer_write appends to a local buffer (no RPC yet), so no error check needed.
  // The actual RPC is sent at flush time (buffer full, table change, or commit).
  tx->buffer_write(db_table_name, key, write_buffer_);

  // Write secondary index entries.
  // Normal transactions check UNIQUE indexes immediately.
  // Oneshot sends UNIQUE index writes to validate-and-commit with row writes.
  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    if (i == table->s->primary_key) continue;

    std::string secondary_key = build_secondary_key_from_row(buf, key_info);

    if (key_info.flags & HA_NOSAME) {
      if (tx->is_oneshot_mode()) {
        tx->buffer_write_secondary_index(db_table_name, key_info.name,
                                         secondary_key, key);
      } else {
        tx->flush_write_buffer();
        tx->choose_table(db_table_name);
        bool ok = tx->write_secondary_index(key_info.name, secondary_key, key);
        if (!ok || tx->is_aborted()) {
          thd_mark_transaction_to_rollback(ha_thd(), 1);
          return HA_ERR_LOCK_DEADLOCK;
        }
      }
    } else {
      tx->buffer_write_secondary_index(db_table_name, key_info.name,
                                        secondary_key, key);
    }
  }

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->add_rowcount_delta(share, db_table_name, +1);

  return 0;
}

int ha_lineairdb::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_TRACE;

  auto key = extract_key_from_mysql(old_data);

  if (key.empty()) {
    key = last_fetched_primary_key_;
  }

  if (key.empty()) {
    key = extract_primary_key_from_ref(ref);
  }

  last_fetched_primary_key_ = key;

  set_write_buffer(new_data);

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  // Buffer the base-row update; read/scan paths and commit flush it later.
  tx->buffer_write(db_table_name, key, write_buffer_);

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
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
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
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
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  // Buffer the base-row delete; read/scan paths and commit flush it later.
  tx->buffer_delete(db_table_name, key);

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
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
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->add_rowcount_delta(share, db_table_name, -1);

  return 0;
}

int ha_lineairdb::index_read_map(uchar *buf, const uchar *key,
                                 key_part_map keypart_map,
                                 enum ha_rkey_function find_flag) {
  DBUG_TRACE;
  HTP_SCOPE(index_read_map);

  stats.records = 0;
  auto tx = get_transaction(ha_thd());
  // QEP is available now (optimizer has run) — auto-generate the prefetch plan
  // if this is an index-driven query (driver reached via index_read).
  maybe_auto_stage_oneshot_plan(ha_thd(), tx);

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  if (!pushed_filter_serialized_.empty()) {
    tx->set_pushed_filter(pushed_filter_serialized_);
  } else {
    tx->clear_pushed_filter();
  }

  KEY *key_info = &table->key_info[active_index];

  // Phase 4: Separation of planning and execution
  build_search_plan(key, keypart_map, find_flag, key_info);

  return execute_plan(buf, tx);
}

/**
 * @brief index_next: The next row after the current cursor position
 */
int ha_lineairdb::index_next(uchar *buf) {
  DBUG_TRACE;
  HTP_SCOPE(index_next);

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }
  tx->choose_table(db_table_name);

  // materialize mode
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_lineairdb::index_next_same(uchar *buf, const uchar *key [[maybe_unused]],
                                  uint key_len [[maybe_unused]]) {
  DBUG_TRACE;
  HTP_SCOPE(index_next_same);

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }
  tx->choose_table(db_table_name);

  // materialize mode
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
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
  HTP_SCOPE(index_prev);

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
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
  HTP_SCOPE(index_first);
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
  HTP_SCOPE(index_last);

  reset_index_search_buffers();
  last_fetched_primary_key_.clear();

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);

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
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
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

// ---------------------------------------------------------------------------
// Predicate Pushdown: serialize MySQL Item tree → FilterExpr protobuf
// ---------------------------------------------------------------------------

/**
 * Recursively serialize a MySQL Item expression tree into a FilterExpr protobuf.
 * Returns false if the Item type is not supported (PP is silently skipped).
 *
 * @param item   MySQL Item node (condition expression)
 * @param expr   FilterExpr protobuf to populate
 * @return true if serialization succeeded
 */
// Compare-type tags shared with the server PredicateEvaluator.
//   0=SIGNED_INT 1=UNSIGNED_INT 2=DOUBLE 3=STRING
static constexpr uint32_t kCmpTypeSignedInt = 0;
static constexpr uint32_t kCmpTypeUnsignedInt = 1;
static constexpr uint32_t kCmpTypeDouble = 2;
static constexpr uint32_t kCmpTypeString = 3;

// Serialize a constant operand (literal, bound `?` param, or const-folded
// expression such as `DATE ? + INTERVAL '1' MONTH`) into a CONST_* node.
// Returns false when the item is not actually constant or cannot be evaluated.
// DATE constants are emitted as CONST_STRING in ISO "YYYY-MM-DD" form because
// DATE columns are stored and compared as ASCII text (compare_type STRING), so
// lexicographic compare matches chronological order.
static bool serialize_const_item(Item *item,
                                 LineairDB::Protocol::FilterExpr *expr) {
  if (item == nullptr || !item->const_item()) return false;

  const enum_field_types dt = item->data_type();
  if (dt == MYSQL_TYPE_DATE || dt == MYSQL_TYPE_NEWDATE) {
    String buf;
    String *s = item->val_str(&buf);  // ISO "YYYY-MM-DD"
    if (item->null_value || s == nullptr) {
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_NULL);
    } else {
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
      expr->set_string_val(s->ptr(), s->length());
    }
    return true;
  }

  switch (item->result_type()) {
    case INT_RESULT:
      if (item->unsigned_flag) {
        expr->set_op(LineairDB::Protocol::FilterExpr::CONST_UINT);
        expr->set_uint_val(item->val_uint());
      } else {
        expr->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
        expr->set_int_val(item->val_int());
      }
      return !item->null_value;
    case REAL_RESULT:
    case DECIMAL_RESULT:
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_DOUBLE);
      expr->set_double_val(item->val_real());
      return !item->null_value;
    default: {
      String buf;
      String *s = item->val_str(&buf);
      if (item->null_value || s == nullptr) {
        expr->set_op(LineairDB::Protocol::FilterExpr::CONST_NULL);
        return true;
      }
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
      expr->set_string_val(s->ptr(), s->length());
      return true;
    }
  }
}

static bool serialize_item(const Item *item,
                           LineairDB::Protocol::FilterExpr *expr) {
  if (!item) return false;

  switch (item->type()) {
    case Item::INT_ITEM: {
      if (item->unsigned_flag) {
        expr->set_op(LineairDB::Protocol::FilterExpr::CONST_UINT);
        expr->set_uint_val(const_cast<Item *>(item)->val_uint());
      } else {
        expr->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
        expr->set_int_val(const_cast<Item *>(item)->val_int());
      }
      return true;
    }
    case Item::REAL_ITEM: {
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_DOUBLE);
      expr->set_double_val(const_cast<Item *>(item)->val_real());
      return true;
    }
    case Item::STRING_ITEM: {
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
      String buf;
      String *s = const_cast<Item *>(item)->val_str(&buf);
      if (s) {
        expr->set_string_val(s->ptr(), s->length());
      }
      return true;
    }
    case Item::DECIMAL_ITEM: {
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_DOUBLE);
      expr->set_double_val(const_cast<Item *>(item)->val_real());
      return true;
    }
    case Item::NULL_ITEM: {
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_NULL);
      return true;
    }
    case Item::FIELD_ITEM: {
      const Item_field *field_item = down_cast<const Item_field *>(item);
      Field *field = field_item->field;
      if (!field) return false;
      expr->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      expr->set_column_index(field->field_index());

      // Set compare_type based on MySQL field type. NOTE: DATE/temporal row
      // values are stored as ASCII text ("YYYY-MM-DD", verified at runtime),
      // not packed binary, so they fall through to STRING and compare
      // lexicographically — which equals chronological order for ISO dates.
      switch (field->result_type()) {
        case INT_RESULT:
          expr->set_compare_type(field->is_unsigned() ? kCmpTypeUnsignedInt
                                                      : kCmpTypeSignedInt);
          break;
        case REAL_RESULT:
        case DECIMAL_RESULT:
          expr->set_compare_type(kCmpTypeDouble);
          break;
        default:
          expr->set_compare_type(kCmpTypeString);
          break;
      }
      return true;
    }
    case Item::COND_ITEM: {
      // Item_cond stores AND / OR children in argument_list(), not arguments().
      auto *cond_item =
          const_cast<Item_cond *>(down_cast<const Item_cond *>(item));
      switch (cond_item->functype()) {
        case Item_func::COND_AND_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
          break;
        case Item_func::COND_OR_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_OR);
          break;
        default:
          return false;
      }

      for (Item &child : *cond_item->argument_list()) {
        if (!serialize_item(&child, expr->add_children())) {
          return false;
        }
      }
      return true;
    }
    case Item::FUNC_ITEM: {
      const Item_func *func = down_cast<const Item_func *>(item);
      Item **args = func->arguments();
      uint arg_count = func->argument_count();

      switch (func->functype()) {
        case Item_func::EQ_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_EQ);
          break;
        case Item_func::NE_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_NE);
          break;
        case Item_func::LT_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_LT);
          break;
        case Item_func::LE_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_LE);
          break;
        case Item_func::GT_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_GT);
          break;
        case Item_func::GE_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_GE);
          break;
        case Item_func::BETWEEN: {
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_BETWEEN);
          auto *between = down_cast<const Item_func_between *>(func);
          expr->set_negated(between->negated);
          break;
        }
        case Item_func::IN_FUNC: {
          auto *in_func = down_cast<const Item_func_in *>(func);
          // Step E2 (Codex 2026-05-29): Q22-class rewrite of
          //   `SUBSTRING(col, 1, N) IN ('a','b',...)` (and not-negated)
          // into
          //   `col LIKE 'a%' OR col LIKE 'b%' OR ...`
          // so the existing OP_LIKE/OP_OR pushdown path applies. Without
          // this, the IN's first child (substring(...)) cannot be
          // represented and the whole filter is dropped — Q22 ships full
          // customer (1.4 GB raw wire vs ~40 MB needed). Pattern-detect:
          // not-negated IN, first arg is FUNC_ITEM named "substr" with 3
          // args (FIELD_ITEM, INT_ITEM=1, INT_ITEM=N), and the IN value
          // list is all STRING_ITEM/varchar constants of length >= N.
          // Restricted to the common SF=1 TPC-H Q22 shape; falls through
          // to the generic OP_IN path when any check fails.
          do {
            if (in_func->negated) break;
            if (arg_count < 2) break;
            const Item *first = args[0];
            if (first == nullptr || first->type() != Item::FUNC_ITEM) break;
            const Item_func *fn = down_cast<const Item_func *>(first);
            if (fn->func_name() == nullptr) break;
            const std::string fname = fn->func_name();
            if (fname != "substr" && fname != "substring") break;
            if (fn->argument_count() != 3) break;
            Item *sa0 = fn->arguments()[0];
            Item *sa1 = fn->arguments()[1];
            Item *sa2 = fn->arguments()[2];
            if (sa0 == nullptr || sa0->type() != Item::FIELD_ITEM) break;
            if (sa1 == nullptr || sa2 == nullptr) break;
            if (!sa1->const_item() || !sa2->const_item()) break;
            const longlong pos = const_cast<Item *>(sa1)->val_int();
            const longlong nlen = const_cast<Item *>(sa2)->val_int();
            if (pos != 1 || nlen <= 0 || nlen > 64) break;
            const Item_field *fld = down_cast<const Item_field *>(sa0);
            if (fld->field == nullptr) break;
            // Build OP_OR of OP_LIKE(field, 'value%') for each remaining
            // arg. Skip and bail if any constant isn't representable.
            expr->set_op(LineairDB::Protocol::FilterExpr::OP_OR);
            bool ok_all = true;
            for (uint i = 1; i < arg_count; ++i) {
              Item *val = args[i];
              if (val == nullptr || !val->const_item()) { ok_all = false; break; }
              String buf;
              String *s = const_cast<Item *>(val)->val_str(&buf);
              if (val->null_value || s == nullptr) { ok_all = false; break; }
              std::string sv(s->ptr(), s->length());
              if (static_cast<longlong>(sv.size()) < nlen) {
                // Constant shorter than prefix length — no row's substring
                // can equal it, so it contributes nothing. Skip silently.
                continue;
              }
              auto *child = expr->add_children();
              child->set_op(LineairDB::Protocol::FilterExpr::OP_LIKE);
              auto *col = child->add_children();
              col->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
              // Inline qep_table_field_index (definition is later in file).
              int field_idx = -1;
              {
                TABLE *t = fld->field->table;
                for (uint k = 0; k < t->s->fields; ++k)
                  if (t->field[k] == fld->field) { field_idx = static_cast<int>(k); break; }
              }
              if (field_idx < 0) { ok_all = false; break; }
              col->set_column_index(field_idx);
              col->set_compare_type(kCmpTypeString);
              auto *pat = child->add_children();
              pat->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
              std::string prefix = sv.substr(0, nlen);
              prefix.push_back('%');
              pat->set_string_val(prefix);
            }
            if (!ok_all || expr->children_size() == 0) {
              expr->clear_op();
              expr->clear_children();
              break;  // fall through to generic IN below
            }
            return true;  // emitted as OR(LIKE...) — done
          } while (false);
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_IN);
          expr->set_negated(in_func->negated);
          break;
        }
        case Item_func::LIKE_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_LIKE);
          break;
        case Item_func::ISNULL_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_IS_NULL);
          break;
        case Item_func::ISNOTNULL_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_IS_NOT_NULL);
          break;
        case Item_func::NOT_FUNC:
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_NOT);
          break;
        default:
          // Const-folded functions such as `DATE ? + INTERVAL '1' MONTH` are
          // not comparison/logical ops but evaluate to a constant at execution
          // time — emit them as a CONST_* node so date-range predicates push.
          return serialize_const_item(const_cast<Item *>(item), expr);
      }

      // Recursively serialize arguments
      for (uint i = 0; i < arg_count; i++) {
        if (!serialize_item(args[i], expr->add_children())) {
          return false;
        }
      }

      // For comparison operators, propagate compare_type from the COLUMN_REF child
      if (expr->children_size() >= 2) {
        for (int i = 0; i < expr->children_size(); i++) {
          if (expr->children(i).op() ==
              LineairDB::Protocol::FilterExpr::COLUMN_REF) {
            uint32_t ct = expr->children(i).compare_type();
            // Set compare_type on all COLUMN_REF children to ensure type matching
            for (int j = 0; j < expr->children_size(); j++) {
              if (j != i && expr->children(j).op() !=
                                LineairDB::Protocol::FilterExpr::COLUMN_REF) {
                expr->mutable_children(j)->set_compare_type(ct);
              }
            }
            break;
          }
        }
      }
      return true;
    }
    default:
      // Bound `?` params, DATE/temporal literals and other constant operands
      // not matched by a dedicated case above.
      return serialize_const_item(const_cast<Item *>(item), expr);
  }
}

// True when one predicate operand is an integer field or integer constant
static bool item_is_limit_safe_scalar(const Item *item) {
  // Accept only expression shapes the server can compare exactly for LIMIT.
  if (item == nullptr) return false;                 // Missing expression
  if (item->type() == Item::INT_ITEM) return true;   // Integer constant

  // Field operands must be integer columns to avoid string/collation mismatch.
  if (item->type() != Item::FIELD_ITEM) return false; // Not a field
  const Item_field *field_item = down_cast<const Item_field *>(item);
  if (field_item->field == nullptr) return false;    // Missing field metadata
  return field_item->field->result_type() == INT_RESULT;
}

// True when WHERE is an AND tree of simple integer comparisons.
// Currently unreferenced: prepare_select_filter_for_tx now always uses the
// partial (not-LIMIT-safe) per-table filter. Kept for when a full-WHERE,
// LIMIT-safe push is reintroduced. [[maybe_unused]] silences -Wunused-function.
[[maybe_unused]] static bool item_is_limit_safe_filter(const Item *item) {
  if (item == nullptr) return true;                  // No WHERE to check

  // AND is safe to recurse; OR is kept local until we add stricter tests.
  if (item->type() == Item::COND_ITEM) {
    // Keep LIMIT filters to AND trees; OR can be added after stricter tests.
    auto *cond_item =
        const_cast<Item_cond *>(down_cast<const Item_cond *>(item));
    if (cond_item->functype() != Item_func::COND_AND_FUNC) return false;
    for (Item &child : *cond_item->argument_list()) {
      if (!item_is_limit_safe_filter(&child)) return false;
    }
    return true;
  }

  // Leaf predicates must be binary comparisons.
  if (item->type() != Item::FUNC_ITEM) return false; // Not a comparison
  const Item_func *func = down_cast<const Item_func *>(item);
  switch (func->functype()) {
    case Item_func::EQ_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GT_FUNC:
    case Item_func::GE_FUNC:
      break;
    default:
      return false;                                  // Complex predicate
  }

  // Both sides must be safe scalar operands.
  if (func->argument_count() != 2) return false;     // Binary compare only
  Item **args = func->arguments();
  return item_is_limit_safe_scalar(args[0]) &&
         item_is_limit_safe_scalar(args[1]);
}

// Enable Oneshot only for DML; DDL must keep the normal transaction path
static bool thd_can_use_oneshot(THD *thd) {
  if (thd == nullptr) return false;                  // Missing session
  if (thd->lex == nullptr) return false;             // Missing SQL state

  switch (thd->lex->sql_command) {
    case SQLCOM_SELECT:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
      return true;
    default:
      return false;
  }
}

// Encode one integer DSL key part into the same bytes as handler keys
static std::string encode_plan_int_key_part(int64_t value, int size = 4) {
  uint64_t raw = 0;
  uint64_t sign_mask = 0;

  switch (size) {
    case 1:
      raw = static_cast<uint8_t>(static_cast<int8_t>(value));
      sign_mask = 0x80ULL;
      break;
    case 2:
      raw = static_cast<uint16_t>(static_cast<int16_t>(value));
      sign_mask = 0x8000ULL;
      break;
    case 4:
      raw = static_cast<uint32_t>(static_cast<int32_t>(value));
      sign_mask = 0x80000000ULL;
      break;
    case 8:
      raw = static_cast<uint64_t>(value);
      sign_mask = 0x8000000000000000ULL;
      break;
    default:
      raw = static_cast<uint32_t>(static_cast<int32_t>(value));
      sign_mask = 0x80000000ULL;
      size = 4;
      break;
  }

  const uint64_t encoded = raw ^ sign_mask;
  std::string out;
  out.push_back(static_cast<char>(kKeyMarkerNotNull));
  out.push_back(static_cast<char>(kKeyTypeInt));
  out.push_back(static_cast<char>((size >> 8) & 0xFF));
  out.push_back(static_cast<char>(size & 0xFF));
  for (int i = size - 1; i >= 0; --i) {
    out.push_back(static_cast<char>((encoded >> (i * 8)) & 0xFF));
  }
  return out;
}

// Encode one string DSL key part into the same bytes as handler keys
static std::string encode_plan_string_key_part(const std::string& value) {
  std::string out;
  out.push_back(static_cast<char>(kKeyMarkerNotNull));
  out.push_back(static_cast<char>(kKeyTypeString));
  out.append(value);
  out.push_back('\0');
  const uint16_t length = static_cast<uint16_t>(value.size());
  out.push_back(static_cast<char>((length >> 8) & 0xFF));
  out.push_back(static_cast<char>(length & 0xFF));
  return out;
}

static bool try_parse_plan_int(const std::string& text, int64_t *value) {
  if (text.empty() || value == nullptr) return false;
  char *end = nullptr;
  *value = std::strtoll(text.c_str(), &end, 10);
  return end == text.c_str() + text.size();
}

[[maybe_unused]] static bool thd_has_ldb_plan(THD *thd) {
  if (thd == nullptr) return false;
  auto it = thd->user_vars.find("_ldb_plan");
  if (it == thd->user_vars.end()) return false;

  auto *entry = it->second.get();
  return entry != nullptr && entry->ptr() != nullptr && entry->length() > 0;
}

// Encode one DSL segment: 42=INT, 42t=TINYINT, 42s=SMALLINT, 42l=BIGINT
static std::string encode_plan_key_segment(const std::string& segment) {
  if (segment.empty()) return {};

  int int_size = 4;
  std::string number = segment;
  const char suffix = segment.back();
  if (suffix == 't') {
    int_size = 1;
    number = segment.substr(0, segment.size() - 1);
  } else if (suffix == 's') {
    int_size = 2;
    number = segment.substr(0, segment.size() - 1);
  } else if (suffix == 'i') {
    int_size = 4;
    number = segment.substr(0, segment.size() - 1);
  } else if (suffix == 'l') {
    int_size = 8;
    number = segment.substr(0, segment.size() - 1);
  }

  int64_t int_value = 0;
  if (try_parse_plan_int(number, &int_value)) {
    return encode_plan_int_key_part(int_value, int_size);
  }
  return encode_plan_string_key_part(segment);
}

static std::vector<std::string> split_plan_text(const std::string& text,
                                                char delimiter) {
  std::vector<std::string> parts;
  std::istringstream stream(text);
  std::string part;
  while (std::getline(stream, part, delimiter)) {
    parts.push_back(part);
  }
  return parts;
}

static std::string normalize_plan_table_name(THD *thd,
                                             const std::string& table_name) {
  if (table_name.empty()) return table_name;
  if (table_name[0] == '.') return table_name;
  if (table_name.find('/') != std::string::npos) return "./" + table_name;

  std::string prefix = "./";
  if (thd != nullptr && thd->db().str != nullptr && thd->db().length > 0) {
    prefix += std::string(thd->db().str, thd->db().length) + "/";
  }
  return prefix + table_name;
}

// Physical-table key from a TABLE's own share: "./<db>/<table>", matching the
// path ha_lineairdb::open() stores in db_table_name (the projection-map key).
// Cross-db safe: derives the db from THIS table's share, NOT THD::db(), so two
// same-named tables in different databases never collapse to one key (which
// would let projection merge their read_sets / trim a needed column).
static std::string physical_table_key(const TABLE *t) {
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

static LineairDBProxy::ReadPlanKeyBinding parse_plan_binding(
    const std::string& spec) {
  LineairDBProxy::ReadPlanKeyBinding binding;
  if (spec.size() < 2 || spec[0] != 'B') return binding;

  size_t pos = 1;
  size_t step_end = spec.find('.', pos);
  if (step_end == std::string::npos) step_end = spec.size();
  binding.source_step = static_cast<uint32_t>(
      std::strtoul(spec.substr(pos, step_end - pos).c_str(), nullptr, 10));
  if (step_end >= spec.size()) return binding;
  pos = step_end + 1;

  size_t type_end = spec.find('.', pos);
  if (type_end == std::string::npos) type_end = spec.size();
  const std::string type = spec.substr(pos, type_end - pos);
  pos = type_end + 1;

  if (type == "K") {
    binding.from_key = true;
  } else if (type == "V") {
    binding.from_key = false;
  } else if (type == "MK") {
    binding.use_midpoint = true;
    binding.from_key = true;
  } else if (type == "M") {
    binding.use_midpoint = true;
  } else if (type.rfind("MCI", 0) == 0 || type.rfind("CI", 0) == 0) {
    const bool midpoint = type.rfind("MCI", 0) == 0;
    const size_t number_pos = midpoint ? 3 : 2;
    const std::string number = type.substr(number_pos);
    char *end = nullptr;
    const long column = std::strtol(number.c_str(), &end, 10);
    binding.use_midpoint = midpoint;
    binding.source_column = static_cast<int32_t>(column + 1);
    binding.column_as_int_key = true;
    if (end != nullptr && *end != '\0') {
      binding.int_delta = std::strtoll(end, nullptr, 10);
    }
    return binding;
  }

  if (pos < spec.size()) {
    size_t offset_end = spec.find('.', pos);
    if (offset_end == std::string::npos) offset_end = spec.size();
    binding.source_offset = static_cast<uint32_t>(
        std::strtoul(spec.substr(pos, offset_end - pos).c_str(), nullptr, 10));
    pos = offset_end + 1;
  }
  if (pos < spec.size()) {
    binding.source_length =
        static_cast<uint32_t>(std::strtoul(spec.substr(pos).c_str(), nullptr, 10));
  }
  return binding;
}

static bool token_is_plan_binding(const std::string& token) {
  if (token.size() < 4 || token[0] != 'B') return false;
  size_t pos = 1;
  while (pos < token.size() &&
         std::isdigit(static_cast<unsigned char>(token[pos]))) {
    ++pos;
  }
  return pos > 1 && pos < token.size() && token[pos] == '.';
}

static void append_plan_key_token(LineairDBProxy::ReadPlanStep *step,
                                  const std::string& token,
                                  bool end_key) {
  if (step == nullptr || token.empty()) return;
  if (token_is_plan_binding(token)) {
    auto binding = parse_plan_binding(token);
    if (end_key) {
      step->end_bindings.push_back(std::move(binding));
    } else {
      step->bindings.push_back(std::move(binding));
    }
    return;
  }

  if (end_key) {
    step->end_key_prefix += encode_plan_key_segment(token);
  } else {
    step->key_prefix += encode_plan_key_segment(token);
  }
}

// Parse read-plan DSL: R=point, S=PK range/prefix scan, SI=secondary scan
static std::vector<LineairDBProxy::ReadPlanStep> parse_plan_steps(
    THD *thd, const std::string& plan_text) {
  std::vector<LineairDBProxy::ReadPlanStep> steps;

  for (const auto& step : split_plan_text(plan_text, ';')) {
    if (step.empty()) continue;
    const auto parts = split_plan_text(step, ':');
    if (parts.size() < 2) continue;

    LineairDBProxy::ReadPlanStep parsed;
    parsed.table_name = normalize_plan_table_name(thd, parts[1]);
    bool end_key = false;
    size_t token_start = 2;
    if (parts[0] == "R") {
      parsed.is_scan = false;
    } else if (parts[0] == "S") {
      parsed.is_scan = true;
    } else if (parts[0] == "SI") {
      if (parts.size() < 3) continue;
      parsed.is_scan = true;
      parsed.index_name = parts[2];
      token_start = 3;
    } else if (parts[0] == "FE") {
      parsed.for_each = true;
    } else if (parts[0] == "FER") {
      // for_each PK-prefix RANGE scan (one range per source row).
      parsed.for_each = true;
      parsed.is_scan = true;
    } else if (parts[0] == "FES") {
      // for_each SECONDARY range scan (one range per source row).
      if (parts.size() < 3) continue;
      parsed.for_each = true;
      parsed.is_scan = true;
      parsed.index_name = parts[2];
      token_start = 3;
    } else {
      continue;
    }

    for (size_t i = token_start; i < parts.size(); ++i) {
      const auto& token = parts[i];
      if (token == "E") {
        end_key = true;
        continue;
      }
      if (token.rfind("limit=", 0) == 0) {
        parsed.scan_limit = static_cast<uint64_t>(
            std::strtoull(token.substr(6).c_str(), nullptr, 10));
        continue;
      }
      if (token.rfind("reverse=", 0) == 0) {
        parsed.reverse_scan = token.substr(8) == "1";
        continue;
      }
      append_plan_key_token(&parsed, token, end_key);
    }
    // S: with no key bounds = full table scan. Use the same 16-byte 0xFF
    // sentinel as get_matching_keys_and_values_from_prefix so the cached
    // entry exactly matches the lookup MySQL will issue for rnd_init.
    if (parsed.is_scan && !parsed.for_each && parsed.key_prefix.empty() &&
        parsed.end_key_prefix.empty() && parsed.bindings.empty() &&
        parsed.end_bindings.empty()) {
      parsed.end_key_prefix.assign(16, '\xff');
    }
    if (!parsed.table_name.empty()) {
      steps.push_back(std::move(parsed));
    }
  }

  return steps;
}

// Read @_ldb_plan once and clear it so the next statement starts clean
static std::string read_and_clear_ldb_plan(THD *thd) {
  std::string plan;
  if (thd == nullptr) return plan;

  auto it = thd->user_vars.find("_ldb_plan");
  if (it == thd->user_vars.end()) return plan;

  auto *entry = it->second.get();
  if (entry == nullptr || entry->ptr() == nullptr || entry->length() == 0) {
    return plan;
  }

  plan.assign(entry->ptr(), entry->length());
  entry->lock();
  entry->set_null_value(STRING_RESULT);
  entry->unlock();
  return plan;
}

static bool build_single_table_filter(THD *thd, TABLE *table,
                                      std::string *out_serialized);

// ---- Phase-3e: auto-generate the prefetch DSL from MySQL's query plan -------
// Instead of the app hardcoding @_ldb_plan, walk MySQL's optimized AccessPath
// tree (the QEP) and emit the equivalent S:/FE/FER/FES steps. Because the plan
// mirrors the optimizer's *actual* join order and access paths, every per-row
// access MySQL will perform is covered by a prefetch step → the whole query is
// served from the local cache in 2 RPCs (prefetch + commit), no per-row RPC.

// Collect the leaf table-access nodes of an AccessPath tree in nested-loop
// drive order (outer before inner). *ok is cleared if we hit a node we cannot
// model, so the caller drops the plan (better no plan than a partial one that
// would fall back mid-query).
// (a) True only for a field whose join key the bound-prefetch path encodes
// EXACTLY as the server expects. The server (build_plan_key →
// encode_column_as_int_key) does strtoll(value) → int32 → a 4-byte signed INT
// key part, and the `4 + length` interior-PK slice assumes that same 4-byte
// layout. So the only fully-safe type is plain signed 4-byte INT
// (MYSQL_TYPE_LONG). TINYINT/SMALLINT/MEDIUMINT encode at 1/2/3-4 bytes,
// BIGINT at 8, and DECIMAL/FLOAT/DOUBLE/YEAR map to LINEAIRDB_INT yet are not
// strtoll-int32 safe; UNSIGNED breaks the sign-flip. All of those bail (→ full
// S: of the target via the uncovered-net, still 2-RPC over-fetch) rather than
// risk a corrupt probe key. TPC-H/TPC-C INTEGER keys are MYSQL_TYPE_LONG.
static bool is_int32_key_field(const Field *f) {
  return f != nullptr && f->type() == MYSQL_TYPE_LONG &&
         f->pack_length() == 4 && !f->is_unsigned();
}

static void collect_qep_leaves(AccessPath *p, std::vector<AccessPath *> *out,
                               bool *ok) {
  if (p == nullptr || !*ok) return;
  switch (p->type) {
    case AccessPath::TABLE_SCAN:
    case AccessPath::INDEX_SCAN:
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::EQ_REF:
    case AccessPath::PUSHED_JOIN_REF:
    case AccessPath::CONST_TABLE:
      out->push_back(p);
      return;
    case AccessPath::NESTED_LOOP_JOIN:
      collect_qep_leaves(p->nested_loop_join().outer, out, ok);
      collect_qep_leaves(p->nested_loop_join().inner, out, ok);
      return;
    case AccessPath::BKA_JOIN:
      collect_qep_leaves(p->bka_join().outer, out, ok);
      collect_qep_leaves(p->bka_join().inner, out, ok);
      return;
    case AccessPath::HASH_JOIN:
      collect_qep_leaves(p->hash_join().outer, out, ok);
      collect_qep_leaves(p->hash_join().inner, out, ok);
      return;
    case AccessPath::FILTER:
      collect_qep_leaves(p->filter().child, out, ok);
      return;
    case AccessPath::SORT:
      collect_qep_leaves(p->sort().child, out, ok);
      return;
    case AccessPath::LIMIT_OFFSET:
      collect_qep_leaves(p->limit_offset().child, out, ok);
      return;
    case AccessPath::AGGREGATE:
      collect_qep_leaves(p->aggregate().child, out, ok);
      return;
    case AccessPath::TEMPTABLE_AGGREGATE:
      // GROUP BY into a temp table — the base join feeds subquery_path.
      collect_qep_leaves(p->temptable_aggregate().subquery_path, out, ok);
      return;
    case AccessPath::STREAM:
      collect_qep_leaves(p->stream().child, out, ok);
      return;
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      collect_qep_leaves(
          p->nested_loop_semijoin_with_duplicate_removal().outer, out, ok);
      collect_qep_leaves(
          p->nested_loop_semijoin_with_duplicate_removal().inner, out, ok);
      return;
    case AccessPath::REMOVE_DUPLICATES:
      collect_qep_leaves(p->remove_duplicates().child, out, ok);
      return;
    case AccessPath::WEEDOUT:
      collect_qep_leaves(p->weedout().child, out, ok);
      return;
    case AccessPath::MATERIALIZE:
      // Derived table / materialized subquery (e.g. Q13's FROM (SELECT ...)).
      // Descend into each materialized operand's subquery so its base tables
      // are prefetched too. The temp table the result feeds is skipped at the
      // leaf level (tmp_table check in auto_generate_plan_from_qep).
      if (p->materialize().param != nullptr)
        for (auto &qb : p->materialize().param->query_blocks)
          collect_qep_leaves(qb.subquery_path, out, ok);
      return;
    default:
      // (b) An AccessPath type we don't model (index merge / ROWID
      // intersection|union / group-index skip-scan / window / UNION APPEND /
      // etc.). Don't abort the whole plan — just stop descending this subtree.
      // Every base table of the statement is still covered as a full S: scan by
      // the uncovered-base-table net in auto_generate_plan_from_qep, so the
      // query degrades to a 2-RPC full-table prefetch (over-fetch) instead of
      // collapsing to per-row NLJ. *ok stays true (best-effort coverage).
      return;
  }
}

static bool qep_leaf_info(AccessPath *p, TABLE **tbl, Index_lookup **ref,
                          bool *is_full_scan, int *full_scan_index) {
  *ref = nullptr;
  *is_full_scan = false;
  *full_scan_index = -1;  // -1 = primary/heap full scan; else secondary index
  switch (p->type) {
    case AccessPath::TABLE_SCAN:
      *tbl = p->table_scan().table; *is_full_scan = true; return true;
    case AccessPath::INDEX_SCAN:
      *tbl = p->index_scan().table; *is_full_scan = true;
      *full_scan_index = p->index_scan().idx; return true;
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
      *tbl = p->ref().table; *ref = p->ref().ref; return true;
    case AccessPath::EQ_REF:
      *tbl = p->eq_ref().table; *ref = p->eq_ref().ref; return true;
    case AccessPath::PUSHED_JOIN_REF:
      *tbl = p->pushed_join_ref().table; *ref = p->pushed_join_ref().ref;
      return true;
    case AccessPath::CONST_TABLE:
      *tbl = p->const_table().table; *ref = p->const_table().ref; return true;
    default:
      return false;
  }
}

// 0-based ordinal of a field within TABLE::field[] (the full serialized row
// includes ALL columns, PK ones too — the server's PredicateEvaluator and
// build_plan_key index by this full ordinal). -1 if not found. Per Codex
// review: the earlier non-PK-only ordinal happened to match for columns before
// a mid-PK part (e.g. lineitem l_partkey/l_suppkey) but is wrong in general
// (e.g. a join on a column after l_linenumber).
static int qep_table_field_index(TABLE *t, Field *f) {
  for (uint i = 0; i < t->s->fields; ++i)
    if (t->field[i] == f) return static_cast<int>(i);
  return -1;
}

// P0 semijoin correctness whitelist. A semijoin membership-reduction ("drop
// probe rows whose key is absent from the source set") is result-preserving
// ONLY between two leaves joined by a plain INNER equi-join in the SAME
// top-level query block. For anti-join (NOT IN / NOT EXISTS) the absent rows
// are exactly the ones to KEEP, so the reduction corrupts results (observed:
// q22 over-count, q21 -> 0 rows). Reject any leaf that is:
//   (a) the inner side of an outer join (its NULL-extended rows must survive),
//   (b) embedded in a semi-join or anti-join nest, or
//   (c) not in the statement's top-level query block (correlated/derived
//       subquery — its equality is not a top-level inner-join constraint).
// The server compares membership keys as raw bytes (lineairdb_rpc.cc:1157), so
// also require a non-nullable join key and byte-compatible field types between
// source and probe.
static bool helios_sj_safe_leaf(const TABLE *t) {
  if (t == nullptr) return false;
  const Table_ref *tr = t->pos_in_table_list;
  if (tr == nullptr) return false;
  if (tr->is_inner_table_of_outer_join()) return false;     // (a)
  for (const Table_ref *emb = tr->embedding; emb != nullptr;  // (b)
       emb = emb->embedding)
    if (emb->is_sj_or_aj_nest()) return false;
  const Query_block *qb = tr->query_block;                   // (c)
  if (qb == nullptr || qb->outer_query_block() != nullptr) return false;
  return true;
}

// Source/probe join keys must be byte-compatible for the server's raw-byte
// membership test, and non-nullable (a NULL key cannot be matched soundly).
static bool helios_sj_keys_compatible(const Field *src, const Field *probe) {
  if (src == nullptr || probe == nullptr) return false;
  if (src->is_nullable() || probe->is_nullable()) return false;
  if (src->type() != probe->type()) return false;
  if (src->pack_length() != probe->pack_length()) return false;
  if (src->result_type() == STRING_RESULT &&
      src->charset() != probe->charset())
    return false;
  return true;
}

// Duplicate-fetch dedup. Two read-plan steps that fetch byte-identical data
// (same physical table, same access shape + key bindings + filter + aggregate +
// semijoins) ship that data twice. The transaction cache is keyed by physical
// table + key (lookup_local_range_scan / local_read_set_, step/alias-agnostic),
// so every alias handler resolves to whichever cached entry matches the probe —
// the duplicate fetches are pure redundant network transfer (TPC-H q21:
// lineitem fetched 3x by the l1/l2/l3 self-join aliases, all by orders.o_orderkey).
// Keep the first step of each identical-signature group, drop the rest, and
// remap every source_step reference (bindings/end_bindings/semijoins) to the
// survivor. Steps that differ in filter/aggregate/semijoin/access-shape are NOT
// merged (their fetched row sets differ). OCC is unaffected: the same physical
// rows observed once vs thrice carry the same version; record_stateless_read /
// activate_range_validation already dedup. Alias predicates (q21 l_receiptdate>
// l_commitdate, l_suppkey<>...) are applied by MySQL post-fetch, not at fetch,
// so sharing the candidate row set preserves result multiplicity.
static void dedup_identical_fetch_steps(
    std::vector<LineairDBProxy::ReadPlanStep> &steps) {
  const bool dbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
  auto put_u = [](std::string &s, uint64_t v) {
    for (int i = 0; i < 8; ++i) s.push_back((char)((v >> (8 * i)) & 0xff));
  };
  auto put_s = [&](std::string &s, const std::string &v) {
    put_u(s, v.size());
    s.append(v);
  };
  std::vector<int> canonical_old(steps.size());     // earliest step with same sig
  std::unordered_map<std::string, int> sig_to_canon;
  for (size_t i = 0; i < steps.size(); ++i) {
    const auto &st = steps[i];
    std::string sig;
    put_s(sig, st.table_name);
    put_s(sig, st.key_prefix);
    put_s(sig, st.end_key_prefix);
    put_s(sig, st.index_name);
    put_s(sig, st.filter_serialized);
    put_s(sig, st.aggregate_serialized);
    sig.push_back(st.is_scan ? 1 : 0);
    sig.push_back(st.for_each ? 1 : 0);
    sig.push_back(st.reverse_scan ? 1 : 0);
    put_u(sig, st.scan_limit);
    auto add_b = [&](const std::vector<LineairDBProxy::ReadPlanKeyBinding> &bs) {
      put_u(sig, bs.size());
      for (const auto &b : bs) {
        // canonicalize the source ref so two consumers of duplicate sources
        // still compare equal (sources have a smaller index => already set).
        put_u(sig, (uint64_t)(uint32_t)canonical_old[b.source_step]);
        put_u(sig, b.source_row);
        put_u(sig, b.source_offset);
        put_u(sig, b.source_length);
        put_u(sig, (uint64_t)(uint32_t)b.source_column);
        put_u(sig, (uint64_t)b.int_delta);
        sig.push_back(b.use_midpoint ? 1 : 0);
        sig.push_back(b.from_key ? 1 : 0);
        sig.push_back(b.column_as_int_key ? 1 : 0);
      }
    };
    add_b(st.bindings);
    add_b(st.end_bindings);
    put_u(sig, st.semijoins.size());
    for (const auto &sj : st.semijoins) {
      put_u(sig, (uint64_t)canonical_old[sj.source_step]);
      put_u(sig, sj.source_column);
      put_u(sig, sj.probe_column);
      put_s(sig, sj.source_filter);
    }
    auto it = sig_to_canon.find(sig);
    if (it == sig_to_canon.end()) {
      sig_to_canon.emplace(std::move(sig), (int)i);
      canonical_old[i] = (int)i;
    } else {
      canonical_old[i] = it->second;
    }
  }
  // No duplicates => leave `steps` untouched and return. This MUST be checked
  // before the move-compaction below: moving survivors into `kept` empties
  // steps[i], so returning after the moves (when nothing was dropped) would
  // hand the caller a vector of moved-from (empty) steps and break every query
  // that has no duplicate fetch (observed: full-scan queries deadlocked).
  if (sig_to_canon.size() == steps.size()) return;
  // Compact survivors (canonical_old[i]==i), build old->new index map.
  std::vector<int> old_to_new(steps.size(), -1);
  std::vector<LineairDBProxy::ReadPlanStep> kept;
  kept.reserve(steps.size());
  for (size_t i = 0; i < steps.size(); ++i) {
    if (canonical_old[i] == (int)i) {
      old_to_new[i] = (int)kept.size();
      kept.push_back(std::move(steps[i]));
    } else if (dbg) {
      std::fprintf(stderr,
          "[DEDUP] drop step%zu (dup of step%d) tbl=%s idx=%s for_each=%d\n",
          i, canonical_old[i], steps[i].table_name.c_str(),
          steps[i].index_name.c_str(), steps[i].for_each ? 1 : 0);
    }
  }
  auto remap = [&](uint32_t &src) {
    src = (uint32_t)old_to_new[canonical_old[src]];  // canon is always a survivor
  };
  for (auto &st : kept) {
    for (auto &b : st.bindings) remap(b.source_step);
    for (auto &b : st.end_bindings) remap(b.source_step);
    for (auto &sj : st.semijoins) remap(sj.source_step);
  }
  if (dbg)
    std::fprintf(stderr, "[DEDUP] %zu -> %zu steps\n", steps.size(), kept.size());
  steps = std::move(kept);
}

// Auto-generate prefetch steps from the current SELECT's QEP. Returns false if
// the plan cannot be fully modelled (caller then runs without a prefetch plan).
static bool auto_generate_plan_from_qep(
    THD *thd, std::vector<LineairDBProxy::ReadPlanStep> *out) {
  if (thd == nullptr || thd->lex == nullptr || thd->lex->unit == nullptr)
    return false;
  Query_block *qb = thd->lex->unit->first_query_block();
  const bool dbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
  if (qb == nullptr || qb->join == nullptr) {
    if (dbg) std::fprintf(stderr, "[QEP] no qb/join (qb=%p join=%p)\n",
                          (void*)qb, (void*)(qb?qb->join:nullptr));
    return false;
  }
  AccessPath *root = qb->join->root_access_path();
  if (root == nullptr) { if (dbg) std::fprintf(stderr, "[QEP] no root_access_path\n"); return false; }

  std::vector<AccessPath *> leaves;
  bool ok = true;
  collect_qep_leaves(root, &leaves, &ok);
  if (dbg) std::fprintf(stderr, "[QEP] root_type=%d collect_ok=%d leaves=%zu\n",
                        (int)root->type, ok?1:0, leaves.size());
  // (b) Do NOT bail on incomplete coverage or zero leaves: the uncovered-base-
  // table net below prefetches every remaining base table as a full S: scan, so
  // an unmodelled plan shape degrades to a 2-RPC full-table prefetch rather than
  // per-row NLJ. We only give up (return false) if NO step at all can be formed
  // (checked at the end via out->empty()).

  std::unordered_map<TABLE *, int> tbl_step;
  std::vector<LineairDBProxy::ReadPlanStep> steps;

  // (b) Count how many times each base table NAME is referenced in the
  // statement. A full-range S: scan caches rows under a (table,full-range) key;
  // if the cached rows were pruned by a pushed single-table filter, a DIFFERENT
  // reference to the same table that full-scans it (e.g. the two branches of a
  // UNION, or a self-join) would wrongly reuse that filtered slice and miss
  // rows. So a full-range filter is only sound when the table is referenced
  // exactly once. (FER/FES per-probe entries key on distinct start_keys and
  // don't collide, so they keep their filters.)
  std::unordered_map<std::string, int> name_refs;
  for (auto *tl = thd->lex->query_tables; tl != nullptr; tl = tl->next_global) {
    if (tl->table == nullptr || tl->table->s == nullptr) continue;
    if (tl->table->s->tmp_table != NO_TMP_TABLE) continue;
    name_refs[physical_table_key(tl->table)]++;
  }
  steps.reserve(leaves.size());

  // Compile ONE leaf access path into a plan step. Returns 1=added, 0=skipped
  // (temp table or a table already covered by an earlier step — e.g. a
  // correlated subquery re-scanning an outer table), -1=cannot model. Shared by
  // the main join tree and the dependent/derived subquery walk below.
  auto compile_leaf = [&](AccessPath *leaf) -> int {
    TABLE *t = nullptr; Index_lookup *ref = nullptr; bool full_scan = false;
    int full_scan_index = -1;
    if (!qep_leaf_info(leaf, &t, &ref, &full_scan, &full_scan_index)) return -1;
    if (t == nullptr || t->s == nullptr) return -1;
    // Materialized derived/temp tables live only inside MySQL — skip.
    if (t->s->tmp_table != NO_TMP_TABLE) return 0;
    // Already covered (the driver, or an outer table a correlated subquery
    // re-scans by the same key — the existing step's cache serves it).
    if (tbl_step.find(t) != tbl_step.end()) return 0;

    LineairDBProxy::ReadPlanStep step;
    step.table_name = physical_table_key(t);

    if (full_scan || ref == nullptr) {
      // Full-scan leaf → full-range prefetch + its single-table filter. Sound at
      // ANY position (driver, hash-join side, or nested-loop inner rescanned per
      // outer row — all hit the cached full range). A secondary INDEX_SCAN
      // (full_scan_index != primary) prefetches that secondary index (SI:) so
      // MySQL's index scan hits the secondary cache instead of falling back.
      step.is_scan = true;
      step.end_key_prefix.assign(16, '\xff');  // full-range sentinel
      if (full_scan_index >= 0 &&
          full_scan_index != static_cast<int>(t->s->primary_key))
        step.index_name = t->key_info[full_scan_index].name;
      // (b) Push the single-table filter onto a full-range scan only when this
      // table is referenced once (else a differently-filtered sibling scan
      // would collide on the shared full-range cache key — see name_refs).
      std::string f;
      auto nr = name_refs.find(step.table_name);
      if ((nr == name_refs.end() || nr->second <= 1) &&
          build_single_table_filter(thd, t, &f) && !f.empty())
        step.filter_serialized = f;
    } else {
      // ref / eq_ref join. Derive one binding PER key part: the lookup key is
      // the concatenation of all key parts (build_plan_key appends each binding
      // in order). Single-part covers FE/FER/FES on one column; multi-part
      // covers composite-key eq_ref like partsupp PRIMARY (ps_partkey from one
      // source table, ps_suppkey from another).
      if (ref->key_parts == 0 || ref->items == nullptr) return -1;
      uint bound_parts = 0;
      int first_src = -1;
      for (uint kp = 0; kp < ref->key_parts; ++kp) {
        Item *val = ref->items[kp];
        if (val == nullptr) return -1;
        val = val->real_item();
        if (val->type() != Item::FIELD_ITEM) return -1;
        Field *sf = down_cast<Item_field *>(val)->field;
        if (sf == nullptr || sf->table == nullptr) return -1;
        auto it = tbl_step.find(sf->table);
        if (it == tbl_step.end()) {
          // Source isn't a prefetched step. If it's a materialized/temp table
          // (e.g. Q15 joins supplier on revenue0.supplier_no, where revenue0 is
          // a GROUP BY view materialized into a temp table), the per-row key
          // values don't exist until runtime, so a bound FE/FER/FES is
          // impossible. Skip this leaf so the uncovered-base-table net below
          // prefetches the TARGET table in full (S:) instead — still 2-RPC, no
          // NLJ. A real (non-temp) source not yet seen stays conservative.
          if (sf->table->s != nullptr &&
              sf->table->s->tmp_table != NO_TMP_TABLE)
            return 0;
          return -1;  // source not an earlier step
        }

        // Multi-keypart bindings concatenate into one lookup key, but the
        // server applies the SAME source row index to every binding. That is
        // only correct when all key parts come from the SAME source step (one
        // flattened row = one tuple). If a later key part binds from a
        // DIFFERENT step (e.g. lineitem (l_partkey<-partsupp, l_suppkey<-
        // supplier)), the rows are uncorrelated, so we TRUNCATE to the leading
        // same-source prefix and let the runtime composite probe be served by
        // the prefix-probe in lookup_local_*_scan. (Codex review.)
        if (kp == 0) first_src = it->second;
        else if (it->second != first_src) break;

        LineairDBProxy::ReadPlanKeyBinding b;
        b.source_step = static_cast<uint32_t>(it->second);

        TABLE *st = sf->table;
        const uint spk = st->s->primary_key;
        int pk_pos = -1, pk_parts = 0;
        if (spk != MAX_KEY) {
          KEY &k = st->key_info[spk];
          pk_parts = static_cast<int>(k.user_defined_key_parts);
          for (int j = 0; j < pk_parts; ++j)
            if (k.key_part[j].field == sf) { pk_pos = j; break; }
        }
        // (a) The target index key-part this binding probes. Its key encoding
        // must match what we feed; all bound join paths require BOTH the source
        // and this target part to be plain signed 4-byte INT (the only layout
        // the int-key/byte-copy path reproduces exactly). Otherwise bail → the
        // target is prefetched as a full S: scan by the uncovered-net instead.
        Field *tf = (ref->key >= 0 && ref->key < static_cast<int>(t->s->keys))
                        ? t->key_info[ref->key].key_part[kp].field
                        : nullptr;
        if (!is_int32_key_field(tf)) return -1;

        if (pk_pos == 0 && pk_parts == 1) {
          if (!is_int32_key_field(sf)) return -1;  // source PK must be INT4 too
          b.from_key = true;                       // whole single-column PK
        } else if (pk_pos >= 0) {
          // Any PK part (including an interior one, e.g. binding from
          // partsupp.ps_suppkey = PK part 1). Slice it out of the source's
          // encoded composite key: offset = sum of preceding parts' encoded
          // lengths, length = this part's encoded length. (Codex: stop
          // special-casing PK part 0.)
          // (a) The offset math `4 + length` only holds for INT-encoded parts
          // (header 4 + value). STRING parts encode as 5 + length and shift the
          // offset, so an interior slice over a composite key containing any
          // non-INT part up to pk_pos would cut the wrong bytes. Bail (skip →
          // full S: of the target via the uncovered-net) unless every part
          // [0..pk_pos] is INT-encoded.
          KEY &k = st->key_info[spk];
          for (int j = 0; j <= pk_pos; ++j) {
            if (!is_int32_key_field(k.key_part[j].field))
              return -1;  // non-4-byte-INT composite key part → can't slice safely
          }
          b.from_key = true;
          uint off = 0;
          for (int j = 0; j < pk_pos; ++j) off += 4 + k.key_part[j].length;
          b.source_offset = off;
          b.source_length = 4 + k.key_part[pk_pos].length;
        } else {
          // (a) Value-column binding re-encodes the source column as an INT key
          // server-side (strtoll). Only valid when the source column is an
          // integer type; for VARCHAR/DATE/DECIMAL/etc. the probe key would be
          // wrong, so bail (skip → full S: of the target via the uncovered-net,
          // still 2-RPC over-fetch) instead of emitting a corrupt int key. Only
          // 4-byte SIGNED integers survive the server's strtoll→int32 path.
          if (!is_int32_key_field(sf)) return -1;
          const int fi = qep_table_field_index(st, sf);  // full field ordinal
          if (fi < 0) return -1;
          b.source_column = fi + 1;  // server extracts (source_column-1)
          b.column_as_int_key = true;
        }
        step.bindings.push_back(std::move(b));
        ++bound_parts;
      }
      if (bound_parts == 0) return -1;

      // Child access op. Use bound_parts (after any truncation), not
      // ref->key_parts: a truncated composite is a PREFIX, not a unique point.
      const int cidx = ref->key;
      if (cidx < 0) return -1;
      const bool child_primary = (cidx == static_cast<int>(t->s->primary_key));
      const uint child_pk_parts =
          (t->s->primary_key != MAX_KEY)
              ? t->key_info[t->s->primary_key].user_defined_key_parts
              : 0;
      step.for_each = true;
      if (child_primary && bound_parts >= child_pk_parts) {
        step.is_scan = false;                    // FE: PK point read
      } else if (child_primary) {
        step.is_scan = true;                     // FER: PK-prefix range
      } else {
        step.is_scan = true;                     // FES: secondary range
        step.index_name = t->key_info[cidx].name;
      }
    }

    tbl_step[t] = static_cast<int>(steps.size());  // step index (skips shift it)
    steps.push_back(std::move(step));
    return 1;
  };  // compile_leaf

  // Main join tree: compile each leaf best-effort. (b) An unmodelled leaf
  // (compile_leaf < 0, e.g. a binding source that isn't a prefetched step) is
  // SKIPPED rather than aborting the whole plan; its base table is then covered
  // as a full S: scan by the uncovered-base-table net below. Worst case the
  // whole query degrades to full-table prefetch (2-RPC, over-fetch), never NLJ.
  for (size_t i = 0; i < leaves.size(); ++i)
    (void)compile_leaf(leaves[i]);

  // Dependent / derived subqueries: compile each nested query block's leaves
  // too (sharing tbl_step), so correlated subquery scans (e.g. Q20's per-row
  // SUM over lineitem keyed by the outer partsupp, Q2's MIN over partsupp) are
  // also prefetched. An unmodellable subquery leaf is best-effort (left to the
  // full-S: safety net below / stateless fallback) rather than aborting.
  std::vector<Query_block *> stack{qb};
  while (!stack.empty()) {
    Query_block *b = stack.back();
    stack.pop_back();
    for (Query_expression *u = b->first_inner_query_expression(); u != nullptr;
         u = u->next_query_expression()) {
      for (Query_block *sub = u->first_query_block(); sub != nullptr;
           sub = sub->next_query_block()) {
        stack.push_back(sub);
        if (sub->join == nullptr || sub->join->root_access_path() == nullptr)
          continue;
        std::vector<AccessPath *> sl;
        bool sok = true;
        collect_qep_leaves(sub->join->root_access_path(), &sl, &sok);
        if (!sok) continue;
        for (AccessPath *lf : sl) (void)compile_leaf(lf);
      }
    }
  }

  // Cover base tables that the main join tree did NOT reach — typically tables
  // that live only in a subquery (e.g. Q18's IN(SELECT ... FROM lineitem GROUP
  // BY ...), Q16's NOT IN(SELECT ... FROM supplier ...)). Prefetch each as a
  // full S: scan so the subquery's scan is served from cache and the statement
  // stays 2-RPC. (Correlated subqueries probed by a secondary index need a
  // bound FES instead; those are handled separately / may still fall back.)
  std::unordered_set<std::string> net_full_scans;  // (b) one full-S per name
  for (auto *tl = thd->lex->query_tables; tl != nullptr; tl = tl->next_global) {
    TABLE *bt = tl->table;
    if (bt == nullptr || bt->s == nullptr) continue;
    if (bt->s->tmp_table != NO_TMP_TABLE) continue;       // skip derived/temp
    if (tbl_step.find(bt) != tbl_step.end()) continue;    // already covered
    const std::string name = physical_table_key(bt);
    // (b) Emit at most one full-range S: per table NAME. A second reference to
    // the same table (e.g. the other UNION branch) shares the table-keyed
    // cache, so one complete unfiltered scan serves all of them.
    if (!net_full_scans.insert(name).second) continue;
    LineairDBProxy::ReadPlanStep step;
    step.table_name = name;
    step.is_scan = true;
    step.end_key_prefix.assign(16, '\xff');
    // Push the filter only when this table is referenced exactly once; multiple
    // references may full-scan it under different predicates and would collide
    // on the shared full-range cache key (see name_refs).
    std::string f;
    auto nr = name_refs.find(name);
    if ((nr == name_refs.end() || nr->second <= 1) &&
        build_single_table_filter(thd, bt, &f) && !f.empty())
      step.filter_serialized = f;
    tbl_step[bt] = static_cast<int>(steps.size());
    steps.push_back(std::move(step));
  }

  // Phase-1A / per-table OR-union: drop redundant inner FER (primary-prefix
  // for_each scan) steps when a TRUE full-cover unfiltered S: step already
  // covers the same physical table. For TPC-H Q21 the planner sees 3 lineitem
  // aliases (l1, l2, l3); name_refs>1 has already disabled filter pushdown on
  // l1's S: so step0 is an unfiltered full scan = OR-union(TRUE, TRUE, TRUE).
  // l2/l3's FER are redundant copies of the same data. With this drop they
  // are not emitted; MySQL's inner index_read_map probes fall through to
  // step0's full-cover entry via lookup_local_range_scan's empty-start bucket
  // + the slice_range_entry_fast path.
  //
  // Eligibility (S_outer eligible to subsume a FER S_inner):
  //   - S_outer: is_scan && !for_each && index_name.empty() && bindings.empty()
  //              && end_bindings.empty() && key_prefix.empty()
  //              && end_key_prefix == 0xff*16 && scan_limit == 0
  //              && filter_serialized.empty()
  //   - S_inner: for_each && is_scan && index_name.empty()  (FER only)
  //   - same table_name; S_inner is not a binding source of any later step
  //
  // FES (secondary) is NOT subsumed: a primary full scan does not provide a
  // secondary-key index entry. Phase-2 work.
  {
    static const std::string kFullEnd16(16, '\xff');
    auto is_full_cover = [&](const LineairDBProxy::ReadPlanStep& s) -> bool {
      return s.is_scan && !s.for_each && s.index_name.empty() &&
             s.bindings.empty() && s.end_bindings.empty() &&
             s.key_prefix.empty() && s.end_key_prefix == kFullEnd16 &&
             s.scan_limit == 0 && s.filter_serialized.empty();
    };
    std::unordered_map<std::string, size_t> full_coverer;
    for (size_t i = 0; i < steps.size(); ++i)
      if (is_full_cover(steps[i]))
        full_coverer.try_emplace(steps[i].table_name, i);

    std::unordered_set<uint32_t> bound_source_steps;
    for (const auto &s : steps) {
      for (const auto &b : s.bindings)     bound_source_steps.insert(b.source_step);
      for (const auto &b : s.end_bindings) bound_source_steps.insert(b.source_step);
    }

    std::vector<bool> drop(steps.size(), false);
    size_t n_drop = 0;
    for (size_t i = 0; i < steps.size(); ++i) {
      const auto &s = steps[i];
      if (!(s.for_each && s.is_scan && s.index_name.empty())) continue;
      auto it = full_coverer.find(s.table_name);
      if (it == full_coverer.end() || it->second == i) continue;
      if (bound_source_steps.count(static_cast<uint32_t>(i))) continue;
      drop[i] = true;
      ++n_drop;
    }

    // Phase-4 Q15 (Codex 2026-05-29): also drop duplicate S: (full-cover,
    // unfiltered) steps for the same physical table. Q15 emits THREE
    // lineitem S: scans (CREATE VIEW + 2× expansion in main SELECT),
    // identical params, identical result. Keep the first, drop later
    // duplicates. The MySQL handler's index_read_map / rnd_next on the
    // dropped alias falls through to lookup_local_range_scan's full-cover
    // bucket (the kept step's entry).
    size_t n_drop_dup_s = 0;
    for (size_t i = 0; i < steps.size(); ++i) {
      if (drop[i]) continue;
      const auto &s = steps[i];
      if (!is_full_cover(s)) continue;
      auto it = full_coverer.find(s.table_name);
      if (it == full_coverer.end() || it->second == i) {
        // i is not the duplicate. Either it IS the coverer (it->second == i)
        // or there is no recorded coverer (shouldn't happen here). Skip.
        continue;
      }
      // This S: shares table_name with full_coverer[s.table_name] which is
      // earlier; the previous loop's dedup ran only on FER. Drop this S:.
      if (bound_source_steps.count(static_cast<uint32_t>(i))) continue;
      drop[i] = true;
      ++n_drop_dup_s;
    }
    n_drop += n_drop_dup_s;

    if (n_drop > 0) {
      std::vector<size_t> new_idx(steps.size(), SIZE_MAX);
      size_t out_i = 0;
      for (size_t i = 0; i < steps.size(); ++i)
        if (!drop[i]) new_idx[i] = out_i++;
      std::vector<LineairDBProxy::ReadPlanStep> kept;
      kept.reserve(out_i);
      for (size_t i = 0; i < steps.size(); ++i) {
        if (drop[i]) continue;
        LineairDBProxy::ReadPlanStep s = std::move(steps[i]);
        for (auto &b : s.bindings)
          b.source_step = static_cast<uint32_t>(new_idx[b.source_step]);
        for (auto &b : s.end_bindings)
          b.source_step = static_cast<uint32_t>(new_idx[b.source_step]);
        kept.push_back(std::move(s));
      }
      steps = std::move(kept);
      if (std::getenv("HELIOS_FE_DEBUG"))
        std::fprintf(stderr,
                     "[QEP] phase1a-dedup dropped %zu redundant step(s) "
                     "(dup-FER plus dup-S:=%zu)\n",
                     n_drop, n_drop_dup_s);
    }
  }

  if (std::getenv("HELIOS_FE_DEBUG")) {
    for (size_t i = 0; i < steps.size(); ++i) {
      const auto &s = steps[i];
      const char *op = (!s.for_each) ? "S"
                       : (!s.is_scan) ? "FE"
                       : (s.index_name.empty()) ? "FER" : "FES";
      std::string bs;
      for (const auto &b : s.bindings) {
        char buf[96];
        std::snprintf(buf, sizeof(buf), " B%u.%s col=%d off=%u len=%u",
                      b.source_step, b.from_key ? "K" : "CI", b.source_column,
                      b.source_offset, b.source_length);
        bs += buf;
      }
      std::fprintf(stderr, "[QEP] step%zu %s:%s idx=%s%s\n", i, op,
                   s.table_name.c_str(), s.index_name.c_str(), bs.c_str());
    }
  }

  // Phase-9 semijoin reduction (gated HELIOS_ENABLE_SEMIJOIN, default off).
  // Goal: for a high-fanout probe step (e.g. lineitem by partsupp.ps_partkey),
  // if its probe join key is in the same equality class as a column of an
  // EARLIER, selectively-filtered step (e.g. part with p_name LIKE '%green%'),
  // ship only probe rows whose key is among that filtered step's surviving keys.
  // The join itself stays on the compute side; this only drops rows that cannot
  // join, so results are unchanged.
  //
  // Equality classes come from the join conditions (Index_lookup ref): for each
  // bound leaf, ref->items[kp] (a source Field) == the target index key_part
  // field. We union-find over (table,column) field pointers, map each class to
  // its steps, find a class member step that (a) is earlier and (b) carries a
  // single-table filter, and attach a SemijoinFilter to the later high-fanout
  // step. q9: class {part.p_partkey, partsupp.ps_partkey, lineitem.l_partkey};
  // part (step3, green) prunes lineitem (step4).
  if (std::getenv("HELIOS_ENABLE_SEMIJOIN") != nullptr) {
    // union-find over Field*
    std::unordered_map<Field *, Field *> uf;
    std::function<Field *(Field *)> find = [&](Field *x) -> Field * {
      auto it = uf.find(x);
      if (it == uf.end()) { uf[x] = x; return x; }
      if (it->second == x) return x;
      Field *r = find(it->second);
      uf[x] = r;
      return r;
    };
    auto unite = [&](Field *a, Field *b) { uf[find(a)] = find(b); };
    // Re-derive equality edges from the join refs of every leaf.
    auto add_edges = [&](AccessPath *leaf) {
      TABLE *t = nullptr; Index_lookup *ref = nullptr;
      bool fs = false; int fsi = -1;
      if (!qep_leaf_info(leaf, &t, &ref, &fs, &fsi) || ref == nullptr) return;
      for (uint kp = 0; kp < ref->key_parts; ++kp) {
        Item *val = ref->items ? ref->items[kp] : nullptr;
        if (val == nullptr) continue;
        val = val->real_item();
        if (val->type() != Item::FIELD_ITEM) continue;
        Field *sf = down_cast<Item_field *>(val)->field;
        Field *tf = (ref->key >= 0 && ref->key < (int)t->s->keys)
                        ? t->key_info[ref->key].key_part[kp].field : nullptr;
        if (sf && tf) unite(sf, tf);
      }
    };
    for (AccessPath *lf : leaves) add_edges(lf);
    // Which steps have a single-table filter (selective source candidates)?
    // Map each step's TABLE* (via tbl_step) and its primary-key field's class.
    // For each FER/FES probe step, look for an earlier filtered step in the same
    // class and attach a semijoin on the probe's PK field.
    for (auto &kvp : tbl_step) {
      TABLE *probe_t = kvp.first;
      int probe_step = kvp.second;
      if (probe_step < 0 || probe_step >= (int)steps.size()) continue;
      auto &ps = steps[probe_step];
      if (!(ps.for_each && ps.is_scan)) continue;  // only FER/FES high-fanout
      // P0: never reduce a probe that is anti/semi/outer/subquery — dropping
      // its "unmatched" rows would change results (q21/q22).
      if (!helios_sj_safe_leaf(probe_t)) continue;
      // probe join key field: the index field this step probes (l_partkey).
      // Recover it from the step's first binding target — but bindings don't
      // store the target field. Instead use the table's secondary index field
      // for FES, or PK first part for FER. For q9 lineitem FES on l_partkey the
      // index key_part[0] field is l_partkey.
      Field *probe_field = nullptr;
      if (!ps.index_name.empty()) {
        for (uint k = 0; k < probe_t->s->keys; ++k)
          if (ps.index_name == probe_t->key_info[k].name) {
            probe_field = probe_t->key_info[k].key_part[0].field; break;
          }
      } else if (probe_t->s->primary_key != MAX_KEY) {
        probe_field = probe_t->key_info[probe_t->s->primary_key].key_part[0].field;
      }
      if (probe_field == nullptr || uf.find(probe_field) == uf.end()) continue;
      Field *cls = find(probe_field);
      // find an earlier filtered step in the same class
      for (auto &kvp2 : tbl_step) {
        TABLE *src_t = kvp2.first;
        int src_step = kvp2.second;
        if (src_step >= probe_step || src_step < 0 ||
            src_step >= (int)steps.size()) continue;
        // Redundant-semijoin guard: if the probe already FER-probes FROM this
        // source step (a binding's source_step == src_step) AND that source is
        // fetched-filtered, the FER only fetches keys present in the filtered
        // source, so the membership reduction is a no-op. Skipping it keeps the
        // probe's fetch signature identical across self-join aliases so
        // dedup_identical_fetch_steps can collapse them (q21: l1's redundant
        // orders->lineitem semijoin otherwise blocks the lineitem 3->1 merge).
        // (q7 differs: nation is NOT fetched-filtered, so its semijoin stays.)
        {
          bool probe_fers_from_src = false;
          for (const auto &b : ps.bindings)
            if ((int)b.source_step == src_step) { probe_fers_from_src = true; break; }
          if (probe_fers_from_src && steps[src_step].is_scan &&
              !steps[src_step].filter_serialized.empty())
            continue;
        }
        // The source must carry a selective single-table predicate (e.g.
        // part.p_name LIKE '%green%'). We do NOT stamp it onto the source step:
        // the source may be a join inner the executor point-probes for EVERY
        // outer row, so dropping its non-matching rows from the prefetch cache
        // would cause cache misses. Instead the predicate travels in the
        // semijoin; the server builds the membership set from only the source
        // rows that satisfy it, while still shipping the source step in full.
        std::string sf_filter;
        if (steps[src_step].is_scan && !steps[src_step].filter_serialized.empty())
          sf_filter = steps[src_step].filter_serialized;  // already filtered at fetch
        else if (!build_single_table_filter(thd, src_t, &sf_filter) ||
                 sf_filter.empty())
          continue;  // no selective predicate → not a useful semijoin source
        // does src_t have a field in this class? use its PK (the join key).
        if (src_t->s->primary_key == MAX_KEY) continue;
        Field *src_pk = src_t->key_info[src_t->s->primary_key].key_part[0].field;
        if (uf.find(src_pk) == uf.end() || find(src_pk) != cls) continue;
        // P0: source must also be a plain inner-join leaf, and the source/probe
        // join keys must be byte-compatible & non-nullable for the server's
        // raw-byte membership test.
        if (!helios_sj_safe_leaf(src_t)) continue;
        if (!helios_sj_keys_compatible(src_pk, probe_field)) continue;
        if (src_t->pos_in_table_list->query_block !=
            probe_t->pos_in_table_list->query_block)
          continue;  // must be the same top-level query block
        // attach: server collects src_pk values from source rows passing
        // sf_filter, drops probe rows whose probe_field is absent.
        const int sc = qep_table_field_index(src_t, src_pk);
        const int pc = qep_table_field_index(probe_t, probe_field);
        if (sc < 0 || pc < 0) continue;
        LineairDBProxy::ReadPlanStep::Semijoin sj;
        sj.source_step = (uint32_t)src_step;
        sj.source_column = (uint32_t)sc;
        sj.probe_column = (uint32_t)pc;
        // Only carry the filter as a source_filter when the source step ships
        // its rows UNFILTERED (FE point-read / unfiltered scan). If the source
        // scan is already filtered at fetch, its shipped rows are the reduced
        // set, so collect from all of them (empty source_filter).
        if (!(steps[src_step].is_scan &&
              !steps[src_step].filter_serialized.empty())) {
          sj.source_filter = sf_filter;
          // The server evaluates source_filter (full-row field indices) over the
          // source step's shipped rows; disable that step's projection so it
          // ships full rows whose layout matches the filter's column indices.
          // The source is the small filtered table (e.g. part), so shipping it
          // unprojected is cheap vs. the probe-side rows we prune.
          steps[src_step].projection.clear();
          steps[src_step].projection_num_columns = 0;
        }
        ps.semijoins.push_back(sj);
        if (dbg)
          std::fprintf(stderr,
            "[QEP] semijoin: step%d(%s) probe_col=%d <- step%d(%s) src_col=%d\n",
            probe_step, ps.table_name.c_str(), pc, src_step,
            steps[src_step].table_name.c_str(), sc);
        break;  // one semijoin source per probe step (PoC)
      }
    }
  }

  dedup_identical_fetch_steps(steps);  // collapse byte-identical duplicate fetches
  *out = std::move(steps);
  return !out->empty();
}

// Stage the plan, deriving a per-step single-table filter for each FER (PK-
// prefix range) join-prefetch step's OWN table (e.g. lineitem.l_shipdate for
// FER:lineitem) so deep join tables are pruned server-side. FES (secondary) is
// excluded — slice_secondary_entry rebuilds result_keys from the shipped
// secondary keys, so a filter that drops rows makes commit's full-range re-walk
// mismatch (secondary_range_result_changed). The driver (primary-PK S:) keeps
// its execute-time stamped filter from pushed_filter_.
static void stage_plan_with_fer_filters(
    THD *thd, LineairDBTransaction *tx,
    std::vector<LineairDBProxy::ReadPlanStep> steps) {
  if (thd->lex != nullptr) {
    for (auto &step : steps) {
      const bool needs_staged_filter =
          step.is_scan && step.for_each && step.index_name.empty();
      if (!needs_staged_filter) continue;
      // (b) Count references to this table name. A FER step caches per-probe
      // ranges keyed by (table, start_key); if the table is referenced more
      // than once (self-join, UNION branches) the filter/predicate differs per
      // reference but the cache key does not, so a filtered slice from one
      // reference would be wrongly reused by another. Only push the filter when
      // the table is referenced exactly once. (Mirrors name_refs in
      // auto_generate_plan_from_qep for full-range scans.)
      int refs = 0;
      TABLE *match = nullptr;
      for (auto *tl = thd->lex->query_tables; tl != nullptr;
           tl = tl->next_global) {
        if (tl->table == nullptr || tl->table->s == nullptr) continue;
        if (physical_table_key(tl->table) != step.table_name)
          continue;
        ++refs;
        match = tl->table;
      }
      if (refs != 1 || match == nullptr) continue;  // multi-ref → no FER filter
      std::string f;
      if (build_single_table_filter(thd, match, &f) && !f.empty())
        step.filter_serialized = f;
    }
  }
  tx->stage_oneshot_plan(std::move(steps));
}

// Called at begin (external_lock). Only handles an explicit @_ldb_plan override
// (the QEP is NOT available yet here — external_lock runs before optimize). The
// normal QEP-based auto-generation is deferred to maybe_auto_stage_oneshot_plan,
// called from rnd_init/index_init once the optimizer has built the join plan.
static void execute_oneshot_plan_if_present(THD *thd,
                                            LineairDBTransaction *tx) {
  if (tx == nullptr || !tx->is_oneshot_mode()) return;
  const std::string plan_text = read_and_clear_ldb_plan(thd);
  if (plan_text.empty()) return;  // auto-gen deferred until the QEP exists
  auto steps = parse_plan_steps(thd, plan_text);
  if (steps.empty()) return;
  tx->set_oneshot_plan_resolved(true);
  stage_plan_with_fer_filters(thd, tx, std::move(steps));
}

// Called at the first rnd_init/index_init, where the optimizer has finished and
// JOIN::root_access_path() exists. Auto-generates the prefetch plan from the QEP
// exactly once. If the plan cannot be fully modelled, oneshot is turned off so
// the statement runs as a plain (correct) query instead of falling back per-row.
static void maybe_auto_stage_oneshot_plan(THD *thd, LineairDBTransaction *tx) {
  if (tx == nullptr || !tx->is_oneshot_mode() || tx->oneshot_plan_resolved())
    return;
  tx->set_oneshot_plan_resolved(true);
  std::vector<LineairDBProxy::ReadPlanStep> steps;
  if (!auto_generate_plan_from_qep(thd, &steps) || steps.empty()) {
    if (std::getenv("HELIOS_FE_DEBUG"))
      std::fprintf(stderr, "[QEP] auto-gen produced no plan → oneshot off\n");
    tx->set_oneshot_mode(false);
    return;
  }
  if (std::getenv("HELIOS_FE_DEBUG"))
    std::fprintf(stderr, "[QEP] auto-gen staged %zu steps\n", steps.size());

  // --- Projection pushdown planning (v1) -----------------------------------
  // For a pure SELECT, ask the server to return base-row VALUES trimmed to the
  // columns this statement reads (table->read_set). v1 only projects
  // SINGLE-REFERENCE tables (no self-join), skips value-binding-source steps
  // (the server extracts binding columns positionally from the full value),
  // generated-column tables, and the no-benefit case (all columns read).
  // Uniform per table => one tx projection entry per table; the decoder uses it
  // (validated by the parsed column count, so a stray full row still decodes).
  if (thd->lex->sql_command == SQLCOM_SELECT) {
    // Per PHYSICAL table (normalized "./db/table", NOT bare name — cross-db
    // safe): UNION the read_set of every alias, so a self-joined table
    // (Q21 lineitem l1/l2/l3) ships one uniform column set that is a SUPERSET
    // of each alias's read_set. Each alias decodes the union and reads only its
    // own columns (extras are harmless). One tx projection entry per table.
    std::unordered_map<std::string, std::vector<bool>> union_rs;  // table->per-field
    std::unordered_map<std::string, uint32_t> fields_of;
    std::unordered_map<std::string, bool> gcol_of;
    for (auto *tl = thd->lex->query_tables; tl != nullptr;
         tl = tl->next_global) {
      if (tl->table == nullptr || tl->table->s == nullptr) continue;
      std::string n = physical_table_key(tl->table);
      TABLE *T = tl->table;
      const uint32_t nf = T->s->fields;
      fields_of[n] = nf;
      auto &u = union_rs[n];
      if (u.size() < nf) u.resize(nf, false);
      bool g = false;
      for (uint f = 0; f < nf; ++f) {
        if (T->field[f]->is_gcol()) g = true;
        if (bitmap_is_set(T->read_set, f) && f < u.size()) u[f] = true;
      }
      gcol_of[n] = gcol_of.count(n) ? (gcol_of[n] || g) : g;
    }
    // Bindings that read a SOURCE step's VALUE positionally (option-2, Codex
    // 2026-05-29). Two forms:
    //   - column form (source_column>0): the server extracts column
    //     (source_column-1) from the value. PROJECTABLE: (a) force that column
    //     into the source table's kept[] so it survives the trim, and (b) remap
    //     source_column to its projected position (done after kept_of below).
    //   - byte-slice form (!from_key && source_column==0): uses source_offset/
    //     length into the raw value; NOT remappable -> keep the source full.
    // (Previously ALL value-binding sources were excluded -> Q21 lineitem shipped
    //  all 16 cols = 1.3GB. Now lineitem projects to its read_set union.)
    std::unordered_set<std::string> unsafe_src;
    auto force_binding_col = [&](const auto &b) {
      if (b.from_key || b.source_step >= steps.size()) return;
      const std::string &src = steps[b.source_step].table_name;
      auto fo = fields_of.find(src);
      if (fo == fields_of.end()) return;
      if (b.source_column > 0) {
        const uint32_t fi = static_cast<uint32_t>(b.source_column - 1);
        if (fi >= fo->second) { unsafe_src.insert(src); return; }
        auto &u = union_rs[src];
        if (u.size() < fo->second) u.resize(fo->second, false);
        u[fi] = true;  // keep the binding's source column through the trim
      } else {
        unsafe_src.insert(src);  // byte-slice form: not remappable
      }
    };
    for (const auto &s : steps) {
      for (const auto &b : s.bindings) force_binding_col(b);
      for (const auto &b : s.end_bindings) force_binding_col(b);
    }
    // Semijoin participants (projection reconciliation). The server reads a
    // semijoin's source_column from the SOURCE step's value and probe_column
    // from THIS (probe) step's value using 0-based ordinals (rpc:1157,1164).
    // Projection trims the shipped row, so full ordinals would mis-extract:
    // Semijoin projection reconciliation. The server builds the membership set
    // from the SOURCE step's SHIPPED rows (previous_results[ss].scan_values,
    // which are projection-trimmed) using source_column (read 0-based) and, in
    // the unfiltered branch, the full-ordinal source_filter. The PROBE is left
    // untouched: fe_reject() runs on the probe's UNTRIMMED row (the server trims
    // afterwards — rpc:1196/1285/1317), so probe_column stays a FULL ordinal and
    // the probe keeps its projection.
    // For the SOURCE, branch on whether a source_filter rides along:
    //  - unfiltered branch (source_filter set): ship full (unsafe_src) so the
    //    full-ordinal filter AND source_column extract correctly. The source is
    //    a small filtered table here, so full ship is cheap.
    //  - filtered branch (source already reduced at fetch, no source_filter):
    //    the source may be LARGE (e.g. q3/q8/q21 orders=1.5M), so keep it
    //    projected — only force-keep source_column and remap it to its packed
    //    position (below). Full-shipping a large source was a net transfer LOSS.
    for (const auto &s : steps) {
      for (const auto &sj : s.semijoins) {
        if (sj.source_step >= steps.size()) continue;
        const std::string &src = steps[sj.source_step].table_name;
        auto fo = fields_of.find(src);
        if (fo == fields_of.end()) continue;
        if (!sj.source_filter.empty()) {
          unsafe_src.insert(src);  // unfiltered branch: small source, ship full
        } else if (static_cast<uint32_t>(sj.source_column) < fo->second) {
          auto &u = union_rs[src];   // filtered branch: keep projected
          if (u.size() < fo->second) u.resize(fo->second, false);
          u[static_cast<uint32_t>(sj.source_column)] = true;
        }
      }
    }
    // Decide kept per eligible table, build a full->projected index map.
    std::unordered_map<std::string, std::vector<uint32_t>> kept_of;
    std::unordered_map<std::string, std::vector<uint32_t>> full_to_proj;
    for (auto &kv : union_rs) {
      const std::string &n = kv.first;
      if (gcol_of[n]) continue;
      if (unsafe_src.count(n)) continue;
      const uint32_t nf = fields_of[n];
      std::vector<uint32_t> kept;
      for (uint32_t f = 0; f < nf; ++f)
        if (kv.second[f]) kept.push_back(f);
      if (kept.empty() || kept.size() == nf) continue;  // no benefit / all cols
      std::vector<uint32_t> f2p(nf, 0);  // full ordinal -> projected pos (1-idx)
      for (uint32_t k = 0; k < kept.size(); ++k) f2p[kept[k]] = k + 1;
      tx->set_table_projection(n, kept);
      full_to_proj.emplace(n, std::move(f2p));
      kept_of.emplace(n, std::move(kept));
    }
    // Remap column-form bindings whose source table is now projected: the server
    // reads (source_column-1) from the projected value, so the index must point
    // at the projected position. Forced into kept above => pos is nonzero.
    auto remap_binding = [&](auto &b) {
      if (b.from_key || b.source_column <= 0 || b.source_step >= steps.size())
        return;
      auto it = full_to_proj.find(steps[b.source_step].table_name);
      if (it == full_to_proj.end()) return;  // source not projected
      const uint32_t fi = static_cast<uint32_t>(b.source_column - 1);
      const uint32_t pos = (fi < it->second.size()) ? it->second[fi] : 0;
      if (pos > 0) b.source_column = static_cast<int32_t>(pos);
    };
    for (auto &s : steps) {
      for (auto &b : s.bindings) remap_binding(b);
      for (auto &b : s.end_bindings) remap_binding(b);
    }
    // Remap each semijoin's source_column to its projected position when the
    // source step is projected (filtered branch). The server reads it 0-based
    // from the trimmed source row; f2p is 1-based (0 => not kept, but we
    // force-kept it above). Full-shipped sources are absent from full_to_proj
    // and keep their full ordinal.
    for (auto &s : steps) {
      for (auto &sj : s.semijoins) {
        if (sj.source_step >= steps.size()) continue;
        auto it = full_to_proj.find(steps[sj.source_step].table_name);
        if (it == full_to_proj.end()) continue;  // source full => full ordinal
        const uint32_t fi = static_cast<uint32_t>(sj.source_column);
        const uint32_t pos = (fi < it->second.size()) ? it->second[fi] : 0;
        if (pos > 0) sj.source_column = pos - 1;  // 1-based f2p -> 0-based packed
      }
    }
    for (auto &s : steps) {
      auto it = kept_of.find(s.table_name);
      if (it == kept_of.end()) continue;
      s.projection = it->second;
      s.projection_num_columns = fields_of[s.table_name];
      if (std::getenv("HELIOS_FE_DEBUG"))
        std::fprintf(stderr, "[QEP] projection tbl=%s keep=%zu/%u\n",
                     s.table_name.c_str(), it->second.size(),
                     fields_of[s.table_name]);
    }
  }
  // -------------------------------------------------------------------------

  // helios Phase-6 range-hash OCC: eligible iff the gate is on AND this is a
  // read-only SELECT (SELECT never installs writes, so the server's commit-time
  // range re-scan has no write-after-stale-read window — see design doc). When
  // eligible, full-cover cache serves skip per-row read-TID recording and the
  // server revalidates via retained footprint digests.
  // Phase-7: ON by default now (read-only scope, verified 22/22 md5 both ways,
  // commit OCC 7-11s -> ~2s on Q21). Opt out with HELIOS_RANGEHASH_OCC=0.
  static const char* rh_env = std::getenv("HELIOS_RANGEHASH_OCC");
  static const bool rangehash_gate =
      (rh_env == nullptr) || (std::strcmp(rh_env, "0") != 0);
  tx->set_rangehash_eligible(rangehash_gate &&
                             thd->lex != nullptr &&
                             thd->lex->sql_command == SQLCOM_SELECT);

  // read-only no-validation mode (Stage 0, measurement gate). Explicit opt-in
  // only (HELIOS_RO_NOVALIDATE=1) + pure SELECT. Skips OCC validation + the
  // commit RPC. WEAKER ISOLATION (no serializability under concurrent writers);
  // correct for the no-writer TPC-H benchmark. Production surface should be a
  // session/global var + thd_tx_is_read_only(); env gate is for measurement.
  // (Codex review 2026-05-29; docs/phase7_readonly_novalidate.md.)
  static const char* ronv_env = std::getenv("HELIOS_RO_NOVALIDATE");
  static const bool ronv_gate = ronv_env != nullptr && ronv_env[0] == '1';
  tx->set_ro_novalidate(ronv_gate && thd->lex != nullptr &&
                        thd->lex->sql_command == SQLCOM_SELECT);

  stage_plan_with_fer_filters(thd, tx, std::move(steps));
  // Execute the prefetch NOW. We are at the driver's rnd_init/index_read, i.e.
  // immediately before the scan that probes the joined tables, so the local
  // cache must be fully populated before the first per-row probe. (Deferring to
  // the first lazy cache access can let early probes — e.g. EQ_REF point reads
  // interleaved with the driver scan — miss the not-yet-fetched cache.)
  tx->execute_pending_oneshot_plan();
}

// Forward declaration: defined after cond_push (reuses serialize_item).
static bool build_single_table_filter(THD *thd, TABLE *table,
                                      std::string *out_serialized);

// Push the SELECT WHERE into this transaction if LIMIT will depend on it
static bool prepare_select_filter_for_tx(THD *thd, TABLE *table,
                                         LineairDBTransaction *tx,
                                         std::string *serialized_filter) {
  // Check the handler state needed to inspect the current SELECT.
  if (tx == nullptr) return false;                   // Missing transaction
  if (thd == nullptr) return false;                  // Missing session
  const bool has_table = (table != nullptr && table->s != nullptr);
  if (!has_table) return false;                      // Missing table

  // Use the join-safe per-table predicate in BOTH oneshot and non-oneshot
  // (NLJ index-scan) paths. The previous non-oneshot path serialized the whole
  // WHERE using THIS table's column count, which silently mis-applies a
  // cross-table join conjunct (e.g. o_orderkey = l_orderkey) as a single-table
  // filter evaluated against the wrong columns. When such a filter is shipped
  // with an index materialize (execute_range_materialize / execute_index_first)
  // it rejects almost every row, collapsing any 3+-table join to a handful of
  // rows (observed: customer⋈orders⋈lineitem returned 1 row instead of 60175).
  // build_single_table_filter keeps only top-level AND conjuncts that reference
  // THIS table, so the server-side filter is always a sound subset of the
  // WHERE; MySQL still re-evaluates the full WHERE, so a dropped join conjunct
  // costs over-fetch, never correctness. Because the pushed filter is a partial
  // subset, a row may pass it yet fail the full WHERE, so the result is never
  // LIMIT-safe — always report not-LIMIT-safe to suppress any pushed LIMIT.
  if (serialized_filter != nullptr) serialized_filter->clear();
  std::string per_table;
  if (build_single_table_filter(thd, table, &per_table)) {
    if (serialized_filter != nullptr) *serialized_filter = per_table;
    tx->set_pushed_filter(per_table);
  } else {
    tx->clear_pushed_filter();
  }
  return false;
}

// Flatten an Item's top-level AND tree into conjuncts that reference ONLY
// `me` (this table). Recurses through nested AND_FUNC nodes. Cross-table and
// constant conjuncts are skipped; dropping them only relaxes the pushed filter.
static void collect_driver_atoms(Item *it, table_map me,
                                 std::vector<Item *> *out) {
  if (it == nullptr) return;
  if (it->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(it)->functype() == Item_func::COND_AND_FUNC) {
    for (Item &child : *down_cast<Item_cond *>(it)->argument_list()) {
      collect_driver_atoms(&child, me, out);
    }
  } else if (it->used_tables() == me) {
    out->push_back(it);
  }
}

// Necessary condition implied by an OR-of-ANDs for table `me`: (A or B or ...)
// implies a predicate P iff every disjunct implies P. Take each disjunct's
// driver-only atoms (used_tables()==me) and emit OR(AND(atoms_i)). If ANY
// disjunct has no driver atom the OR constrains `me` by nothing -> return false
// (push nothing; sound, never stricter than the query). Works whether the OR's
// disjuncts are single-table or mix tables (TPC-H q7:
// ((n1=FR and n2=DE) or (n1=DE and n2=FR)) implies n2 in {DE,FR}).
static bool serialize_or_necessary_condition(
    Item *or_item, table_map me, LineairDB::Protocol::FilterExpr *out) {
  if (or_item == nullptr || or_item->type() != Item::COND_ITEM ||
      down_cast<Item_cond *>(or_item)->functype() != Item_func::COND_OR_FUNC)
    return false;
  out->set_op(LineairDB::Protocol::FilterExpr::OP_OR);
  for (Item &disj : *down_cast<Item_cond *>(or_item)->argument_list()) {
    std::vector<Item *> atoms;
    collect_driver_atoms(&disj, me, &atoms);
    if (atoms.empty()) return false;  // disjunct unconstrained -> unsound
    LineairDB::Protocol::FilterExpr branch;
    if (atoms.size() == 1) {
      if (!serialize_item(atoms[0], &branch)) return false;
    } else {
      branch.set_op(LineairDB::Protocol::FilterExpr::OP_AND);
      for (Item *a : atoms)
        if (!serialize_item(a, branch.add_children())) return false;
    }
    *out->add_children() = std::move(branch);
  }
  return out->children_size() > 0;
}

// Phase-3c/3d: derive a SOUND single-table predicate for the driver scan so the
// oneshot prefetch is pruned the same way MySQL's scan is. We anchor on the
// query block that OWNS this table's scan (pos_in_table_list->query_block) so a
// driver inside a derived table / subquery is reached (global_parameters() only
// gives the outermost WHERE). Two shapes are handled:
//   - top-level AND (or single conjunct): keep conjuncts whose used_tables()==me
//     (a cross-table join conjunct like l_partkey=p_partkey would be unsound to
//     evaluate against one table with the wrong column index, so it is dropped).
//   - top-level OR of disjuncts: a predicate is *implied by* the WHERE iff every
//     disjunct implies it, so we take each disjunct's driver-only atoms and emit
//     OR(AND(atoms_i)). If ANY disjunct has no driver atom, the OR constrains the
//     driver by nothing → push nothing (sound: never stricter than the query).
// serialize_item is reused verbatim so wire encoding/type handling matches the
// proven cond_push path. MySQL re-evaluates the full WHERE, so a dropped/relaxed
// predicate only costs over-fetch, never correctness.
static bool build_single_table_filter(THD *thd, TABLE *table,
                                      std::string *out_serialized) {
  if (out_serialized == nullptr) return false;
  out_serialized->clear();
  if (thd == nullptr || table == nullptr || table->s == nullptr) return false;
  if (table->pos_in_table_list == nullptr) return false;

  Query_block *qb = table->pos_in_table_list->query_block;
  if (qb == nullptr) return false;
  Item *where = qb->where_cond();
  if (where == nullptr) return false;

  const table_map me = table->pos_in_table_list->map();

  std::vector<LineairDB::Protocol::FilterExpr> serialized;

  // Iterate the top-level conjuncts (where is AND => its children; else [where]).
  // For each: a single-table atom (or AND of them) is kept as-is; an OR conjunct
  // — even one that MIXES tables — contributes its single-table necessary
  // condition (this is what lets TPC-H q7's nested OR-of-nation predicate prune
  // n2, which the old top-level-OR-only path missed); a cross-table join
  // conjunct is dropped (unsound to evaluate against one table).
  std::vector<Item *> conjuncts;
  if (where->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(where)->functype() == Item_func::COND_AND_FUNC) {
    for (Item &c : *down_cast<Item_cond *>(where)->argument_list())
      conjuncts.push_back(&c);
  } else {
    conjuncts.push_back(where);
  }
  for (Item *c : conjuncts) {
    if (c->type() == Item::COND_ITEM &&
        down_cast<Item_cond *>(c)->functype() == Item_func::COND_OR_FUNC) {
      LineairDB::Protocol::FilterExpr or_expr;
      if (serialize_or_necessary_condition(c, me, &or_expr))
        serialized.push_back(std::move(or_expr));
    } else {
      std::vector<Item *> atoms;
      collect_driver_atoms(c, me, &atoms);
      for (Item *a : atoms) {
        LineairDB::Protocol::FilterExpr expr;
        if (serialize_item(a, &expr)) serialized.push_back(std::move(expr));
      }
    }
  }
  if (serialized.empty()) return false;

  LineairDB::Protocol::PushedPredicate predicate;
  predicate.set_num_columns(table->s->fields);
  if (serialized.size() == 1) {
    *predicate.mutable_expr() = std::move(serialized[0]);
  } else {
    auto *root = predicate.mutable_expr();
    root->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
    for (auto &expr : serialized) *root->add_children() = std::move(expr);
  }
  predicate.SerializeToString(out_serialized);
  return !out_serialized->empty();
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
  HTP_SCOPE(rnd_init);
  scanned_keys_.clear();
  scanned_values_.clear();
  scan_cache_.clear();
  buffer_position_ = 0;
  last_batch_key_.clear();
  scan_exhausted_ = false;
  borrowed_scan_ = LineairDBTransaction::BorrowedScan{};  // Step3: reset borrow
  last_fetched_primary_key_.clear();
  current_position_ = 0;
  stats.records = 0;

  change_active_index(table->s->primary_key);

  auto tx = get_transaction(ha_thd());
  // QEP is available now (optimizer has run) — auto-generate the prefetch plan.
  maybe_auto_stage_oneshot_plan(ha_thd(), tx);

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    DBUG_RETURN(HA_ERR_LOCK_DEADLOCK);
  }

  tx->choose_table(db_table_name);

  // Predicate pushdown: propagate filter serialized by cond_push() to
  // transaction. Falls back to reading WHERE directly via
  // prepare_select_filter_for_tx() in oneshot mode because MySQL's optimizer
  // does not always call cond_push() on the full-scan path for TPC-H queries
  // (e.g. Q1's lineitem scan with DATE arithmetic). That helper performs the
  // same serialize_item pass and assigns the result via set_pushed_filter,
  // so the deferred oneshot plan can pick up the filter and the server-side
  // S: step actually receives the predicate.
  // Note: MySQL only calls cond_push() from sql_update.cc / sql_delete.cc,
  // not from SELECT optimizer paths (which use idx_cond_push). For SELECT
  // full-table scans pushed_filter_serialized_ is therefore empty here and
  // the deferred oneshot plan ships an unfiltered S: scan to the server.
  // Per Codex review: do NOT fall back to prepare_select_filter_for_tx with
  // qb->where_cond() — that helper serializes the *whole* SELECT predicate
  // using this table's column count, which is unsound for multi-table joins
  // (e.g. l_partkey = p_partkey would be evaluated against lineitem only).
  // Future work: extract per-table predicates via Item-tree walk, or expose
  // inline predicates through the DSL (Phase 3c).
  if (!pushed_filter_serialized_.empty()) {
    tx->set_pushed_filter(pushed_filter_serialized_);
  } else if (tx->is_oneshot_mode() &&
             build_single_table_filter(ha_thd(), table,
                                       &pushed_filter_serialized_)) {
    // Phase-3c: SELECT full-scan path. cond_push() is not invoked for SELECT,
    // so derive a sound single-table predicate from the WHERE and hand it to
    // the tx. execute_read_plan() stamps it onto the S: step whose table
    // matches this scan, so the server filters lineitem rows in-scan instead
    // of shipping the whole table back to the proxy.
    tx->set_pushed_filter(pushed_filter_serialized_);
  } else {
    tx->clear_pushed_filter();
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

  // Phase-7 Step3 (InnoDB fetch-cache analog): if this full-table scan can be
  // served directly from its prefetch range entry, borrow it — no per-row
  // scanned_keys_/scanned_values_ copy and no 6M-entry scan_cache_ map. OCC
  // obligations for the range are recorded once inside borrow_fullcover_pk_scan.
  // rnd_next/rnd_pos read rows via the borrow accessors. Falls back to the copy
  // path below when not borrowable (filtered/bounded/own-writes/non-oneshot).
  // Gate to pure SELECT (Codex review): the borrow invariant is "read-only
  // statement, no future own writes". thd_can_use_oneshot allows UPDATE/DELETE,
  // whose later own-writes a borrowed (un-merged) span would not reflect; those
  // statements take the copy path. (Mirrors the Step4a SELECT gate.)
  THD *const thd_fnb = ha_thd();
  const bool select_scan = thd_fnb != nullptr && thd_fnb->lex != nullptr &&
                           thd_fnb->lex->sql_command == SQLCOM_SELECT;
  borrowed_scan_ =
      select_scan ? tx->borrow_fullcover_pk_scan()
                  : LineairDBTransaction::BorrowedScan{};
  if (borrowed_scan_.ok) {
    if (tx->is_aborted()) {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      DBUG_RETURN(false);
    }
    scan_exhausted_ = true;
    DBUG_RETURN(borrowed_scan_.count > 0);
  }

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
  HTP_SCOPE(rnd_next);

  // Lazily fetch (or borrow) on the first call / batch boundary. In borrow mode
  // scanned_keys_ stays empty, so guard on !borrowed_scan_.ok to avoid re-fetch.
  if (!borrowed_scan_.ok && buffer_position_ >= scanned_keys_.size()) {
    if (scan_exhausted_) {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    if (!fetch_next_batch()) {
      auto tx = get_transaction(ha_thd());
      if (tx->is_aborted()) {
        DBUG_RETURN(HA_ERR_LOCK_DEADLOCK);
      }
      scan_exhausted_ = true;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }

  // Phase-7 Step3 borrow path: serve the row straight from the prefetch range
  // entry (no scanned_keys_/scanned_values_ copy). last_fetched_primary_key_ is
  // kept as an owning copy because position() needs it after rnd_end().
  if (borrowed_scan_.ok) {
    if (buffer_position_ >= borrowed_scan_.count) {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    auto tx = get_transaction(ha_thd());
    const std::string *key = tx->borrowed_key(borrowed_scan_, buffer_position_);
    const std::string *value =
        tx->borrowed_value(borrowed_scan_, buffer_position_);
    buffer_position_++;
    if (key == nullptr || value == nullptr) {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    int error = set_fields_from_lineairdb(
        buf, reinterpret_cast<const std::byte *>(value->data()), value->size());
    if (error == 0) {
      last_fetched_primary_key_ = *key;
    }
    current_position_++;
    DBUG_RETURN(error);
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

void ha_lineairdb::tx_set_pushed_aggregate(const std::string &s) {
  auto tx = get_transaction(ha_thd());
  // The override drives the scan via agg_next_raw, which bypasses the normal
  // read path that selects the table; select it here so execute_read_plan's
  // db_table_key matches this scan step and stamps the aggregate (and filter).
  tx->choose_table(db_table_name);
  tx->set_pushed_aggregate(s);
}
bool ha_lineairdb::tx_ro_novalidate() {
  // The override decides Phase B BEFORE the read path runs set_ro_novalidate()
  // (in maybe_auto_stage_oneshot_plan), so tx->ro_novalidate() is not yet set.
  // Replicate its env gate here; the SQLCOM_SELECT half is already guaranteed
  // by the offload whitelist. (Keeps Phase B OCC-sound = read-only scope.)
  static const char *e = std::getenv("HELIOS_RO_NOVALIDATE");
  return e != nullptr && e[0] == '1';
}
void ha_lineairdb::tx_clear_pushed_aggregate() {
  get_transaction(ha_thd())->clear_pushed_aggregate();
}

// Phase-8 Phase B: like rnd_next, but hand back the raw cached row VALUE bytes
// (a server-produced group row) instead of unpacking into a record buffer.
bool ha_lineairdb::agg_next_raw(std::string_view *out_value) {
  if (!borrowed_scan_.ok && buffer_position_ >= scanned_keys_.size()) {
    if (scan_exhausted_) return false;
    if (!fetch_next_batch()) { scan_exhausted_ = true; return false; }
  }
  if (borrowed_scan_.ok) {
    if (buffer_position_ >= borrowed_scan_.count) return false;
    auto tx = get_transaction(ha_thd());
    const std::string *value = tx->borrowed_value(borrowed_scan_, buffer_position_);
    buffer_position_++;
    if (value == nullptr) return false;
    *out_value = std::string_view(value->data(), value->size());
    current_position_++;
    return true;
  }
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
  HTP_SCOPE(rnd_pos);

  std::string primary_key = extract_primary_key_from_ref(pos);

  if (primary_key.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  // Phase-7 Step3 borrow path: re-read by binary-searching the borrowed range
  // entry's sorted rows (InnoDB Buffer-Pool re-read analog) instead of a
  // separate scan_cache_ map. Falls through to tx->read if not found.
  if (borrowed_scan_.ok) {
    auto tx = get_transaction(ha_thd());
    const std::string *value =
        tx->borrowed_value_for_key(borrowed_scan_, primary_key);
    if (value != nullptr) {
      if (set_fields_from_lineairdb(
              buf, reinterpret_cast<const std::byte *>(value->data()),
              value->size())) {
        return HA_ERR_OUT_OF_MEM;
      }
      last_fetched_primary_key_ = primary_key;
      return 0;
    }
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
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  auto result = tx->read(primary_key);

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
    // If stats_base_records is still 0, try to sync from proxy cache.
    // This covers the case where info() is called before external_lock()
    // (e.g., BenchBase catalog refresh, SHOW TABLE STATUS).
    // We also enter this block when the row count is already seeded but the
    // per-index NDV is not yet loaded (HELIOS_OPT_STATS): otherwise a row count
    // arriving via the begin/end piggyback would permanently suppress the NDV
    // fetch (Codex review #4).
    static const char *opt_stats_env = std::getenv("HELIOS_OPT_STATS");
    static const bool opt_stats_on =
        opt_stats_env != nullptr && opt_stats_env[0] == '1';
    const bool need_rowcount =
        share->stats_base_records.load(std::memory_order_relaxed) == 0;
    const bool need_ndv =
        opt_stats_on &&
        !share->index_ndv_loaded_.load(std::memory_order_relaxed);
    if ((need_rowcount || need_ndv) && !db_table_name.empty()) {
      THD *thd = ha_thd();
      if (thd != nullptr) {
        LineairDBThdCtx *ctx =
            *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, lineairdb_hton));
        if (ctx != nullptr && ctx->proxy) {
          auto find_seed = [&]() -> bool {
            const auto &sc = ctx->proxy->cached_table_stats();
            auto it = sc.find(db_table_name);
            if (it != sc.end() && it->second > 0) {
              share->stats_base_records.store(
                  static_cast<uint64_t>(it->second), std::memory_order_relaxed);
              for (auto &shard : share->rowcount_shards)
                shard.delta.store(0, std::memory_order_relaxed);
              return true;
            }
            return false;
          };
          // Access-path fix: the begin/end table_stats piggyback misses at
          // optimize time under oneshot (deferred tx_begin), so the optimizer
          // would otherwise see stats.records==2 and pick full-scan + bad join
          // order. On a cache-miss, do a transaction-less GET_TABLE_STATS RPC
          // once, then seed the GLOBAL share (persists across connections).
          // GATED (HELIOS_OPT_STATS=1, default OFF). Phase 2: fetch the real row
          // count AND per-index NDV in one RPC, then seed the GLOBAL share.
          // rec_per_key is then computed from real NDV (see set_generic_rec_per_key)
          // instead of the n-th-root heuristic that mis-treated non-unique FK
          // joins as 1:1 -> Q5 explosion. (docs/phase7_optimizer_stats.md)
          if (opt_stats_on &&
              (!find_seed() ||
               !share->index_ndv_loaded_.load(std::memory_order_relaxed))) {
            // Build index descriptors from THIS table's schema (the server is
            // schema-light). Primary index uses "" to match GetPrimaryIndex();
            // num_key_parts = the SECONDARY user key-parts (PK not appended).
            std::vector<std::pair<std::string, uint32_t>> descs;
            if (table != nullptr && table->s != nullptr) {
              for (uint i = 0; i < table->s->keys; i++) {
                KEY *k = table->key_info + i;
                const bool is_pri = (i == table->s->primary_key);
                descs.emplace_back(is_pri ? std::string()
                                          : std::string(k->name ? k->name : ""),
                                   k->user_defined_key_parts);
              }
            }
            const bool force = share->index_ndv_force_refresh_.exchange(
                false, std::memory_order_relaxed);
            const bool fetched =
                ctx->proxy->fetch_table_stats(db_table_name, descs, force);
            if (fetched) {
              find_seed();
              std::lock_guard<std::mutex> g(share->index_ndv_mu_);
              share->index_ndv_.clear();
              for (const auto &kv : ctx->proxy->last_index_ndv()) {
                if (kv.second.first)  // available
                  share->index_ndv_[kv.first] = kv.second.second;
              }
              share->index_ndv_loaded_.store(true, std::memory_order_relaxed);
            }
          }
        }
      }
    }

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
          thd_mark_transaction_to_rollback(thd, 1);
          return HA_ERR_LOCK_DEADLOCK;
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

// ANALYZE TABLE: refresh optimizer statistics from the server. Resets the
// cached base row count so the next info() re-fetches the live count (via the
// GET_TABLE_STATS RPC on cache-miss) and recomputes rec_per_key. The info()
// cache-miss hook makes stats correct WITHOUT this, but ANALYZE TABLE is the
// SQL-standard way to force a refresh (and lets a loader run it post-load).
int ha_lineairdb::analyze(THD *, HA_CHECK_OPT *) {
  DBUG_TRACE;
  if (share != nullptr) {
    share->stats_base_records.store(0, std::memory_order_relaxed);
    for (auto &shard : share->rowcount_shards)
      shard.delta.store(0, std::memory_order_relaxed);
    // Force the next info() to re-fetch NDV AND make the server recompute it
    // (rather than serve its cached value). Without clearing index_ndv_loaded_
    // the fetch is skipped; without force=true the server returns its cache
    // (Codex review #2).
    share->index_ndv_loaded_.store(false, std::memory_order_relaxed);
    share->index_ndv_force_refresh_.store(true, std::memory_order_relaxed);
  }
  info(HA_STATUS_VARIABLE | HA_STATUS_CONST);
  return HA_ADMIN_OK;
}

/**
 * Estimate rec_per_key (average rows matching a key prefix) for each
 * key part of an index.
 *
 * Assumes uniform data distribution: N total rows split evenly across
 * K key parts means each part divides by N^(1/K).
 *
 * Example: 1,000,000 rows, 4-part UNIQUE KEY
 *   per_part = 1000000^(1/4) ≈ 31.6
 *   1 part specified: 1000000 / 31.6   = 31,623 rows
 *   2 parts:          1000000 / 31.6^2 = 1,000 rows
 *   3 parts:          1000000 / 31.6^3 = 32 rows
 *   4 parts (full):   1 row (UNIQUE)
 *
 * See also: NDB's ndb_index_stat_set_rpk (ha_ndb_index_stat.cc:2529).
 */
void ha_lineairdb::set_generic_rec_per_key(KEY *key, uint key_parts,
                                           bool is_primary) {
  bool is_unique = (key->flags & HA_NOSAME);

  // Phase 2: if the server returned real NDV for this index, use it:
  //   rec_per_key[j] = ceil(records / NDV(prefix 0..j)).
  // NDV(prefix) = distinct values of the first j+1 key parts (exact, from the
  // server's ordered live scan). This replaces the n-th-root heuristic that
  // mis-estimated non-unique FK indexes as ~unique (rpk=1) -> bad join orders.
  // Falls back to the heuristic when NDV is absent (gate off, or a non-int /
  // string-keyed index the server marked "unavailable").
  const std::vector<uint64_t> *ndv = nullptr;
  if (share != nullptr &&
      share->index_ndv_loaded_.load(std::memory_order_relaxed)) {
    std::lock_guard<std::mutex> g(share->index_ndv_mu_);
    const std::string lookup_key =
        is_primary ? std::string() : std::string(key->name ? key->name : "");
    auto it = share->index_ndv_.find(lookup_key);
    if (it != share->index_ndv_.end() && it->second.size() >= key_parts) {
      // Copy out under the lock so we can release it before set_records_per_key.
      static thread_local std::vector<uint64_t> ndv_local;
      ndv_local = it->second;
      ndv = &ndv_local;
    }
  }

  // How much each additional key part narrows the result set (heuristic fallback)
  double per_part = std::max(2.0, std::pow(static_cast<double>(stats.records), 1.0 / key_parts));

  for (uint j = 0; j < key_parts; j++) {
    ulong rpk; // records per key
    if ((is_primary || is_unique) && j == key_parts - 1) {
      // All parts specified on a UNIQUE/PK -> exactly 1 row
      rpk = 1;
    } else if (ndv != nullptr && (*ndv)[j] > 0) {
      // Real cardinality: avg rows per distinct prefix value. Integer CEIL
      // (records/NDV rounded up) so e.g. records=10,ndv=6 gives 2, not floor 1
      // which would over-state selectivity as ~unique (Codex review #3).
      const uint64_t rec = static_cast<uint64_t>(stats.records);
      const uint64_t d = (*ndv)[j];
      rpk = static_cast<ulong>(std::max<uint64_t>(1, (rec + d - 1) / d));
    } else {
      // per_part^(j+1) = total divisor for j+1 key parts
      double selectivity = std::pow(per_part, static_cast<double>(j + 1));
      rpk = static_cast<ulong>(std::max(1.0, static_cast<double>(stats.records) / selectivity));
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
  HTP_SCOPE(external_lock);

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
  HTP_SCOPE(start_stmt);
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
  LineairDBThdCtx *&ctx =
      *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, lineairdb_hton));
  if (ctx == nullptr)
    ctx = new LineairDBThdCtx();
  if (!ctx->proxy) {
    std::string host =
        srv_server_host ? srv_server_host : std::string("127.0.0.1");
    int port = static_cast<int>(srv_server_port);
    ctx->proxy = std::make_shared<LineairDBProxy>(host, port);
  }
  if (ctx->tx == nullptr) {
    ctx->tx =
        new LineairDBTransaction(thd, ctx->proxy.get(), lineairdb_hton, FENCE);
    // Phase-3e: oneshot no longer requires an app-supplied @_ldb_plan. When the
    // global toggle is on and the statement is a SELECT/UPDATE/DELETE we enter
    // oneshot mode; execute_oneshot_plan_if_present() then AUTO-GENERATES the
    // prefetch plan from the QEP (or uses @_ldb_plan as an override), and turns
    // oneshot back off if the plan cannot be fully modelled.
    const bool can_use_oneshot =
        (srv_oneshot_execution && thd_can_use_oneshot(thd));
    ctx->tx->set_oneshot_mode(can_use_oneshot);
  }
  if (ctx->tx->is_not_started()) {
    ctx->tx->begin_transaction();
    execute_oneshot_plan_if_present(thd, ctx->tx);
  }
  return ctx->tx;
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
  // NOTE: end_transaction() calls `delete this;` (lineairdb_transaction.cc
  // line 2271 / oneshot_validate_and_commit line 2132). ctx->tx is dangling
  // after the return; clear it. Adding `delete ctx->tx;` here is a
  // DOUBLE-FREE — verified to corrupt MySQL's TABLE_SHARE cache and crash
  // 2-3 statements later. The historical "30 GB RSS retained" is jemalloc
  // decay behaviour (memory returned to arenas, not to the kernel), not a
  // logic leak — tune with MALLOC_CONF=dirty_decay_ms:0 if release is needed.
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
  // end_transaction() self-deletes; ctx->tx is dangling. Do not delete again.
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
    // end_transaction() self-deletes; ctx->tx is dangling. Do not delete again.
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
    // Range conditions after equality prefix -> halve (NDB heuristic)
    if (eq_parts < key_parts_used) {
      estimate = std::max(static_cast<ha_rows>(1), estimate / 2);
    }
  } else {
    // No equality at all -> pure range scan.
    // Heuristic: two-sided range ~5%, one-sided ~10% (NDB fallback).
    if (min_key != nullptr && max_key != nullptr) {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 20);
    } else {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 10);
    }
  }

  // Floor: always return at least 1 row to avoid zero-cost estimates
  if (estimate < 1)
    estimate = 1;

  return estimate;
}

/**
 * Calculate how many key parts are covered by the given key length
 * This is an approximation based on key part sizes
 */
uint ha_lineairdb::calculate_key_parts_from_length(KEY *key, uint key_length) {
  if (key == nullptr || key_length == 0)
    return 0;

  uint parts = 0;
  uint accumulated_length = 0;

  for (uint i = 0; i < key->user_defined_key_parts; i++) {
    KEY_PART_INFO *part = &key->key_part[i];

    // Add length for this key part (including null byte if nullable)
    uint part_length = part->store_length;
    accumulated_length += part_length;

    if (accumulated_length <= key_length) {
      parts++;
    } else {
      break;
    }
  }

  return parts;
}

/**
  @brief
  create() is called to create a database. The variable name will have the
  name of the table.
  @see
  ha_create_table() in handle.cc
*/

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
  proxy->db_create_table(db_table_name);

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

  if (ha_alter_info->handler_flags & ~dominated_flags) {
    // Unsupported operation requested
    return HA_ALTER_INPLACE_NOT_SUPPORTED;
  }

  return HA_ALTER_INPLACE_EXCLUSIVE_LOCK;
}

bool ha_lineairdb::inplace_alter_table(TABLE *altered_table [[maybe_unused]],
                                       Alter_inplace_info *ha_alter_info,
                                       const dd::Table *old_table_def
                                       [[maybe_unused]],
                                       dd::Table *new_table_def
                                       [[maybe_unused]]) {
  DBUG_TRACE;

  userThread = ha_thd();
  auto proxy = get_proxy();

  for (uint i = 0; i < ha_alter_info->index_add_count; i++) {
    uint key_idx = ha_alter_info->index_add_buffer[i];
    KEY *key_info = &ha_alter_info->key_info_buffer[key_idx];

    uint index_type = (key_info->flags & HA_NOSAME) ? LDB_INDEX_UNIQUE : 0;

    // In a disaggregated setup, another MySQL node may have already created
    // this secondary index on the shared LineairDB server. Treat "already
    // exists" as success — the MySQL-side metadata still needs to be updated.
    proxy->db_create_secondary_index(
        db_table_name, std::string(key_info->name), index_type);
  }

  return false;
}

/**
 * @brief Advertise custom MRR for primary key lookups.
 *
 * When MySQL considers using MRR (e.g. for BKA joins), it calls this method
 * to ask the storage engine for cost estimates. We clear HA_MRR_USE_DEFAULT_IMPL
 * for PK lookups so that multi_range_read_init() receives our custom batch path,
 * which sends all keys in a single RPC instead of one RPC per key.
 */
ha_rows ha_lineairdb::multi_range_read_info_const(
    uint keyno, RANGE_SEQ_IF *seq, void *seq_init_param, uint n_ranges,
    uint *bufsz, uint *flags, bool *force_default_mrr, Cost_estimate *cost) {
  ha_rows rows = handler::multi_range_read_info_const(
      keyno, seq, seq_init_param, n_ranges, bufsz, flags, force_default_mrr,
      cost);
  if (rows == HA_POS_ERROR) return rows;

  // Use custom batch MRR for PK point lookups (BKA JOINs).
  // Range scans on secondary indexes must use the default path.
  // Set cost=1 since batch_read sends all keys in a single RPC.
  if (keyno == table->s->primary_key) {
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
  // Use custom batch MRR for PK point lookups (BKA JOINs).
  if (keyno == table->s->primary_key) {
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

  auto tx = get_transaction(ha_thd());
  if (!tx || tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }
  tx->choose_table(db_table_name);

  if (batch_keys.empty()) return 0;

  // Send all keys in a single batch RPC
  auto results = tx->batch_read(batch_keys);

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
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

unsigned char ha_lineairdb::key_part_type_tag(LineairDBFieldType type) {
  switch (type) {
  case LineairDBFieldType::LINEAIRDB_INT:
    return kKeyTypeInt;
  case LineairDBFieldType::LINEAIRDB_STRING:
    return kKeyTypeString;
  case LineairDBFieldType::LINEAIRDB_DATETIME:
    return kKeyTypeDatetime;
  case LineairDBFieldType::LINEAIRDB_OTHER:
  default:
    return kKeyTypeOther;
  }
}

void ha_lineairdb::append_key_part_encoding(std::string &out, bool is_null,
                                            LineairDBFieldType type,
                                            const std::string &payload) {
  constexpr size_t kLengthFieldSize = 2;
  const size_t max_payload_length = std::numeric_limits<uint16_t>::max();
  size_t copy_length = std::min(payload.size(), max_payload_length);

  if (payload.size() > max_payload_length) {
    std::cerr << "[LineairDB][encode_key_part] payload truncated: length="
              << payload.size() << std::endl;
  }

  out.reserve(out.size() + 5 + copy_length);
  out.push_back(
      static_cast<char>(is_null ? kKeyMarkerNull : kKeyMarkerNotNull));
  out.push_back(static_cast<char>(key_part_type_tag(type)));

  if (type == LineairDBFieldType::LINEAIRDB_STRING) {
    // STRING: payload first, then terminator (0x00), then length
    if (copy_length > 0) {
      out.append(payload.data(), copy_length);
    }
    out.push_back('\0'); // terminator to ensure shorter strings sort before
                         // longer ones with same prefix
    uint16_t length_field = static_cast<uint16_t>(copy_length);
    out.push_back(static_cast<char>((length_field >> 8) & 0xFF));
    out.push_back(static_cast<char>(length_field & 0xFF));
  } else {
    // INT, DATETIME, OTHER: length first, then payload (fixed-length types)
    uint16_t length_field = static_cast<uint16_t>(copy_length);
    out.push_back(static_cast<char>((length_field >> 8) & 0xFF));
    out.push_back(static_cast<char>(length_field & 0xFF));

    if (copy_length > 0) {
      out.append(payload.data(), copy_length);
    }
  }
}

/**
 * @brief Generate the end key of a prefix range (the next lexicographic key)
 *
 * By returning the next lexicographic key, all keys that start with the prefix
 * are covered precisely in the form [prefix, end).
 *
 * Example:
 *   prefix = 01 02 FF -> end = 01 03
 *
 * If all bytes are 0xFF, there is no valid next lexicographic key. In that
 * case we return an empty string as a sentinel for "no upper bound", which the
 * caller treats as an open-ended range (e.g., converted to std::nullopt).
 */
std::string ha_lineairdb::build_prefix_range_end(const std::string &prefix) {
  std::string end = prefix;
  for (size_t i = end.size(); i-- > 0;) {
    unsigned char byte = static_cast<unsigned char>(end[i]);
    if (byte != 0xFF) {
      end[i] = static_cast<char>(byte + 1);
      end.resize(i + 1);
      return end;
    }
  }
  // no upper bound
  return std::string();
}

/**
 * @brief Count the number of key parts used in a key_part_map
 *
 * @param key_info KEY structure containing key part information
 * @param keypart_map Bitmap indicating which key parts are used
 * @return Number of consecutive key parts used (from the beginning)
 */
uint ha_lineairdb::count_used_key_parts(const KEY *key_info,
                                        key_part_map keypart_map) {
  uint count = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    if ((keypart_map >> i) & 1)
      count++;
    else
      break;
  }
  return count;
}

/**
 * @brief Build search plan
 *
 * Decision steps:
 * 1. Reset state
 * 2. Extract basic information (used_key_parts, is_unique, has_nullable)
 * 3. Decide op
 * 4. Serialize boundaries
 */
void ha_lineairdb::build_search_plan(const uchar *key, key_part_map keypart_map,
                                     enum ha_rkey_function find_flag,
                                     KEY *key_info) {
  // 1. Reset state
  current_plan_.reset();
  reset_index_search_buffers();
  end_range_exclusive_key_.clear();

  // 2. Extract basic information
  current_plan_.is_primary = (active_index == table->s->primary_key);
  current_plan_.find_flag = find_flag;

  // HA_WHOLE_KEY support
  if (keypart_map == HA_WHOLE_KEY) {
    current_plan_.used_key_parts = key_info->user_defined_key_parts;
    current_plan_.all_parts_specified = true;
  } else {
    current_plan_.used_key_parts = count_used_key_parts(key_info, keypart_map);
    current_plan_.all_parts_specified =
        (current_plan_.used_key_parts == key_info->user_defined_key_parts);
  }

  // Unique check
  current_plan_.is_unique_index = (key_info->flags & HA_NOSAME) != 0;
  current_plan_.has_nullable_parts = (key_info->flags & HA_NULL_PART_KEY) != 0;

  // 3. Decide op
  if (key == nullptr) {
    current_plan_.op = IndexSearchOp::kIndexFirst;
  } else if (find_flag == HA_READ_KEY_EXACT &&
             current_plan_.all_parts_specified &&
             current_plan_.is_unique_index &&
             !current_plan_.has_nullable_parts) {
    current_plan_.op = IndexSearchOp::kUniquePoint;
  } else if (find_flag == HA_READ_KEY_EXACT) {
    current_plan_.op = IndexSearchOp::kSameKeyMaterialize;
  } else if (find_flag == HA_READ_PREFIX) {
    current_plan_.op = IndexSearchOp::kPrefixFirst;
  } else if (find_flag == HA_READ_PREFIX_LAST ||
             find_flag == HA_READ_PREFIX_LAST_OR_PREV) {
    if (find_flag == HA_READ_PREFIX_LAST_OR_PREV &&
        current_plan_.all_parts_specified) {
      current_plan_.op = IndexSearchOp::kPrevKey;
    } else {
      current_plan_.op = IndexSearchOp::kPrefixLast;
    }
  } else if (find_flag == HA_READ_KEY_OR_PREV ||
             find_flag == HA_READ_BEFORE_KEY) {
    current_plan_.op = IndexSearchOp::kPrevKey;
  } else {
    current_plan_.op = IndexSearchOp::kRangeMaterialize;
  }

  // 4. Serialize boundaries
  if (key != nullptr) {
    current_plan_.start_key_serialized =
        convert_key_to_ldbformat(key, keypart_map);

    // same group boundary (prefix operations)
    if (current_plan_.op == IndexSearchOp::kSameKeyMaterialize ||
        current_plan_.op == IndexSearchOp::kPrefixFirst ||
        current_plan_.op == IndexSearchOp::kPrefixLast) {
      current_plan_.same_group_prefix_serialized =
          current_plan_.start_key_serialized;
      current_plan_.same_group_end_serialized =
          build_prefix_range_end(current_plan_.start_key_serialized);
    }
  }

  // end_range processing
  if (end_range != nullptr) {
    current_plan_.end_key_serialized =
        convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

    if (end_range->flag != HA_READ_BEFORE_KEY) {
      // SQL inclusive upper bound must become LineairDB's exclusive upper
      // bound. This is required for both full keys (k <= 30) and partial-key
      // prefix ranges.
      current_plan_.end_key_serialized =
          build_prefix_range_end(current_plan_.end_key_serialized);
    }
  }
}

/**
 * @brief Execute search plan
 * @return 0: success, HA_ERR_*: error
 */
int ha_lineairdb::execute_plan(uchar *buf, LineairDBTransaction *tx) {
  switch (current_plan_.op) {
  case IndexSearchOp::kIndexFirst:
    return execute_index_first(buf, tx);
  case IndexSearchOp::kUniquePoint:
    return execute_unique_point(buf, tx);
  case IndexSearchOp::kSameKeyMaterialize:
    return execute_same_key_materialize(buf, tx);
  case IndexSearchOp::kPrefixFirst:
    return execute_prefix_first(buf, tx);
  case IndexSearchOp::kRangeMaterialize:
    return execute_range_materialize(buf, tx);
  case IndexSearchOp::kPrevKey:
    return execute_prev_key(buf, tx);
  case IndexSearchOp::kPrefixLast:
    return execute_prefix_last(buf, tx);
  default:
    return HA_ERR_WRONG_COMMAND;
  }
}

/**
 * @brief kIndexFirst: full scan when key==nullptr
 */
int ha_lineairdb::execute_index_first(uchar *buf, LineairDBTransaction *tx) {
  std::string start_key = "";
  std::string end_key = current_plan_.end_key_serialized.empty()
                            ? std::string(8, '\xFF')
                            : current_plan_.end_key_serialized;

  if (current_plan_.is_primary) {
    auto key_values = tx->get_matching_keys_and_values_in_range(
        start_key, end_key);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, start_key, end_key);
    batch_fetch_secondary_payloads(tx);
  }

  // phantom detection check
  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief kUniquePoint: exact match on unique index
 * @return HA_ERR_KEY_NOT_FOUND (no match), 0 (success)
 */
int ha_lineairdb::execute_unique_point(uchar *buf, LineairDBTransaction *tx) {
  if (current_plan_.is_primary) {
    auto result = tx->read(current_plan_.start_key_serialized);

    if (tx->is_aborted()) {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (result.first == nullptr || result.second == 0) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    if (set_fields_from_lineairdb(buf, result.first, result.second)) {
      tx->set_status_to_abort();
      return HA_ERR_OUT_OF_MEM;
    }

    // set state for index_next to return EOF
    secondary_index_results_.push_back(current_plan_.start_key_serialized);
    current_position_in_index_ = 1;
    last_fetched_primary_key_ = current_plan_.start_key_serialized;
    return 0;
  } else {
    // Secondary UNIQUE: read_secondary_index → read primary key
    // Proxy adaptation: read_secondary_index returns vector<string> (primary keys)
    auto primary_keys = tx->read_secondary_index(
        current_index_name, current_plan_.start_key_serialized);

    if (tx->is_aborted()) {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    for (auto &pk : primary_keys) {
      secondary_index_results_.push_back(pk);
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    batch_fetch_secondary_payloads(tx);
    return fetch_and_set_current_result(buf, tx);
  }
}

/**
 * @brief kSameKeyMaterialize: exact search (prefix match, non-unique, nullable
 * unique)
 */
int ha_lineairdb::execute_same_key_materialize(uchar *buf,
                                               LineairDBTransaction *tx) {
  const std::string &prefix = current_plan_.same_group_prefix_serialized;
  const std::string &prefix_end = current_plan_.same_group_end_serialized;

  if (current_plan_.is_primary) {
    // Push LIMIT only when the server can also apply the SELECT WHERE.
    const KEY *key = &table->key_info[active_index];
    const bool filter_ready = prepare_select_filter_for_tx(
        ha_thd(), table, tx, &pushed_filter_serialized_);
    const RangeScanLimit scan_limit = range_scan_limit_for_order(
        ha_thd(), key, current_plan_.used_key_parts,
        has_unpushed_filter_ || !filter_ready);

    // Same-key scans can use ASC or DESC LIMIT when ORDER BY matches the key.
    auto key_values = tx->get_matching_keys_and_values_in_range(
        prefix, prefix_end, static_cast<uint64_t>(scan_limit.row_limit),
        scan_limit.reverse_scan);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }

    if (tx->is_aborted()) {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    return fetch_and_set_current_result(buf, tx);
  }

  secondary_index_results_ = tx->get_matching_primary_keys_in_range(
      current_index_name, prefix, prefix_end);
  batch_fetch_secondary_payloads(tx);

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief kPrefixFirst: return first row matching prefix, then continue with
 * normal index_next
 */
int ha_lineairdb::execute_prefix_first(uchar *buf, LineairDBTransaction *tx) {
  const std::string &prefix = current_plan_.same_group_prefix_serialized;
  const std::string &prefix_end = current_plan_.same_group_end_serialized;

  if (current_plan_.is_primary) {
    // Restrict to [prefix, prefix_end) so index_next never leaks non-prefix
    // rows.
    auto key_values = tx->get_matching_keys_and_values_in_range(
        prefix, prefix_end);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }

    if (tx->is_aborted()) {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    return fetch_and_set_current_result(buf, tx);
  }

  // Restrict to [prefix, prefix_end) so index_next never leaks non-prefix
  // rows.
  secondary_index_results_ = tx->get_matching_primary_keys_in_range(
      current_index_name, prefix, prefix_end);
  batch_fetch_secondary_payloads(tx);

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief kRangeMaterialize: range search (AFTER_KEY, KEY_OR_NEXT, etc.)
 */
int ha_lineairdb::execute_range_materialize(uchar *buf,
                                            LineairDBTransaction *tx) {
  std::string effective_start = current_plan_.start_key_serialized;
  std::string effective_end = current_plan_.end_key_serialized;

  // adjust start key based on find_flag
  if (current_plan_.find_flag == HA_READ_AFTER_KEY) {
    effective_start.push_back('\x00'); // exclude start key
  }

  // execute scan
  if (current_plan_.is_primary) {
    // Push LIMIT only when the server can also apply the SELECT WHERE.
    const KEY *key = &table->key_info[active_index];
    const bool filter_ready = prepare_select_filter_for_tx(
        ha_thd(), table, tx, &pushed_filter_serialized_);
    const RangeScanLimit scan_limit = range_scan_limit_for_order(
        ha_thd(), key, current_plan_.used_key_parts,
        has_unpushed_filter_ || !filter_ready);

    // Range scans can use ASC or DESC LIMIT when ORDER BY matches the key.
    auto key_values = tx->get_matching_keys_and_values_in_range(
        effective_start, effective_end,
        static_cast<uint64_t>(scan_limit.row_limit), scan_limit.reverse_scan);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, effective_start, effective_end);
    batch_fetch_secondary_payloads(tx);
  }

  // phantom detection check
  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief kPrevKey: read key or previous key (HA_READ_KEY_OR_PREV /
 * HA_READ_BEFORE_KEY)
 */
int ha_lineairdb::execute_prev_key(uchar *buf, LineairDBTransaction *tx) {
  const std::string &target_key = current_plan_.start_key_serialized;
  // HA_READ_BEFORE_KEY : SQL < target → already exclusive end.
  // HA_READ_KEY_OR_PREV: SQL <= target → convert via build_prefix_range_end so
  //                      [begin, end) scan keeps target itself.
  const bool exclude_target = (current_plan_.find_flag == HA_READ_BEFORE_KEY);
  const std::string effective_end =
      exclude_target ? target_key : build_prefix_range_end(target_key);

  if (current_plan_.is_primary) {
    auto key_values =
        tx->get_matching_keys_and_values_in_range("", effective_end);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, "", effective_end);
    batch_fetch_secondary_payloads(tx);
  }

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  current_position_in_index_ = secondary_index_results_.size() - 1;
  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief kPrefixLast: last row in prefix range
 * @note Materialize mode returns the last row directly (slow but correct)
 */
int ha_lineairdb::execute_prefix_last(uchar *buf, LineairDBTransaction *tx) {
  if (current_plan_.find_flag == HA_READ_PREFIX_LAST_OR_PREV) {
    const std::string &prefix = current_plan_.same_group_prefix_serialized;
    const std::string &prefix_end = current_plan_.same_group_end_serialized;

    if (current_plan_.is_primary) {
      // Push LIMIT only when the server can also apply the SELECT WHERE.
      const KEY *key = &table->key_info[active_index];
      const bool filter_ready = prepare_select_filter_for_tx(
          ha_thd(), table, tx, &pushed_filter_serialized_);
      const RangeScanLimit scan_limit = range_scan_limit_for_order(
          ha_thd(), key, current_plan_.used_key_parts,
          has_unpushed_filter_ || !filter_ready);
      const bool push_desc_limit =
          (scan_limit.row_limit == 1 && scan_limit.reverse_scan);

      // Prefix-last can use reverse scan for ORDER BY ... DESC LIMIT 1.
      auto key_values = tx->get_matching_keys_and_values_in_range(
          prefix, prefix_end,
          push_desc_limit ? static_cast<uint64_t>(scan_limit.row_limit) : 0,
          push_desc_limit);
      if (key_values.empty()) {
        // HA_READ_PREFIX_LAST_OR_PREV may fall back to the previous prefix.
        key_values = tx->get_matching_keys_and_values_in_range(
            "", prefix,
            push_desc_limit ? static_cast<uint64_t>(scan_limit.row_limit) : 0,
            push_desc_limit);
      }
      for (auto &kv : key_values) {
        secondary_index_results_.push_back(kv.first);
        secondary_index_payloads_.push_back(std::move(kv.second));
      }
    } else {
      secondary_index_results_ = tx->get_matching_primary_keys_in_range(
          current_index_name, prefix, prefix_end);
      if (secondary_index_results_.empty()) {
        secondary_index_results_ = tx->get_matching_primary_keys_in_range(
            current_index_name, "", prefix);
      }
      batch_fetch_secondary_payloads(tx);
    }

    if (tx->is_aborted()) {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_END_OF_FILE;
    }

    current_position_in_index_ = secondary_index_results_.size() - 1;
    return fetch_and_set_current_result(buf, tx);
  }

  // materialize mode
  if (current_plan_.is_primary) {
    // Push LIMIT only when the server can also apply the SELECT WHERE.
    const KEY *key = &table->key_info[active_index];
    const bool filter_ready = prepare_select_filter_for_tx(
        ha_thd(), table, tx, &pushed_filter_serialized_);
    const RangeScanLimit scan_limit = range_scan_limit_for_order(
        ha_thd(), key, current_plan_.used_key_parts,
        has_unpushed_filter_ || !filter_ready);
    const bool push_desc_limit =
        (scan_limit.row_limit == 1 && scan_limit.reverse_scan);

    // Prefix-last materialization only uses DESC LIMIT 1.
    auto key_values = tx->get_matching_keys_and_values_in_range(
        current_plan_.same_group_prefix_serialized,
        current_plan_.same_group_end_serialized,
        push_desc_limit ? static_cast<uint64_t>(scan_limit.row_limit) : 0,
        push_desc_limit);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, current_plan_.same_group_prefix_serialized,
        current_plan_.same_group_end_serialized);
    batch_fetch_secondary_payloads(tx);
  }

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  // get the last element
  current_position_in_index_ = secondary_index_results_.size() - 1;
  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief Pre-fetch all row data for a secondary index scan result in one RPC.
 *
 * A secondary index query is a two-step process:
 *   1) Index scan → returns a list of primary keys (secondary_index_results_)
 *   2) Row fetch  → read each row by primary key
 * Without this, step 2 would issue one READ RPC per row (N rows = N RPCs).
 * This method does step 2 in bulk: it sends all primary keys in a single
 * batch_read RPC and stores the results in secondary_index_payloads_.
 * When fetch_and_set_current_result() later returns rows one by one,
 * the data is already in memory — no further RPCs needed.
 */
void ha_lineairdb::batch_fetch_secondary_payloads(LineairDBTransaction *tx) {
  if (secondary_index_results_.empty()) return;

  auto results = tx->batch_read(secondary_index_results_);

  secondary_index_payloads_.clear();
  secondary_index_payloads_.reserve(results.size());
  for (auto &r : results) {
    secondary_index_payloads_.push_back(
        r.first ? std::move(r.second) : std::string());
  }
}

/**
 * @brief Fetch and set the current result from secondary_index_results_
 *
 * This helper function reads the primary key at current_position_in_index_,
 * fetches the data from LineairDB, and sets the fields in the buffer.
 *
 * @param buf Buffer to store the result
 * @param tx Transaction object
 * @return 0 on success, error code on failure
 */
int ha_lineairdb::fetch_and_set_current_result(uchar *buf,
                                               LineairDBTransaction *tx) {
  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  std::string primary_key =
      secondary_index_results_[current_position_in_index_];

  tx->choose_table(db_table_name);

  const bool has_inline_value =
      current_position_in_index_ < secondary_index_payloads_.size();
  const std::byte *value_ptr = nullptr;
  size_t value_size = 0;

  if (has_inline_value) {
    const std::string &inline_value =
        secondary_index_payloads_[current_position_in_index_];
    value_ptr = reinterpret_cast<const std::byte *>(inline_value.data());
    value_size = inline_value.size();
  } else {
    auto result = tx->read(primary_key);
    if (tx->is_aborted()) {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }
    if (result.first == nullptr || result.second == 0) {
      return HA_ERR_KEY_NOT_FOUND;
    }
    value_ptr = result.first;
    value_size = result.second;
  }

  if (set_fields_from_lineairdb(buf, value_ptr, value_size)) {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }

  current_position_in_index_++;
  last_fetched_primary_key_ = primary_key;
  return 0;
}

/**
 * @brief Serialize a single field value to LineairDB key format
 *
 * This helper function converts a MySQL Field to LineairDB's sortable key
 * format based on its type. This eliminates code duplication across different
 * key handling functions.
 *
 * @param field MySQL Field object
 * @return Serialized key string
 */
std::string ha_lineairdb::serialize_key_from_field(Field *field) {
  const bool is_null = field->is_null();
  enum_field_types mysql_type = field->type();
  LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

  std::string payload;

  if (!is_null) {
    switch (ldb_type) {
    case LineairDBFieldType::LINEAIRDB_INT: {
      int64_t value = field->val_int();
      size_t field_len = field->pack_length();

      uchar buf[8] = {0};
      if (field_len == 1) {
        buf[0] = static_cast<uchar>(value & 0xFF);
      } else if (field_len == 2) {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
      } else if (field_len == 4) {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
        buf[2] = static_cast<uchar>((value >> 16) & 0xFF);
        buf[3] = static_cast<uchar>((value >> 24) & 0xFF);
      } else {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
        buf[2] = static_cast<uchar>((value >> 16) & 0xFF);
        buf[3] = static_cast<uchar>((value >> 24) & 0xFF);
        buf[4] = static_cast<uchar>((value >> 32) & 0xFF);
        buf[5] = static_cast<uchar>((value >> 40) & 0xFF);
        buf[6] = static_cast<uchar>((value >> 48) & 0xFF);
        buf[7] = static_cast<uchar>((value >> 56) & 0xFF);
        field_len = 8;
      }

      payload = encode_int_key(buf, field_len);
      break;
    }

    case LineairDBFieldType::LINEAIRDB_DATETIME: {
      size_t field_len = field->pack_length();
      std::string raw(field_len, '\0');
      field->get_key_image(reinterpret_cast<uchar *>(raw.data()), field_len,
                           Field::itRAW);
      payload = encode_datetime_key(reinterpret_cast<const uchar *>(raw.data()),
                                    field_len, mysql_type);
      break;
    }

    case LineairDBFieldType::LINEAIRDB_STRING: {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }

    case LineairDBFieldType::LINEAIRDB_OTHER:
    default: {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }
    }
  }

  std::string encoded;
  append_key_part_encoding(encoded, is_null, ldb_type, payload);
  return encoded;
}

std::string ha_lineairdb::build_secondary_key_from_row(const uchar *row_buffer,
                                                       const KEY &key_info) {
  // Temporarily set read_set to include all columns
  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);

  // Calculate the offset between row_buffer and record[0]
  ptrdiff_t offset = row_buffer - table->record[0];

  // Construct the secondary key
  std::string secondary_key;
  for (uint part_idx = 0; part_idx < key_info.user_defined_key_parts;
       part_idx++) {
    auto key_part = key_info.key_part[part_idx];
    Field *field = table->field[key_part.fieldnr - 1];

    // Adjust the Field pointer to match row_buffer
    field->move_field_offset(offset);

    // Serialize each key part and concatenate
    secondary_key += serialize_key_from_field(field);

    // Restore the Field pointer back to original position
    field->move_field_offset(-offset);
  }

  // Restore the original read_set
  tmp_restore_column_map(table->read_set, org_bitmap);

  return secondary_key;
}

void ha_lineairdb::store_primary_key_in_ref(const std::string &primary_key) {
  if (table == nullptr || table->s == nullptr || ref == nullptr) {
    return;
  }

  const size_t ref_length_local = ref_length;
  if (ref_length_local < sizeof(uint16_t)) {
    return;
  }

  if (primary_key.size() > std::numeric_limits<uint16_t>::max()) {
    std::cerr << "[LineairDB][position] primary key length exceeds uint16_t: "
              << primary_key.size() << std::endl;
    return;
  }

  const size_t payload_capacity = ref_length_local - sizeof(uint16_t);
  if (primary_key.size() > payload_capacity) {
    std::cerr
        << "[LineairDB][position] primary key length exceeds ref capacity: "
        << primary_key.size() << " > " << payload_capacity << std::endl;
    return;
  }

  const uint16_t key_length = static_cast<uint16_t>(primary_key.size());
  std::memcpy(ref, &key_length, sizeof(uint16_t));

  if (key_length > 0) {
    std::memcpy(ref + sizeof(uint16_t), primary_key.data(), key_length);
  }

  const size_t remaining = payload_capacity - key_length;
  if (remaining > 0) {
    std::memset(ref + sizeof(uint16_t) + key_length, 0, remaining);
  }
}

std::string ha_lineairdb::extract_primary_key_from_ref(const uchar *pos) const {
  if (pos == nullptr || table == nullptr || table->s == nullptr) {
    return {};
  }

  const size_t ref_length_local = ref_length;
  if (ref_length_local < sizeof(uint16_t)) {
    return {};
  }

  uint16_t key_length = 0;
  std::memcpy(&key_length, pos, sizeof(uint16_t));

  if (key_length == 0) {
    return {};
  }

  if (sizeof(uint16_t) + key_length > ref_length_local) {
    return {};
  }

  std::string key(reinterpret_cast<const char *>(pos + sizeof(uint16_t)),
                  key_length);

  return key;
}

bool ha_lineairdb::uses_hidden_primary_key() const {
  if (table == nullptr || table->s == nullptr) {
    return false;
  }
  return table->s->primary_key == MAX_KEY;
}

std::string ha_lineairdb::serialize_hidden_primary_key(uint64_t row_id) const {
  std::ostringstream oss;
  oss << std::hex << std::setw(16) << std::setfill('0') << row_id;
  return oss.str();
}

std::string ha_lineairdb::generate_hidden_primary_key() {
  if (share == nullptr) {
    share = get_share();
  }
  uint64_t row_id =
      share->next_hidden_pk.fetch_add(1, std::memory_order_relaxed);
  std::string key = serialize_hidden_primary_key(row_id);
  return key;
}

std::string ha_lineairdb::extract_key(const uchar *buf) {
  if (is_primary_key_exists()) {
    return extract_key_from_mysql(buf);
  } else {
    return autogenerate_key();
  }
}

std::string ha_lineairdb::extract_key_from_mysql(const uchar *row_buffer) {
  std::string complete_key;

  // Guard: return empty if no explicit primary key exists
  if (!is_primary_key_exists() || key_part == nullptr || num_key_parts == 0) {
    return complete_key;
  }

  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);
  ptrdiff_t offset = row_buffer - table->record[0];

  for (size_t i = 0; i < num_key_parts; ++i) {
    auto field_index = key_part[i].fieldnr - 1;
    Field *field = table->field[field_index];

    field->move_field_offset(offset);
    complete_key += serialize_key_from_field(field);
    field->move_field_offset(-offset);
  }

  tmp_restore_column_map(table->read_set, org_bitmap);

  return complete_key;
}

std::string ha_lineairdb::autogenerate_key() {
  return generate_hidden_primary_key();
}

/**
 * @brief Encode INT key from MySQL format to LineairDB sortable format
 *
 * Converts little-endian integer to big-endian with sign bit flipped.
 * This ensures correct lexicographic ordering: negative < 0 < positive
 *
 * @param data MySQL key data (little-endian)
 * @param len Key length (1, 2, 4, or 8 bytes)
 * @return Big-endian binary string with sign bit flipped
 */
std::string ha_lineairdb::encode_int_key(const uchar *data, size_t len) {
  uint64_t value = 0;

  if (len == 1) {
    value = static_cast<uint8_t>(data[0]);
  } else if (len == 2) {
    value =
        static_cast<uint16_t>(data[0]) | (static_cast<uint16_t>(data[1]) << 8);
  } else if (len == 4) {
    value = static_cast<uint32_t>(data[0]) |
            (static_cast<uint32_t>(data[1]) << 8) |
            (static_cast<uint32_t>(data[2]) << 16) |
            (static_cast<uint32_t>(data[3]) << 24);
  } else if (len == 8) {
    value = static_cast<uint64_t>(data[0]) |
            (static_cast<uint64_t>(data[1]) << 8) |
            (static_cast<uint64_t>(data[2]) << 16) |
            (static_cast<uint64_t>(data[3]) << 24) |
            (static_cast<uint64_t>(data[4]) << 32) |
            (static_cast<uint64_t>(data[5]) << 40) |
            (static_cast<uint64_t>(data[6]) << 48) |
            (static_cast<uint64_t>(data[7]) << 56);
  } else {
    // Unsupported length
    return std::string();
  }

  // Flip sign bit for correct sorting
  // This makes: negative numbers < 0 < positive numbers
  if (len == 1) {
    value ^= 0x80ULL;
  } else if (len == 2) {
    value ^= 0x8000ULL;
  } else if (len == 4) {
    value ^= 0x80000000ULL;
  } else if (len == 8) {
    value ^= 0x8000000000000000ULL;
  }

  // Convert to big-endian
  char buf[8];
  size_t output_len = len;
  for (size_t i = 0; i < output_len; i++) {
    buf[i] = static_cast<char>((value >> ((output_len - 1 - i) * 8)) & 0xFF);
  }

  return std::string(buf, output_len);
}

/**
 * @brief Encode DATETIME key from MySQL format to LineairDB format
 *
 * MYSQL_TYPE_DATE / MYSQL_TYPE_NEWDATE: 3 bytes stored in little-endian.
 * Must be converted to big-endian for correct lexicographic sorting.
 * DATETIME2, TIMESTAMP2, TIME2: already in big-endian sortable format.
 */
std::string ha_lineairdb::encode_datetime_key(const uchar *data, size_t len,
                                              enum_field_types mysql_type) {
  if (mysql_type == MYSQL_TYPE_DATE || mysql_type == MYSQL_TYPE_NEWDATE) {
    char buf[3];
    buf[0] = static_cast<char>(data[2]);
    buf[1] = static_cast<char>(data[1]);
    buf[2] = static_cast<char>(data[0]);
    return std::string(buf, 3);
  }
  return std::string(reinterpret_cast<const char *>(data), len);
}

/**
 * @brief Encode VARCHAR key from MySQL format to LineairDB format
 *
 * MySQL stores VARCHAR keys with a 2-byte length prefix (little-endian).
 * We extract the actual string data without padding.
 *
 * @param data MySQL VARCHAR key data (length prefix + string + padding)
 * @param len Total key length
 * @return Actual string data without prefix or padding
 */
std::string ha_lineairdb::encode_string_key(const uchar *data, size_t len) {
  if (len < 2)
    return std::string();

  // First 2 bytes are length (little-endian)
  uint16_t str_len =
      static_cast<uint16_t>(data[0]) | (static_cast<uint16_t>(data[1]) << 8);

  if (str_len == 0 || len < 2 + str_len) {
    // Invalid or empty string
    return std::string();
  }

  // Return actual string data (skip 2-byte prefix, exclude padding)
  return std::string(reinterpret_cast<const char *>(data + 2), str_len);
}

/**
 * @brief Convert MySQL binary composite key format to LineairDB sortable key
 * format
 *
 * This function handles composite keys by processing each key part
 * sequentially:
 * - Reads key_part_map to determine which parts are used
 * - Converts each part to sortable format based on its type
 * - Concatenates all parts into a single sortable string
 *
 * Key formats by type:
 * - INT: Little-endian to big-endian + sign bit flip (for correct sorting)
 * - DATETIME: Pass through as-is (already sortable)
 * - STRING (VARCHAR): Extract actual data (remove length prefix and padding)
 *
 * @param key MySQL binary key data (concatenated byte array)
 * @param keypart_map Bitmap indicating which key parts are used
 * @return LineairDB formatted key string (concatenated sortable format)
 */
std::string ha_lineairdb::convert_key_to_ldbformat(const uchar *key,
                                                   key_part_map keypart_map) {
  KEY *key_info = &table->key_info[active_index];
  std::string result;
  const uchar *key_ptr = key;

  // Process each key part sequentially
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    // Check if this key part is used in the query
    if (!((keypart_map >> i) & 1)) {
      break; // Remaining parts are not used (prefix scan)
    }

    KEY_PART_INFO *kp = &key_info->key_part[i];
    Field *field = kp->field;
    bool is_null = false;
    if (kp->null_bit) {
      is_null = (*key_ptr != 0);
      key_ptr++; // Skip NULL flag byte

      if (is_null) {
        key_ptr += (kp->store_length - 1);
        append_key_part_encoding(result, true,
                                 convert_mysql_type_to_lineairdb(field->type()),
                                 std::string());
        continue;
      }
    }

    uint data_len = kp->length;
    const uchar *data_ptr = key_ptr;

    if (kp->key_part_flag & HA_VAR_LENGTH_PART) {
      data_len = uint2korr(data_ptr);
      data_ptr += 2; // Skip length prefix
      key_ptr = data_ptr;
    }

    enum_field_types mysql_type = field->type();
    LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

    std::string payload;
    switch (ldb_type) {
    case LineairDBFieldType::LINEAIRDB_INT:
      payload = encode_int_key(data_ptr, data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_DATETIME:
      payload = encode_datetime_key(data_ptr, data_len, mysql_type);
      break;

    case LineairDBFieldType::LINEAIRDB_STRING:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_OTHER:
    default:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;
    }

    append_key_part_encoding(result, false, ldb_type, payload);

    if (kp->key_part_flag & HA_VAR_LENGTH_PART) {
      key_ptr += kp->length;
    } else {
      key_ptr += kp->length;
    }
  }

  return result;
}

/**
 * @brief This function only extracts the type of key for
 *        tables that have single key
 *
 * @return bytes Key type is int
 * @return 0 Key type is not int
 */
bool ha_lineairdb::is_primary_key_type_int() {
  ha_base_keytype integer_types[] = {
      HA_KEYTYPE_SHORT_INT, HA_KEYTYPE_USHORT_INT, HA_KEYTYPE_LONG_INT,
      HA_KEYTYPE_ULONG_INT, HA_KEYTYPE_LONGLONG,   HA_KEYTYPE_ULONGLONG,
      HA_KEYTYPE_INT24,     HA_KEYTYPE_UINT24,     HA_KEYTYPE_INT8};
  assert(table->s->keys == 1);
  ha_base_keytype key_type = primary_key_type;
  return std::find(std::begin(integer_types), std::end(integer_types),
                   key_type) != std::end(integer_types);
}

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
  HTP_SCOPE(set_fields);  // nested inside rnd_next/rnd_pos/fetch_* timings
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

  // Projection pushdown: if this table's cached VALUES were trimmed to a subset
  // of columns, the parsed row holds only the kept columns in order; map the
  // k-th present column to field index kept[k]. Non-kept fields are left
  // untouched (MySQL won't read columns outside read_set). null flags are kept
  // FULL, so is_null_in_record(buf) is correct per field.
  const std::vector<uint32_t> *kept = nullptr;
  {
    auto tx = get_transaction(ha_thd());
    if (tx != nullptr) kept = tx->table_projection(db_table_name);
  }
  // Use the projected mapping ONLY when the value actually has the projected
  // column count. This self-corrects against any table-name/key mismatch or a
  // full row that slipped into a projected table (decode it positionally as
  // full): projected value has kept->size() columns, a full value has s->fields.
  if (kept != nullptr && ldbField.get_row_size() != kept->size()) {
    kept = nullptr;
  }
  // Step4a fix (Codex safety review 2026-05-29): skipping non-read_set columns
  // is sound ONLY for a pure SELECT serve. DML (UPDATE/DELETE) reuses this row
  // buffer: update_row/delete_row rebuild the old row + ALL secondary keys from
  // every column, and the engine does NOT advertise HA_PARTIAL_COLUMN_READ, so
  // MySQL may leave secondary-key/untouched columns out of read_set. Skipping
  // them then corrupts secondary indexes / the base row. For any non-SELECT
  // statement, materialize the full row (original behavior). TPC-H is all
  // SELECT, so the set_fields win is preserved where it matters.
  THD *const thd_for_serve = ha_thd();
  const bool select_serve =
      thd_for_serve != nullptr && thd_for_serve->lex != nullptr &&
      thd_for_serve->lex->sql_command == SQLCOM_SELECT;
  // Step4a: skip Field::store for columns MySQL won't read this statement
  // (not in table->read_set). set_fields is the per-serve chokepoint (Q21 SF1:
  // 13.1M calls, 5.9s) and a full lineitem row is ~16 columns while a query
  // typically reads ~4. The projection path already relies on "non-read columns
  // are safe to leave untouched"; this extends the same guard to every column.
  // null flags for all columns are copied into buf above, so skipped columns
  // stay null-consistent. Per-serve read_set is the right mask even for
  // self-joins: each alias's handler reads only its own columns (the prefetch
  // already widened the cached value to the union across aliases). Text->binary
  // re-parse (Field::store) only happens for columns actually consumed.
  if (kept != nullptr) {
    for (size_t k = 0; k < kept->size(); ++k) {
      const uint32_t fi = (*kept)[k];
      if (fi >= table->s->fields) break;  // safety: malformed projection
      if (select_serve && !bitmap_is_set(table->read_set, fi))
        continue;  // pure SELECT: skip store for columns MySQL won't read
      Field *f = table->field[fi];
      const auto mysqlFieldValue = ldbField.get_column_of_row(k);
      if (f->is_nullable() && f->is_null_in_record(buf)) {
        f->set_null();
      } else {
        f->store(mysqlFieldValue.data(), mysqlFieldValue.size(), &my_charset_bin,
                 CHECK_FIELD_WARN);
        Field *arr[2] = {f, nullptr};
        if (store_blob_to_field(arr)) return HA_ERR_OUT_OF_MEM;
      }
    }
  } else {
    /**
     * store each column value to corresponding field (full row)
     */
    size_t columnIndex = 0;
    for (Field **field = table->field; *field; field++) {
      // Advance the positional column index for EVERY column (the full-row
      // value holds all columns in order), then skip the store for columns not
      // in read_set.
      const auto mysqlFieldValue = ldbField.get_column_of_row(columnIndex++);
      if (select_serve &&
          !bitmap_is_set(table->read_set, (*field)->field_index()))
        continue;
      if ((*field)->is_nullable() && (*field)->is_null_in_record(buf)) {
        (*field)->set_null();
      } else {
        (*field)->store(mysqlFieldValue.data(), mysqlFieldValue.size(),
                        &my_charset_bin, CHECK_FIELD_WARN);
        if (store_blob_to_field(field))
          return HA_ERR_OUT_OF_MEM;
      }
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
static MYSQL_SYSVAR_BOOL(oneshot_execution, srv_oneshot_execution,
                         PLUGIN_VAR_OPCMDARG,
                         "Enable experimental one-shot execution.", nullptr,
                         nullptr, false);

static SYS_VAR *lineairdb_system_variables[] = {
    MYSQL_SYSVAR(server_host),
    MYSQL_SYSVAR(server_port),
    MYSQL_SYSVAR(oneshot_execution),
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
} mysql_declare_plugin_end;
