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
#include "helios_gate.hh"
#include "lineairdb_keyenc.hh"

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
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "lineairdb_field_types.h"
#include "lineairdb_prefetch.hh"
#include "lineairdb_pushdown.hh"
#include "lineairdb.pb.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/key.h"  // key_restore (Phase-22 D1 trailing-range decode)
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/item_subselect.h"               // Item_in_subselect (Phase-16)
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "sql/sql_optimizer.h"             // JOIN
#include "sql/join_optimizer/access_path.h"  // AccessPath (QEP tree)
#include "sql/join_optimizer/walk_access_paths.h"  // WalkAccessPaths (Phase-22 D2)
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
  // Opt-in (default-OFF): the OLTP-catastrophic feature, enabled only by an
  // explicit HELIOS_AGG_PUSHDOWN=1 (or true/on/yes). Previously presence-based,
  // which wrongly treated `=0` as enabled (Phase-22 / Codex review #4).
  static const bool on = helios::gate_default_off("HELIOS_AGG_PUSHDOWN");
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

// HAVING support (Phase-16, q18's `HAVING SUM(l_quantity) > 300` class): a
// single comparison of one BARE aggregate against one constant. The executor
// computes the aggregate per group (an extra non-emitted accumulator slot)
// and filters groups before emission — in BOTH phases, so correctness never
// depends on which phase runs.
namespace {
struct HeliosHavingDesc {
  Item_sum *sum = nullptr;  // bare aggregate (nullptr = no HAVING)
  HeliosAggKind kind = HK_COUNT;
  Item *arg = nullptr;                 // aggregate argument (nullptr=COUNT*)
  Item_result rtype = INT_RESULT;      // accumulation type (DECIMAL/INT only)
  Item_func::Functype op = Item_func::EQ_FUNC;  // comparison, sum on the LEFT
  Item *cnst = nullptr;                // constant side
};
}  // namespace

// Validate one bare aggregate under the same rules helios_plan_outputs applies
// to SELECT outputs. REAL aggregates are rejected here (HAVING compares via
// exact my_decimal; double would drift).
static bool helios_classify_having_sum(Item_sum *s, HeliosHavingDesc *out) {
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
    case Item_sum::SUM_FUNC: out->kind = HK_SUM; break;
    case Item_sum::AVG_FUNC: out->kind = HK_AVG; break;
    default: return false;
  }
  out->rtype = s->result_type();
  if (out->kind != HK_COUNT && out->rtype != DECIMAL_RESULT &&
      out->rtype != INT_RESULT)
    return false;  // REAL / STRING aggregates: not exactly comparable
  out->arg = (s->argument_count() > 0) ? s->arguments()[0] : nullptr;
  if (out->arg != nullptr &&
      (out->arg->has_subquery() || out->arg->has_aggregation() ||
       out->arg->is_non_deterministic()))
    return false;
  out->sum = s;
  return true;
}

// Parse qb->having_cond(). Returns true with out->sum==nullptr when there is
// no HAVING; false when a HAVING exists but is not the supported shape.
static bool helios_parse_having(Query_block *qb, HeliosHavingDesc *out) {
  *out = HeliosHavingDesc();
  Item *h = qb->having_cond();
  if (h == nullptr) return true;
  // Strip the speculative IN->EXISTS conjuncts (marked created_by_in2exists):
  // they implement the per-outer-row EXISTS strategy and are NOT part of the
  // materialized subquery this override replaces (the offload whitelist only
  // admits IN units whose strategy is SUBQ_MATERIALIZATION). q18's inner
  // HAVING arrives as (sum(l_quantity) > 300) AND (<cache>(o_orderkey) =
  // <ref_null_helper>(l_orderkey)) — only the first conjunct is the user's.
  if (h->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(h)->functype() == Item_func::COND_AND_FUNC) {
    Item *user_having = nullptr;
    for (Item &c : *down_cast<Item_cond *>(h)->argument_list()) {
      if (c.created_by_in2exists()) continue;
      if (user_having != nullptr) return false;  // >1 user conjunct
      user_having = &c;
    }
    if (user_having == nullptr) return true;  // injected-only: no user HAVING
    h = user_having;
  } else if (h->created_by_in2exists()) {
    return true;  // pure injected HAVING; the user wrote none
  }
  if (h->type() != Item::FUNC_ITEM) return false;
  Item_func *f = down_cast<Item_func *>(h);
  Item_func::Functype op = f->functype();
  switch (op) {
    case Item_func::EQ_FUNC: case Item_func::NE_FUNC:
    case Item_func::LT_FUNC: case Item_func::LE_FUNC:
    case Item_func::GT_FUNC: case Item_func::GE_FUNC: break;
    default: return false;
  }
  if (f->argument_count() != 2) return false;
  Item *a = f->arguments()[0]->real_item();  // unwrap Item_aggregate_ref
  Item *b = f->arguments()[1]->real_item();
  Item *sum_side = nullptr, *const_side = nullptr;
  bool sum_on_left = false;
  if (a->type() == Item::SUM_FUNC_ITEM && b->const_item()) {
    sum_side = a; const_side = b; sum_on_left = true;
  } else if (b->type() == Item::SUM_FUNC_ITEM && a->const_item()) {
    sum_side = b; const_side = a; sum_on_left = false;
  } else {
    return false;
  }
  if (const_side->has_subquery() || const_side->is_non_deterministic())
    return false;
  const Item_result crt = const_side->result_type();
  if (crt != INT_RESULT && crt != DECIMAL_RESULT) return false;
  if (!helios_classify_having_sum(down_cast<Item_sum *>(sum_side), out))
    return false;
  // Normalize: sum on the LEFT of the comparison.
  if (sum_on_left) {
    out->op = op;
  } else {
    switch (op) {
      case Item_func::LT_FUNC: out->op = Item_func::GT_FUNC; break;
      case Item_func::LE_FUNC: out->op = Item_func::GE_FUNC; break;
      case Item_func::GT_FUNC: out->op = Item_func::LT_FUNC; break;
      case Item_func::GE_FUNC: out->op = Item_func::LE_FUNC; break;
      default: out->op = op; break;  // EQ/NE symmetric
    }
  }
  out->cnst = const_side;
  return true;
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
  // No UNION / set operations: the enclosing query expression must be a plain
  // single query block (is_simple lives on Query_expression, not Query_block).
  Query_expression *qe = qb->master_query_expression();
  if (qe == nullptr || !qe->is_simple()) return false;
  // Top-level blocks are taken over via Query_expression::
  // ExecuteIteratorQuery (sql_union.cc:1711). An INNER unit's JOIN may be
  // taken over too (Phase-16) — its rows are stored by the MaterializeIterator
  // hook — but ONLY when the unit is UNCORRELATED and executed once
  // (uncacheable==0): a correlated subquery (TPC-H q17/q20's per-outer-row
  // aggregate) re-executes per outer row with outer references our executor
  // cannot see, and in_to_exists rewrites mark the unit uncacheable too.
  const bool inner_unit = qb->outer_query_block() != nullptr;
  static const bool aggdbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
  if (inner_unit && qe->uncacheable != 0) {
    if (aggdbg)
      std::fprintf(stderr, "[AGGPD] inner reject: uncacheable=%x\n",
                   (unsigned)qe->uncacheable);
    return false;
  }
  // MySQL-UNMODIFIED constraint: an inner unit may be hijacked ONLY when it
  // executes through Query_expression::execute -> ExecuteIteratorQuery, whose
  // override_executor_func bypass is IN-TREE MySQL (sql_union.cc:1711). That
  // is the uncorrelated SCALAR subquery path (SubqueryWithResult::exec).
  // Derived tables and materialized IN consume their unit through
  // MaterializeIterator, which has no in-tree hook — installing the override
  // there would stage group rows nobody consumes (cache-miss abort). Never
  // hijack those.
  if (inner_unit) {
    if (qe->item == nullptr ||
        qe->item->substype() != Item_subselect::SINGLEROW_SUBS) {
      if (aggdbg)
        std::fprintf(stderr, "[AGGPD] inner reject: not a scalar unit\n");
      return false;
    }
  }
  if (qb->leaf_table_count != 1) return false;     // exactly one base table
  if (!qb->is_grouped()) return false;             // GROUP BY or aggregate funcs
  if (qb->is_distinct()) return false;             // DISTINCT not handled
  if (qb->has_limit()) return false;               // LIMIT/OFFSET handled later
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) return false;  // no ROLLUP
  if (qb->has_windows()) return false;                  // no window functions
  // HAVING: one bare-aggregate-vs-constant comparison (q18's
  // SUM(l_quantity) > 300); anything else stays on the normal path. The
  // executor evaluates it per group in BOTH phases.
  if (qb->having_cond() != nullptr) {
    HeliosHavingDesc hd;
    if (!helios_parse_having(qb, &hd)) {
      if (aggdbg && inner_unit)
        std::fprintf(stderr, "[AGGPD] inner reject: having shape\n");
      return false;
    }
  }

  // GROUP BY items: plain base columns, STRING or INT result. String keys are
  // encoded collation-correctly via strnxfrm in the executor. INT keys (q15's
  // l_suppkey / q18's l_orderkey) group correctly by the same byte-equality,
  // but their padded-binary emission order is not numeric order — only
  // acceptable when no ORDER BY rides on the override.
  std::vector<Item *> gitems;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next) {
    Item *gi = *g->item;
    if (gi->type() != Item::FIELD_ITEM) return false;
    const Item_result grt = gi->result_type();
    if (grt != STRING_RESULT && grt != INT_RESULT) return false;
    if (grt == INT_RESULT && qb->order_list.elements != 0) return false;
    if (gi->is_temporal() || gi->data_type() == MYSQL_TYPE_JSON) return false;
    if (gi->has_aggregation() || gi->has_subquery()) return false;
    gitems.push_back(gi);
  }
  // Inner units additionally require: the single leaf is OUR engine (the
  // hook fires per matching leaf, but stay defensive) and the read-only
  // no-validate prefetch scope — the staged plan is how group rows travel,
  // and OLTP/stateful statements must never be hijacked.
  if (inner_unit) {
    TABLE *lt = qb->leaf_tables != nullptr ? qb->leaf_tables->table : nullptr;
    if (lt == nullptr || lt->file == nullptr ||
        lt->file->ht != lineairdb_hton) {
      if (aggdbg) std::fprintf(stderr, "[AGGPD] inner reject: leaf engine\n");
      return false;
    }
    if (!down_cast<ha_lineairdb *>(lt->file)->tx_ro_novalidate()) {
      if (aggdbg) std::fprintf(stderr, "[AGGPD] inner reject: not ro_novalidate\n");
      return false;
    }
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

// Phase-B spec builder, shared by the override executor (top-level) and the
// optimize-time inner-unit registration so the two can never diverge. Returns
// true and fills the serialized AggregateSpec + the output->spec mappings
// when EVERY output column (and the HAVING aggregate, if any) is computable
// server-side under the read-only no-validate scope.
static bool helios_build_phase_b_spec(
    TABLE *t, bool ro_novalidate, const std::vector<HeliosOut> &outs,
    const std::vector<Item *> &gitems, const HeliosHavingDesc &having,
    std::vector<int> *out_agg, std::vector<int> *out_grp, int *h_agg_pos,
    std::string *spec_ser) {
  const size_t n = outs.size();
  out_agg->assign(n, -1);
  out_grp->assign(n, -1);
  *h_agg_pos = -1;
  spec_ser->clear();
  // OCC soundness (Codex P1): group rows carry synthetic key/tid, so per-row
  // TID / range validation cannot catch concurrent writes — read-only
  // no-validate scope only.
  if (!ro_novalidate) return false;
  // Nullable GROUP BY columns (Codex P1): the server's extract_value_column
  // cannot distinguish NULL from empty.
  for (Item *gi : gitems)
    if (gi->is_nullable()) return false;
  int agg_pos = 0;
  for (size_t c = 0; c < n; ++c) {
    if (outs[c].kind == HK_PASS) {
      Field *of = down_cast<Item_field *>(outs[c].orig)->field;
      for (size_t g = 0; g < gitems.size(); ++g)
        if (down_cast<Item_field *>(gitems[g])->field == of) {
          (*out_grp)[c] = static_cast<int>(g);
          break;
        }
      if ((*out_grp)[c] < 0) return false;  // passthrough not a group col
    } else {
      (*out_agg)[c] = agg_pos++;
    }
  }
  LineairDB::Protocol::AggregateSpec spec;
  spec.set_num_columns(t->s->fields);
  for (Item *gi : gitems)
    spec.add_group_columns(down_cast<Item_field *>(gi)->field->field_index());
  for (size_t c = 0; c < n; ++c) {
    if (outs[c].kind == HK_PASS) continue;
    auto *af = spec.add_aggs();
    if (outs[c].kind == HK_COUNT) {
      af->set_kind(LineairDB::Protocol::AggFunc::AGG_COUNT);
    } else {
      af->set_kind(outs[c].kind == HK_SUM
                       ? LineairDB::Protocol::AggFunc::AGG_SUM
                       : LineairDB::Protocol::AggFunc::AGG_AVG);
      // Only DECIMAL-result SUM/AVG go server-side (exact int128 path).
      if (outs[c].rtype != DECIMAL_RESULT || outs[c].arg == nullptr ||
          !helios_serialize_arith(outs[c].arg, af->mutable_arg()))
        return false;
    }
    af->set_result_scale(0);
  }
  if (having.sum != nullptr) {
    // The HAVING aggregate rides as one extra (non-emitted) agg column.
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
    // Server-side HAVING for SUM/COUNT: the server drops failing groups at
    // emission, so only passing groups ship (q18: 1.5M -> ~57). AVG keeps
    // proxy-side filtering (div_precincrement semantics live here). The
    // executor's having_passes re-checks the survivors either way.
    if (having.kind != HK_AVG && having.cnst != nullptr) {
      uint32_t hop = 0;
      switch (having.op) {
        case Item_func::GT_FUNC: hop = 1; break;
        case Item_func::GE_FUNC: hop = 2; break;
        case Item_func::LT_FUNC: hop = 3; break;
        case Item_func::LE_FUNC: hop = 4; break;
        case Item_func::EQ_FUNC: hop = 5; break;
        case Item_func::NE_FUNC: hop = 6; break;
        default: hop = 0; break;
      }
      my_decimal cval;
      my_decimal *cp = having.cnst->val_decimal(&cval);
      if (hop != 0 && cp != nullptr && !having.cnst->null_value) {
        String cstr;
        if (my_decimal2string(E_DEC_FATAL_ERROR, cp, &cstr) == E_DEC_OK) {
          spec.set_having_op(hop);
          spec.set_having_agg_index(static_cast<uint32_t>(*h_agg_pos));
          spec.set_having_const(std::string(cstr.ptr(), cstr.length()));
        }
      }
    }
  }
  spec.SerializeToString(spec_ser);
  return true;
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

  // HAVING: parsed shape was verified by the offload whitelist; re-derive it
  // (deterministic) and evaluate the constant once. A NULL constant filters
  // every group (SQL: comparison with NULL is never TRUE).
  HeliosHavingDesc having;
  if (!helios_parse_having(qb, &having)) {
    my_error(ER_INTERNAL_ERROR, MYF(0), "helios having shape diverged");
    return true;
  }
  const bool has_having = having.sum != nullptr;
  // Accumulator slots per group: n visible outputs + 1 hidden HAVING slot.
  const size_t n_slots = n + (has_having ? 1 : 0);
  my_decimal having_const;
  bool having_const_null = false;
  if (has_having) {
    my_decimal *d = having.cnst->val_decimal(&having_const);
    having_const_null = having.cnst->null_value || d == nullptr;
    if (d != nullptr && d != &having_const) having_const = *d;
  }
  const int having_div_inc =
      static_cast<int>(thd->variables.div_precincrement);
  // Group filter shared by both phases; slot `n` holds the HAVING aggregate.
  const auto having_passes = [&](std::vector<HeliosAccum> &g) -> bool {
    if (!has_having) return true;
    if (having_const_null) return false;
    HeliosAccum &a = g[n];
    my_decimal val;
    if (having.kind == HK_COUNT) {
      int2my_decimal(E_DEC_FATAL_ERROR, a.cnt, false, &val);
    } else {
      if (a.cnt == 0) return false;  // SUM/AVG over no rows -> NULL -> filtered
      if (having.kind == HK_AVG) {
        my_decimal cnt_dec;
        int2my_decimal(E_DEC_FATAL_ERROR, a.cnt, false, &cnt_dec);
        if (!a.dec_init) my_decimal_set_zero(&a.dec);
        my_decimal_div(E_DEC_FATAL_ERROR, &val, &a.dec, &cnt_dec,
                       having_div_inc);
      } else {
        val = a.dec;
      }
    }
    const int cmp = my_decimal_cmp(&val, &having_const);
    switch (having.op) {
      case Item_func::GT_FUNC: return cmp > 0;
      case Item_func::GE_FUNC: return cmp >= 0;
      case Item_func::LT_FUNC: return cmp < 0;
      case Item_func::LE_FUNC: return cmp <= 0;
      case Item_func::EQ_FUNC: return cmp == 0;
      case Item_func::NE_FUNC: return cmp != 0;
      default: return false;
    }
  };

  // ---- Phase-8 Phase B: server-side aggregation ----------------------------
  // When every aggregate in this query is one the server can compute (B0:
  // COUNT(*) only), push GROUP BY + the aggregates to the server: it returns one
  // group row per group (via the step's scan_values) and we just format + emit
  // them here. Single server + single scan ⇒ groups are final; we still merge by
  // collation key (cheap, robust to any batching) and emit in sorted order.
  {
    bool kinds_ok = true;
    for (const HeliosOut &o : outs)
      if (o.kind != HK_PASS && o.kind != HK_COUNT &&
          o.kind != HK_SUM && o.kind != HK_AVG) kinds_ok = false;
    ha_lineairdb *hl = down_cast<ha_lineairdb *>(t->file);
    const bool inner_unit = qb->outer_query_block() != nullptr;
    std::vector<int> out_agg, out_grp;
    int h_agg_pos = -1;
    std::string spec_ser;
    const bool server_b =
        kinds_ok &&
        helios_build_phase_b_spec(t, hl->tx_ro_novalidate(), outs, gitems,
                                  having, &out_agg, &out_grp, &h_agg_pos,
                                  &spec_ser);
    if (std::getenv("HELIOS_FE_DEBUG"))
      std::fprintf(stderr,
                   "[AGGSRV] server_b=%d inner=%d having=%d ser_bytes=%zu n_out=%zu\n",
                   (int)server_b, (int)inner_unit, (int)has_having,
                   spec_ser.size(), n);
    if (inner_unit) {
      // Inner units: the spec was registered at optimize time; the autogen
      // stamp pass decided DURING PLAN STAGING whether the staged step really
      // returns group rows (the registered filter must have matched the
      // step's attached one). Staging fires on the first table access —
      // trigger it via rnd_init, then read the decision.
      int err = t->file->ha_rnd_init(true);
      if (err) {
        t->file->print_error(err, MYF(0));
        return true;
      }
      if (hl->tx_is_aborted()) {
        t->file->ha_rnd_end();
        my_error(ER_LOCK_DEADLOCK, MYF(0));
        return true;
      }
      if (!hl->tx_inner_agg_stamped()) {
        // Raw rows were staged — aggregate them proxy-side instead.
        t->file->ha_rnd_end();
        goto phase_a_fallthrough;
      }
      if (!server_b || !hl->tx_begin_inner_agg_consume(spec_ser)) {
        // Stamped means the step ships GROUP rows; without a consumable spec
        // Phase A would aggregate group rows as base rows. Fail loudly.
        t->file->ha_rnd_end();
        my_error(ER_INTERNAL_ERROR, MYF(0),
                 "helios inner agg stamped but unconsumable");
        return true;
      }
    } else {
      if (!server_b) {
        // Spec not fully server-aggregatable — fall through to Phase A. (No
        // tx state was set yet.)
        goto phase_a_fallthrough;
      }
      if (!hl->tx_set_pushed_aggregate(spec_ser)) {
        // WHERE not fully pushable -> the server would aggregate unfiltered
        // rows; Phase A evaluates the WHERE locally instead.
        goto phase_a_fallthrough;
      }
      int err = t->file->ha_rnd_init(true);
      if (err) {
        hl->tx_clear_pushed_aggregate();
        t->file->print_error(err, MYF(0));
        return true;
      }
    }
    {
      const size_t ng = gitems.size();
      const int n_aggs =
          static_cast<int>(std::count_if(outs.begin(), outs.end(),
                                         [](const HeliosOut &o) {
                                           return o.kind != HK_PASS;
                                         })) +
          (has_having ? 1 : 0);

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
          v.resize(n_slots);
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
        // One accumulation routine for visible aggregates (slot c, spec
        // position out_agg[c]) and the hidden HAVING aggregate (slot n, spec
        // position h_agg_pos).
        const auto accumulate_agg = [&](HeliosAccum &a, HeliosAggKind kind,
                                        int agg_pos) {
          const size_t vi = 1 + ng + 2 * agg_pos;  // value column in fv
          const size_t ci = vi + 1;                // count column in fv
          if (kind == HK_COUNT) {
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
        };
        for (size_t c = 0; c < n; ++c) {
          const HeliosOut &o = outs[c];
          if (o.kind == HK_PASS) continue;
          accumulate_agg((*grp)[c], o.kind, out_agg[c]);
        }
        if (has_having)
          accumulate_agg((*grp)[n], having.kind, h_agg_pos);
      }
      t->file->ha_rnd_end();
      hl->tx_clear_pushed_aggregate();
      // A staging abort during the read must not surface as an empty (or
      // truncated) aggregate (Codex P2): agg_next_raw returns false on abort,
      // so distinguish abort from EOF here and fail the statement.
      if (hl->tx_is_aborted()) {
        my_error(ER_LOCK_DEADLOCK, MYF(0));
        return true;
      }

      mem_root_deque<Item *> row(thd->mem_root);
      if (helios_make_output_caches(join, &row)) return true;
      const int div_inc = static_cast<int>(thd->variables.div_precincrement);
      for (auto &kv : groups) {
        std::vector<HeliosAccum> &g = kv.second;
        if (!having_passes(g)) continue;
        for (size_t c = 0; c < n; ++c) {
          Item_cache *cache = down_cast<Item_cache *>(row[c]);
          const HeliosOut &o = outs[c];
          HeliosAccum &a = g[c];
          if (o.kind == HK_PASS) {
            if (a.p_null) { cache->store_null(); continue; }
            cache->null_value = false;
            // The group key arrived as raw column bytes (p_str); the cache
            // cell is typed after the ORIGINAL item (Item_cache_int for an
            // INT key like q18's l_orderkey), so convert per result type.
            switch (o.rtype) {
              case INT_RESULT: {
                std::string s(a.p_str.ptr(), a.p_str.length());
                down_cast<Item_cache_int *>(cache)->store_value(
                    cache, std::strtoll(s.c_str(), nullptr, 10));
                break;
              }
              case DECIMAL_RESULT: {
                my_decimal d;
                str2my_decimal(E_DEC_FATAL_ERROR, a.p_str.ptr(),
                               a.p_str.length(), &my_charset_bin, &d);
                down_cast<Item_cache_decimal *>(cache)->store_value(cache, &d);
                break;
              }
              case REAL_RESULT: {
                std::string s(a.p_str.ptr(), a.p_str.length());
                down_cast<Item_cache_real *>(cache)->store_value(
                    cache, std::strtod(s.c_str(), nullptr));
                break;
              }
              default:
                down_cast<Item_cache_str *>(cache)->store_value(cache,
                                                                a.p_str);
                break;
            }
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
    v.resize(n_slots);
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
        v.resize(n_slots);
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
    if (has_having) {  // hidden HAVING aggregate (slot n); DECIMAL/INT only
      HeliosAccum &a = (*grp)[n];
      if (having.kind == HK_COUNT) {
        ++a.cnt;
      } else {
        my_decimal *d = having.arg->val_decimal(&dec_buf);
        if (!having.arg->null_value && d != nullptr) {
          if (!a.dec_init) { a.dec = *d; a.dec_init = true; }
          else {
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
    if (!having_passes(g)) continue;
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

// Best-effort optimize-time Phase-B registration for an offloadable INNER
// unit: build the full spec (visible aggregates + HAVING) and verify the
// inner WHERE is EXACTLY pushable, then register both on the tx. The autogen
// stamp pass later attaches the spec to the staged scan step (group rows);
// without a registration the override still runs, aggregating the staged raw
// rows proxy-side (Phase A).
static void helios_register_inner_aggregate(THD *thd, JOIN *join) {
  Query_block *qb = join->query_block;
  TABLE *t = qb->leaf_tables->table;
  std::vector<HeliosOut> outs;
  if (!helios_plan_outputs(join, &outs)) return;
  std::vector<Item *> gitems;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next)
    gitems.push_back(*g->item);
  HeliosHavingDesc having;
  if (!helios_parse_having(qb, &having)) return;
  ha_lineairdb *hl = down_cast<ha_lineairdb *>(t->file);
  std::vector<int> out_agg, out_grp;
  int h_agg_pos = -1;
  std::string spec_ser;
  if (!helios_build_phase_b_spec(t, hl->tx_ro_novalidate(), outs, gitems,
                                 having, &out_agg, &out_grp, &h_agg_pos,
                                 &spec_ser))
    return;
  // Group rows cannot be re-filtered by MySQL: the WHERE must be EXACTLY the
  // filter the staged step will carry. No WHERE = no filter required.
  std::string filter_ser;
  if (qb->where_cond() != nullptr) {
    bool exact = false;
    if (!build_single_table_filter(thd, t, &filter_ser, &exact) ||
        filter_ser.empty() || !exact)
      return;
  }
  hl->tx_register_inner_aggregate(spec_ser, filter_ser);
  if (std::getenv("HELIOS_FE_DEBUG"))
    std::fprintf(stderr, "[AGGPD] inner unit registered (spec=%zuB filter=%zuB)\n",
                 spec_ser.size(), filter_ser.size());
}

// GroupedSummary registration (Phase-16 entry 25): a DERIVED inner unit
// (q15's view clones) whose shape is "single table, GROUP BY one column,
// SUM(a * (1 - b)) only, exact WHERE" is served by the handler hijacking its
// own scan — MySQL re-aggregates synthetic rows idempotently, so the core
// stays unmodified. Self-gating: anything off-shape simply doesn't register.
static void helios_try_register_gs(THD *thd, JOIN *join) {
  if (thd == nullptr || join == nullptr || thd->lex == nullptr) return;
  if (thd->lex->sql_command != SQLCOM_SELECT || thd->lex->is_explain()) return;
  Query_block *qb = join->query_block;
  if (qb == nullptr) return;
  Query_expression *qe = qb->master_query_expression();
  if (qe == nullptr || !qe->is_simple()) return;
  // DERIVED inner units only (no Item): scalar/IN subqueries take other paths.
  if (qb->outer_query_block() == nullptr || qe->item != nullptr) return;
  if (qe->uncacheable != 0) return;
  if (qb->leaf_table_count != 1 || !qb->is_grouped()) return;
  if (qb->is_distinct() || qb->having_cond() != nullptr || qb->has_limit() ||
      qb->olap != UNSPECIFIED_OLAP_TYPE || qb->has_windows() ||
      qb->order_list.elements != 0)
    return;
  TABLE *t = qb->leaf_tables != nullptr ? qb->leaf_tables->table : nullptr;
  if (t == nullptr || t->file == nullptr || t->file->ht != lineairdb_hton)
    return;
  ha_lineairdb *hl = down_cast<ha_lineairdb *>(t->file);
  if (!hl->tx_ro_novalidate()) return;
  // ONE group column, a plain non-nullable field.
  if (qb->group_list.elements != 1) return;
  Item *gi = *qb->group_list.first->item;
  if (gi->type() != Item::FIELD_ITEM || gi->is_nullable()) return;
  Field *gf = down_cast<Item_field *>(gi)->field;
  // Outputs: passthrough == the group column, plus EXACTLY ONE
  // SUM(a * (1 - b)) with a,b non-nullable DECIMAL fields (q15's revenue).
  Item_sum *the_sum = nullptr;
  for (Item *it : VisibleFields(qb->fields)) {
    if (it->type() == Item::SUM_FUNC_ITEM) {
      Item_sum *sm = down_cast<Item_sum *>(it);
      if (the_sum != nullptr || sm->sum_func() != Item_sum::SUM_FUNC ||
          sm->has_wf() || sm->has_subquery() || sm->argument_count() != 1)
        return;
      the_sum = sm;
    } else if (it->type() == Item::FIELD_ITEM) {
      if (down_cast<Item_field *>(it)->field != gf) return;
    } else {
      return;
    }
  }
  if (the_sum == nullptr) return;
  // Recognize a * (1 - b) (either MUL arg order).
  Item *arg = the_sum->arguments()[0];
  if (arg->type() != Item::FUNC_ITEM) return;
  Item_func *mul = down_cast<Item_func *>(arg);
  if (mul->functype() != Item_func::MUL_FUNC || mul->argument_count() != 2)
    return;
  Item *m0 = mul->arguments()[0], *m1 = mul->arguments()[1];
  Item_field *fa = nullptr;
  Item_func *sub = nullptr;
  if (m0->type() == Item::FIELD_ITEM && m1->type() == Item::FUNC_ITEM) {
    fa = down_cast<Item_field *>(m0);
    sub = down_cast<Item_func *>(m1);
  } else if (m1->type() == Item::FIELD_ITEM && m0->type() == Item::FUNC_ITEM) {
    fa = down_cast<Item_field *>(m1);
    sub = down_cast<Item_func *>(m0);
  } else {
    return;
  }
  if (sub->functype() != Item_func::MINUS_FUNC || sub->argument_count() != 2)
    return;
  Item *s0 = sub->arguments()[0], *s1 = sub->arguments()[1];
  if (s0->type() != Item::INT_ITEM || s0->val_int() != 1 ||
      s1->type() != Item::FIELD_ITEM)
    return;
  Item_field *fb = down_cast<Item_field *>(s1);
  Field *cfa = fa->field, *cfb = fb->field;
  // Carrier feasibility: a, b DECIMAL scale 2, SIGNED, non-nullable, and the
  // three roles pairwise DISTINCT (Codex P0-2: GROUP BY a / SUM(x*(1-x))
  // would silently drop part of the synthetic assignment). b must be able to
  // store values in [0, 2) (precision-scale >= 1).
  if (cfa == cfb || cfa == gf || cfb == gf) return;
  if (cfa->is_nullable() || cfb->is_nullable()) return;
  if (cfa->is_unsigned() || cfb->is_unsigned()) return;  // row2 may be -0.01
  if (cfa->type() != MYSQL_TYPE_NEWDECIMAL ||
      cfb->type() != MYSQL_TYPE_NEWDECIMAL || cfa->decimals() != 2 ||
      cfb->decimals() != 2)
    return;
  if (cfb->field_length < 5) return;  // "1.00" must fit (prec-scale >= 1)
  // WHERE: exact serialization, then a TEMPLATE row that the actual WHERE
  // Item accepts (the synthetic rows must pass MySQL's re-evaluated FILTER).
  std::string filter_ser;
  std::vector<std::pair<Field *, std::string>> where_cands;
  Item *where = qb->where_cond();
  if (where != nullptr) {
    bool exact = false;
    if (!build_single_table_filter(thd, t, &filter_ser, &exact) ||
        filter_ser.empty() || !exact)
      return;
    // Candidate constants: FIELD >= C / FIELD = C / FIELD BETWEEN C AND C2
    // contribute C for their column; verification below uses the REAL Item.
    std::vector<Item *> conj;
    if (where->type() == Item::COND_ITEM &&
        down_cast<Item_cond *>(where)->functype() == Item_func::COND_AND_FUNC) {
      for (Item &c : *down_cast<Item_cond *>(where)->argument_list())
        conj.push_back(&c);
    } else {
      conj.push_back(where);
    }
    for (Item *c : conj) {
      if (c->type() != Item::FUNC_ITEM) continue;
      Item_func *f = down_cast<Item_func *>(c);
      const auto ft = f->functype();
      if ((ft != Item_func::GE_FUNC && ft != Item_func::EQ_FUNC &&
           ft != Item_func::BETWEEN) ||
          f->argument_count() < 2)
        continue;
      Item *lhs = f->arguments()[0]->real_item();
      Item *rhs = f->arguments()[1];
      if (lhs->type() != Item::FIELD_ITEM || !rhs->const_item()) continue;
      String sbuf, *sv = rhs->val_str(&sbuf);
      if (sv == nullptr || rhs->null_value) continue;
      where_cands.emplace_back(down_cast<Item_field *>(lhs)->field,
                               std::string(sv->ptr(), sv->length()));
    }
  }
  // The EXACT filter enumerates every WHERE column (serialize_item emits
  // COLUMN_REF per field). Those columns must be DISJOINT from
  // {group, a, b}: synthetic rows overwrite those three, so any WHERE atom
  // touching them would be re-evaluated against fabricated values
  // (Codex P0-1 — a '<' atom on l_discount was not catchable before).
  // Disjointness also makes the template verification below an exact proof:
  // every WHERE-referenced column holds the SAME bytes at verification time
  // and at emission time.
  if (!filter_ser.empty()) {
    LineairDB::Protocol::PushedPredicate pred;
    if (!pred.ParseFromString(filter_ser) || !pred.has_expr()) return;
    std::vector<uint32_t> wcols;
    std::function<void(const LineairDB::Protocol::FilterExpr &)> collect =
        [&](const LineairDB::Protocol::FilterExpr &e) {
          if (e.op() == LineairDB::Protocol::FilterExpr::COLUMN_REF)
            wcols.push_back(e.column_index());
          for (const auto &c : e.children()) collect(c);
        };
    collect(pred.expr());
    for (uint32_t c : wcols) {
      if (c == gf->field_index() || c == cfa->field_index() ||
          c == cfb->field_index())
        return;
    }
  }
  // read_set must be covered by {group, a, b, WHERE-candidate columns}.
  for (uint i = 0; i < t->s->fields; ++i) {
    if (!bitmap_is_set(t->read_set, i)) continue;
    Field *fld = t->field[i];
    if (fld == gf || fld == cfa || fld == cfb) continue;
    bool covered = false;
    for (auto &wc : where_cands)
      if (wc.first == fld) covered = true;
    if (!covered) return;
  }
  // Build the per-column ASCII template ("0" placeholders are never stored:
  // the read_set store-skip ignores columns outside the read_set).
  std::vector<std::string> tmpl(t->s->fields, "0");
  for (auto &wc : where_cands) tmpl[wc.first->field_index()] = wc.second;
  // Verify the template passes the REAL WHERE: store candidates into
  // record[0] (saved/restored) and evaluate the Item tree once.
  if (where != nullptr) {
    std::vector<uchar> saved(t->record[0], t->record[0] + t->s->rec_buff_length);
    const enum_check_fields saved_check = thd->check_for_truncated_fields;
    thd->check_for_truncated_fields = CHECK_FIELD_IGNORE;  // no warning spam
    my_bitmap_map *org = tmp_use_all_columns(t, t->write_set);
    for (auto &wc : where_cands)
      wc.first->store(wc.second.data(), wc.second.size(), &my_charset_bin);
    tmp_restore_column_map(t->write_set, org);
    const longlong pass = where->val_int();
    const bool was_null = where->null_value;
    thd->check_for_truncated_fields = saved_check;
    memcpy(t->record[0], saved.data(), saved.size());
    if (thd->is_error() || was_null || pass == 0) return;
  }
  // Spec: GROUP BY gf + SUM(a * (1 - b)) — serialized via the shared arith
  // path so the server computes the exact scale-4 sums.
  LineairDB::Protocol::AggregateSpec spec;
  spec.set_num_columns(t->s->fields);
  spec.add_group_columns(gf->field_index());
  auto *af = spec.add_aggs();
  af->set_kind(LineairDB::Protocol::AggFunc::AGG_SUM);
  if (!helios_serialize_arith(arg, af->mutable_arg())) return;
  af->set_result_scale(0);
  LineairDBTransaction::GsRegistration reg;
  spec.SerializeToString(&reg.spec);
  reg.filter = std::move(filter_ser);
  reg.template_cols = std::move(tmpl);
  reg.group_col = gf->field_index();
  reg.col_a = cfa->field_index();
  reg.col_b = cfb->field_index();
  if (std::getenv("HELIOS_FE_DEBUG"))
    std::fprintf(stderr, "[GS] registered table=%s group=%u a=%u b=%u\n",
                 t->s->table_name.str, reg.group_col, reg.col_a, reg.col_b);
  hl->tx_register_gs(std::move(reg));
}

// Phase-17 q18 grouped-semijoin recognition. An IN subquery
//   outer.col IN (SELECT gcol FROM T GROUP BY gcol HAVING SUM(x) > c)
// lets us reduce the outer scan to rows whose col is one of the qualifying
// groups (positive membership = the IN set exactly). Records the intent on the
// tx; the autogen turns it into a server-side AGGREGATE step over T plus a
// semijoin on the outer scan. MySQL stays unmodified and still materializes
// the subquery normally (this only prunes the outer staging). Self-gating:
// off-shape simply doesn't register. Gate HELIOS_Q18_SEMIJOIN (default OFF).
static std::string helios_phys_table_key(const TABLE *t) {
  if (t == nullptr || t->s == nullptr) return std::string();
  const TABLE_SHARE *s = t->s;
  std::string key = "./";
  if (s->db.str != nullptr && s->db.length > 0) key.append(s->db.str, s->db.length);
  key.push_back('/');
  if (s->table_name.str != nullptr && s->table_name.length > 0)
    key.append(s->table_name.str, s->table_name.length);
  return key;
}

static void helios_try_register_grouped_semijoin(THD *thd, JOIN *join) {
  // Default ON; HELIOS_Q18_SEMIJOIN=0 disables (A/B measurement + off-switch).
  static const char *q18_env = std::getenv("HELIOS_Q18_SEMIJOIN");
  static const bool gate = q18_env == nullptr || std::strcmp(q18_env, "0") != 0;
  if (!gate) return;
  if (thd == nullptr || join == nullptr || thd->lex == nullptr) return;
  if (thd->lex->sql_command != SQLCOM_SELECT || thd->lex->is_explain()) return;
  const bool aggdbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
  Query_block *qb = join->query_block;
  if (qb == nullptr || qb->outer_query_block() == nullptr) return;  // inner only
  Query_expression *qe = qb->master_query_expression();
  if (qe == nullptr || !qe->is_simple() || qe->uncacheable != 0) return;
  // The unit's Item must be an IN subquery.
  if (qe->item == nullptr ||
      qe->item->substype() != Item_subselect::IN_SUBS)
    return;
  Item_in_subselect *insub = down_cast<Item_in_subselect *>(qe->item);
  if (insub->left_expr == nullptr) return;
  // Reducing the outer to the IN set is sound ONLY for a positive IN. upper_item
  // points at a NOT/NOP wrapper (NOT IN / ALL-SOME negation); reject it. (A
  // nested IN under OR/CASE is not separately detected here — the shape gating
  // makes it unlikely, and md5 covers the TPC-H surface.)
  if (insub->upper_item != nullptr) return;
  // Inner shape: exactly one base table, grouped by ONE plain non-nullable
  // column, the SELECT list is just that column, plus a HAVING aggregate.
  if (qb->leaf_table_count != 1 || !qb->is_grouped()) return;
  if (qb->is_distinct() || qb->has_limit() ||
      qb->olap != UNSPECIFIED_OLAP_TYPE || qb->has_windows() ||
      qb->group_list.elements != 1 || qb->having_cond() == nullptr)
    return;
  // The agg step scans T unfiltered: a subquery WHERE would change which rows
  // aggregate, so the membership set would no longer equal the IN set. q18 has
  // no inner WHERE; reject anything that does.
  if (qb->where_cond() != nullptr) return;
  TABLE *t = qb->leaf_tables != nullptr ? qb->leaf_tables->table : nullptr;
  if (t == nullptr || t->file == nullptr || t->file->ht != lineairdb_hton)
    return;
  ha_lineairdb *hl = down_cast<ha_lineairdb *>(t->file);
  if (!hl->tx_ro_novalidate()) return;
  Item *gi = *qb->group_list.first->item;
  if (gi->type() != Item::FIELD_ITEM || gi->is_nullable()) return;
  // The (single) SELECT output must BE the group column (q18: SELECT l_orderkey).
  std::vector<HeliosOut> outs;
  if (!helios_plan_outputs(join, &outs)) return;
  if (outs.size() != 1 || outs[0].kind != HK_PASS) return;
  // HAVING: one bare-aggregate-vs-const (helios_parse_having's q18 shape).
  HeliosHavingDesc having;
  if (!helios_parse_having(qb, &having) || having.sum == nullptr) return;
  std::vector<Item *> gitems{gi};
  std::vector<int> out_agg, out_grp;
  int h_agg_pos = -1;
  std::string spec_ser;
  if (!helios_build_phase_b_spec(t, hl->tx_ro_novalidate(), outs, gitems, having,
                                 &out_agg, &out_grp, &h_agg_pos, &spec_ser))
    return;
  // Server-side HAVING must be live (else the agg ships every group, and the
  // membership set would not equal the IN set without proxy re-filtering).
  {
    LineairDB::Protocol::AggregateSpec chk;
    if (!chk.ParseFromString(spec_ser) || chk.having_op() == 0) return;
  }
  // The IN's left expr must be a single outer base-table column on our engine.
  Item *lhs = insub->left_expr->real_item();
  if (lhs->type() != Item::FIELD_ITEM) return;
  Field *of = down_cast<Item_field *>(lhs)->field;
  if (of == nullptr || of->table == nullptr || of->table->file == nullptr ||
      of->table->file->ht != lineairdb_hton)
    return;
  TABLE *ot = of->table;
  if (ot == t) return;  // the outer table must differ from the aggregated one
  int probe_col = -1;
  for (uint i = 0; i < ot->s->fields; ++i)
    if (ot->field[i] == of) { probe_col = static_cast<int>(i); break; }
  if (probe_col < 0) return;
  // Soundness: the outer table must be a plain inner-join leaf in the top-level
  // query block (an outer-join inner or a semi/anti-join nest member would lose
  // NULL-extended / membership rows when we prune it). And the membership test
  // is a raw-byte compare of the inner group column vs outer.col, so the two
  // fields must be byte-compatible and non-nullable.
  {
    Field *grp_f = down_cast<Item_field *>(gi)->field;
    const Table_ref *otr = ot->pos_in_table_list;
    if (otr == nullptr) return;
    if (otr->is_inner_table_of_outer_join()) return;
    for (const Table_ref *emb = otr->embedding; emb != nullptr;
         emb = emb->embedding)
      if (emb->is_sj_or_aj_nest()) return;
    if (otr->query_block == nullptr ||
        otr->query_block->outer_query_block() != nullptr)
      return;
    if (grp_f == nullptr || grp_f->is_nullable() || of->is_nullable()) return;
    if (grp_f->type() != of->type() ||
        grp_f->pack_length() != of->pack_length() ||
        grp_f->is_unsigned() != of->is_unsigned())
      return;
    if (grp_f->result_type() == STRING_RESULT &&
        grp_f->charset() != of->charset())
      return;
  }
  const std::string inner_key = helios_phys_table_key(t);
  const std::string outer_key = helios_phys_table_key(ot);
  if (inner_key.empty() || outer_key.empty()) return;

  // Phase B (gate HELIOS_Q18_GS): also serve the INNER subquery scan
  // synthetically so MySQL never stages T's raw rows. The aggregated column
  // (l_quantity) itself holds the exact group sum, so one synthetic row per
  // surviving group re-aggregates idempotently. Requires the HAVING aggregate
  // to be SUM over a plain non-nullable column of T distinct from the group
  // column, and that the inner read_set touches ONLY {group, sum} (every other
  // read column would be served the "0" template). Decide eligibility BEFORE
  // registering anything so the two registrations stay consistent.
  // Default ON; HELIOS_Q18_GS=0 disables the inner synthetic serving (Phase B).
  static const char *gs_env = std::getenv("HELIOS_Q18_GS");
  static const bool gs_gate = gs_env == nullptr || std::strcmp(gs_env, "0") != 0;
  bool serve_inner = false;
  int sum_col = -1, grp_col = -1;
  Field *gf = down_cast<Item_field *>(gi)->field;
  if (gs_gate && having.kind == HK_SUM && having.arg != nullptr) {
    Item *sarg = having.arg->real_item();
    if (sarg->type() == Item::FIELD_ITEM && !sarg->is_nullable()) {
      Field *sumf = down_cast<Item_field *>(sarg)->field;
      if (sumf != nullptr && sumf->table == t && sumf != gf) {
        for (uint i = 0; i < t->s->fields; ++i) {
          if (t->field[i] == sumf) sum_col = static_cast<int>(i);
          if (t->field[i] == gf) grp_col = static_cast<int>(i);
        }
        if (sum_col >= 0 && grp_col >= 0) {
          serve_inner = true;
          for (uint i = 0; i < t->s->fields; ++i) {
            if (!bitmap_is_set(t->read_set, i)) continue;
            if (static_cast<int>(i) != sum_col &&
                static_cast<int>(i) != grp_col) {
              serve_inner = false;
              break;
            }
          }
        }
      }
    }
  }

  LineairDBTransaction::GroupedSemijoin gs;
  gs.inner_table_key = inner_key;
  gs.agg_spec = spec_ser;  // copy: the GS below reuses spec_ser
  gs.outer_table_key = outer_key;
  gs.outer_probe_column = static_cast<uint32_t>(probe_col);
  if (aggdbg)
    std::fprintf(stderr,
                 "[GSEMI] registered inner=%s outer=%s probe_col=%d serve=%d\n",
                 inner_key.c_str(), outer_key.c_str(), probe_col,
                 (int)serve_inner);
  hl->tx_register_grouped_semijoin(std::move(gs));

  if (!serve_inner) return;
  LineairDBTransaction::GsRegistration reg;
  reg.spec = std::move(spec_ser);
  reg.filter.clear();  // q18 inner has no WHERE
  reg.template_cols.assign(t->s->fields, "0");
  reg.group_col = static_cast<uint32_t>(grp_col);
  reg.col_a = static_cast<uint32_t>(sum_col);
  reg.col_b = static_cast<uint32_t>(sum_col);  // masked by col_a in emit_row
  reg.single_sum = true;
  hl->tx_register_gs(std::move(reg));
}

static int lineairdb_push_to_engine(THD *thd, AccessPath *root_path,
                                    JOIN *join) {
  if (!helios_agg_pushdown_enabled()) return 0;
  helios_try_register_gs(thd, join);  // self-gating (derived inner units)
  helios_try_register_grouped_semijoin(thd, join);  // self-gating (q18 IN)
  if (!helios_offloadable_shape(thd, join)) return 0;
  // Phase-22 D2: do NOT hijack a BOUNDED, SMALL aggregate into a full table scan.
  // helios_override_executor drives ha_rnd_init (a full-table-scan RPC); for a
  // PK-prefix / ref / range access with few rows (TPC-C Delivery SUM over ~10
  // order_line rows) that becomes a ~300k full scan -> AGG-on OLTP collapse. When
  // the optimizer already chose a bounded+small access path, leave the override
  // uninstalled so MySQL's normal bounded pipeline + native aggregation run.
  // TABLE_SCAN / full INDEX_SCAN (TPC-H q1/q6/q18, large estimate) keep firing.
  if (root_path != nullptr) {
    const AccessPath *leaf = nullptr;
    WalkAccessPaths(root_path, join, WalkAccessPathPolicy::ENTIRE_TREE,
                    [&leaf](AccessPath *p, const JOIN *) -> bool {
                      if (GetBasicTable(p) != nullptr) {
                        leaf = p;
                        return true;  // first base-table leaf -> stop the walk
                      }
                      return false;
                    });
    // ④-review hardening: act ONLY on the block's own single base table
    // (helios_offloadable_shape already enforced leaf_table_count==1). Fail
    // closed (keep firing AGG) if the walked leaf is somehow not that table.
    const TABLE *leaf_tbl = (leaf != nullptr) ? GetBasicTable(leaf) : nullptr;
    if (leaf_tbl != nullptr && join->query_block != nullptr &&
        join->query_block->leaf_tables != nullptr &&
        leaf_tbl == join->query_block->leaf_tables->table) {
      const double rows = leaf->num_output_rows();
      const bool bounded = leaf->type == AccessPath::REF ||
                           leaf->type == AccessPath::REF_OR_NULL ||
                           leaf->type == AccessPath::EQ_REF ||
                           leaf->type == AccessPath::INDEX_RANGE_SCAN;
      static const double kMinScanRows = [] {
        const char *e = std::getenv("HELIOS_AGG_MIN_SCAN_ROWS");
        return (e && e[0]) ? std::atof(e) : 1000.0;
      }();
      if (bounded && rows >= 0.0 && rows < kMinScanRows) {
        if (std::getenv("HELIOS_FE_DEBUG"))
          std::fprintf(stderr,
                       "[AGGPD] skip override: bounded access type=%d rows=%.0f\n",
                       static_cast<int>(leaf->type), rows);
        return 0;  // bounded+small -> normal bounded execution + native agg
      }
    }
  }
  if (std::getenv("HELIOS_FE_DEBUG"))
    std::fprintf(stderr, "[AGGPD] installing override_executor_func%s\n",
                 join->query_block->outer_query_block() != nullptr
                     ? " (inner unit)"
                     : "");
  if (join->query_block->outer_query_block() != nullptr)
    helios_register_inner_aggregate(thd, join);
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

  // GroupedSummary on the index-scan path (Phase-17 q18): the inner subquery
  // scans T by an ordered PRIMARY index. autogen dropped its base scan, so
  // serve the synthetic group rows here (gs_fill_buffers fills both the rnd and
  // index buffers). Fires once at the full-scan start (no search key); the rest
  // come from index_next reading secondary_index_results_.
  if (tx->gs_skipped(table) && key == nullptr) {
    reset_index_search_buffers();
    last_fetched_primary_key_.clear();
    if (int e = gs_fill_buffers(tx)) return e;
    if (secondary_index_results_.empty()) return HA_ERR_END_OF_FILE;
    return fetch_and_set_current_result(buf, tx);
  }

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
    // A limit-staged prefetch result holds only the first N rows of the
    // range; past them the real table may have more, so EOF would be a lie.
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
    // See index_next: a truncated materialization must not fake EOF.
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

  // GroupedSummary: autogen dropped this leaf's scan step (recorded on the
  // tx), so serve synthetic group rows instead of staged base rows.
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

bool ha_lineairdb::tx_set_pushed_aggregate(const std::string &s) {
  auto tx = get_transaction(ha_thd());
  // The override drives the scan via agg_next_raw, which bypasses the normal
  // read path that selects the table; select it here so execute_read_plan's
  // db_table_key matches this scan step and stamps the aggregate (and filter).
  tx->choose_table(db_table_name);
  // The server aggregates over the staged scan's rows, so the statement's
  // WHERE must ride along as the step filter — otherwise the aggregate counts
  // unfiltered rows (q1 N/O group, q6 152x). Fully-serializable WHERE only;
  // on failure the caller falls back to Phase A (local WHERE evaluation).
  // NOTE (Codex P1): prepare's return value ANDs in limit-safety (integer
  // comparisons only), which would reject q1/q6's DATE/DECIMAL filters; the
  // aggregate gate only needs FULL SERIALIZATION, observable as a non-empty
  // installed filter (or no WHERE at all).
  prepare_select_filter_for_tx(ha_thd(), table, tx, nullptr);
  const Item *agg_where = nullptr;
  if (ha_thd()->lex != nullptr && ha_thd()->lex->unit != nullptr) {
    Query_block *qb = ha_thd()->lex->unit->global_parameters();
    if (qb != nullptr) agg_where = qb->where_cond();
  }
  if (agg_where != nullptr) {
    if (tx->get_pushed_filter().empty()) {
      return false;  // WHERE exists but could not be serialized at all
    }
    // Non-emptiness is NOT fullness: build_single_table_filter returns the
    // table-local NECESSARY condition, which may be weaker than the WHERE
    // (an unserializable conjunct silently drops). Rows the dropped conjunct
    // excludes would then join the aggregate with no MySQL recheck possible.
    // Require EXACT (verbatim, complete) serialization.
    bool exact = false;
    std::string probe;
    if (!build_single_table_filter(ha_thd(), table, &probe, &exact) || !exact) {
      tx->clear_pushed_filter();
      return false;
    }
  }
  tx->set_pushed_aggregate(s);
  return true;
}

// Phase-16 inner-unit aggregation wrappers.
void ha_lineairdb::tx_register_inner_aggregate(const std::string &spec,
                                               const std::string &filter) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return;
  tx->choose_table(db_table_name);
  tx->register_inner_aggregate_current(spec, filter, table);
}
bool ha_lineairdb::tx_inner_agg_stamped() {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return false;
  tx->choose_table(db_table_name);
  return tx->inner_agg_stamped_current(table);
}
bool ha_lineairdb::tx_begin_inner_agg_consume(const std::string &expect_spec) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return false;
  tx->choose_table(db_table_name);
  return tx->begin_inner_agg_consume(expect_spec);
}

void ha_lineairdb::tx_register_gs(LineairDBTransaction::GsRegistration reg) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return;
  tx->register_gs(table, std::move(reg));
}

void ha_lineairdb::tx_register_grouped_semijoin(
    LineairDBTransaction::GroupedSemijoin gs) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return;
  tx->register_grouped_semijoin(std::move(gs));
}

// GroupedSummary consume: fetch server group rows for this leaf (raw, no
// cache ingest), decompose each group's exact scale-4 SUM into one or two
// synthetic rows whose re-aggregation by MySQL is idempotent, and fill the
// scan buffers rnd_next serves from. Anything unexpected fails CLOSED
// (HA_ERR_INTERNAL_ERROR): the staged plan no longer carries this table's
// base rows, so there is no silent fallback.
int ha_lineairdb::gs_fill_buffers(LineairDBTransaction *tx) {
  const LineairDBTransaction::GsRegistration *reg = tx->gs_registration(table);
  if (reg == nullptr) {
    my_error(ER_INTERNAL_ERROR, MYF(0), "helios gs skipped but unregistered");
    return HA_ERR_INTERNAL_ERROR;
  }
  // q18 unification: if a grouped-semijoin agg step already aggregated this
  // table in the main prefetch, reuse those exact group rows instead of a
  // second full-table aggregation RPC. Falls back to the RPC otherwise (q15).
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
  // Group row layout: [null_flags][group key][sum][count], agg_emit_field
  // framing (same parser as the Phase-B executor).
  auto parse_fields = [](std::string_view row,
                         std::vector<std::string_view> *fv,
                         std::vector<bool> *nul) -> bool {
    size_t off = 0;
    while (off < row.size()) {
      uint8_t bs = static_cast<uint8_t>(row[off++]);
      if (bs == 0xFF) { fv->emplace_back(); nul->push_back(true); continue; }
      if (off + bs > row.size()) return false;
      size_t len = 0;
      for (uint8_t i = 0; i < bs; ++i)
        len |= static_cast<size_t>(static_cast<uint8_t>(row[off + i]))
               << (8 * i);
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
  // Synthetic rows get UNIQUE synthetic keys and a scan_cache_ entry so the
  // position()/rnd_pos() rowid-reread contract holds (Codex P1-2): a sort or
  // rowid materialization above this scan re-serves the synthetic row from
  // scanned_values_, exactly like a normal cached scan. The keys never reach
  // the server or the row cache (this scan is buffer-local).
  const auto emit_row = [&](const std::string &gkey, const std::string &a_str,
                            const std::string &b_str) {
    LineairDBField ldb;
    ldb.set_null_field(zero_nulls.data(), zero_nulls.size());
    std::string value = ldb.get_null_field();
    for (uint i = 0; i < nf; ++i) {
      const std::string *src = &reg->template_cols[i];
      if (i == reg->group_col) src = &gkey;
      else if (i == reg->col_a) src = &a_str;
      else if (i == reg->col_b) src = &b_str;
      ldb.set_lineairdb_field(src->c_str(), src->size());
      value += ldb.get_lineairdb_field();
    }
    std::string skey = "\x01gs:" + gkey + ":" +
                       std::to_string(scanned_keys_.size());
    const size_t idx = scanned_keys_.size();
    scanned_keys_.push_back(skey);
    // Index-scan path (q18's inner uses an ordered PRIMARY index scan, not a
    // table scan): fetch_and_set_current_result serves the inline payload, so
    // the synthetic key is a placeholder. Both buffer sets are filled so rnd
    // (q15) and index (q18) consumers each find their rows.
    secondary_index_results_.push_back(skey);
    secondary_index_payloads_.push_back(value);
    scan_cache_[std::move(skey)] = idx;
    scanned_values_.emplace_back(
        reinterpret_cast<const std::byte *>(value.data()),
        reinterpret_cast<const std::byte *>(value.data()) + value.size());
  };
  String dec_str;
  for (const std::string &grow : groups) {
    std::vector<std::string_view> fv;
    std::vector<bool> nul;
    if (!parse_fields(grow, &fv, &nul) || fv.size() != 4 || nul[1] || nul[2]) {
      my_error(ER_INTERNAL_ERROR, MYF(0), "helios gs group row malformed");
      return HA_ERR_INTERNAL_ERROR;
    }
    const std::string gkey(fv[1]);
    // Phase-17 q18 single-column SUM: the aggregated column itself holds the
    // exact sum, so emit ONE synthetic row per group (col_a = sum) — MySQL's
    // SUM over that single row reproduces the value, and re-applied HAVING
    // passes (the server already kept only qualifying groups). col_b is unused.
    if (reg->single_sum) {
      // Field::store fails closed on overflow of col_a (Codex parity with the
      // carrier-overflow guard below): a group sum exceeding the column's
      // precision would be silently clipped.
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
      continue;
    }
    // Exact decomposition of S (scale<=4) into a*(1-b) contributions with
    // a, b at scale 2:  S = S2 + r,  S2 = trunc2(S), |r| < 0.01.
    //   row1: a=S2,  b=0.00              -> contributes S2 exactly
    //   row2 (r>0): a=0.01,  b=1-100r    -> 0.01*(100r) = r exactly
    //   row2 (r<0): a=-0.01, b=1+100r    -> -0.01*(-100r) = r exactly
    my_decimal S, S2, r;
    str2my_decimal(E_DEC_FATAL_ERROR, fv[2].data(), fv[2].size(),
                   &my_charset_bin, &S);
    my_decimal_round(E_DEC_FATAL_ERROR, &S, 2, true /*truncate*/, &S2);
    my_decimal_sub(E_DEC_FATAL_ERROR, &r, &S, &S2);
    dec_str.length(0);
    my_decimal2string(E_DEC_FATAL_ERROR, &S2, &dec_str);
    const std::string a1(dec_str.ptr(), dec_str.length());
    // Carrier overflow fails CLOSED (Codex P1-1): a group sum whose integer
    // digits exceed col_a's precision would be silently clipped by
    // Field::store. col_a is DECIMAL(p,2): max integer digits = p - 2;
    // a1 holds sign + digits + '.' + 2 frac digits.
    {
      size_t digits = 0;
      for (char ch : a1) {
        if (ch == '.') break;
        if (ch >= '0' && ch <= '9') ++digits;
      }
      const uint max_int_digits =
          table->field[reg->col_a]->field_length >= 5
              ? static_cast<uint>(table->field[reg->col_a]->field_length) - 4
              : 0;  // signed DECIMAL(p,2): field_length = p+2, int digits = p-2
      if (digits > max_int_digits) {
        my_error(ER_INTERNAL_ERROR, MYF(0), "helios gs carrier overflow");
        return HA_ERR_INTERNAL_ERROR;
      }
    }
    emit_row(gkey, a1, "0.00");
    if (!my_decimal_is_zero(&r)) {
      my_decimal r100, hundred, one, b2;
      int2my_decimal(E_DEC_FATAL_ERROR, 100, false, &hundred);
      int2my_decimal(E_DEC_FATAL_ERROR, 1, false, &one);
      my_decimal_mul(E_DEC_FATAL_ERROR, &r100, &r, &hundred);
      const bool neg = r.sign();
      if (!neg) {
        my_decimal_sub(E_DEC_FATAL_ERROR, &b2, &one, &r100);   // 1 - 100r
      } else {
        my_decimal_add(E_DEC_FATAL_ERROR, &b2, &one, &r100);   // 1 + 100r
      }
      dec_str.length(0);
      my_decimal2string(E_DEC_FATAL_ERROR, &b2, &dec_str);
      emit_row(gkey, neg ? "-0.01" : "0.01",
               std::string(dec_str.ptr(), dec_str.length()));
    }
  }
  scan_exhausted_ = true;
  if (std::getenv("HELIOS_FE_DEBUG"))
    std::fprintf(stderr, "[GS] served groups=%zu rows=%zu\n", groups.size(),
                 scanned_values_.size());
  return 0;
}
bool ha_lineairdb::tx_is_aborted() {
  auto tx = get_transaction(ha_thd());
  return tx == nullptr || tx->is_aborted();
}

bool ha_lineairdb::tx_ro_novalidate() {
  // The override decides Phase B BEFORE the read path begins the tx (which is
  // where ro_novalidate_ gets set), so consult the sysvar gates directly; the
  // SQLCOM_SELECT / read-only half is already guaranteed by the offload
  // whitelist. Phase B additionally requires PREFETCH mode: its group rows
  // arrive via the staged read plan, while the stateful scan path returns
  // BASE rows that the group-row parser would (loudly) reject as malformed.
  return srv_prefetch_execution && srv_prefetch_ro_novalidate;
}
void ha_lineairdb::tx_clear_pushed_aggregate() {
  get_transaction(ha_thd())->clear_pushed_aggregate();
}

// Phase-8 Phase B: like rnd_next, but hand back the raw cached row VALUE bytes
// (a server-produced group row) instead of unpacking into a record buffer.
bool ha_lineairdb::agg_next_raw(std::string_view *out_value) {
  // (The old branch had a zero-copy borrowed-scan fast path here; this branch
  // materializes scan rows into scanned_values_, so serve from there.)
  // Surface staging aborts: without this, a failed/missed aggregate staging
  // would read as an EMPTY but successful aggregate (Codex P2).
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
    // Phase-22: default-ON (companion to HELIOS_COST_V2; the cost model needs
    // the per-index NDV/rec_per_key it loads). Disabled by HELIOS_OPT_STATS=0.
    static const bool opt_stats_on = helios::gate_default_on("HELIOS_OPT_STATS");
    const bool need_rowcount =
        share->stats_base_records.load(std::memory_order_relaxed) == 0;
    // NDV staleness (plan-stability fix): refetch when the row count has
    // drifted >20% from the count at NDV-fetch time. Trigger on SELECT
    // statements only — refetching during bulk loads would recompute the
    // NDV over a growing table once per drift step. The first SELECT after
    // a load settles the (records, NDV) pair once, deterministically.
    bool ndv_stale = false;
    if (opt_stats_on &&
        share->index_ndv_loaded_.load(std::memory_order_relaxed)) {
      const uint64_t at_fetch =
          share->index_ndv_records_.load(std::memory_order_relaxed);
      const uint64_t now_base =
          share->stats_base_records.load(std::memory_order_relaxed);
      const uint64_t hi = std::max(at_fetch, now_base);
      const uint64_t lo = std::min(at_fetch, now_base);
      THD *sthd = ha_thd();
      const bool is_select = sthd != nullptr && sthd->lex != nullptr &&
                             sthd->lex->sql_command == SQLCOM_SELECT;
      ndv_stale = is_select && hi > 0 && (hi - lo) * 5 > hi;  // >20% drift
      if (ndv_stale) {
        share->index_ndv_force_refresh_.store(true, std::memory_order_relaxed);
        share->index_ndv_loaded_.store(false, std::memory_order_relaxed);
      }
    }
    const bool need_ndv =
        opt_stats_on &&
        !share->index_ndv_loaded_.load(std::memory_order_relaxed);
    if ((need_rowcount || need_ndv) && !db_table_name.empty()) {
      THD *thd = ha_thd();
      if (thd != nullptr) {
        LineairDBThdCtx *&ctx =
            *reinterpret_cast<LineairDBThdCtx **>(thd_ha_data(thd, lineairdb_hton));
        // True cold optimizer path (EXPLAIN / first statement before any
        // table access): no THD ctx or proxy exists yet, so the stats RPC
        // below would silently never run (Codex review). With the stats gate
        // ON, create the proxy lazily — that RPC is the gate's whole point.
        // Gate OFF keeps the old behavior (no connection from info()).
        if (opt_stats_on && ctx == nullptr) ctx = new LineairDBThdCtx();
        if (opt_stats_on && ctx != nullptr && !ctx->proxy) {
          std::string host =
              srv_server_host ? srv_server_host : std::string("127.0.0.1");
          ctx->proxy = std::make_shared<LineairDBProxy>(
              host, static_cast<int>(srv_server_port));
        }
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
          // Pre-existing (UNGATED) path: seed the row count from the
          // begin/end table_stats piggyback cache. Must run regardless of
          // HELIOS_OPT_STATS — gating it regressed the default row-count
          // seeding (Codex review).
          const bool seeded = find_seed();
          // Access-path fix: the begin/end table_stats piggyback misses at
          // optimize time under oneshot (deferred tx_begin), so the optimizer
          // would otherwise see stats.records==2 and pick full-scan + bad join
          // order. On a cache-miss, do a transaction-less GET_TABLE_STATS RPC
          // once, then seed the GLOBAL share (persists across connections).
          // GATED (HELIOS_OPT_STATS, Phase-22 default-ON; disable with
          // HELIOS_OPT_STATS=0). Phase 2: fetch the real row
          // count AND per-index NDV in one RPC, then seed the GLOBAL share.
          // rec_per_key is then computed from real NDV (see set_generic_rec_per_key)
          // instead of the n-th-root heuristic that mis-treated non-unique FK
          // joins as 1:1 -> Q5 explosion. (docs/phase7_optimizer_stats.md)
          if (opt_stats_on &&
              (!seeded ||
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
            if (ctx->proxy->fetch_table_stats(db_table_name, descs, force)) {
              find_seed();
              std::lock_guard<std::mutex> g(share->index_ndv_mu_);
              share->index_ndv_.clear();
              for (const auto &kv : ctx->proxy->last_index_ndv()) {
                if (kv.second.first)  // available
                  share->index_ndv_[kv.first] = kv.second.second;
              }
              // Phase-22: seed the per-index range histogram from the SAME fetch
              // (independent of NDV availability; DATE indexes get a histogram but
              // no NDV). Only well-formed, monotone snapshots are admitted.
              share->index_hist_.clear();
              for (const auto &kv : ctx->proxy->last_index_hist()) {
                const auto &h = kv.second;
                if (!h.available || h.bounds.empty() ||
                    h.bounds.size() != h.cum.size())
                  continue;
                // defense-in-depth (review r-impl): admit only a well-formed
                // snapshot — monotone non-decreasing cum AND ascending bounds.
                // (A non-snapshot-consistent two-pass build under concurrent
                // writes could otherwise publish a non-monotone array; read-only
                // TPC-H never hits this, but reject it rather than trust it.)
                bool mono = true;
                for (size_t i = 1; i < h.cum.size() && mono; i++)
                  if (h.cum[i] < h.cum[i - 1] || h.bounds[i] < h.bounds[i - 1])
                    mono = false;
                if (!mono) continue;
                LineairDB_share::RangeHist rh;
                rh.bounds = h.bounds;
                rh.cum = h.cum;
                share->index_hist_[kv.first] = std::move(rh);
              }
              if (std::getenv("HELIOS_STATS_DEBUG") != nullptr) {
                size_t recv = ctx->proxy->last_index_hist().size();
                fprintf(stderr,
                        "[RANGEHIST] seed %s: received=%zu seeded=%zu\n",
                        db_table_name.c_str(), recv, share->index_hist_.size());
              }
              // Remember the row count this NDV snapshot belongs to (the
              // staleness check above compares future counts against it).
              share->index_ndv_records_.store(
                  share->stats_base_records.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
              share->index_ndv_loaded_.store(true, std::memory_order_relaxed);
              if (std::getenv("HELIOS_STATS_DEBUG") != nullptr) {
                std::string dbg = "[STATS] ndv fetch " + db_table_name +
                                  " records=" +
                                  std::to_string(share->stats_base_records.load(
                                      std::memory_order_relaxed));
                for (const auto &kv : share->index_ndv_) {
                  dbg += " " + (kv.first.empty() ? std::string("PRIMARY")
                                                 : kv.first) + "=[";
                  for (uint64_t v : kv.second) dbg += std::to_string(v) + ",";
                  dbg += "]";
                }
                std::fprintf(stderr, "%s\n", dbg.c_str());
              }
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
    if (std::getenv("HELIOS_STATS_DEBUG") != nullptr) {
      std::fprintf(stderr,
                   "[STATS] info %s records=%lld (base=%lld delta=%lld) "
                   "ndv_loaded=%d ndv_at=%llu stale_refetch=%d\n",
                   db_table_name.c_str(), (long long)total, (long long)base,
                   (long long)delta_sum,
                   (int)share->index_ndv_loaded_.load(std::memory_order_relaxed),
                   (unsigned long long)share->index_ndv_records_.load(
                       std::memory_order_relaxed),
                   (int)ndv_stale);
    }

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
    auto it = share->index_ndv_.find(
        is_primary ? std::string() : std::string(key->name ? key->name : ""));
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
      // Inner-unit aggregate registrations are statement-scoped: a stale
      // registration could stamp a LATER statement's scan step.
      (*ctx_slot)->tx->clear_inner_aggregates();
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
// Phase-22 D1: decode a fixed-width integer key part from a range endpoint's key
// buffer via the official key_restore path (signedness / key-format / byte-order
// safe; raw byte reads are not). Restores into the scratch record[1] so record[0]
// is untouched, then reads the field there via move_field_offset. Returns false on
// any shape we don't handle -> caller falls back to the /2 heuristic.
static bool helios_decode_keypart_int(TABLE *table, KEY *key, uint part,
                                      const key_range *kr, longlong *out) {
  if (table == nullptr || kr == nullptr || kr->key == nullptr) return false;
  if (part >= key->user_defined_key_parts) return false;
  const KEY_PART_INFO &kp = key->key_part[part];
  Field *f = kp.field;
  if (f == nullptr || f->result_type() != INT_RESULT) return false;  // fixed int only
  if (f->is_nullable()) return false;                   // skip null-marker complexity
  if (kp.key_part_flag & HA_REVERSE_SORT) return false; // ascending only
  uchar *scratch = table->record[1];
  if (scratch == nullptr) return false;
  key_restore(scratch, const_cast<uchar *>(kr->key), key, kr->length);
  const ptrdiff_t delta = static_cast<ptrdiff_t>(scratch - table->record[0]);
  f->move_field_offset(delta);
  *out = f->val_int();
  f->move_field_offset(-delta);
  return true;
}

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
    // Range after the equality prefix. Phase-22 D1: the eq prefix bounds the
    // group; the trailing RANGE must narrow it. Averaged rec_per_key can't see a
    // range, so the old /2 returned the WHOLE prefix group (order_line PK
    // (w,d,o,n): rec_per_key[1]=records/NDV(w,d)~=30000, /2~=17802 for a 20-order
    // window vs ~273 actual = 66x over -> 38x TPC-C collapse on the OPT_STATS join
    // path). Estimate instead, InnoDB-index-dive style (SOTA survey + Codex GO):
    //   rows ~= (#trailing key-values in [lo,hi]) * rec_per_key[eq_parts]
    // where rec_per_key[eq_parts] = avg rows per distinct trailing value. Only
    // under HELIOS_OPT_STATS (NDV-real rec_per_key); fail-closed to /2 on any
    // non-int / nullable / descending / missing-endpoint / decode miss.
    if (eq_parts < key_parts_used) {
      static const bool trail_on =
          helios::gate_default_on("HELIOS_OPT_STATS") &&
          helios::gate_default_on("HELIOS_TRAIL_RANGE");
      longlong lo = 0, hi = 0;
      if (trail_on && eq_parts < key->user_defined_key_parts &&
          key->rec_per_key[eq_parts] > 0 &&
          helios_decode_keypart_int(table, key, eq_parts, min_key, &lo) &&
          helios_decode_keypart_int(table, key, eq_parts, max_key, &hi) &&
          hi >= lo) {
        const double rpk_deep = static_cast<double>(key->rec_per_key[eq_parts]);
        // #distinct trailing values in the range (endpoint inclusivity is within
        // estimate noise; floor 1 covers hi==lo / half-open).
        const double range_vals = (hi > lo) ? static_cast<double>(hi - lo) : 1.0;
        double est = range_vals * rpk_deep;
        const double cap = static_cast<double>(key->rec_per_key[rpk_idx]);
        if (cap >= 1.0 && est > cap) est = cap;  // can't exceed the eq-prefix group
        if (est < 1.0) est = 1.0;                // floor 1 (Codex: no lower clamp)
        estimate = static_cast<ha_rows>(est);
      } else {
        estimate = std::max(static_cast<ha_rows>(1), estimate / 2);  // fallback
      }
    }
  } else {
    // No equality at all -> pure range scan on the LEADING column.
    // Heuristic baseline: two-sided ~5%, one-sided ~10% (NDB fallback).
    if (min_key != nullptr && max_key != nullptr) {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 20);
    } else {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 10);
    }
    // Phase-22 (HELIOS_RANGE_HIST, default OFF): replace the distribution-blind flat
    // fraction with a server-built EQUI-DEPTH boundary histogram on the leading key
    // part (fixes the dominant one-sided q3/q10 underestimate). Applied as a FLOOR
    // (never below the heuristic) so it can only RAISE an underestimate, never lower
    // a correct-or-high one (grounded-reviewer arbitration). READ-ONLY over
    // SHARE-resident state seeded in info() -> ZERO plan-time RPC. Fail-closed to the
    // heuristic on any absence/size/encoding mismatch. Step estimator (B buckets):
    // inclusive/exclusive bound nuance is within bucket granularity (feasibility).
    // Phase-22: kept OPT-IN (default-OFF). The fullidx/noidx sweep showed it is
    // NEUTRAL in the default (AGG-off) deployment (its q3/q10 q-error benefit only
    // manifests in the AGG-on analytical regime, where it took the suite
    // 31.75->28.42s), and Codex (#2) cautioned that a floor-only estimate bump
    // can still flip plans on unmeasured workloads. So it rides with
    // HELIOS_AGG_PUSHDOWN as an analytical opt-in (enable with HELIOS_RANGE_HIST=1).
    // FLOOR-only + fail-closed to the heuristic on any absence/encoding mismatch.
    static const bool hist_gate = helios::gate_default_off("HELIOS_RANGE_HIST");
    if (hist_gate && key->name != nullptr && share != nullptr) {
      std::lock_guard<std::mutex> g(share->index_ndv_mu_);
      auto hit = share->index_hist_.find(key->name);
      if (hit != share->index_hist_.end()) {
        const LineairDB_share::RangeHist &h = hit->second;
        if (!h.bounds.empty() && h.bounds.size() == h.cum.size()) {
          const double total_live = static_cast<double>(h.cum.back());
          // # live rows whose leading-part <= ek (step; ascending bounds, monotone cum)
          auto rank_le = [&](const std::string &ek) -> double {
            if (ek >= h.bounds.back()) return static_cast<double>(h.cum.back());
            auto it = std::upper_bound(h.bounds.begin(), h.bounds.end(), ek);
            if (it == h.bounds.begin()) return 0.0;
            return static_cast<double>(h.cum[(it - h.bounds.begin()) - 1]);
          };
          const key_part_map lead = 1;  // leading column only
          double lo = 0.0, hi = total_live;
          bool enc_ok = true;  // a PRESENT bound that fails to encode => fail closed
          if (min_key != nullptr) {
            std::string ek = lineairdb_keyenc::convert_key_to_ldbformat(
                table, inx, min_key->key, lead);
            if (ek.empty()) enc_ok = false; else lo = rank_le(ek);
          }
          if (enc_ok && max_key != nullptr) {
            std::string ek = lineairdb_keyenc::convert_key_to_ldbformat(
                table, inx, max_key->key, lead);
            if (ek.empty()) enc_ok = false; else hi = rank_le(ek);
          }
          const double est = enc_ok ? (hi - lo) : -1.0;  // -1 => keep heuristic
          if (std::getenv("HELIOS_STATS_DEBUG") != nullptr) {
            fprintf(stderr,
                    "[RANGEHIST] idx=%s bounds=%zu total=%.0f lo=%.0f hi=%.0f "
                    "est=%.0f heuristic=%lld\n",
                    key->name, h.bounds.size(), total_live, lo, hi, est,
                    (long long)estimate);
          }
          if (est >= 1.0 && static_cast<ha_rows>(est) > estimate)
            estimate = static_cast<ha_rows>(est);  // FLOOR: only ever raise
        }
      } else if (std::getenv("HELIOS_STATS_DEBUG") != nullptr) {
        fprintf(stderr, "[RANGEHIST] idx=%s NO histogram in share (count=%zu)\n",
                key->name, share->index_hist_.size());
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

bool ha_lineairdb::backfill_commit_chunk(
    std::vector<LineairDBProxy::BatchOp> &ops) {
  if (ops.empty()) return true;

  // Each chunk is its OWN transaction (NOT the ALTER statement's ctx->tx): the
  // server's WriteSecondaryIndex does a per-write linear scan of the tx
  // read_set_/write_set_ (transaction_impl.cpp:359-386), so accumulating all
  // backfill rows in one transaction is O(rows^2). A fresh transaction per
  // chunk bounds each scan to the chunk size -> O(rows * chunk). FENCE
  // (== false) matches ctx->tx, so no per-chunk durability fence. Sequential
  // synchronous commits keep correctness: non-unique PK-list adds merge-commute
  // and a cross-chunk UNIQUE duplicate aborts against the persistent
  // already-initialized index leaf.
  auto *chunk_tx = new LineairDBTransaction(ha_thd(), get_proxy(),
                                            lineairdb_hton, FENCE);
  chunk_tx->set_prefetch_mode(false);
  chunk_tx->begin_transaction();
  chunk_tx->choose_table(db_table_name);
  for (auto &op : ops) {
    chunk_tx->buffer_write_secondary_index(op.table_name, op.index_name,
                                           op.secondary_key, op.primary_key);
    if (chunk_tx->is_aborted()) break;  // UNIQUE dup against committed data
  }
  ops.clear();
  // end_transaction() flushes the buffer, runs the OCC commit, and deletes the
  // transaction object (so chunk_tx must not be used afterwards).
  return chunk_tx->end_transaction();
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

  // Phase-21 Step-2: CREATE INDEX backfill. The index is registered empty on
  // the server, then populated from the existing rows by scanning the table and
  // reusing the SAME canonical path the load uses (build_secondary_key_from_row
  // + the secondary-index write), so the backfilled keys are byte-identical to
  // a load-built index (zero encoding drift). ADD INDEX holds an EXCLUSIVE lock
  // (check_if_supported_inplace_alter), so the committed row set is invariant
  // during the build and a single offline scan is correct.
  //
  // The base-row SCAN runs in the ALTER statement's transaction (reads only;
  // its commit validates those reads at DDL end). The SI WRITES are committed
  // in independent kBackfillWriteChunkRows-sized transactions
  // (backfill_commit_chunk) instead of the ALTER transaction, because the
  // server's per-write WriteSecondaryIndex scan makes a single all-rows commit
  // O(rows^2). Success is returned only after the whole scan + all chunk commits
  // finish, so MySQL commits the index to its data dictionary (optimizer-visible)
  // only when it is fully built; a failed chunk commit (e.g. a UNIQUE duplicate)
  // fails the DDL and leaves the index invisible.
  //
  // The per-write WriteSecondaryIndex scan is O(distinct keys in the open chunk
  // transaction), so a smaller write chunk keeps it small for high-cardinality
  // (composite) indexes.
  //
  // NOTE: read-chunking (bounded PK ranges) was tried and removed. The scan
  // response uses a flat binary codec with NO 2GB protobuf limit, and the
  // server's unbounded-end Scan (transaction_impl.cpp:565) collects ALL
  // remaining keys per call regardless of row_limit (row_limit caps only the
  // returned rows, not the server-side key collection). So bounded reads added
  // O(rows^2 / read_chunk) server work and did NOT bound server memory -- a
  // single ordered full-table scan (O(rows) once) is both correct and faster.
  // Bounding the server-side scan would need a true limited-scan path in
  // LineairDB core (relax the line-565 early-stop to honor row_limit on an
  // unbounded end); that is a separate optimization.
  static const uint64_t kBackfillWriteChunkRows = 2000;

  // On any add/backfill failure, drop the SIs this ALTER already created so the
  // engine matches MySQL's data-dictionary rollback (which discards a failed
  // ALTER's indexes) instead of keeping an orphaned, succeeded-then-rolled-back
  // index. (A single-statement same-name modify DROP X|ADD X still loses the old
  // X if the new X's backfill fails -- a non-transactional-DDL limit that needs
  // temp-name staging to close; rare and not exercised by TPC-H. Codex Step-3
  // impl-review HIGH-2, documented.) Codex Step-3 impl-review HIGH-1.
  std::vector<std::string> created_this_alter;
  auto fail_alter = [&]() {
    for (const std::string &n : created_this_alter)
      proxy->db_drop_secondary_index(db_table_name, n);
    return true;
  };

  for (uint i = 0; i < ha_alter_info->index_add_count; i++) {
    const uint buf_idx = ha_alter_info->index_add_buffer[i];
    const KEY *def_key = &ha_alter_info->key_info_buffer[buf_idx];
    const std::string idx_name(def_key->name);
    const uint index_type = (def_key->flags & HA_NOSAME) ? LDB_INDEX_UNIQUE : 0;

    // CRITICAL: key_info_buffer uses a 0-based KEY_PART_INFO::fieldnr, but
    // build_secondary_key_from_row reads table->field[fieldnr - 1] (the 1-based
    // runtime TABLE::key_info convention). Passing the buffer KEY as-is would
    // read one column to the left and silently build a corrupt index. Resolve
    // the runtime KEY by NAME from altered_table->key_info (as InnoDB does);
    // index_add_buffer entries are offsets into key_info_buffer, NOT indices
    // into altered_table->key_info.
    const KEY *new_key = nullptr;
    for (uint k = 0; k < altered_table->s->keys; k++) {
      if (altered_table->key_info[k].name != nullptr &&
          idx_name == altered_table->key_info[k].name) {
        new_key = &altered_table->key_info[k];
        break;
      }
    }
    if (new_key == nullptr) {
      // The added index must exist in the new table definition; refuse to build
      // a wrong index rather than guess.
      return fail_alter();
    }

    // Phase-21 Step-3 (clean-slate / FIX-5): drop any pre-existing same-name
    // index BEFORE creating the fresh one. CreateSecondaryIndex is a no-op on an
    // existing name, so without this an index left from a prior failed/repeated
    // CREATE (dirty server) or a single-statement same-name modify (DROP X|ADD X)
    // would be backfilled on top -> double/wrong entries. Idempotent (no-op if
    // absent). Pure drops (names NOT re-added) are processed AFTER all adds
    // succeed (below) so an add/backfill failure cannot leave a MySQL-visible
    // index missing on the engine (the Codex Step-3 review BLOCKER).
    proxy->db_drop_secondary_index(db_table_name, idx_name);
    proxy->db_create_secondary_index(db_table_name, idx_name, index_type);
    created_this_alter.push_back(idx_name);

    // Read the whole table in one ordered scan under the EXCLUSIVE lock through
    // the ALTER transaction (reads only; its commit validates those reads at
    // DDL end). For each row reconstruct the record and emit an SI write into
    // the open write chunk; commit each kBackfillWriteChunkRows-sized chunk as
    // its own transaction.
    auto scan_tx = get_transaction(ha_thd());
    if (scan_tx->is_aborted()) return fail_alter();
    scan_tx->choose_table(db_table_name);
    auto rows = scan_tx->get_matching_keys_and_values_from_prefix(std::string());
    if (scan_tx->is_aborted()) return fail_alter();

    bool failed = false;
    std::vector<LineairDBProxy::BatchOp> write_chunk;
    write_chunk.reserve(kBackfillWriteChunkRows);
    for (auto &kv : rows) {
      if (kv.second.empty()) continue;  // tombstone (server already skips)
      if (set_fields_from_lineairdb(
              table->record[0],
              reinterpret_cast<const std::byte *>(kv.second.data()),
              kv.second.size()) != 0) {
        failed = true;
        break;
      }
      LineairDBProxy::BatchOp op;
      op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexWrite;
      op.table_name = db_table_name;
      op.index_name = idx_name;
      op.primary_key = kv.first;  // stored PK == canonical extract_key output
      op.secondary_key = build_secondary_key_from_row(table->record[0], *new_key);
      write_chunk.push_back(std::move(op));
      if (write_chunk.size() >= kBackfillWriteChunkRows) {
        if (!backfill_commit_chunk(write_chunk)) {  // aborts (e.g. UNIQUE dup)
          failed = true;
          break;
        }
      }
    }
    // Free any blob memory set_fields_from_lineairdb allocated during the scan.
    blobroot.Clear();

    if (failed || scan_tx->is_aborted()) return fail_alter();
    // Commit the final partial chunk.
    if (!backfill_commit_chunk(write_chunk)) return fail_alter();
  }

  // Phase-21 Step-3: process pure DROP INDEX (names dropped but NOT re-added) —
  // e.g. a user DROP INDEX, or the FK-auto-index removed when CREATE INDEX
  // replaces it (MySQL sends DROP old-name | ADD new-name together). Done AFTER
  // all adds succeed so an add/backfill failure above (return true) leaves these
  // indexes intact and MySQL's DD rollback stays consistent with the engine
  // (Codex Step-3 review BLOCKER). A same-name DROP X | ADD X (modify) was
  // already cleared+rebuilt by the add loop's drop-if-exists, so it is skipped
  // here. EXCLUSIVE lock => no concurrent DML. index_drop_buffer[j] is a KEY*
  // into the OLD table def; read its name directly (unlike index_add_buffer,
  // which is an offset into key_info_buffer).
  for (uint j = 0; j < ha_alter_info->index_drop_count; j++) {
    const KEY *drop_key = ha_alter_info->index_drop_buffer[j];
    if (drop_key == nullptr || drop_key->name == nullptr) continue;
    const std::string drop_name(drop_key->name);
    bool re_added = false;
    for (uint i = 0; i < ha_alter_info->index_add_count; i++) {
      const KEY *add_key =
          &ha_alter_info->key_info_buffer[ha_alter_info->index_add_buffer[i]];
      if (add_key->name != nullptr && drop_name == add_key->name) {
        re_added = true;
        break;
      }
    }
    if (re_added) continue;  // cleared+rebuilt by the add loop's drop-if-exists
    proxy->db_drop_secondary_index(db_table_name, drop_name);  // idempotent
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

// Phase-22 M2 gate for read_cost()'s per-row materialisation charge. Returns
// true when the non-covering scan really materialises base rows per-row to the
// proxy (-> charge), false when the rows are bulk-prefetched / server-aggregated
// (-> skip). Reads ONLY prepare-time Query_block + THD state (group/leaf shape),
// which exists at best_access_path/read_cost time (JOIN::optimize make_join_plan);
// it must NOT touch tx aggregate state or the finalized QEP, which are set later
// at push_to_engine. The "single-table grouped read-only SELECT under prefetch"
// shape is the necessary precondition shared by helios_try_register_gs /
// helios_offloadable_shape, so the cost model and the executor agree on which
// scans are per-row-materialised. Conservative default (missing context) = charge
// (today's proven M1 pricing -> never under-prices a real per-row join scan).
bool ha_lineairdb::helios_charge_materialise(uint index) const {
  const TABLE *t = table;
  if (t == nullptr || t->in_use == nullptr) return true;
  // Phase-22 A2a: a clustered-PRIMARY-KEY range/ref has NO index->PK double-hop
  // (the PK scan brings the row directly; helios_ref_cost's transfer term already
  // covers it), so it must NOT pay the per-row PK materialisation RPC. Without
  // this, a tiny-table OLTP PK UPDATE (warehouse 1-2 rows / district 10) was
  // priced ABOVE a full scan (PK 58.6 > scan 53.1), flipped to type=ALL, and then
  // rejected by the prefetch path ("legacy DML full/reverse table scan") -> the
  // TPC-C goodput collapse. TPC-H-orthogonal: non-covering SECONDARY refs
  // (index != primary_key) still charge; eq_ref NLJ is priced at
  // sql_planner.cc:433 via engine_cost, never through read_cost/this gate.
  if (t->s != nullptr && t->s->primary_key != MAX_KEY &&
      index == t->s->primary_key)
    return false;
  THD *thd = t->in_use;
  if (thd->lex == nullptr) return true;
  if (thd->lex->sql_command != SQLCOM_SELECT || thd->lex->is_explain())
    return true;                                    // DML / EXPLAIN -> per-row
  if (t->reginfo.lock_type > TL_READ) return true;  // locked read -> not bulk-staged
  if (!lineairdb_predict_prefetch_mode(thd)) return true;  // no bulk prefetch
  Table_ref *tr = t->pos_in_table_list;
  if (tr == nullptr) return true;
  Query_block *qb = tr->query_block;
  if (qb == nullptr) return true;
  // Single-table grouped read-only SELECT = the bulk-staged / server-aggregated
  // shape (q15 view body, q1/q6): rows are NOT per-row materialised to the proxy.
  if (qb->leaf_table_count != 1 || !qb->is_grouped()) return true;
  if (qb->is_distinct() || qb->olap != UNSPECIFIED_OLAP_TYPE || qb->has_windows())
    return true;
  return false;  // bulk/agg-served -> skip the materialisation charge
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

  // Projection pushdown: if this table's cached VALUES were trimmed to a
  // subset of columns, the parsed row holds only the kept columns in order;
  // map the k-th present column to field index kept[k]. Non-kept fields are
  // left untouched (MySQL won't read columns outside read_set; projection is
  // only planned for ro_novalidate SELECT, so no DML ever rebuilds a row from
  // this buffer). null flags stay FULL, so is_null_in_record(buf) is correct.
  //
  // Memoized per statement: set_fields is the per-row chokepoint and the tx
  // lookup + string-keyed map find per row cost every mode ~5% OLTP. The tx
  // (and thus the pointee) outlives the statement; query_id change refreshes.
  THD *const thd_for_serve = ha_thd();
  const uint64_t serve_query_id =
      thd_for_serve != nullptr
          ? static_cast<uint64_t>(thd_for_serve->query_id)
          : 0;
  // Epoch guard: optimize-time unit serves can stamp the memo BEFORE the
  // statement-root staging registers projection layouts; any registration
  // bumps the process-wide epoch and forces a refresh.
  const uint64_t proj_epoch = LineairDBTransaction::projection_global_epoch();
  if (serve_memo_query_id_ != serve_query_id ||
      serve_memo_proj_epoch_ != proj_epoch) {
    serve_memo_query_id_ = serve_query_id;
    serve_memo_proj_epoch_ = proj_epoch;
    auto tx = get_transaction(thd_for_serve);
    serve_memo_projection_ =
        tx != nullptr ? tx->table_projection(db_table_name) : nullptr;
    // Skipping Field::store for non-read_set columns is sound ONLY for a pure
    // SELECT serve: DML reuses this row buffer to rebuild the old row and ALL
    // secondary keys, and MySQL may leave untouched columns out of read_set
    // (the engine does not advertise HA_PARTIAL_COLUMN_READ).
    serve_memo_select_ = thd_for_serve != nullptr &&
                         thd_for_serve->lex != nullptr &&
                         thd_for_serve->lex->sql_command == SQLCOM_SELECT;
  }
  const std::vector<uint32_t> *kept = serve_memo_projection_;
  // Use the projected mapping ONLY when the value actually has the projected
  // column count: self-corrects against a full row that slipped into a
  // projected table (unit-episode staging ships full rows).
  if (kept != nullptr && ldbField.get_row_size() != kept->size()) {
    kept = nullptr;
  }
  const bool select_serve = serve_memo_select_;

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

  /**
   * store each column value to corresponding field (full row). Text->binary
   * re-parse (Field::store) only happens for columns the statement reads —
   * the per-serve read_set skip is the set_fields chokepoint win (the old
   * branch measured 13.1M calls / 5.9s on q21 SF=1 before it).
   */
  size_t columnIndex = 0;
  for (Field **field = table->field; *field; field++) {
    if (columnIndex >= ldbField.get_row_size()) break;  // short/trimmed value
    const auto mysqlFieldValue = ldbField.get_column_of_row(columnIndex++);
    if (select_serve &&
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
    "analytical read-only workloads); the commit RPC is elided entirely.",
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
