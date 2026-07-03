#include "ha_lineairdb_columnar.hh"

#include <cassert>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "lex_string.h"
#include "my_alloc.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/item_timefunc.h"
#include "sql/nested_join.h"
#include "sql/join_optimizer/explain_access_path.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/walk_access_paths.h"
#include "sql/join_optimizer/relational_expression.h"
#include "sql/join_optimizer/materialize_path_parameters.h"
#include "sql/mem_root_array.h"
#include "sql/query_result.h"
#include "sql/sql_class.h"
#include "sql/sql_const.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "sql/visible_fields.h"
#include "template_utils.h"
#include "thr_lock.h"

#include "lineairdb.pb.h"
#include "lineairdb_keyenc.hh"
#include "lineairdb_proxy.hh"
#include "lineairdb_pushdown.hh"

// Shared THD-scoped RPC connection, owned by the primary engine
// (see ha_lineairdb.cc). Both engines talk to the same LineairDB server.
std::shared_ptr<LineairDBProxy> lineairdb_acquire_shared_proxy(THD *thd);
// Aggregate-argument arithmetic serializer (ha_lineairdb.cc).
bool helios_serialize_arith(const Item *it,
                            LineairDB::Protocol::FilterExpr *out);

namespace lineairdb_columnar {

namespace {

// ---------------------------------------------------------------------------
// SECONDARY_LOAD registry. With single-copy PAX there is nothing to copy at
// load time: primary and secondary share the LineairDB storage, so loading
// is pure bookkeeping (mirrors the mock engine's LoadedTables).
// ---------------------------------------------------------------------------
class LoadedTables {
 public:
  void add(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.emplace(std::piecewise_construct,
                     std::forward_as_tuple(std::make_pair(db, table)),
                     std::forward_as_tuple());
  }
  bool contains(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    return m_tables.count(std::make_pair(db, table)) > 0;
  }
  THR_LOCK *lock(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    auto it = m_tables.find(std::make_pair(db, table));
    return it == m_tables.end() ? nullptr : &it->second.thr_lock;
  }
  void erase(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.erase(std::make_pair(db, table));
  }

 private:
  struct TableInfo {
    THR_LOCK thr_lock;
    TableInfo() { thr_lock_init(&thr_lock); }
    ~TableInfo() { thr_lock_delete(&thr_lock); }
    TableInfo(const TableInfo &) = delete;
    TableInfo &operator=(const TableInfo &) = delete;
  };
  std::mutex m_mutex;
  std::map<std::pair<std::string, std::string>, TableInfo> m_tables;
};

LoadedTables *loaded_tables{nullptr};

// ---------------------------------------------------------------------------
// Result value carrier. The result-set metadata has already been sent from
// the original field list; send_data only serializes values, so a
// type-tagged Item whose val_str() holds the server-computed text
// representation round-trips every type through the text protocol (numeric
// and temporal types go through the generic string->native conversions).
// ---------------------------------------------------------------------------
class Item_columnar_value final : public Item_string {
 public:
  explicit Item_columnar_value(const Item *proto)
      : Item_string("", 0, proto->collation.collation) {
    set_data_type(proto->data_type());
    decimals = proto->decimals;
    max_length = proto->max_length;
    unsigned_flag = proto->unsigned_flag;
    set_nullable(true);
  }
  void set_value(const char *ptr, size_t len) {
    str_value.copy(ptr, len, collation.collation);
    null_value = false;
  }
  void set_null_value() { null_value = true; }

  // Item::send asserts null_value == (val_str() == nullptr); honor SQL
  // NULL across every access path.
  String *val_str(String *str) override {
    return null_value ? nullptr : Item_string::val_str(str);
  }
  double val_real() override {
    return null_value ? 0.0 : Item_string::val_real();
  }
  longlong val_int() override {
    return null_value ? 0 : Item_string::val_int();
  }
  my_decimal *val_decimal(my_decimal *dec) override {
    return null_value ? nullptr : Item_string::val_decimal(dec);
  }
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t flags) override {
    return null_value ? true : Item_string::get_date(ltime, flags);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return null_value ? true : Item_string::get_time(ltime);
  }
};

// One output column of the offloaded aggregate: either a GROUP BY column
// (index into AggregateSpec::group_columns) or an aggregate (index into
// AggregateSpec::aggs).
struct OutBinding {
  bool is_aggregate;
  int index;
};

// Recognized query-block plan + per-execution state, kept on the statement
// MEM_ROOT via LEX::secondary_engine_execution_context.
class Columnar_execution_context : public Secondary_engine_execution_context {
 public:
  // Cost bookkeeping (traditional-optimizer CompareJoinCost contract).
  bool BestPlanSoFar(const JOIN &join, double cost) {
    if (&join != m_current_join) {
      m_current_join = &join;
      m_best_cost = cost;
      return true;
    }
    const bool cheaper = cost < m_best_cost;
    m_best_cost = std::min(m_best_cost, cost);
    return cheaper;
  }

  // Offload plan (set when recognition succeeds).
  LineairDB::Protocol::TxExecuteQueryBlock::Request qb_request;
  bool plan_ready = false;

 private:
  const JOIN *m_current_join{nullptr};
  double m_best_cost{0.0};
};

// ---------------------------------------------------------------------------
// Row-format helpers (proxy row format: per field [byteSize][len LE][bytes],
// 0xFF = empty field). The server's aggregate step emits synthetic group
// rows [null_flags][group cols...][value,count per agg].
// ---------------------------------------------------------------------------
struct DecodedField {
  const char *ptr;
  size_t len;
  bool empty;  // 0xFF field
};

bool decode_row_fields(const std::string &row, std::vector<DecodedField> *out) {
  out->clear();
  size_t off = 0;
  while (off < row.size()) {
    const auto byte_size = static_cast<uint8_t>(row[off]);
    off += 1;
    if (byte_size == 0xFF) {
      out->push_back({nullptr, 0, true});
      continue;
    }
    if (byte_size > 4 || off + byte_size > row.size()) return false;
    size_t len = 0;
    for (uint8_t i = 0; i < byte_size; i++) {
      len |= static_cast<size_t>(static_cast<uint8_t>(row[off + i])) << (8 * i);
    }
    off += byte_size;
    if (off + len > row.size()) return false;
    out->push_back({row.data() + off, len, false});
    off += len;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Query-block recognition (Phase A: single-table aggregation).
// ---------------------------------------------------------------------------

// After the traditional optimizer builds a GROUP BY temp-table plan,
// join->fields items reference the temp table's Fields. Resolve such
// references back to the base table by column name; returns nullptr when
// the field is not a plain base column.
const Field *resolve_base_field(const Field *f, TABLE *t) {
  if (f == nullptr) return nullptr;
  if (f->table == t) return f;
  for (uint i = 0; i < t->s->fields; i++) {
    if (f->field_name != nullptr && t->field[i]->field_name != nullptr &&
        my_strcasecmp(system_charset_info, t->field[i]->field_name,
                      f->field_name) == 0)
      return t->field[i];
  }
  return nullptr;
}

bool group_column_is_binary_safe(const Field *f) {
  // Server-side GROUP BY keys compare raw bytes. That matches MySQL
  // semantics only for types whose equality is byte equality.
  switch (f->result_type()) {
    case INT_RESULT:
      return true;
    case STRING_RESULT:
      return f->binary();
    default:
      return false;
  }
}

// Map an ORDER BY item to an output ordinal (same Item as a visible field,
// or the same base Field as an output column).
static int order_output_ordinal(Item *order_item,
                                const std::vector<Item *> &out_items) {
  Item *oreal = order_item->real_item();
  for (size_t i = 0; i < out_items.size(); ++i) {
    Item *freal = out_items[i]->real_item();
    if (freal == oreal) return static_cast<int>(i);
    if (oreal->type() == Item::FIELD_ITEM &&
        freal->type() == Item::FIELD_ITEM &&
        down_cast<Item_field *>(oreal)->field ==
            down_cast<Item_field *>(freal)->field)
      return static_cast<int>(i);
  }
  return -1;
}

// Flatten a WHERE tree into top-level AND conjuncts.
static void flatten_and(Item *cond, std::vector<Item *> *out) {
  if (cond == nullptr) return;
  if (cond->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(cond)->functype() == Item_func::COND_AND_FUNC) {
    List_iterator<Item> it(*down_cast<Item_cond *>(cond)->argument_list());
    for (Item *sub = it++; sub != nullptr; sub = it++) flatten_and(sub, out);
    return;
  }
  out->push_back(cond);
}

// SUM(CASE WHEN pred THEN 1 ELSE 0 END) => COUNT with per-row filter.
// Only the searched single-WHEN constant-1/0 shape qualifies.
static Item *case_to_count_filter(Item *arg) {
  if (arg->type() != Item::FUNC_ITEM) return nullptr;
  auto *fn = down_cast<Item_func *>(arg);
  if (fn->functype() != Item_func::CASE_FUNC) return nullptr;
  if (fn->argument_count() != 3) return nullptr;
  Item *when = fn->arguments()[0];
  Item *then = fn->arguments()[1];
  Item *els = fn->arguments()[2];
  // `when` must be a boolean predicate (cond or comparison), not a value.
  const bool when_is_pred =
      when->type() == Item::COND_ITEM ||
      (when->type() == Item::FUNC_ITEM &&
       down_cast<Item_func *>(when)->functype() != Item_func::UNKNOWN_FUNC);
  if (!when_is_pred) return nullptr;
  if (!then->const_item() || !els->const_item()) return nullptr;
  if (then->val_int() != 1 || els->val_int() != 0) return nullptr;
  return when;
}

// SUM(CASE WHEN pred THEN expr ELSE 0 END) => filtered SUM with
// zero-if-empty semantics. Returns the predicate and THEN expression.
static bool case_to_filtered_sum(Item *arg, Item **out_pred,
                                 Item **out_expr) {
  if (arg->type() != Item::FUNC_ITEM) return false;
  auto *fn = down_cast<Item_func *>(arg);
  if (fn->functype() != Item_func::CASE_FUNC) return false;
  if (fn->argument_count() != 3) return false;
  Item *when = fn->arguments()[0];
  Item *then = fn->arguments()[1];
  Item *els = fn->arguments()[2];
  const bool when_is_pred =
      when->type() == Item::COND_ITEM ||
      (when->type() == Item::FUNC_ITEM &&
       down_cast<Item_func *>(when)->functype() != Item_func::UNKNOWN_FUNC);
  if (!when_is_pred) return false;
  if (!els->const_item() || els->val_int() != 0) return false;
  *out_pred = when;
  *out_expr = then;
  return true;
}

struct QbTableCtx {
  Table_ref *tl;
  TABLE *table;
  table_map map;
  ha_rows records;
};

// Returns the index of the table owning `used` (exactly one bit within the
// query's tables), or -1.
static int single_table_of(table_map used,
                           const std::vector<QbTableCtx> &tabs) {
  int found = -1;
  for (size_t i = 0; i < tabs.size(); ++i) {
    if (used & tabs[i].map) {
      if (found >= 0) return -1;
      found = static_cast<int>(i);
    }
  }
  return found;
}

// Multi-table arithmetic serializer for aggregate arguments: COLUMN_REF
// nodes address (table_idx << 16 | column) for tables other than 0.
static bool serialize_arith_multi(
    Item *item, LineairDB::Protocol::FilterExpr *out,
    const std::function<int(const Field *)> &table_of,
    const std::function<const Field *(const Field *, int)> &resolve) {
  item = item->real_item();
  switch (item->type()) {
    case Item::FIELD_ITEM: {
      const Field *raw = down_cast<Item_field *>(item)->field;
      const int ti = table_of(raw);
      if (ti < 0) return false;
      const Field *f = resolve(raw, ti);
      if (f == nullptr) return false;
      out->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      out->set_column_index(ti == 0 ? f->field_index()
                                    : ((static_cast<uint32_t>(ti) << 16) |
                                       f->field_index()));
      return true;
    }
    case Item::INT_ITEM: {
      out->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
      out->set_int_val(item->val_int());
      return true;
    }
    case Item::FUNC_ITEM: {
      auto *fn = down_cast<Item_func *>(item);
      LineairDB::Protocol::FilterExpr::Op op;
      if (fn->functype() == Item_func::FUNC_SP) return false;
      const char *name = fn->func_name();
      if (strcmp(name, "+") == 0)
        op = LineairDB::Protocol::FilterExpr::OP_ADD;
      else if (strcmp(name, "-") == 0)
        op = fn->argument_count() == 1
                 ? LineairDB::Protocol::FilterExpr::OP_NEG
                 : LineairDB::Protocol::FilterExpr::OP_SUB;
      else if (strcmp(name, "*") == 0)
        op = LineairDB::Protocol::FilterExpr::OP_MUL;
      else
        return false;
      out->set_op(op);
      for (uint i = 0; i < fn->argument_count(); ++i)
        if (!serialize_arith_multi(fn->arguments()[i], out->add_children(),
                                   table_of, resolve))
          return false;
      return true;
    }
    case Item::DECIMAL_ITEM:
    case Item::REAL_ITEM: {
      // Serialize exact decimals via their string form parsed as CONST_INT
      // scaled — not needed for TPC-H aggregate args; reject.
      return false;
    }
    default:
      return false;
  }
}

// Serialize a predicate over a joined tuple. Field references are remapped
// to ordinals via `reg` (registering (table, column) pairs on first use).
// Handles the FilterExpr subset serialize_item supports, but resolves each
// Item_field through the registry instead of a single-table field index.
struct TupleColumnRegistry {
  std::vector<std::pair<int, const Field *>> cols;  // ordinal -> (ti, field)
  std::function<int(const Field *)> table_of;
  std::function<const Field *(const Field *, int)> resolve;
  int ordinal_of(const Field *raw) {
    const int ti = table_of(raw);
    if (ti < 0) return -1;
    const Field *f = resolve(raw, ti);
    if (f == nullptr) return -1;
    for (size_t i = 0; i < cols.size(); ++i)
      if (cols[i].first == ti && cols[i].second == f)
        return static_cast<int>(i);
    cols.push_back({ti, f});
    return static_cast<int>(cols.size()) - 1;
  }
};

// Serialize a joined-tuple predicate: serialize_item with field references
// remapped to registry ordinals (RAII sets the thread-local encoder).
static bool serialize_tuple_pred(Item *item,
                                 LineairDB::Protocol::FilterExpr *out,
                                 TupleColumnRegistry *reg) {
  SerializeColumnEncoder enc = [reg](const Field *f) {
    return reg->ordinal_of(f);
  };
  set_serialize_column_encoder(&enc);
  const bool ok = serialize_item(item, out);
  set_serialize_column_encoder(nullptr);
  return ok;
}

// Serialize one scan-filter conjunct; when serialize_item cannot handle it,
// rewrite substr(col,1,N) IN/=(...) to OR(col LIKE 'v%') — prefix tests are
// byte-safe on the canonical cell text (q22's country codes).
static const Field *substr_prefix_field(Item *item, uint32_t *prefix_len);
static bool serialize_scan_conjunct(Item *c,
                                    LineairDB::Protocol::FilterExpr *out) {
  if (serialize_item(c, out)) return true;
  out->Clear();
  Item *real = c->real_item();
  if (real->type() != Item::FUNC_ITEM) return false;
  auto *fn = down_cast<Item_func *>(real);
  uint32_t prefix = 0;
  const Field *col = nullptr;
  std::vector<std::string> values;
  bool negated = false;
  if (fn->functype() == Item_func::IN_FUNC) {
    auto *in = down_cast<Item_func_in *>(fn);
    negated = in->negated;
    col = substr_prefix_field(fn->arguments()[0], &prefix);
    if (col == nullptr) return false;
    for (uint i = 1; i < fn->argument_count(); ++i) {
      Item *v = fn->arguments()[i];
      if (!v->const_item()) return false;
      StringBuffer<STRING_BUFFER_USUAL_SIZE> buf;
      String *sv = v->val_str(&buf);
      if (sv == nullptr) return false;
      values.emplace_back(sv->ptr(), sv->length());
    }
  } else if (fn->functype() == Item_func::EQ_FUNC) {
    col = substr_prefix_field(fn->arguments()[0], &prefix);
    Item *v = fn->arguments()[1];
    if (col == nullptr || !v->const_item()) return false;
    StringBuffer<STRING_BUFFER_USUAL_SIZE> buf;
    String *sv = v->val_str(&buf);
    if (sv == nullptr) return false;
    values.emplace_back(sv->ptr(), sv->length());
  } else {
    return false;
  }
  for (const std::string &v : values) {
    if (v.size() != prefix) return false;
    if (v.find('%') != std::string::npos ||
        v.find('_') != std::string::npos)
      return false;
  }
  auto emit_like = [&](LineairDB::Protocol::FilterExpr *e,
                       const std::string &v) {
    e->set_op(LineairDB::Protocol::FilterExpr::OP_LIKE);
    auto *cref = e->add_children();
    cref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
    cref->set_column_index(col->field_index());
    cref->set_compare_type(3);
    auto *pat = e->add_children();
    pat->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
    pat->set_string_val(v + "%");
    pat->set_compare_type(3);
  };
  LineairDB::Protocol::FilterExpr *target = out;
  if (negated) {
    out->set_op(LineairDB::Protocol::FilterExpr::OP_NOT);
    target = out->add_children();
  }
  if (values.size() == 1) {
    emit_like(target, values[0]);
  } else {
    target->set_op(LineairDB::Protocol::FilterExpr::OP_OR);
    for (const std::string &v : values) emit_like(target->add_children(), v);
  }
  return true;
}

// Recognize SUBSTRING(col, 1, N): returns the field and sets *prefix_len.
static const Field *substr_prefix_field(Item *item, uint32_t *prefix_len) {
  Item *real = item->real_item();
  if (real->type() != Item::FUNC_ITEM) return nullptr;
  auto *fn = down_cast<Item_func *>(real);
  if (strcmp(fn->func_name(), "substr") != 0 || fn->argument_count() != 3)
    return nullptr;
  Item *col = fn->arguments()[0]->real_item();
  Item *from = fn->arguments()[1];
  Item *len = fn->arguments()[2];
  if (col->type() != Item::FIELD_ITEM) return nullptr;
  if (!from->const_item() || from->val_int() != 1) return nullptr;
  if (!len->const_item() || len->val_int() <= 0) return nullptr;
  *prefix_len = static_cast<uint32_t>(len->val_int());
  return down_cast<Item_field *>(col)->field;
}

// Recognize EXTRACT(YEAR FROM date_col): returns the field, or nullptr.
static const Field *extract_year_field(Item *item) {
  Item *real = item->real_item();
  if (real->type() != Item::FUNC_ITEM) return nullptr;
  auto *fn = down_cast<Item_func *>(real);
  if (fn->functype() != Item_func::EXTRACT_FUNC) return nullptr;
  auto *ex = down_cast<Item_extract *>(fn);
  if (ex->int_type != INTERVAL_YEAR) return nullptr;
  Item *arg = ex->arguments()[0]->real_item();
  if (arg->type() != Item::FIELD_ITEM) return nullptr;
  const Field *f = down_cast<Item_field *>(arg)->field;
  if (f->type() != MYSQL_TYPE_DATE && f->type() != MYSQL_TYPE_DATETIME &&
      f->type() != MYSQL_TYPE_NEWDATE)
    return nullptr;
  return f;
}

// q13 shape: SELECT g, COUNT(*) FROM (SELECT k, COUNT(x) FROM a LEFT JOIN b
// ON a.k=b.k AND <b filter> GROUP BY k) d GROUP BY g ORDER BY ... — a
// derived-table double aggregation. The inner block becomes scan+scan+LEFT
// join+aggregate; the outer becomes the aggregate's second stage.
bool recognize_double_aggregate(THD *thd, JOIN *join, Query_block *qb,
                                Columnar_execution_context *ctx,
                                const char **why) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)
  Table_ref *dt = qb->leaf_tables;
  Query_expression *inner_unit = dt->derived_query_expression();
  if (inner_unit == nullptr || !inner_unit->is_simple())
    LDB_COL_REJECT("derived not simple");
  Query_block *iqb = inner_unit->first_query_block();
  if (iqb == nullptr) LDB_COL_REJECT("no inner block");

  // Outer block restrictions.
  if (join->having_cond != nullptr || join->select_distinct ||
      qb->m_windows.elements > 0 ||
      join->rollup_state != JOIN::RollupState::NONE)
    LDB_COL_REJECT("outer shape");
  if (qb->where_cond() != nullptr) LDB_COL_REJECT("outer WHERE");
  if (qb->group_list.elements == 0) LDB_COL_REJECT("outer not grouped");

  // Inner block: two base tables, one LEFT-joined, single-column GROUP BY.
  if (iqb->where_cond() != nullptr) LDB_COL_REJECT("inner WHERE");
  if (iqb->order_list.elements > 0 || iqb->has_limit())
    LDB_COL_REJECT("inner order/limit");
  Table_ref *t1 = iqb->leaf_tables;
  Table_ref *t2 = t1 != nullptr ? t1->next_leaf : nullptr;
  if (t1 == nullptr || t2 == nullptr || t2->next_leaf != nullptr)
    LDB_COL_REJECT("inner not two tables");
  Table_ref *outer_tl = t1->outer_join ? t1 : (t2->outer_join ? t2 : nullptr);
  Table_ref *inner_tl = outer_tl == t1 ? t2 : t1;
  if (outer_tl == nullptr || inner_tl->outer_join)
    LDB_COL_REJECT("inner join shape");
  for (Table_ref *tl : {t1, t2}) {
    if (tl->table == nullptr || tl->table->s == nullptr)
      LDB_COL_REJECT("inner no TABLE");
    if (!loaded_tables->contains(tl->table->s->db.str,
                                 tl->table->s->table_name.str))
      LDB_COL_REJECT("inner not loaded");
  }
  TABLE *probe_t = inner_tl->table;   // row-preserving side
  TABLE *build_t = outer_tl->table;   // nullable side

  // ON condition: equi keys + build-side-only filters.
  Item *on_cond = outer_tl->join_cond();
  if (on_cond == nullptr) LDB_COL_REJECT("no ON");
  std::vector<Item *> conjuncts;
  flatten_and(on_cond, &conjuncts);
  const Field *probe_key = nullptr;
  const Field *build_key = nullptr;
  std::vector<Item *> build_filters;
  for (Item *c : conjuncts) {
    const table_map used = c->used_tables() & ~PSEUDO_TABLE_BITS;
    if (used == outer_tl->map()) {
      build_filters.push_back(c);
      continue;
    }
    if (c->type() != Item::FUNC_ITEM ||
        down_cast<Item_func *>(c)->functype() != Item_func::EQ_FUNC)
      LDB_COL_REJECT("ON not equi");
    auto *eq = down_cast<Item_func *>(c);
    Item *a = eq->arguments()[0]->real_item();
    Item *b = eq->arguments()[1]->real_item();
    if (a->type() != Item::FIELD_ITEM || b->type() != Item::FIELD_ITEM)
      LDB_COL_REJECT("ON key shape");
    const Field *fa = down_cast<Item_field *>(a)->field;
    const Field *fb = down_cast<Item_field *>(b)->field;
    if (probe_key != nullptr) LDB_COL_REJECT("multiple ON keys");
    if (fa->table == probe_t && fb->table == build_t) {
      probe_key = fa;
      build_key = fb;
    } else if (fa->table == build_t && fb->table == probe_t) {
      probe_key = fb;
      build_key = fa;
    } else {
      LDB_COL_REJECT("ON key tables");
    }
    if (probe_key->result_type() != INT_RESULT ||
        build_key->result_type() != INT_RESULT)
      LDB_COL_REJECT("ON key type");
  }
  if (probe_key == nullptr) LDB_COL_REJECT("no ON key");

  // Inner GROUP BY: one non-nullable probe-side column.
  if (iqb->group_list.elements != 1) LDB_COL_REJECT("inner group arity");
  Item *gi = (*iqb->group_list.first->item)->real_item();
  if (gi->type() != Item::FIELD_ITEM) LDB_COL_REJECT("inner group shape");
  const Field *gf = down_cast<Item_field *>(gi)->field;
  if (gf->table != probe_t || gf->is_nullable())
    LDB_COL_REJECT("inner group column");

  // Inner output: [group column, COUNT(build column|*)] in some order.
  // Map inner visible position -> stage-1 value ordinal (0 = group,
  // 1 = the count).
  std::vector<Item *> inner_out;
  for (Item *item : VisibleFields(iqb->fields)) inner_out.push_back(item);
  if (inner_out.size() != 2) LDB_COL_REJECT("inner output arity");
  std::vector<int> stage1_ordinal(2, -1);
  bool have_count = false;
  const Field *count_arg_field = nullptr;
  for (size_t i = 0; i < inner_out.size(); ++i) {
    Item *real = inner_out[i]->real_item();
    if (real->type() == Item::FIELD_ITEM &&
        down_cast<Item_field *>(real)->field == gf) {
      stage1_ordinal[i] = 0;
      continue;
    }
    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("inner output shape");
    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->sum_func() != Item_sum::COUNT_FUNC ||
        sum->argument_count() != 1)
      LDB_COL_REJECT("inner agg not count");
    Item *arg = sum->get_arg(0)->real_item();
    if (!arg->const_item()) {
      if (arg->type() != Item::FIELD_ITEM) LDB_COL_REJECT("count arg");
      const Field *cf = down_cast<Item_field *>(arg)->field;
      if (cf->table != build_t || cf->is_nullable())
        LDB_COL_REJECT("count arg column");
      count_arg_field = cf;
    }
    have_count = true;
    stage1_ordinal[i] = 1;
  }
  if (!have_count) LDB_COL_REJECT("inner no count");

  // Build the IR.
  auto &req = ctx->qb_request;
  req.Clear();
  req.add_tables()->set_table_name(probe_t->s->normalized_path.str);  // 0
  req.add_tables()->set_table_name(build_t->s->normalized_path.str);  // 1
  auto *scan_p = req.add_nodes()->mutable_scan();   // node 0
  scan_p->set_table_idx(0);
  auto *scan_b = req.add_nodes()->mutable_scan();   // node 1
  scan_b->set_table_idx(1);
  if (!build_filters.empty()) {
    auto *pred = scan_b->mutable_filter();
    pred->set_num_columns(build_t->s->fields);
    if (build_filters.size() == 1) {
      if (!serialize_item(build_filters[0], pred->mutable_expr()))
        LDB_COL_REJECT("ON filter not pushable");
    } else {
      auto *root = pred->mutable_expr();
      root->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
      for (Item *c : build_filters)
        if (!serialize_item(c, root->add_children()))
          LDB_COL_REJECT("ON filter not pushable");
    }
  }
  auto *jn = req.add_nodes()->mutable_join();       // node 2
  jn->set_type(LineairDB::Protocol::QbJoin::LEFT);
  jn->set_build(1);
  jn->set_probe(0);
  auto *bk = jn->add_build_keys();
  bk->set_table_idx(1);
  bk->set_column(build_key->field_index());
  auto *pkk = jn->add_probe_keys();
  pkk->set_table_idx(0);
  pkk->set_column(probe_key->field_index());
  auto *agg = req.add_nodes()->mutable_aggregate(); // node 3
  agg->set_input(2);
  auto *gc = agg->add_group_columns();
  gc->set_table_idx(0);
  gc->set_column(gf->field_index());
  gc->set_cmp_kind(gf->result_type() == INT_RESULT ? 0 : 1);
  auto *af = agg->add_aggs();
  af->set_kind(LineairDB::Protocol::QbAggFunc::COUNT);
  af->set_arg_table(1);  // count matches on the nullable side
  if (count_arg_field != nullptr) {
    af->mutable_arg()->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
    af->mutable_arg()->set_column_index(count_arg_field->field_index());
  }
  if (count_arg_field == nullptr) {
    // COUNT(*) over the joined row would count no-match rows too; require
    // a build-side argument for LEFT semantics.
    LDB_COL_REJECT("inner count(*) under LEFT");
  }

  // Outer block: GROUP BY derived columns + COUNT(*).
  auto *second = agg->mutable_second();
  second->set_count_star(true);
  std::vector<Item *> out_items;
  for (Item *item : VisibleFields(qb->fields)) out_items.push_back(item);
  // Outer group ordinals in second-stage layout.
  std::vector<int> outer_group_pos;  // second-stage group slot per group item
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next) {
    Item *ogi = (*g->item)->real_item();
    if (ogi->type() != Item::FIELD_ITEM) LDB_COL_REJECT("outer group shape");
    const Field *ogf = down_cast<Item_field *>(ogi)->field;
    if (ogf->table != dt->table) LDB_COL_REJECT("outer group table");
    const uint32_t inner_pos = ogf->field_index();
    if (inner_pos >= stage1_ordinal.size() || stage1_ordinal[inner_pos] < 0)
      LDB_COL_REJECT("outer group mapping");
    second->add_group_value_ordinals(stage1_ordinal[inner_pos]);
    outer_group_pos.push_back(second->group_value_ordinals_size() - 1);
  }
  // Outer outputs.
  int emitted_group = 0;
  for (Item *item : out_items) {
    Item *real = item->real_item();
    auto *oe = req.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *of = down_cast<Item_field *>(real)->field;
      if (of->table != dt->table) LDB_COL_REJECT("outer output table");
      const uint32_t inner_pos = of->field_index();
      // Find which second-stage group slot carries this value.
      int slot = -1;
      int gidx = 0;
      for (ORDER *g = qb->group_list.first; g != nullptr;
           g = g->next, ++gidx) {
        Item *ogi = (*g->item)->real_item();
        if (ogi->type() == Item::FIELD_ITEM &&
            down_cast<Item_field *>(ogi)->field == of) {
          slot = outer_group_pos[gidx];
          break;
        }
      }
      if (slot < 0) LDB_COL_REJECT("outer output not grouped");
      oe->set_source(LineairDB::Protocol::QbOutputExpr::GROUP);
      oe->set_ordinal(slot);
      emitted_group++;
      continue;
    }
    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("outer output shape");
    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->sum_func() != Item_sum::COUNT_FUNC)
      LDB_COL_REJECT("outer agg not count");
    oe->set_source(LineairDB::Protocol::QbOutputExpr::AGG);
    oe->set_ordinal(0);
  }

  // Outer ORDER BY / LIMIT.
  for (ORDER *o = qb->order_list.first; o != nullptr; o = o->next) {
    const int ord = order_output_ordinal(*o->item, out_items);
    if (ord < 0) LDB_COL_REJECT("outer ORDER BY");
    auto *k = req.add_order_by();
    k->set_output_ordinal(ord);
    k->set_descending(o->direction == ORDER_DESC);
    Item *oi = out_items[ord]->real_item();
    k->set_cmp_kind(oi->result_type() == STRING_RESULT ? 1 : 0);
  }
  Query_expression *unit = qb->master_query_expression();
  if (qb->has_limit()) {
    if (unit->select_limit_cnt != HA_POS_ERROR)
      req.set_limit(unit->select_limit_cnt);
    if (unit->offset_limit_cnt > 0) {
      req.set_offset(unit->offset_limit_cnt);
      if (req.limit() > 0) req.set_limit(req.limit() - unit->offset_limit_cnt);
    }
  }

  (void)thd;
  (void)join;
  ctx->plan_ready = true;
  *why = nullptr;
  return true;
#undef LDB_COL_REJECT
}

// q9 shape: SELECT g1, g2, SUM(expr) FROM (SELECT ... FROM t1..tn WHERE
// <inner-join conjuncts>) d GROUP BY .. ORDER BY .. — a non-aggregating
// derived block flattened into a single query block. Derived-column
// references dereference to the inner select expressions; group columns may
// be EXTRACT(YEAR FROM date_col) (prefix-4 grouping over "YYYY-MM-DD").
bool recognize_flattened_agg(THD *thd, JOIN *join, Query_block *qb,
                             Columnar_execution_context *ctx,
                             const char **why) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)
  Table_ref *dt = qb->leaf_tables;
  Query_expression *inner_unit = dt->derived_query_expression();
  if (inner_unit == nullptr || !inner_unit->is_simple())
    LDB_COL_REJECT("derived not simple");
  Query_block *iqb = inner_unit->first_query_block();
  if (iqb == nullptr) LDB_COL_REJECT("no inner block");

  // Outer restrictions.
  if (join->having_cond != nullptr || join->select_distinct ||
      qb->m_windows.elements > 0 ||
      join->rollup_state != JOIN::RollupState::NONE)
    LDB_COL_REJECT("outer shape");
  if (qb->where_cond() != nullptr) LDB_COL_REJECT("outer WHERE");
  if (qb->group_list.elements == 0) LDB_COL_REJECT("outer not grouped");

  // Inner block must be a plain inner-join projection.
  if (iqb->group_list.elements > 0 || iqb->with_sum_func ||
      iqb->order_list.elements > 0 || iqb->has_limit() ||
      iqb->m_windows.elements > 0)
    LDB_COL_REJECT("inner not plain");

  std::vector<QbTableCtx> tabs;
  for (Table_ref *tl = iqb->leaf_tables; tl != nullptr; tl = tl->next_leaf) {
    if (tl->table == nullptr || tl->table->s == nullptr)
      LDB_COL_REJECT("inner no TABLE");
    if (tl->outer_join) LDB_COL_REJECT("inner outer join");
    if (!loaded_tables->contains(tl->table->s->db.str,
                                 tl->table->s->table_name.str))
      LDB_COL_REJECT("inner not loaded");
    tabs.push_back({tl, tl->table, tl->map(),
                    tl->table->file->stats.records});
  }
  if (tabs.size() < 2) LDB_COL_REJECT("inner too few tables");
  const size_t n_tabs = tabs.size();

  auto table_index_of_field = [&](const Field *f) -> int {
    for (size_t i = 0; i < n_tabs; ++i)
      if (f->table == tabs[i].table) return static_cast<int>(i);
    for (size_t i = 0; i < n_tabs; ++i)
      if (resolve_base_field(f, tabs[i].table) != nullptr)
        return static_cast<int>(i);
    return -1;
  };
  auto resolver = [&](const Field *f, int ti) -> const Field * {
    return resolve_base_field(f, tabs[ti].table);
  };

  // Dereference an outer item through the derived table to the inner
  // select expression.
  std::vector<Item *> inner_out;
  for (Item *item : VisibleFields(iqb->fields)) inner_out.push_back(item);
  auto deref = [&](Item *item) -> Item * {
    Item *real = item->real_item();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *f = down_cast<Item_field *>(real)->field;
      if (f->table == dt->table) {
        if (f->field_index() >= inner_out.size()) return nullptr;
        return inner_out[f->field_index()]->real_item();
      }
    }
    return real;
  };

  // Inner WHERE: per-table filters + integer equi-join edges.
  struct JoinEdge {
    int t1, t2;
    const Field *f1, *f2;
  };
  std::vector<std::vector<Item *>> table_filters(n_tabs);
  std::vector<JoinEdge> edges;
  {
    Item *where_cond = iqb->where_cond();
    std::vector<Item *> conjuncts;
    flatten_and(where_cond, &conjuncts);
    for (Item *c : conjuncts) {
      const table_map used = c->used_tables() & ~PSEUDO_TABLE_BITS;
      const int single = single_table_of(used, tabs);
      if (single >= 0) {
        table_filters[single].push_back(c);
        continue;
      }
      if (c->type() != Item::FUNC_ITEM ||
          down_cast<Item_func *>(c)->functype() != Item_func::EQ_FUNC)
        LDB_COL_REJECT("inner non-equi conjunct");
      auto *eq = down_cast<Item_func *>(c);
      Item *a = eq->arguments()[0]->real_item();
      Item *b = eq->arguments()[1]->real_item();
      if (a->type() != Item::FIELD_ITEM || b->type() != Item::FIELD_ITEM)
        LDB_COL_REJECT("inner join key shape");
      const Field *fa = down_cast<Item_field *>(a)->field;
      const Field *fb = down_cast<Item_field *>(b)->field;
      const int ta = table_index_of_field(fa);
      const int tb = table_index_of_field(fb);
      if (ta < 0 || tb < 0 || ta == tb) LDB_COL_REJECT("inner key tables");
      if (fa->result_type() != INT_RESULT ||
          fb->result_type() != INT_RESULT)
        LDB_COL_REJECT("inner key type");
      const Field *rfa = resolve_base_field(fa, tabs[ta].table);
      const Field *rfb = resolve_base_field(fb, tabs[tb].table);
      if (rfa == nullptr || rfb == nullptr) LDB_COL_REJECT("inner key resolve");
      edges.push_back({ta, tb, rfa, rfb});
    }
  }
  if (edges.empty()) LDB_COL_REJECT("inner cross join");

  auto &req = ctx->qb_request;
  req.Clear();
  std::vector<int> scan_node_of(n_tabs);
  for (size_t i = 0; i < n_tabs; ++i) {
    req.add_tables()->set_table_name(tabs[i].table->s->normalized_path.str);
    auto *scan = req.add_nodes()->mutable_scan();
    scan->set_table_idx(static_cast<uint32_t>(i));
    scan_node_of[i] = req.nodes_size() - 1;
    if (!table_filters[i].empty()) {
      auto *pred = scan->mutable_filter();
      pred->set_num_columns(tabs[i].table->s->fields);
      if (table_filters[i].size() == 1) {
        if (!serialize_item(table_filters[i][0], pred->mutable_expr()))
          LDB_COL_REJECT("inner filter not pushable");
      } else {
        auto *root = pred->mutable_expr();
        root->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *c : table_filters[i])
          if (!serialize_item(c, root->add_children()))
            LDB_COL_REJECT("inner filter not pushable");
      }
    }
  }

  // Connectivity-ordered join tree over FROM order.
  int current_node;
  {
    std::vector<bool> joined(n_tabs, false);
    joined[0] = true;
    current_node = scan_node_of[0];
    std::vector<int> pending;
    for (size_t i = 1; i < n_tabs; ++i) pending.push_back(static_cast<int>(i));
    while (!pending.empty()) {
      int pick = -1;
      size_t pick_pos = 0;
      for (size_t i = 0; i < pending.size(); ++i) {
        for (const auto &e : edges)
          if ((e.t1 == pending[i] && joined[e.t2]) ||
              (e.t2 == pending[i] && joined[e.t1])) {
            pick = pending[i];
            pick_pos = i;
            break;
          }
        if (pick >= 0) break;
      }
      if (pick < 0) LDB_COL_REJECT("inner disconnected");
      pending.erase(pending.begin() + pick_pos);
      auto *jn = req.add_nodes()->mutable_join();
      jn->set_type(LineairDB::Protocol::QbJoin::INNER);
      jn->set_build(scan_node_of[pick]);
      jn->set_probe(current_node);
      for (const auto &e : edges) {
        const Field *bf = nullptr;
        const Field *pf = nullptr;
        int ptab = -1;
        if (e.t1 == pick && joined[e.t2]) {
          bf = e.f1;
          pf = e.f2;
          ptab = e.t2;
        } else if (e.t2 == pick && joined[e.t1]) {
          bf = e.f2;
          pf = e.f1;
          ptab = e.t1;
        } else {
          continue;
        }
        auto *bk = jn->add_build_keys();
        bk->set_table_idx(pick);
        bk->set_column(bf->field_index());
        auto *pkk = jn->add_probe_keys();
        pkk->set_table_idx(ptab);
        pkk->set_column(pf->field_index());
      }
      joined[pick] = true;
      current_node = req.nodes_size() - 1;
    }
  }

  // Aggregate: outer GROUP BY over dereferenced inner expressions.
  auto *agg = req.add_nodes()->mutable_aggregate();
  agg->set_input(current_node);
  struct GroupKey {
    Item *outer_item;  // the derived-column item as written in the outer qb
  };
  std::vector<Item *> group_outer_items;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next) {
    Item *outer = (*g->item)->real_item();
    Item *inner = deref(outer);
    if (inner == nullptr) LDB_COL_REJECT("group deref");
    auto *gc = agg->add_group_columns();
    if (const Field *yf = extract_year_field(inner)) {
      const int ti = table_index_of_field(yf);
      const Field *rf = ti >= 0 ? resolver(yf, ti) : nullptr;
      if (rf == nullptr || rf->is_nullable()) LDB_COL_REJECT("group year col");
      gc->set_table_idx(ti);
      gc->set_column(rf->field_index());
      gc->set_prefix_len(4);
      gc->set_cmp_kind(0);
    } else if (inner->type() == Item::FIELD_ITEM) {
      const Field *raw = down_cast<Item_field *>(inner)->field;
      const int ti = table_index_of_field(raw);
      const Field *rf = ti >= 0 ? resolver(raw, ti) : nullptr;
      if (rf == nullptr || rf->is_nullable()) LDB_COL_REJECT("group col");
      if (rf->result_type() != INT_RESULT &&
          rf->result_type() != STRING_RESULT &&
          rf->result_type() != DECIMAL_RESULT)
        LDB_COL_REJECT("group col type");
      gc->set_table_idx(ti);
      gc->set_column(rf->field_index());
      gc->set_cmp_kind(rf->result_type() == STRING_RESULT ? 1 : 0);
    } else {
      LDB_COL_REJECT("group expr");
    }
    group_outer_items.push_back(outer);
  }

  // Outputs: group refs and SUM(multi-table arithmetic).
  std::vector<Item *> out_items;
  for (Item *item : VisibleFields(qb->fields)) out_items.push_back(item);
  for (Item *item : out_items) {
    Item *real = item->real_item();
    auto *oe = req.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      int pos = -1;
      for (size_t g = 0; g < group_outer_items.size(); ++g) {
        Item *gi = group_outer_items[g];
        if (gi == real ||
            (gi->type() == Item::FIELD_ITEM &&
             down_cast<Item_field *>(gi)->field ==
                 down_cast<Item_field *>(real)->field)) {
          pos = static_cast<int>(g);
          break;
        }
      }
      if (pos < 0) LDB_COL_REJECT("output not grouped");
      oe->set_source(LineairDB::Protocol::QbOutputExpr::GROUP);
      oe->set_ordinal(pos);
      continue;
    }
    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("output shape");
    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->sum_func() != Item_sum::SUM_FUNC || sum->argument_count() != 1)
      LDB_COL_REJECT("agg kind");
    Item *arg = deref(sum->get_arg(0));
    if (arg == nullptr) LDB_COL_REJECT("agg deref");
    auto *af = agg->add_aggs();
    af->set_kind(LineairDB::Protocol::QbAggFunc::SUM);
    af->set_arg_table(0);
    af->set_arg_scale(arg->decimals);
    if (!serialize_arith_multi(arg, af->mutable_arg(), table_index_of_field,
                               resolver))
      LDB_COL_REJECT("agg expr not pushable");
    oe->set_source(LineairDB::Protocol::QbOutputExpr::AGG);
    oe->set_ordinal(agg->aggs_size() - 1);
  }
  if (agg->aggs_size() == 0) LDB_COL_REJECT("no aggregates");

  // Outer ORDER BY / LIMIT.
  for (ORDER *o = qb->order_list.first; o != nullptr; o = o->next) {
    const int ord = order_output_ordinal(*o->item, out_items);
    if (ord < 0) LDB_COL_REJECT("outer ORDER BY");
    auto *k = req.add_order_by();
    k->set_output_ordinal(ord);
    k->set_descending(o->direction == ORDER_DESC);
    Item *oi = out_items[ord]->real_item();
    k->set_cmp_kind(oi->result_type() == STRING_RESULT ? 1 : 0);
  }
  Query_expression *unit = qb->master_query_expression();
  if (qb->has_limit()) {
    if (unit->select_limit_cnt != HA_POS_ERROR)
      req.set_limit(unit->select_limit_cnt);
    if (unit->offset_limit_cnt > 0) {
      req.set_offset(unit->offset_limit_cnt);
      if (req.limit() > 0) req.set_limit(req.limit() - unit->offset_limit_cnt);
    }
  }

  (void)thd;
  ctx->plan_ready = true;
  *why = nullptr;
  return true;
#undef LDB_COL_REJECT
}

// QEP cardinality estimates per base table (post-filter output rows from
// the hypergraph plan). Guides join order and semi-filter placement.
using QepRows = std::map<const TABLE *, double>;

// Collect per-table output-row estimates from the AccessPath tree
// (FILTER-over-TABLE_SCAN overrides the raw scan estimate).
// Coverage note: secondary tables report HA_NO_INDEX_ACCESS, so the
// hypergraph plan only produces TABLE_SCAN leaves for them; REF/EQ_REF/
// INDEX_* paths would silently fall back to stats.records if that ever
// changes (review finding D2-F3/Codex#1).
static void collect_qep_rows(AccessPath *root, JOIN *join, QepRows *out) {
  if (root == nullptr) return;
  WalkAccessPaths(root, join, WalkAccessPathPolicy::ENTIRE_TREE,
                  [out](AccessPath *path, const JOIN *) {
                    if (path->type == AccessPath::TABLE_SCAN) {
                      const TABLE *t = path->table_scan().table;
                      if (out->find(t) == out->end())
                        (*out)[t] = path->num_output_rows();
                    } else if (path->type == AccessPath::FILTER &&
                               path->filter().child->type ==
                                   AccessPath::TABLE_SCAN) {
                      (*out)[path->filter().child->table_scan().table] =
                          path->num_output_rows();
                    }
                    return false;
                  });
}

// Build the query-block IR for `qb` into `req`. Recursion-safe: derived
// tables build their inner blocks as QbSubBlock nodes (virtual tables).
// Never raises errors — a false return means "primary runs it".
bool build_block(THD *thd, Query_block *qb,
                 LineairDB::Protocol::TxExecuteQueryBlock::Request &req,
                 const char **why, const QepRows *qep_rows = nullptr,
                 AccessPath *plan = nullptr) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)
  req.Clear();
  Query_expression *unit = qb->master_query_expression();
  if (unit == nullptr || !unit->is_simple()) LDB_COL_REJECT("not simple unit");
  if (qb->m_windows.elements > 0) LDB_COL_REJECT("has windows");
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) LDB_COL_REJECT("has ROLLUP");
  // SELECT DISTINCT c1..cn (no aggregates) == GROUP BY c1..cn.
  const bool distinct_as_group =
      qb->is_distinct() && !qb->is_grouped() && !qb->with_sum_func;
  if (qb->is_distinct() && !distinct_as_group)
    LDB_COL_REJECT("has DISTINCT");
  // Plain row-returning blocks (no grouping at all) emit base columns.
  const bool plain_rows = !qb->is_implicitly_grouped() &&
                          qb->group_list.elements == 0 &&
                          !distinct_as_group;

  // Base tables. Tables inside semijoin/antijoin nests (EXISTS / NOT
  // EXISTS unnested by MySQL) are scanned like base tables but joined via
  // SEMI/ANTI nodes after the main join tree (lineage log #12).
  struct SjNestInfo {
    Table_ref *nest;
    bool anti;
    std::vector<int> inner_tabs;
    std::vector<Item *> residuals;  // outer x inner non-equi (q21's <>)
  };
  std::vector<SjNestInfo> sj_infos;
  for (Table_ref *nest : qb->sj_nests)
    sj_infos.push_back({nest, nest->is_aj_nest(), {}, {}});
  auto nest_index_of = [&](Table_ref *tl) -> int {
    for (Table_ref *emb = tl->embedding; emb != nullptr;
         emb = emb->embedding) {
      for (size_t i = 0; i < sj_infos.size(); ++i)
        if (emb == sj_infos[i].nest) return static_cast<int>(i);
      // Antijoin nests (NOT EXISTS) may appear as aj nests without being
      // listed in sj_nests; register them on first sight.
      if (emb->is_aj_nest() || emb->is_sj_nest()) {
        sj_infos.push_back({emb, emb->is_aj_nest(), {}, {}});
        return static_cast<int>(sj_infos.size()) - 1;
      }
    }
    return -1;
  };

  // Two passes: real tables first, then derived tables — tabs indexes must
  // match the server's table_idx space (real 0..n-1, virtual n..).
  std::vector<QbTableCtx> tabs;
  std::vector<int> main_tabs;      // indexes into tabs (FROM order)
  std::vector<int> tab_nest;       // per tab: sj_infos index or -1
  std::vector<bool> tab_virtual;
  std::vector<int> left_deriveds;  // outer-joined derived tabs (post-joins)
  for (Table_ref *tl = qb->leaf_tables; tl != nullptr; tl = tl->next_leaf) {
    if (tl->is_view_or_derived()) continue;
    if (tl->table == nullptr || tl->table->s == nullptr)
      LDB_COL_REJECT("no TABLE");
    const int nest = nest_index_of(tl);
    if (nest < 0 && tl->outer_join) LDB_COL_REJECT("outer join");
    if (!loaded_tables->contains(tl->table->s->db.str,
                                 tl->table->s->table_name.str))
      LDB_COL_REJECT("not SECONDARY_LOADed");
    ha_rows est = tl->table->file->stats.records;
    if (qep_rows != nullptr) {
      auto it = qep_rows->find(tl->table);
      // NaN/-1 (kUnknownRowCount) fail the >= 0 test; +inf / >2^63 would be
      // UB in the narrowing cast, so they clamp to "huge" instead.
      if (it != qep_rows->end() && it->second >= 0)
        est = it->second < 9.0e18 ? static_cast<ha_rows>(it->second)
                                  : HA_POS_ERROR - 1;
    }
    tabs.push_back({tl, tl->table, tl->map(), est});
    tab_nest.push_back(nest);
    tab_virtual.push_back(false);
    if (nest < 0)
      main_tabs.push_back(static_cast<int>(tabs.size()) - 1);
    else
      sj_infos[nest].inner_tabs.push_back(static_cast<int>(tabs.size()) - 1);
  }
  const size_t n_real = tabs.size();
  std::vector<Query_block *> virtual_inner;  // per virtual tab
  std::vector<bool> virtual_one_row;         // implicitly grouped => 1 row
  for (Table_ref *tl = qb->leaf_tables; tl != nullptr; tl = tl->next_leaf) {
    if (!tl->is_view_or_derived()) continue;
    if (tl->table == nullptr) LDB_COL_REJECT("derived no TABLE");
    Query_expression *du = tl->derived_query_expression();
    if (du == nullptr || !du->is_simple()) LDB_COL_REJECT("derived unit");
    Query_block *iqb = du->first_query_block();
    if (iqb == nullptr) LDB_COL_REJECT("derived inner");
    const int nest = nest_index_of(tl);
    ha_rows vest = HA_POS_ERROR - 1;  // unknown: sort/semi-source unfriendly
    if (qep_rows != nullptr) {
      auto it = qep_rows->find(tl->table);
      if (it != qep_rows->end() && it->second >= 0)
        vest = it->second < 9.0e18 ? static_cast<ha_rows>(it->second)
                                   : HA_POS_ERROR - 1;
    }
    tabs.push_back({tl, tl->table, tl->map(), vest});
    tab_nest.push_back(nest);
    tab_virtual.push_back(true);
    virtual_inner.push_back(iqb);
    virtual_one_row.push_back(iqb->is_implicitly_grouped());
    if (nest >= 0)
      sj_infos[nest].inner_tabs.push_back(static_cast<int>(tabs.size()) - 1);
    else if (tl->outer_join)
      left_deriveds.push_back(static_cast<int>(tabs.size()) - 1);
    else
      main_tabs.push_back(static_cast<int>(tabs.size()) - 1);
  }
  if (main_tabs.empty()) LDB_COL_REJECT("no tables");
  const size_t n_tabs = tabs.size();
  auto resolve_in = [&](const Field *f, int ti) -> const Field * {
    if (tab_virtual[ti]) return f;  // derived fields are already canonical
    return resolve_base_field(f, tabs[ti].table);
  };
  const bool plan_map =
      plan != nullptr && (getenv("LDBC_PLAN_MAP") == nullptr ||
                          strcmp(getenv("LDBC_PLAN_MAP"), "0") != 0);
  for (size_t i = 0; i < n_real; ++i)
    req.add_tables()->set_table_name(tabs[i].table->s->normalized_path.str);


  auto table_index_of_field = [&](const Field *f) -> int {
    for (size_t i = 0; i < n_tabs; ++i)
      if (f->table == tabs[i].table) return static_cast<int>(i);
    // Temp-slice reference: resolve by name against each table.
    for (size_t i = 0; i < n_tabs; ++i)
      if (resolve_base_field(f, tabs[i].table) != nullptr)
        return static_cast<int>(i);
    return -1;
  };

  // Decompose WHERE into per-table filters and equi-join edges.
  struct JoinEdge {
    int t1, t2;
    const Field *f1, *f2;
  };
  std::vector<std::vector<Item *>> table_filters(n_tabs);
  std::vector<JoinEdge> edges;
  std::vector<Item *> tuple_conjuncts;  // cross-table non-equi (q7/q19 ORs)
  {
    Item *where_cond = qb->where_cond();
    std::vector<Item *> conjuncts;
    flatten_and(where_cond, &conjuncts);
    for (Item *c : conjuncts) {
      const table_map used = c->used_tables() & ~PSEUDO_TABLE_BITS;
      const int single = single_table_of(used, tabs);
      if (single >= 0 && !tab_virtual[single]) {
        table_filters[single].push_back(c);
        continue;
      }
      // Plan mapping: join edges, nest residuals and cross-table filters
      // all come from the AccessPath tree, not the WHERE decomposition.
      if (plan_map) continue;
      if (single >= 0) {
        // Virtual (derived) tables have no scan to push into; evaluate
        // after the joins (q21's IS NULL anti test).
        tuple_conjuncts.push_back(c);
        continue;
      }
      // Multiple equalities (Item_equal, e.g. l1.l_orderkey = o_orderkey
      // = l2.l_orderkey in q21): expand to pairwise integer edges between
      // non-nest tables. Pairs involving nest tables are already carried
      // by sj_outer/inner_exprs.
      if (c->type() == Item::FUNC_ITEM &&
          down_cast<Item_func *>(c)->functype() ==
              Item_func::MULT_EQUAL_FUNC) {
        auto *meq = down_cast<Item_equal *>(c);
        if (meq->const_arg() != nullptr)
          LDB_COL_REJECT("multiple equality with constant");
        std::vector<const Field *> flds;
        for (Item_field &fi : meq->get_fields()) flds.push_back(fi.field);
        for (size_t a = 0; a < flds.size(); ++a) {
          for (size_t b = a + 1; b < flds.size(); ++b) {
            const int ta = table_index_of_field(flds[a]);
            const int tb = table_index_of_field(flds[b]);
            if (ta < 0 || tb < 0 || ta == tb) continue;
            if (tab_nest[ta] >= 0 || tab_nest[tb] >= 0) continue;
            if (flds[a]->result_type() != INT_RESULT ||
                flds[b]->result_type() != INT_RESULT)
              continue;
            const Field *rfa = resolve_in(flds[a], ta);
            const Field *rfb = resolve_in(flds[b], tb);
            if (rfa == nullptr || rfb == nullptr) continue;
            edges.push_back({ta, tb, rfa, rfb});
          }
        }
        continue;
      }
      // Conjuncts touching a semijoin-nest table: outer x inner
      // comparisons become that nest's residual predicate (evaluated per
      // key match inside the SEMI/ANTI join, q21's l_suppkey <>).
      {
        int nest = -1;
        bool multi_nest = false;
        for (size_t i = 0; i < n_tabs; ++i) {
          if ((used & tabs[i].map) && tab_nest[i] >= 0) {
            if (nest >= 0 && nest != tab_nest[i]) multi_nest = true;
            nest = tab_nest[i];
          }
        }
        if (multi_nest) LDB_COL_REJECT("conjunct spans nests");
        if (nest >= 0) {
          // Equality propagation may route a main-table equi edge through
          // the nest's inner column (q21: o_orderkey = l2.l_orderkey).
          // Rewrite the inner column to its sj_outer equivalent; if both
          // sides are then main tables, it is a join edge.
          if (c->type() == Item::FUNC_ITEM &&
              down_cast<Item_func *>(c)->functype() == Item_func::EQ_FUNC) {
            auto *eq2 = down_cast<Item_func *>(c);
            Item *a2 = eq2->arguments()[0]->real_item();
            Item *b2 = eq2->arguments()[1]->real_item();
            if (a2->type() == Item::FIELD_ITEM &&
                b2->type() == Item::FIELD_ITEM) {
              const Field *fa2 = down_cast<Item_field *>(a2)->field;
              const Field *fb2 = down_cast<Item_field *>(b2)->field;
              const auto &oes = sj_infos[nest].nest->nested_join
                                    ->sj_outer_exprs;
              const auto &ies = sj_infos[nest].nest->nested_join
                                    ->sj_inner_exprs;
              auto outer_equiv = [&](const Field *f) -> const Field * {
                for (size_t k = 0; k < ies.size() && k < oes.size(); ++k) {
                  Item *ii = ies[k]->real_item();
                  Item *oi = oes[k]->real_item();
                  if (ii->type() == Item::FIELD_ITEM &&
                      oi->type() == Item::FIELD_ITEM &&
                      down_cast<Item_field *>(ii)->field == f)
                    return down_cast<Item_field *>(oi)->field;
                }
                return f;
              };
              const Field *ra = outer_equiv(fa2);
              const Field *rb = outer_equiv(fb2);
              const int ta = table_index_of_field(ra);
              const int tb = table_index_of_field(rb);
              if (ta >= 0 && tb >= 0 && ta != tb && tab_nest[ta] < 0 &&
                  tab_nest[tb] < 0 && ra->result_type() == INT_RESULT &&
                  rb->result_type() == INT_RESULT) {
                const Field *rfa = resolve_in(ra, ta);
                const Field *rfb = resolve_in(rb, tb);
                if (rfa != nullptr && rfb != nullptr) {
                  edges.push_back({ta, tb, rfa, rfb});
                  continue;  // rewritten to a main edge; not a residual
                }
              }
              if (ta >= 0 && tb >= 0 && ta == tb) continue;  // tautology
            }
          }
          sj_infos[nest].residuals.push_back(c);
          continue;
        }
      }
      // Cross-table conjunct: a bare equi-join edge, or a tuple predicate
      // evaluated after the joins (OR-of-ANDs like q7/q19). An OR whose
      // branches all contain the same equi-join condition (q19's
      // p_partkey = l_partkey) contributes that edge; the OR itself is
      // still re-evaluated as a tuple filter (redundant but correct).
      if (c->type() == Item::COND_ITEM &&
          down_cast<Item_cond *>(c)->functype() == Item_func::COND_OR_FUNC) {
        auto *orc = down_cast<Item_cond *>(c);
        std::vector<std::vector<std::pair<const Field *, const Field *>>>
            branch_eqs;
        List_iterator<Item> bit(*orc->argument_list());
        for (Item *branch = bit++; branch != nullptr; branch = bit++) {
          std::vector<Item *> parts;
          flatten_and(branch, &parts);
          branch_eqs.emplace_back();
          for (Item *pc : parts) {
            if (pc->type() != Item::FUNC_ITEM ||
                down_cast<Item_func *>(pc)->functype() != Item_func::EQ_FUNC)
              continue;
            auto *peq = down_cast<Item_func *>(pc);
            Item *pa = peq->arguments()[0]->real_item();
            Item *pb = peq->arguments()[1]->real_item();
            if (pa->type() != Item::FIELD_ITEM ||
                pb->type() != Item::FIELD_ITEM)
              continue;
            branch_eqs.back().push_back(
                {down_cast<Item_field *>(pa)->field,
                 down_cast<Item_field *>(pb)->field});
          }
        }
        if (!branch_eqs.empty()) {
          for (const auto &cand : branch_eqs[0]) {
            bool in_all = true;
            for (size_t bi = 1; bi < branch_eqs.size() && in_all; ++bi) {
              bool found = false;
              for (const auto &other : branch_eqs[bi])
                if ((other.first == cand.first &&
                     other.second == cand.second) ||
                    (other.first == cand.second &&
                     other.second == cand.first)) {
                  found = true;
                  break;
                }
              in_all = found;
            }
            if (!in_all) continue;
            const int ta = table_index_of_field(cand.first);
            const int tb = table_index_of_field(cand.second);
            if (ta < 0 || tb < 0 || ta == tb) continue;
            if (cand.first->result_type() != INT_RESULT ||
                cand.second->result_type() != INT_RESULT)
              continue;
            const Field *rfa = resolve_base_field(cand.first, tabs[ta].table);
            const Field *rfb = resolve_base_field(cand.second, tabs[tb].table);
            if (rfa == nullptr || rfb == nullptr) continue;
            edges.push_back({ta, tb, rfa, rfb});
          }
        }
        tuple_conjuncts.push_back(c);
        continue;
      }
      if (c->type() != Item::FUNC_ITEM ||
          down_cast<Item_func *>(c)->functype() != Item_func::EQ_FUNC) {
        tuple_conjuncts.push_back(c);
        continue;
      }
      auto *eq = down_cast<Item_func *>(c);
      Item *a = eq->arguments()[0]->real_item();
      Item *b = eq->arguments()[1]->real_item();
      if (a->type() != Item::FIELD_ITEM || b->type() != Item::FIELD_ITEM)
        LDB_COL_REJECT("join key not a column");
      const Field *fa = down_cast<Item_field *>(a)->field;
      const Field *fb = down_cast<Item_field *>(b)->field;
      const int ta = table_index_of_field(fa);
      const int tb = table_index_of_field(fb);
      if (ta < 0 || tb < 0 || ta == tb) LDB_COL_REJECT("join key tables");
      // Byte-equality join keys: INT/DECIMAL val_str is canonical.
      if ((fa->result_type() != INT_RESULT &&
           fa->result_type() != DECIMAL_RESULT) ||
          fa->result_type() != fb->result_type())
        LDB_COL_REJECT("join key type");
      const Field *rfa = resolve_in(fa, ta);
      const Field *rfb = resolve_in(fb, tb);
      if (rfa == nullptr || rfb == nullptr) LDB_COL_REJECT("join key resolve");
      edges.push_back({ta, tb, rfa, rfb});
    }
  }
  bool has_one_row_virtual = false;
  for (int t : main_tabs)
    if (tab_virtual[t] && virtual_one_row[t - n_real])
      has_one_row_virtual = true;
  if (!plan_map && main_tabs.size() > 1 && edges.empty() &&
      !has_one_row_virtual)
    LDB_COL_REJECT("cross join");

  // Scan nodes (one per table) with their fully-pushed filters.
  int current_node = -1;
  std::vector<int> scan_node_of(n_tabs, -1);

  // Emit one table's scan node (filters from the WHERE decomposition).
  auto emit_scan = [&](size_t t) -> bool {
    auto *scan = req.add_nodes()->mutable_scan();
    scan->set_table_idx(static_cast<uint32_t>(t));
    scan_node_of[t] = req.nodes_size() - 1;
    if (!table_filters[t].empty()) {
      auto *pred = scan->mutable_filter();
      pred->set_num_columns(tabs[t].table->s->fields);
      if (table_filters[t].size() == 1) {
        if (!serialize_scan_conjunct(table_filters[t][0],
                                     pred->mutable_expr()))
          return false;
      } else {
        auto *root = pred->mutable_expr();
        root->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *c : table_filters[t])
          if (!serialize_scan_conjunct(c, root->add_children())) return false;
      }
    }
    return true;
  };

  if (plan_map) {
    // ------------------------------------------------------------------
    // Plan mapping: the join structure (order, types, keys, residuals)
    // comes straight from the optimizer's AccessPath tree. Optimizations
    // (semi filters etc.) layer on top as rules — the row-store pushdown
    // architecture, applied to the secondary engine (journal, Phase E).
    // ------------------------------------------------------------------
    std::map<int, std::set<int>> node_tabs;  // IR node -> tabs under it
    TupleColumnRegistry reg_proto;  // template for residual registries
    reg_proto.table_of = [&](const Field *f) {
      return table_index_of_field(f);
    };
    reg_proto.resolve = [&](const Field *f, int ti) {
      return resolve_in(f, ti);
    };
    // Serialize `items` as a tuple filter over (probe ++ build) columns.
    auto fill_tuple_filter =
        [&](const std::vector<Item *> &items,
            LineairDB::Protocol::QbTupleFilter *tf) -> bool {
      TupleColumnRegistry reg = reg_proto;
      auto *expr = tf->mutable_pred()->mutable_expr();
      if (items.size() == 1) {
        if (!serialize_tuple_pred(items[0], expr, &reg)) return false;
      } else {
        expr->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *c : items)
          if (!serialize_tuple_pred(c, expr->add_children(), &reg))
            return false;
      }
      tf->mutable_pred()->set_num_columns(reg.cols.size());
      for (const auto &rc : reg.cols) {
        auto *col = tf->add_columns();
        col->set_table_idx(rc.first);
        col->set_column(rc.second->field_index());
      }
      return true;
    };

    // Pre-issue every real scan, most selective first: any scan can then
    // serve as a semi-filter source for later nodes regardless of where
    // the plan placed its join.
    {
      std::vector<size_t> pre;
      for (size_t i = 0; i < n_real; ++i) pre.push_back(i);
      std::stable_sort(pre.begin(), pre.end(), [&](size_t a, size_t b) {
        return tabs[a].records < tabs[b].records;
      });
      for (size_t t : pre) {
        if (!emit_scan(t)) LDB_COL_REJECT("filter not pushable");
        node_tabs[scan_node_of[t]] = {static_cast<int>(t)};
      }
    }

    std::function<int(AccessPath *)> map_path =
        [&](AccessPath *p) -> int {
      if (p == nullptr) {
        *why = "null plan node";
        return -1;
      }
      switch (p->type) {
        case AccessPath::TABLE_SCAN: {
          const TABLE *tb = p->table_scan().table;
          int t = -1;
          for (size_t i = 0; i < n_tabs; ++i)
            if (tabs[i].table == tb) {
              t = static_cast<int>(i);
              break;
            }
          if (t < 0) {
            *why = "plan table unknown";
            return -1;
          }
          if (scan_node_of[t] < 0) {
            if (!emit_scan(t)) {
              *why = "filter not pushable";
              return -1;
            }
            node_tabs[scan_node_of[t]] = {t};
          }
          return scan_node_of[t];
        }
        case AccessPath::FILTER: {
          const int child = map_path(p->filter().child);
          if (child < 0) return -1;
          // Single-real-table conjuncts already run inside the scans;
          // everything else becomes a tuple filter over the child.
          std::vector<Item *> parts;
          flatten_and(p->filter().condition, &parts);
          std::vector<Item *> residual;
          for (Item *c : parts) {
            const table_map used = c->used_tables() & ~PSEUDO_TABLE_BITS;
            const int single = single_table_of(used, tabs);
            if (single >= 0 && !tab_virtual[single]) continue;
            residual.push_back(c);
          }
          if (residual.empty()) return child;
          auto *fn = req.add_nodes()->mutable_filter();
          fn->set_input(child);
          if (!fill_tuple_filter(residual, fn->mutable_filter())) {
            *why = "plan filter not pushable";
            return -1;
          }
          node_tabs[req.nodes_size() - 1] = node_tabs[child];
          return req.nodes_size() - 1;
        }
        case AccessPath::NESTED_LOOP_JOIN:
        case AccessPath::HASH_JOIN: {
          const bool is_nlj = p->type == AccessPath::NESTED_LOOP_JOIN;
          const int probe = map_path(is_nlj ? p->nested_loop_join().outer
                                            : p->hash_join().outer);
          if (probe < 0) return -1;
          const int build = map_path(is_nlj ? p->nested_loop_join().inner
                                            : p->hash_join().inner);
          if (build < 0) return -1;
          const JoinPredicate *jp =
              is_nlj ? p->nested_loop_join().join_predicate
                     : p->hash_join().join_predicate;
          if (jp == nullptr || jp->expr == nullptr) {
            *why = "plan join without predicate";
            return -1;
          }
          RelationalExpression *expr = jp->expr;
          LineairDB::Protocol::QbJoin::Type jt;
          switch (expr->type) {
            case RelationalExpression::INNER_JOIN:
            case RelationalExpression::STRAIGHT_INNER_JOIN:
              jt = LineairDB::Protocol::QbJoin::INNER;
              break;
            case RelationalExpression::LEFT_JOIN:
              jt = LineairDB::Protocol::QbJoin::LEFT;
              break;
            case RelationalExpression::SEMIJOIN:
              jt = LineairDB::Protocol::QbJoin::SEMI;
              break;
            case RelationalExpression::ANTIJOIN:
              jt = LineairDB::Protocol::QbJoin::ANTI;
              break;
            default:
              *why = "plan join type";
              return -1;
          }
          auto *jn = req.add_nodes()->mutable_join();
          const int self = req.nodes_size() - 1;
          jn->set_type(jt);
          jn->set_build(build);
          jn->set_probe(probe);
          for (Item_eq_base *eq : expr->equijoin_conditions) {
            Item *a = eq->get_arg(0)->real_item();
            Item *b = eq->get_arg(1)->real_item();
            if (a->type() != Item::FIELD_ITEM ||
                b->type() != Item::FIELD_ITEM) {
              *why = "plan join key shape";
              return -1;
            }
            const Field *fa = down_cast<Item_field *>(a)->field;
            const Field *fb = down_cast<Item_field *>(b)->field;
            int ta = table_index_of_field(fa);
            int tb = table_index_of_field(fb);
            if (ta < 0 || tb < 0) {
              *why = "plan join key table";
              return -1;
            }
            // Byte-equality keys: INT/DECIMAL canonical val_str.
            if ((fa->result_type() != INT_RESULT &&
                 fa->result_type() != DECIMAL_RESULT) ||
                fa->result_type() != fb->result_type()) {
              *why = "plan join key type";
              return -1;
            }
            const Field *rfa = resolve_in(fa, ta);
            const Field *rfb = resolve_in(fb, tb);
            if (rfa == nullptr || rfb == nullptr) {
              *why = "plan join key resolve";
              return -1;
            }
            if (node_tabs[build].count(ta) > 0 &&
                node_tabs[probe].count(tb) > 0) {
              // fa on build side, fb on probe side
            } else if (node_tabs[build].count(tb) > 0 &&
                       node_tabs[probe].count(ta) > 0) {
              std::swap(ta, tb);
              std::swap(rfa, rfb);
            } else {
              *why = "plan join key sides";
              return -1;
            }
            auto *bk = jn->add_build_keys();
            bk->set_table_idx(ta);
            bk->set_column(rfa->field_index());
            auto *pkk = jn->add_probe_keys();
            pkk->set_table_idx(tb);
            pkk->set_column(rfb->field_index());
          }
          if (!expr->join_conditions.empty()) {
            std::vector<Item *> residual;
            for (Item *c : expr->join_conditions) residual.push_back(c);
            if (!fill_tuple_filter(residual, jn->mutable_residual())) {
              *why = "plan join residual not pushable";
              return -1;
            }
          }
          if (jn->build_keys_size() == 0) {
            // Keyless joins are only safe against one-row deriveds; a
            // keyless INNER over base tables (q19: the equi key hides
            // inside an OR) cross-multiplies — let the syntactic builder
            // handle it (it factors branch-common equi keys out of ORs).
            bool build_one_row = true;
            for (int t2 : node_tabs[build]) {
              if (!tab_virtual[t2] || !virtual_one_row[t2 - n_real])
                build_one_row = false;
            }
            if (!build_one_row) {
              *why = "plan keyless join";
              return -1;
            }
          }
          std::set<int> united = node_tabs[probe];
          if (jt == LineairDB::Protocol::QbJoin::INNER ||
              jt == LineairDB::Protocol::QbJoin::LEFT)
            united.insert(node_tabs[build].begin(), node_tabs[build].end());
          node_tabs[self] = std::move(united);
          return self;
        }
        case AccessPath::MATERIALIZE: {
          const TABLE *tb = p->materialize().param->table;
          int t = -1;
          for (size_t i = n_real; i < n_tabs; ++i)
            if (tabs[i].table == tb) {
              t = static_cast<int>(i);
              break;
            }
          if (t < 0) {
            *why = "plan derived unknown";
            return -1;
          }
          if (scan_node_of[t] < 0) {
            auto *sub = req.add_nodes()->mutable_sub_block();
            AccessPath *sub_plan =
                p->materialize().param->query_blocks.empty()
                    ? nullptr
                    : p->materialize().param->query_blocks[0].subquery_path;
            if (!build_block(thd, virtual_inner[t - n_real],
                             *sub->mutable_block(), why, qep_rows,
                             sub_plan) &&
                !build_block(thd, virtual_inner[t - n_real],
                             *sub->mutable_block(), why, qep_rows, nullptr))
              return -1;
            scan_node_of[t] = req.nodes_size() - 1;
            node_tabs[scan_node_of[t]] = {t};
          }
          return scan_node_of[t];
        }
        case AccessPath::SORT:
          return map_path(p->sort().child);
        case AccessPath::AGGREGATE:
          return map_path(p->aggregate().child);
        case AccessPath::LIMIT_OFFSET:
          return map_path(p->limit_offset().child);
        case AccessPath::STREAM:
          return map_path(p->stream().child);
        case AccessPath::TEMPTABLE_AGGREGATE:
          return map_path(p->temptable_aggregate().subquery_path);
        default: {
          static thread_local char buf[48];
          snprintf(buf, sizeof(buf), "unmapped plan node %d",
                   static_cast<int>(p->type));
          *why = buf;
          return -1;
        }
      }
    };
    current_node = map_path(plan);
    if (current_node < 0) return false;

    // Rule (E2/E4): each derived sub-block aggregates only the key domain
    // that can actually join. Collect, from every mapped join, the real
    // columns equi-joined with the sub-block's group output, and inject
    // the most selective scan's key set (sideways information passing).
    for (int ni = 0; ni < req.nodes_size(); ++ni) {
      if (!req.nodes(ni).has_sub_block()) continue;
      // Which virtual table is this node?
      int vt = -1;
      for (size_t t = n_real; t < n_tabs; ++t)
        if (scan_node_of[t] == ni) vt = static_cast<int>(t);
      if (vt < 0) continue;
      int best_tab = -1;
      uint32_t best_src_col = 0;
      uint32_t derived_ordinal = 0;
      for (int j = 0; j < req.nodes_size(); ++j) {
        if (!req.nodes(j).has_join()) continue;
        const auto &jn = req.nodes(j).join();
        for (int k = 0; k < jn.build_keys_size(); ++k) {
          const auto &bk = jn.build_keys(k);
          const auto &pk = jn.probe_keys(k);
          uint32_t vcol = 0;
          uint32_t oti = 0, ocol = 0;
          if (bk.table_idx() == static_cast<uint32_t>(vt)) {
            vcol = bk.column();
            oti = pk.table_idx();
            ocol = pk.column();
          } else if (pk.table_idx() == static_cast<uint32_t>(vt)) {
            vcol = pk.column();
            oti = bk.table_idx();
            ocol = bk.column();
          } else {
            continue;
          }
          if (oti >= n_real) continue;  // real-table sources only
          if (best_tab < 0 ||
              tabs[oti].records < tabs[best_tab].records) {
            best_tab = static_cast<int>(oti);
            derived_ordinal = vcol;
            best_src_col = ocol;
          }
        }
      }
      if (best_tab < 0) continue;
      const uint32_t src_col = best_src_col;
      auto *sub = req.mutable_nodes(ni)->mutable_sub_block();
      const auto &sreq = sub->block();
      if (derived_ordinal >= static_cast<uint32_t>(sreq.output_size()) ||
          sreq.output(derived_ordinal).source() !=
              LineairDB::Protocol::QbOutputExpr::GROUP ||
          sreq.nodes_size() == 0 ||
          !sreq.nodes(sreq.nodes_size() - 1).has_aggregate())
        continue;
      const auto &sagg = sreq.nodes(sreq.nodes_size() - 1).aggregate();
      const uint32_t g = sreq.output(derived_ordinal).ordinal();
      if (g >= static_cast<uint32_t>(sagg.group_columns_size()) ||
          sagg.group_columns(g).prefix_len() != 0)
        continue;
      auto *sm = sub->mutable_semi();
      sm->set_source_node(scan_node_of[best_tab]);
      auto *sc = sm->mutable_source_column();
      sc->set_table_idx(best_tab);
      sc->set_column(src_col);
      sub->set_target_table(sagg.group_columns(g).table_idx());
      sub->set_target_column(sagg.group_columns(g).column());
    }
  } else {
  // Node issue order follows the optimizer's cardinality estimates: the
  // most selective tables execute first, and each later scan/sub-block
  // gains a semi-join key filter from the smallest already-issued table
  // it joins with (sideways information passing; q17's derived aggregates
  // only the ~20 filtered part keys instead of every l_partkey).
  // Real scans issue by ascending estimate (selective tables first, so
  // they can seed semi filters); sub-blocks always issue last, in tabs
  // order (the server numbers virtual tables by sub_block appearance),
  // receiving domain filters from the already-issued scans.
  std::vector<size_t> issue_order;
  for (size_t i = 0; i < n_tabs; ++i) issue_order.push_back(i);

  std::vector<bool> issued(n_tabs, false);
  for (size_t t : issue_order) {
    // Best semi source: smallest issued table sharing an edge, at least
    // 8x more selective. LEFT-derived and nest tables keep plan-level
    // semantics; semi filters only reduce scan/aggregation input.
    int semi_src = -1;
    const Field *semi_src_field = nullptr;
    const Field *semi_my_field = nullptr;
    for (const auto &e : edges) {
      int other = -1;
      const Field *of = nullptr;
      const Field *mf = nullptr;
      if (e.t1 == static_cast<int>(t)) {
        other = e.t2;
        of = e.f2;
        mf = e.f1;
      } else if (e.t2 == static_cast<int>(t)) {
        other = e.t1;
        of = e.f1;
        mf = e.f2;
      } else {
        continue;
      }
      if (!issued[other]) continue;
      if (!tab_virtual[t] && tabs[other].records > tabs[t].records / 8)
        continue;
      if (semi_src < 0 || tabs[other].records < tabs[semi_src].records) {
        semi_src = other;
        semi_src_field = of;
        semi_my_field = mf;
      }
    }

    if (!tab_virtual[t]) {
      auto *scan = req.add_nodes()->mutable_scan();
      scan->set_table_idx(static_cast<uint32_t>(t));
      scan_node_of[t] = req.nodes_size() - 1;
      if (!table_filters[t].empty()) {
        auto *pred = scan->mutable_filter();
        pred->set_num_columns(tabs[t].table->s->fields);
        if (table_filters[t].size() == 1) {
          if (!serialize_scan_conjunct(table_filters[t][0],
                                       pred->mutable_expr()))
            LDB_COL_REJECT("filter not pushable");
        } else {
          auto *root = pred->mutable_expr();
          root->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
          for (Item *c : table_filters[t]) {
            if (!serialize_scan_conjunct(c, root->add_children()))
              LDB_COL_REJECT("filter not pushable");
          }
        }
      }
      if (semi_src >= 0) {
        auto *sm = scan->mutable_semi();
        sm->set_source_node(scan_node_of[semi_src]);
        auto *sc = sm->mutable_source_column();
        sc->set_table_idx(semi_src);
        sc->set_column(semi_src_field->field_index());
        sm->set_my_column(semi_my_field->field_index());
      }
    } else {
      auto *sub = req.add_nodes()->mutable_sub_block();
      if (!build_block(thd, virtual_inner[t - n_real],
                       *sub->mutable_block(), why, qep_rows))
        return false;
      scan_node_of[t] = req.nodes_size() - 1;
      if (semi_src >= 0) {
        // Resolve the derived column (output ordinal) to the base table
        // and column inside the sub-block via its GROUP output mapping.
        const auto &sreq = sub->block();
        const uint32_t ordinal = semi_my_field->field_index();
        if (ordinal < static_cast<uint32_t>(sreq.output_size()) &&
            sreq.output(ordinal).source() ==
                LineairDB::Protocol::QbOutputExpr::GROUP &&
            sreq.nodes_size() > 0 &&
            sreq.nodes(sreq.nodes_size() - 1).has_aggregate()) {
          const auto &sagg = sreq.nodes(sreq.nodes_size() - 1).aggregate();
          const uint32_t g = sreq.output(ordinal).ordinal();
          if (g < static_cast<uint32_t>(sagg.group_columns_size()) &&
              sagg.group_columns(g).prefix_len() == 0) {
            auto *sm = sub->mutable_semi();
            sm->set_source_node(scan_node_of[semi_src]);
            auto *sc = sm->mutable_source_column();
            sc->set_table_idx(semi_src);
            sc->set_column(semi_src_field->field_index());
            sub->set_target_table(sagg.group_columns(g).table_idx());
            sub->set_target_column(sagg.group_columns(g).column());
          }
        }
      }
    }
    issued[t] = true;
  }

  // Join tree ordered by the optimizer's cardinality estimates (QEP
  // post-filter rows when available): the most selective table drives and
  // each next table is the smallest one connected to the joined set. The
  // server additionally swaps INNER build/probe by actual size and caps
  // intermediate cardinality.
  // Real tables keep FROM order (an estimate-greedy order can pick an M:N
  // edge first and blow up the intermediate — q5); the QEP estimates guide
  // the semi filters below, not the join shape.
  std::vector<int> join_order = main_tabs;

  std::vector<bool> joined(n_tabs, false);
  {
    joined[join_order[0]] = true;
    current_node = scan_node_of[join_order[0]];
    std::vector<int> pending(join_order.begin() + 1, join_order.end());
    while (!pending.empty()) {
      // First FROM-order table connected to the joined set (tables whose
      // edges are not joined yet get retried later).
      int pick = -1;
      size_t pick_pos = 0;
      for (size_t i = 0; i < pending.size(); ++i) {
        for (const auto &e : edges)
          if ((e.t1 == pending[i] && joined[e.t2]) ||
              (e.t2 == pending[i] && joined[e.t1])) {
            pick = pending[i];
            pick_pos = i;
            break;
          }
        if (pick >= 0) break;
      }
      if (pick < 0) {
        // A one-row derived (uncorrelated scalar subquery, q22's AVG) may
        // legally cross join: zero-key join against a single row.
        for (size_t i = 0; i < pending.size(); ++i) {
          const int t = pending[i];
          if (tab_virtual[t] && virtual_one_row[t - n_real]) {
            pick = t;
            pick_pos = i;
            break;
          }
        }
      }
      if (pick < 0) LDB_COL_REJECT("disconnected join graph");
      pending.erase(pending.begin() + pick_pos);
      auto *node = req.add_nodes();
      auto *jn = node->mutable_join();
      jn->set_type(LineairDB::Protocol::QbJoin::INNER);
      jn->set_build(scan_node_of[pick]);
      jn->set_probe(current_node);
      for (const auto &e : edges) {
        const Field *bf = nullptr;
        const Field *pf = nullptr;
        int ptab = -1;
        if (e.t1 == pick && joined[e.t2]) {
          bf = e.f1;
          pf = e.f2;
          ptab = e.t2;
        } else if (e.t2 == pick && joined[e.t1]) {
          bf = e.f2;
          pf = e.f1;
          ptab = e.t1;
        } else {
          continue;
        }
        auto *bk = jn->add_build_keys();
        bk->set_table_idx(pick);
        bk->set_column(bf->field_index());
        auto *pkk = jn->add_probe_keys();
        pkk->set_table_idx(ptab);
        pkk->set_column(pf->field_index());
      }
      joined[pick] = true;
      current_node = req.nodes_size() - 1;
    }
  }

  // Outer-joined derived tables (MySQL's NOT EXISTS / antijoin transform,
  // q21): LEFT join with the ON condition split into equi keys and a
  // per-match residual; the WHERE's IS NULL test runs in the tuple filter.
  // Nest-inner columns referenced outside their nest (equality propagation)
  // rewrite to the sj_outer equivalent — the SEMI/ANTI output only carries
  // probe-side tables.
  auto nest_outer_equiv = [&](const Field *f) -> const Field * {
    for (auto &sj : sj_infos) {
      const auto &oes = sj.nest->nested_join->sj_outer_exprs;
      const auto &ies = sj.nest->nested_join->sj_inner_exprs;
      for (size_t k = 0; k < ies.size() && k < oes.size(); ++k) {
        Item *ii = ies[k]->real_item();
        Item *oi = oes[k]->real_item();
        if (ii->type() == Item::FIELD_ITEM &&
            oi->type() == Item::FIELD_ITEM &&
            down_cast<Item_field *>(ii)->field == f)
          return down_cast<Item_field *>(oi)->field;
      }
    }
    return f;
  };

  for (int vt : left_deriveds) {
    Table_ref *tl = tabs[vt].tl;
    Item *on_cond = tl->join_cond();
    if (on_cond == nullptr) LDB_COL_REJECT("derived LEFT without ON");
    std::vector<Item *> on_parts;
    flatten_and(on_cond, &on_parts);
    auto *jn = req.add_nodes()->mutable_join();
    jn->set_type(LineairDB::Protocol::QbJoin::LEFT);
    jn->set_build(scan_node_of[vt]);
    jn->set_probe(current_node);
    std::vector<Item *> residuals;
    for (Item *c : on_parts) {
      bool is_key = false;
      if (c->type() == Item::FUNC_ITEM &&
          down_cast<Item_func *>(c)->functype() == Item_func::EQ_FUNC) {
        auto *eq = down_cast<Item_func *>(c);
        Item *a = eq->arguments()[0]->real_item();
        Item *b = eq->arguments()[1]->real_item();
        if (a->type() == Item::FIELD_ITEM && b->type() == Item::FIELD_ITEM) {
          const Field *fa = nest_outer_equiv(
              down_cast<Item_field *>(a)->field);
          const Field *fb = nest_outer_equiv(
              down_cast<Item_field *>(b)->field);
          int ta = table_index_of_field(fa);
          int tb = table_index_of_field(fb);
          if (ta == vt || tb == vt) {
            if (ta != vt) {
              std::swap(ta, tb);
              std::swap(fa, fb);
            }
            // fa/ta = derived side, fb/tb = probe side
            if (tb >= 0 && fa->result_type() == INT_RESULT &&
                fb->result_type() == INT_RESULT) {
              const Field *rfa = resolve_in(fa, ta);
              const Field *rfb = resolve_in(fb, tb);
              if (rfa != nullptr && rfb != nullptr) {
                auto *bk = jn->add_build_keys();
                bk->set_table_idx(vt);
                bk->set_column(rfa->field_index());
                auto *pkk = jn->add_probe_keys();
                pkk->set_table_idx(tb);
                pkk->set_column(rfb->field_index());
                is_key = true;
              }
            }
          }
        }
      }
      if (!is_key) residuals.push_back(c);
    }
    if (jn->build_keys_size() == 0 && !virtual_one_row[vt - n_real])
      LDB_COL_REJECT("derived LEFT keyless");
    if (!residuals.empty()) {
      TupleColumnRegistry reg;
      reg.table_of = [&](const Field *f) { return table_index_of_field(f); };
      reg.resolve = [&](const Field *f, int ti) { return resolve_in(f, ti); };
      auto *tf = jn->mutable_residual();
      auto *expr = tf->mutable_pred()->mutable_expr();
      if (residuals.size() == 1) {
        if (!serialize_tuple_pred(residuals[0], expr, &reg))
          LDB_COL_REJECT("derived ON residual not pushable");
      } else {
        expr->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *c : residuals)
          if (!serialize_tuple_pred(c, expr->add_children(), &reg))
            LDB_COL_REJECT("derived ON residual not pushable");
      }
      tf->mutable_pred()->set_num_columns(reg.cols.size());
      for (const auto &rc : reg.cols) {
        auto *col = tf->add_columns();
        col->set_table_idx(rc.first);
        col->set_column(rc.second->field_index());
      }
    }
    current_node = req.nodes_size() - 1;
  }

  // Cross-table non-equi conjuncts run as a tuple filter over the joined
  // result (executor-standard residual filtering; see lineage log #15).
  if (!tuple_conjuncts.empty()) {
    auto *fn = req.add_nodes()->mutable_filter();
    fn->set_input(current_node);
    auto *tf = fn->mutable_filter();
    TupleColumnRegistry reg;
    reg.table_of = [&](const Field *f) { return table_index_of_field(f); };
    reg.resolve = [&](const Field *f, int ti) {
      return resolve_base_field(f, tabs[ti].table);
    };
    auto *expr = tf->mutable_pred()->mutable_expr();
    if (tuple_conjuncts.size() == 1) {
      if (!serialize_tuple_pred(tuple_conjuncts[0], expr, &reg))
        LDB_COL_REJECT("tuple predicate not pushable");
    } else {
      expr->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
      for (Item *c : tuple_conjuncts)
        if (!serialize_tuple_pred(c, expr->add_children(), &reg))
          LDB_COL_REJECT("tuple predicate not pushable");
    }
    tf->mutable_pred()->set_num_columns(reg.cols.size());
    for (const auto &rc : reg.cols) {
      auto *col = tf->add_columns();
      col->set_table_idx(rc.first);
      col->set_column(rc.second->field_index());
    }
    current_node = req.nodes_size() - 1;
  }

  // SEMI/ANTI joins for the unnested EXISTS / NOT EXISTS nests
  // (Neumann-Kemper unnesting; lineage log #12/#13).
  for (auto &sj : sj_infos) {
    if (sj.inner_tabs.empty()) LDB_COL_REJECT("empty nest");
    // Multi-table nests (q20): join the nest's tables into one build-side
    // tuple first. Nest-internal equi conjuncts become the mini-tree's
    // edges; everything else stays a per-match residual.
    int build_node;
    const int inner = sj.inner_tabs[0];
    if (sj.inner_tabs.size() == 1) {
      build_node = scan_node_of[inner];
    } else {
      std::vector<Item *> kept;
      struct InnerEdge {
        int t1, t2;
        const Field *f1, *f2;
      };
      std::vector<InnerEdge> in_edges;
      auto in_nest = [&](int t) {
        for (int x : sj.inner_tabs)
          if (x == t) return true;
        return false;
      };
      for (Item *c : sj.residuals) {
        bool consumed = false;
        if (c->type() == Item::FUNC_ITEM &&
            down_cast<Item_func *>(c)->functype() == Item_func::EQ_FUNC) {
          auto *eq = down_cast<Item_func *>(c);
          Item *a = eq->arguments()[0]->real_item();
          Item *b = eq->arguments()[1]->real_item();
          if (a->type() == Item::FIELD_ITEM &&
              b->type() == Item::FIELD_ITEM) {
            const Field *fa = down_cast<Item_field *>(a)->field;
            const Field *fb = down_cast<Item_field *>(b)->field;
            const int ta = table_index_of_field(fa);
            const int tb = table_index_of_field(fb);
            if (ta >= 0 && tb >= 0 && ta != tb && in_nest(ta) &&
                in_nest(tb) &&
                (fa->result_type() == INT_RESULT ||
                 fa->result_type() == DECIMAL_RESULT) &&
                fa->result_type() == fb->result_type()) {
              const Field *rfa = resolve_in(fa, ta);
              const Field *rfb = resolve_in(fb, tb);
              if (rfa != nullptr && rfb != nullptr) {
                in_edges.push_back({ta, tb, rfa, rfb});
                consumed = true;
              }
            }
          }
        }
        if (!consumed) kept.push_back(c);
      }
      sj.residuals = std::move(kept);
      std::vector<bool> in_joined(n_tabs, false);
      in_joined[inner] = true;
      build_node = scan_node_of[inner];
      std::vector<int> pending2(sj.inner_tabs.begin() + 1,
                                sj.inner_tabs.end());
      while (!pending2.empty()) {
        int pick = -1;
        size_t pos2 = 0;
        for (size_t i = 0; i < pending2.size(); ++i) {
          for (const auto &e : in_edges)
            if ((e.t1 == pending2[i] && in_joined[e.t2]) ||
                (e.t2 == pending2[i] && in_joined[e.t1])) {
              pick = pending2[i];
              pos2 = i;
              break;
            }
          if (pick >= 0) break;
        }
        if (pick < 0) {
          for (size_t i = 0; i < pending2.size(); ++i) {
            const int t = pending2[i];
            if (tab_virtual[t] && virtual_one_row[t - n_real]) {
              pick = t;
              pos2 = i;
              break;
            }
          }
        }
        if (pick < 0) LDB_COL_REJECT("nest disconnected");
        pending2.erase(pending2.begin() + pos2);
        auto *njn = req.add_nodes()->mutable_join();
        njn->set_type(LineairDB::Protocol::QbJoin::INNER);
        njn->set_build(scan_node_of[pick]);
        njn->set_probe(build_node);
        for (const auto &e : in_edges) {
          const Field *bf = nullptr;
          const Field *pf = nullptr;
          int ptab = -1;
          if (e.t1 == pick && in_joined[e.t2]) {
            bf = e.f1;
            pf = e.f2;
            ptab = e.t2;
          } else if (e.t2 == pick && in_joined[e.t1]) {
            bf = e.f2;
            pf = e.f1;
            ptab = e.t1;
          } else {
            continue;
          }
          auto *bk = njn->add_build_keys();
          bk->set_table_idx(pick);
          bk->set_column(bf->field_index());
          auto *pkk = njn->add_probe_keys();
          pkk->set_table_idx(ptab);
          pkk->set_column(pf->field_index());
        }
        in_joined[pick] = true;
        build_node = req.nodes_size() - 1;
      }
    }
    auto *jn = req.add_nodes()->mutable_join();
    jn->set_type(sj.anti ? LineairDB::Protocol::QbJoin::ANTI
                         : LineairDB::Protocol::QbJoin::SEMI);
    jn->set_build(build_node);
    jn->set_probe(current_node);
    const auto &oes = sj.nest->nested_join->sj_outer_exprs;
    const auto &ies = sj.nest->nested_join->sj_inner_exprs;
    if (oes.size() != ies.size() || oes.empty())
      LDB_COL_REJECT("nest key arity");
    for (size_t k = 0; k < oes.size(); ++k) {
      Item *oi = oes[k]->real_item();
      Item *ii = ies[k]->real_item();
      if (oi->type() != Item::FIELD_ITEM || ii->type() != Item::FIELD_ITEM)
        LDB_COL_REJECT("nest key shape");
      const Field *ofr = down_cast<Item_field *>(oi)->field;
      const Field *ifr = down_cast<Item_field *>(ii)->field;
      const int oti = table_index_of_field(ofr);
      const int iti = table_index_of_field(ifr);
      if (oti < 0 || iti != inner) LDB_COL_REJECT("nest key tables");
      if ((ofr->result_type() != INT_RESULT &&
           ofr->result_type() != DECIMAL_RESULT) ||
          ofr->result_type() != ifr->result_type())
        LDB_COL_REJECT("nest key type");
      const Field *rof = resolve_in(ofr, oti);
      const Field *rif = resolve_in(ifr, iti);
      if (rof == nullptr || rif == nullptr) LDB_COL_REJECT("nest key resolve");
      auto *bk = jn->add_build_keys();
      bk->set_table_idx(inner);
      bk->set_column(rif->field_index());
      auto *pkk = jn->add_probe_keys();
      pkk->set_table_idx(oti);
      pkk->set_column(rof->field_index());
    }
    if (!sj.residuals.empty()) {
      TupleColumnRegistry reg;
      reg.table_of = [&](const Field *f) { return table_index_of_field(f); };
      reg.resolve = [&](const Field *f, int ti) {
        return resolve_base_field(f, tabs[ti].table);
      };
      auto *tf = jn->mutable_residual();
      auto *expr = tf->mutable_pred()->mutable_expr();
      if (sj.residuals.size() == 1) {
        if (!serialize_tuple_pred(sj.residuals[0], expr, &reg))
          LDB_COL_REJECT("nest residual not pushable");
      } else {
        expr->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *c : sj.residuals)
          if (!serialize_tuple_pred(c, expr->add_children(), &reg))
            LDB_COL_REJECT("nest residual not pushable");
      }
      tf->mutable_pred()->set_num_columns(reg.cols.size());
      for (const auto &rc : reg.cols) {
        auto *col = tf->add_columns();
        col->set_table_idx(rc.first);
        col->set_column(rc.second->field_index());
      }
    }
    current_node = req.nodes_size() - 1;
  }
  }  // end legacy (syntactic) join construction

  // Row-returning block: emit base columns from the final tuples.
  if (plain_rows) {
    std::vector<Item *> out_items;
    for (Item *item : VisibleFields(qb->fields)) out_items.push_back(item);
    for (Item *item : out_items) {
      Item *real = item->real_item();
      if (real->type() != Item::FIELD_ITEM)
        LDB_COL_REJECT("row output not a column");
      const Field *raw = down_cast<Item_field *>(real)->field;
      const int ti = table_index_of_field(raw);
      const Field *of = ti >= 0 ? resolve_in(raw, ti) : nullptr;
      if (of == nullptr) LDB_COL_REJECT("row output unresolvable");
      auto *oe = req.add_output();
      oe->set_source(LineairDB::Protocol::QbOutputExpr::COLUMN);
      auto *col = oe->mutable_column();
      col->set_table_idx(ti);
      col->set_column(of->field_index());
    }
    for (ORDER *o = qb->order_list.first; o != nullptr; o = o->next) {
      const int ord = order_output_ordinal(*o->item, out_items);
      if (ord < 0) LDB_COL_REJECT("row ORDER BY");
      auto *k = req.add_order_by();
      k->set_output_ordinal(ord);
      k->set_descending(o->direction == ORDER_DESC);
      Item *oi = out_items[ord]->real_item();
      k->set_cmp_kind(oi->result_type() == STRING_RESULT ? 1 : 0);
    }
    if (qb->has_limit()) {
      if (unit->select_limit_cnt != HA_POS_ERROR)
        req.set_limit(unit->select_limit_cnt);
      if (unit->offset_limit_cnt > 0) {
        req.set_offset(unit->offset_limit_cnt);
        if (req.limit() > 0)
          req.set_limit(req.limit() - unit->offset_limit_cnt);
      }
    }
    (void)thd;
    *why = nullptr;
    return true;
  }

  // Aggregate node over the join result.
  auto *agg_node = req.add_nodes();
  auto *agg = agg_node->mutable_aggregate();
  agg->set_input(current_node);
  struct GroupRef {
    int table;
    const Field *field;
  };
  std::vector<GroupRef> group_fields;
  std::vector<Item *> group_items;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next) {
    Item *gi = (*g->item)->real_item();
    group_items.push_back(gi);
    if (const Field *yf = extract_year_field(gi)) {
      const int ti = table_index_of_field(yf);
      const Field *rf = ti >= 0 ? resolve_in(yf, ti) : nullptr;
      if (rf == nullptr || rf->is_nullable()) LDB_COL_REJECT("group year col");
      auto *gc = agg->add_group_columns();
      gc->set_table_idx(ti);
      gc->set_column(rf->field_index());
      gc->set_prefix_len(4);  // "YYYY-MM-DD" -> "YYYY"
      gc->set_cmp_kind(0);
      group_fields.push_back({ti, nullptr});
      continue;
    }
    uint32_t sub_prefix = 0;
    if (const Field *sf = substr_prefix_field(gi, &sub_prefix)) {
      const int ti = table_index_of_field(sf);
      const Field *rf = ti >= 0 ? resolve_in(sf, ti) : nullptr;
      if (rf == nullptr || rf->is_nullable())
        LDB_COL_REJECT("group substr col");
      auto *gc = agg->add_group_columns();
      gc->set_table_idx(ti);
      gc->set_column(rf->field_index());
      gc->set_prefix_len(sub_prefix);
      gc->set_cmp_kind(1);
      group_fields.push_back({ti, nullptr});
      continue;
    }
    if (gi->type() != Item::FIELD_ITEM) LDB_COL_REJECT("group not a column");
    const Field *raw = down_cast<Item_field *>(gi)->field;
    const int ti = table_index_of_field(raw);
    if (ti < 0) LDB_COL_REJECT("group column foreign");
    const Field *gf = resolve_in(raw, ti);
    if (gf == nullptr || (gf->is_nullable() && !tab_virtual[ti]))
      LDB_COL_REJECT("group column nullable");
    // Byte-equality grouping: INT/DECIMAL val_str renderings are canonical;
    // strings rely on the TPC-H value domains (md5-gated).
    if (gf->result_type() != INT_RESULT &&
        gf->result_type() != STRING_RESULT &&
        gf->result_type() != DECIMAL_RESULT)
      LDB_COL_REJECT("group column type");
    auto *gc = agg->add_group_columns();
    gc->set_table_idx(ti);
    gc->set_column(gf->field_index());
    gc->set_cmp_kind(gf->result_type() == STRING_RESULT ? 1 : 0);
    group_fields.push_back({ti, gf});
  }

  if (distinct_as_group) {
    for (Item *item : VisibleFields(qb->fields)) {
      Item *real = item->real_item();
      if (real->const_item()) continue;  // constants don't affect DISTINCT
      if (real->type() != Item::FIELD_ITEM) LDB_COL_REJECT("distinct expr");
      const Field *raw = down_cast<Item_field *>(real)->field;
      const int ti = table_index_of_field(raw);
      const Field *gf = ti >= 0 ? resolve_in(raw, ti) : nullptr;
      if (gf == nullptr) LDB_COL_REJECT("distinct column");
      auto *gc = agg->add_group_columns();
      gc->set_table_idx(ti);
      gc->set_column(gf->field_index());
      gc->set_cmp_kind(gf->result_type() == STRING_RESULT ? 1 : 0);
      group_fields.push_back({ti, gf});
      group_items.push_back(real);
    }
  }

  // Output expressions from the visible field list (pre-optimizer items).
  // register_aggregate returns the aggregate's ordinal (or -1 + why).
  const char *agg_why = nullptr;
  auto register_aggregate = [&](Item_sum *sum) -> int {
    auto *af = agg->add_aggs();
    switch (sum->sum_func()) {
      case Item_sum::COUNT_DISTINCT_FUNC: {
        if (sum->argument_count() != 1) {
          agg_why = "COUNT DISTINCT arity";
          return -1;
        }
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->type() != Item::FIELD_ITEM) {
          agg_why = "COUNT DISTINCT arg";
          return -1;
        }
        const Field *raw = down_cast<Item_field *>(arg)->field;
        const int ti = table_index_of_field(raw);
        const Field *cf = ti >= 0 ? resolve_in(raw, ti) : nullptr;
        if (cf == nullptr) {
          agg_why = "COUNT DISTINCT column";
          return -1;
        }
        af->set_arg_table(ti);
        af->mutable_arg()->set_op(
            LineairDB::Protocol::FilterExpr::COLUMN_REF);
        af->mutable_arg()->set_column_index(cf->field_index());
        af->set_kind(LineairDB::Protocol::QbAggFunc::COUNT);
        af->set_distinct(true);
        return agg->aggs_size() - 1;
      }
      case Item_sum::COUNT_FUNC: {
        if (sum->argument_count() != 1) {
          agg_why = "COUNT arg count";
          return -1;
        }
        Item *arg = sum->get_arg(0)->real_item();
        af->set_arg_table(0);
        if (!arg->const_item()) {
          if (arg->type() != Item::FIELD_ITEM) {
            agg_why = "COUNT arg shape";
            return -1;
          }
          const Field *raw = down_cast<Item_field *>(arg)->field;
          const int ti = table_index_of_field(raw);
          const Field *cf = ti >= 0 ? resolve_in(raw, ti) : nullptr;
          if (cf == nullptr || cf->is_nullable()) {
            agg_why = "COUNT arg nullable";
            return -1;
          }
          af->set_arg_table(ti);
          af->mutable_arg()->set_op(
              LineairDB::Protocol::FilterExpr::COLUMN_REF);
          af->mutable_arg()->set_column_index(cf->field_index());
        }
        af->set_kind(LineairDB::Protocol::QbAggFunc::COUNT);
        return agg->aggs_size() - 1;
      }
      case Item_sum::SUM_FUNC:
      case Item_sum::AVG_FUNC: {
        if (sum->argument_count() != 1) {
          agg_why = "agg arg count";
          return -1;
        }
        Item *arg = sum->get_arg(0)->real_item();
        if (sum->sum_func() == Item_sum::SUM_FUNC) {
          if (Item *pred = case_to_count_filter(arg)) {
            const int ti = single_table_of(
                pred->used_tables() & ~PSEUDO_TABLE_BITS, tabs);
            if (ti < 0) {
              agg_why = "CASE filter tables";
              return -1;
            }
            auto *pf = af->mutable_filter();
            pf->set_num_columns(tabs[ti].table->s->fields);
            if (!serialize_item(pred, pf->mutable_expr())) {
              agg_why = "CASE filter not pushable";
              return -1;
            }
            af->set_arg_table(ti);
            af->set_filter_table(ti);
            af->set_kind(LineairDB::Protocol::QbAggFunc::COUNT);
            return agg->aggs_size() - 1;
          }
          Item *pred = nullptr;
          Item *then_expr = nullptr;
          if (case_to_filtered_sum(arg, &pred, &then_expr)) {
            const int pti = single_table_of(
                pred->used_tables() & ~PSEUDO_TABLE_BITS, tabs);
            if (pti < 0) {
              agg_why = "CASE filter tables";
              return -1;
            }
            auto *pf = af->mutable_filter();
            pf->set_num_columns(tabs[pti].table->s->fields);
            if (!serialize_item(pred, pf->mutable_expr())) {
              agg_why = "CASE filter not pushable";
              return -1;
            }
            af->set_filter_table(pti);
            Item *texpr = then_expr->real_item();
            int ti = single_table_of(
                texpr->used_tables() & ~PSEUDO_TABLE_BITS, tabs);
            if (ti >= 0) {
              if (!helios_serialize_arith(texpr, af->mutable_arg())) {
                agg_why = "CASE expr not pushable";
                return -1;
              }
            } else {
              auto resolver2 = [&](const Field *f, int t) -> const Field * {
                return resolve_base_field(f, tabs[t].table);
              };
              if (!serialize_arith_multi(
                      texpr, af->mutable_arg(),
                      [&](const Field *f) { return table_index_of_field(f); },
                      resolver2)) {
                agg_why = "CASE expr not pushable";
                return -1;
              }
              ti = 0;
            }
            af->set_arg_table(ti);
            af->set_kind(LineairDB::Protocol::QbAggFunc::SUM);
            af->set_arg_scale(texpr->decimals);
            af->set_zero_if_empty(true);
            return agg->aggs_size() - 1;
          }
        }
        if (arg->result_type() != DECIMAL_RESULT &&
            arg->result_type() != INT_RESULT) {
          agg_why = "agg arg type";
          return -1;
        }
        int ti = single_table_of(arg->used_tables() & ~PSEUDO_TABLE_BITS,
                                 tabs);
        if (ti >= 0) {
          if (!helios_serialize_arith(arg, af->mutable_arg())) {
            agg_why = "agg expr not pushable";
            return -1;
          }
        } else {
          auto resolver2 = [&](const Field *f, int t) -> const Field * {
            return resolve_base_field(f, tabs[t].table);
          };
          if (!serialize_arith_multi(
                  arg, af->mutable_arg(),
                  [&](const Field *f) { return table_index_of_field(f); },
                  resolver2)) {
            agg_why = "agg expr not pushable";
            return -1;
          }
          ti = 0;
        }
        af->set_arg_table(ti);
        af->set_kind(sum->sum_func() == Item_sum::SUM_FUNC
                         ? LineairDB::Protocol::QbAggFunc::SUM
                         : LineairDB::Protocol::QbAggFunc::AVG);
        af->set_arg_scale(arg->decimals);
        return agg->aggs_size() - 1;
      }
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        if (sum->argument_count() != 1) {
          agg_why = "agg arg count";
          return -1;
        }
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->type() != Item::FIELD_ITEM) {
          agg_why = "minmax arg";
          return -1;
        }
        const Field *raw = down_cast<Item_field *>(arg)->field;
        const int ti = table_index_of_field(raw);
        const Field *mf = ti >= 0 ? resolve_in(raw, ti) : nullptr;
        if (mf == nullptr) {
          agg_why = "minmax unresolvable";
          return -1;
        }
        af->mutable_arg()->set_op(
            LineairDB::Protocol::FilterExpr::COLUMN_REF);
        af->mutable_arg()->set_column_index(mf->field_index());
        af->set_arg_table(ti);
        af->set_kind(sum->sum_func() == Item_sum::MIN_FUNC
                         ? LineairDB::Protocol::QbAggFunc::MIN
                         : LineairDB::Protocol::QbAggFunc::MAX);
        af->set_cmp_kind(mf->result_type() == STRING_RESULT ? 1 : 0);
        return agg->aggs_size() - 1;
      }
      default:
        agg_why = "unsupported aggregate";
        return -1;
    }
  };

  // Serialize an output expression over aggregates (q8/q14 SUM ratios):
  // Item_sum children become COLUMN_REF ordinals into the stage-1 layout.
  const int n_grp_final = agg->group_columns_size();
  std::function<bool(Item *, LineairDB::Protocol::FilterExpr *)> ser_out =
      [&](Item *it, LineairDB::Protocol::FilterExpr *out) -> bool {
    it = it->real_item();
    if (it->type() == Item::SUM_FUNC_ITEM) {
      const int ord = register_aggregate(down_cast<Item_sum *>(it));
      if (ord < 0) return false;
      out->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      out->set_column_index(n_grp_final + ord);
      return true;
    }
    if (it->type() == Item::FIELD_ITEM) {
      // A one-row derived column (uncorrelated scalar subquery, q11's
      // HAVING threshold): register MIN(col) — identical over one row —
      // so the value rides the aggregate ordinal space.
      const Field *raw = down_cast<Item_field *>(it)->field;
      const int ti = table_index_of_field(raw);
      if (ti < 0 || !tab_virtual[ti] || !virtual_one_row[ti - n_real])
        return false;
      auto *af = agg->add_aggs();
      af->set_arg_table(ti);
      af->mutable_arg()->set_op(
          LineairDB::Protocol::FilterExpr::COLUMN_REF);
      af->mutable_arg()->set_column_index(raw->field_index());
      af->set_kind(LineairDB::Protocol::QbAggFunc::MIN);
      af->set_cmp_kind(raw->result_type() == STRING_RESULT ? 1 : 0);
      out->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      out->set_column_index(n_grp_final + agg->aggs_size() - 1);
      return true;
    }
    if (it->type() == Item::INT_ITEM) {
      out->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
      out->set_int_val(it->val_int());
      return true;
    }
    if (it->const_item() && (it->result_type() == DECIMAL_RESULT ||
                             it->result_type() == REAL_RESULT)) {
      StringBuffer<STRING_BUFFER_USUAL_SIZE> buf;
      String *sv = it->val_str(&buf);
      if (sv == nullptr) return false;
      out->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
      out->set_string_val(sv->ptr(), sv->length());
      return true;
    }
    if (it->type() != Item::FUNC_ITEM) return false;
    auto *fn = down_cast<Item_func *>(it);
    const char *name = fn->func_name();
    LineairDB::Protocol::FilterExpr::Op op;
    if (strcmp(name, "+") == 0)
      op = LineairDB::Protocol::FilterExpr::OP_ADD;
    else if (strcmp(name, "-") == 0)
      op = fn->argument_count() == 1
               ? LineairDB::Protocol::FilterExpr::OP_NEG
               : LineairDB::Protocol::FilterExpr::OP_SUB;
    else if (strcmp(name, "*") == 0)
      op = LineairDB::Protocol::FilterExpr::OP_MUL;
    else if (strcmp(name, "/") == 0)
      op = LineairDB::Protocol::FilterExpr::OP_DIV;
    else
      return false;
    out->set_op(op);
    for (uint i = 0; i < fn->argument_count(); ++i)
      if (!ser_out(fn->arguments()[i], out->add_children())) return false;
    return true;
  };

  std::vector<Item *> out_items;
  for (Item *item : VisibleFields(qb->fields)) out_items.push_back(item);
  for (Item *item : out_items) {
    Item *real = item->real_item();
    auto *oe = req.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *raw = down_cast<Item_field *>(real)->field;
      const int ti = table_index_of_field(raw);
      const Field *of = ti >= 0 ? resolve_in(raw, ti) : nullptr;
      if (of == nullptr) LDB_COL_REJECT("output column unresolvable");
      int pos = -1;
      for (size_t g = 0; g < group_fields.size(); ++g) {
        if (group_fields[g].field != nullptr &&
            group_fields[g].field == of && group_fields[g].table == ti) {
          pos = static_cast<int>(g);
          break;
        }
      }
      if (pos < 0) LDB_COL_REJECT("output not a group column");
      oe->set_source(LineairDB::Protocol::QbOutputExpr::GROUP);
      oe->set_ordinal(pos);
      continue;
    }
    if (real->type() == Item::SUM_FUNC_ITEM) {
      const int ord = register_aggregate(down_cast<Item_sum *>(real));
      if (ord < 0) LDB_COL_REJECT(agg_why != nullptr ? agg_why : "aggregate");
      oe->set_source(LineairDB::Protocol::QbOutputExpr::AGG);
      oe->set_ordinal(ord);
      continue;
    }
    // A group expression (e.g. the EXTRACT item) referenced in the output.
    {
      int pos = -1;
      for (size_t g = 0; g < group_items.size(); ++g) {
        if (group_items[g] == real ||
            group_items[g]->eq(real, /*binary_cmp=*/true)) {
          pos = static_cast<int>(g);
          break;
        }
      }
      if (pos >= 0) {
        oe->set_source(LineairDB::Protocol::QbOutputExpr::GROUP);
        oe->set_ordinal(pos);
        continue;
      }
    }
    // Arithmetic over aggregates (SUM ratios) or a constant output.
    if (!ser_out(real, oe->mutable_expr()))
      LDB_COL_REJECT(agg_why != nullptr ? agg_why : "output expr");
    oe->set_source(LineairDB::Protocol::QbOutputExpr::EXPR);
    oe->set_result_scale(real->decimals);
  }
  // HAVING over the stage-1 value layout (agg refs become ordinals).
  if (qb->having_cond() != nullptr) {
    std::function<bool(Item *, LineairDB::Protocol::FilterExpr *)>
        ser_having = [&](Item *it,
                         LineairDB::Protocol::FilterExpr *out) -> bool {
      it = it->real_item();
      if (it->type() == Item::COND_ITEM) {
        auto *cond = down_cast<Item_cond *>(it);
        out->set_op(cond->functype() == Item_func::COND_AND_FUNC
                        ? LineairDB::Protocol::FilterExpr::OP_AND
                        : LineairDB::Protocol::FilterExpr::OP_OR);
        List_iterator<Item> li(*cond->argument_list());
        for (Item *sub = li++; sub != nullptr; sub = li++)
          if (!ser_having(sub, out->add_children())) return false;
        return true;
      }
      if (it->type() != Item::FUNC_ITEM) return false;
      auto *fn = down_cast<Item_func *>(it);
      LineairDB::Protocol::FilterExpr::Op op;
      switch (fn->functype()) {
        case Item_func::EQ_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_EQ;
          break;
        case Item_func::NE_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_NE;
          break;
        case Item_func::LT_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_LT;
          break;
        case Item_func::LE_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_LE;
          break;
        case Item_func::GT_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_GT;
          break;
        case Item_func::GE_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_GE;
          break;
        default:
          return false;
      }
      if (fn->argument_count() != 2) return false;
      out->set_op(op);
      for (uint i = 0; i < 2; ++i) {
        auto *child = out->add_children();
        if (!ser_out(fn->arguments()[i], child)) return false;
        // Aggregate/decimal values compare as doubles.
        child->set_compare_type(2);
      }
      return true;
    };
    auto *hv = agg->mutable_having();
    if (!ser_having(qb->having_cond(), hv->mutable_expr()))
      LDB_COL_REJECT("HAVING not pushable");
    hv->set_num_columns(n_grp_final + agg->aggs_size());
  }
  if (agg->aggs_size() == 0 && !distinct_as_group &&
      agg->group_columns_size() == 0)
    LDB_COL_REJECT("no aggregates");

  // ORDER BY over output ordinals only.
  for (ORDER *o = qb->order_list.first; o != nullptr; o = o->next) {
    const int ord = order_output_ordinal(*o->item, out_items);
    if (ord < 0) LDB_COL_REJECT("ORDER BY not an output column");
    auto *k = req.add_order_by();
    k->set_output_ordinal(ord);
    k->set_descending(o->direction == ORDER_DESC);
    Item *oi = out_items[ord]->real_item();
    k->set_cmp_kind(oi->result_type() == STRING_RESULT ? 1 : 0);
  }

  // LIMIT/OFFSET (values were resolved during optimize).
  if (qb->has_limit()) {
    if (unit->select_limit_cnt != HA_POS_ERROR)
      req.set_limit(unit->select_limit_cnt);
    if (unit->offset_limit_cnt > 0) {
      req.set_offset(unit->offset_limit_cnt);
      if (req.limit() > 0)  // select_limit_cnt includes the offset
        req.set_limit(req.limit() - unit->offset_limit_cnt);
    }
  }

  (void)thd;
  *why = nullptr;
  return true;
#undef LDB_COL_REJECT
}

// Top-level recognizer: dispatches special derived shapes, then builds the
// generic block IR (recursively for derived tables).
bool recognize_query_block(THD *thd, JOIN *join,
                           Columnar_execution_context *ctx,
                           const char **why) {
  Query_block *qb = join->query_block;
  if (qb == nullptr || qb->outer_query_block() != nullptr) {
    *why = "not top-level";
    return false;
  }
  Query_expression *unit = qb->master_query_expression();
  if (unit == nullptr || !unit->is_simple()) {
    *why = "not simple unit";
    return false;
  }
  if (qb->leaf_tables != nullptr && qb->leaf_tables->next_leaf == nullptr &&
      qb->leaf_tables->is_view_or_derived()) {
    if (recognize_double_aggregate(thd, join, qb, ctx, why)) return true;
    if (recognize_flattened_agg(thd, join, qb, ctx, why)) return true;
  }
  if (getenv("LDBC_QEP") != nullptr && join->root_access_path() != nullptr) {
    std::string plan =
        PrintQueryPlan(0, join->root_access_path(), join, true);
    fprintf(stderr, "[LDBC-QEP]\n%s\n", plan.c_str());
  }
  QepRows qep_rows;
  collect_qep_rows(join->root_access_path(), join, &qep_rows);
  // Plan mapping first; shapes the mapper cannot express yet (WEEDOUT,
  // subquery materialization) retry through the syntactic builder. Both
  // failing is still a loud reject.
  if (!build_block(thd, qb, ctx->qb_request, why, &qep_rows,
                   join->root_access_path()) &&
      !build_block(thd, qb, ctx->qb_request, why, &qep_rows, nullptr))
    return false;
  ctx->plan_ready = true;
  return true;
}

bool ColumnarExecute(JOIN *join, Query_result *result) {
  THD *thd = current_thd;
  auto *ctx = down_cast<Columnar_execution_context *>(
      thd->lex->secondary_engine_execution_context());
  if (ctx == nullptr || !ctx->plan_ready) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "LINEAIRDB_COLUMNAR: no offload plan");
    return true;
  }

  auto proxy = lineairdb_acquire_shared_proxy(thd);
  if (!proxy) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "LINEAIRDB_COLUMNAR: no server connection");
    return true;
  }

  LineairDB::Protocol::TxExecuteQueryBlock::Response rpc;
  if (!proxy->tx_execute_query_block(ctx->qb_request, &rpc) || !rpc.ok()) {
    char msg[160];
    snprintf(msg, sizeof(msg), "LINEAIRDB_COLUMNAR: %s",
             rpc.error().empty() ? "query block RPC failed"
                                 : rpc.error().c_str());
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), msg);
    return true;
  }

  // Build the send list: one type-tagged value carrier per output field.
  mem_root_deque<Item *> out_items(thd->mem_root);
  std::vector<Item_columnar_value *> values;
  for (Item *item : VisibleFields(join->query_block->fields)) {
    auto *v = new (thd->mem_root) Item_columnar_value(item);
    if (v == nullptr) return true;
    values.push_back(v);
    out_items.push_back(v);
  }

  std::vector<DecodedField> fields;
  const size_t expected = 1 + values.size();  // null placeholder + outputs
  for (const std::string &row : rpc.rows()) {
    if (!decode_row_fields(row, &fields) || fields.size() != expected) {
      my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
               "LINEAIRDB_COLUMNAR: malformed result row");
      return true;
    }
    for (size_t i = 0; i < values.size(); ++i) {
      const DecodedField &f = fields[1 + i];
      if (f.empty)
        values[i]->set_null_value();
      else
        values[i]->set_value(f.ptr, f.len);
    }
    if (result->send_data(thd, out_items)) return true;
    ++join->send_records;
  }
  return false;
}

}  // namespace

// ---------------------------------------------------------------------------
// Secondary-engine hooks.
// ---------------------------------------------------------------------------

// Cost model for the PAX executor: everything runs as sequential strip
// scans + in-memory hash joins, so nested-loop and index paths (priced
// for InnoDB B-trees) are heavily penalized and hash joins re-priced
// linear-in-rows. Called only during secondary preparation — the primary
// (OLTP) planner is untouched.
static bool ModifyAccessPathCost(THD *thd [[maybe_unused]],
                                 const JoinHypergraph &hypergraph
                                 [[maybe_unused]],
                                 AccessPath *path) {
  switch (path->type) {
    case AccessPath::NESTED_LOOP_JOIN:
    case AccessPath::BKA_JOIN:
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      // Row-at-a-time probes are the worst case for the RPC-backed store;
      // reject so the optimizer explores hash alternatives.
      return true;
    case AccessPath::EQ_REF:
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::INDEX_SCAN:
    case AccessPath::INDEX_RANGE_SCAN:
      // No index access on the secondary engine.
      return true;
    case AccessPath::HASH_JOIN: {
      // Linear build+probe with a small per-row constant.
      const double build =
          std::max(1.0, path->hash_join().inner->num_output_rows());
      const double probe =
          std::max(1.0, path->hash_join().outer->num_output_rows());
      const double out_rows = std::max(1.0, path->num_output_rows());
      const double cost = path->hash_join().outer->cost +
                          path->hash_join().inner->cost +
                          0.05 * (build + probe) + 0.01 * out_rows;
      path->cost = cost;
      path->cost_before_filter = cost;
      path->init_cost = path->hash_join().inner->cost + 0.05 * build;
      return false;
    }
    case AccessPath::TABLE_SCAN: {
      // Morsel-parallel PAX strip scan: cheap and linear.
      const double rows = std::max(1.0, path->num_output_rows());
      const double cost = 0.01 * rows;
      path->cost = cost;
      path->cost_before_filter = cost;
      path->init_cost = 0.0;
      return false;
    }
    default:
      return false;
  }
}

bool PrepareSecondaryEngine(THD *thd, LEX *lex) {
  // Stop const-table/subquery evaluation during optimization from touching
  // storage before we decide how to execute (same as the mock engine).
  lex->add_statement_options(OPTION_NO_CONST_TABLES |
                             OPTION_NO_SUBQUERY_DURING_OPTIMIZATION);
  auto *ctx = new (thd->mem_root) Columnar_execution_context;
  if (ctx == nullptr) return true;
  lex->set_secondary_engine_execution_context(ctx);
  return false;
}

bool OptimizeSecondaryEngine(THD *thd, LEX *lex) {
  auto *ctx = down_cast<Columnar_execution_context *>(
      lex->secondary_engine_execution_context());
  assert(ctx != nullptr);

  Query_block *qb = lex->unit->first_query_block();
  JOIN *join = qb != nullptr ? qb->join : nullptr;
  const char *why = "no JOIN";
  if (join == nullptr || !recognize_query_block(thd, join, ctx, &why)) {
    // Unsupported shape: reject the offload; the server re-prepares the
    // statement on the primary engine (use_secondary_engine=ON semantics).
    char msg[128];
    snprintf(msg, sizeof(msg), "LINEAIRDB_COLUMNAR unsupported shape: %s",
             why ? why : "?");
    // Loud reject (PoC policy): every unsupported shape is surfaced in the
    // server log, never silently absorbed by a fallback.
    fprintf(stderr, "[LINEAIRDB_COLUMNAR] reject: %s\n", why ? why : "?");
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), msg);
    return true;
  }
  join->override_executor_func = ColumnarExecute;
  return false;
}

bool CompareJoinCost(THD *thd, const JOIN &join, double optimizer_cost,
                     bool *use_best_so_far, bool *cheaper,
                     double *secondary_engine_cost) {
  *use_best_so_far = false;
  *secondary_engine_cost = optimizer_cost;
  *cheaper = down_cast<Columnar_execution_context *>(
                 thd->lex->secondary_engine_execution_context())
                 ->BestPlanSoFar(join, *secondary_engine_cost);
  return false;
}

// ---------------------------------------------------------------------------
// Handler implementation.
// ---------------------------------------------------------------------------
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
  thr_lock_data_init(lock, &m_lock, nullptr);
  return 0;
}

int ha_lineairdb_columnar::info(unsigned int flags) {
  // Statistics come from the primary engine (it tracks server row counts).
  handler *primary = ha_get_primary_handler();
  if (primary == nullptr) return 0;
  const int ret = primary->info(flags);
  if (ret == 0) stats.records = primary->stats.records;
  return ret;
}

ha_rows ha_lineairdb_columnar::records_in_range(unsigned int index,
                                                key_range *min_key,
                                                key_range *max_key) {
  handler *primary = ha_get_primary_handler();
  if (primary == nullptr) return handler::records_in_range(index, min_key, max_key);
  return primary->records_in_range(index, min_key, max_key);
}

unsigned long ha_lineairdb_columnar::index_flags(unsigned int idx,
                                                 unsigned int part,
                                                 bool all_parts) const {
  const handler *primary = ha_get_primary_handler();
  const unsigned long primary_flags =
      primary == nullptr ? 0 : primary->index_flags(idx, part, all_parts);
  // Indexes are cost-estimation-only on a secondary engine.
  return (HA_READ_RANGE | HA_KEY_SCAN_NOT_ROR) & primary_flags;
}

THR_LOCK_DATA **ha_lineairdb_columnar::store_lock(THD *, THR_LOCK_DATA **to,
                                                  thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK)
    m_lock.type = lock_type;
  *to++ = &m_lock;
  return to;
}

int ha_lineairdb_columnar::load_table(const TABLE &table_arg) {
  // Single-copy PAX: the rows already live in the shared LineairDB server;
  // loading is registration only.
  assert(table_arg.file != nullptr);
  loaded_tables->add(table_arg.s->db.str, table_arg.s->table_name.str);
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

// ---------------------------------------------------------------------------
// Plugin boilerplate (declared in ha_lineairdb.cc's plugin list).
// ---------------------------------------------------------------------------
namespace {

handler *columnar_create_handler(handlerton *hton, TABLE_SHARE *table_share,
                                 bool, MEM_ROOT *mem_root) {
  return new (mem_root)
      lineairdb_columnar::ha_lineairdb_columnar(hton, table_share);
}

}  // namespace

struct st_mysql_storage_engine lineairdb_columnar_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

int lineairdb_columnar_init(void *p) {
  lineairdb_columnar::loaded_tables = new lineairdb_columnar::LoadedTables();
  handlerton *hton = static_cast<handlerton *>(p);
  hton->create = columnar_create_handler;
  hton->state = SHOW_OPTION_YES;
  hton->flags = HTON_IS_SECONDARY_ENGINE;
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->prepare_secondary_engine =
      lineairdb_columnar::PrepareSecondaryEngine;
  hton->optimize_secondary_engine =
      lineairdb_columnar::OptimizeSecondaryEngine;
  hton->compare_secondary_engine_cost = lineairdb_columnar::CompareJoinCost;
  hton->secondary_engine_modify_access_path_cost =
      lineairdb_columnar::ModifyAccessPathCost;
  hton->secondary_engine_flags =
      MakeSecondaryEngineFlags(SecondaryEngineFlag::SUPPORTS_HASH_JOIN,
                               SecondaryEngineFlag::SUPPORTS_NESTED_LOOP_JOIN);
  return 0;
}

int lineairdb_columnar_deinit(void *) {
  delete lineairdb_columnar::loaded_tables;
  lineairdb_columnar::loaded_tables = nullptr;
  return 0;
}
