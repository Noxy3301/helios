#include "aggregate_pushdown.hh"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lineairdb.pb.h"
#include "lineairdb_keyenc.hh"
#include "lineairdb_pushdown.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/item_subselect.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/walk_access_paths.h"
#include "sql/my_decimal.h"
#include "sql/query_result.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "sql/visible_fields.h"

// LineairDB aggregate pushdown internals. This translation unit validates
// eligible single-table GROUP BY plans, installs the executor override, and
// bridges server-produced group rows back into handler state.

extern handlerton *lineairdb_hton;

namespace {
enum class LineairDBAggKind { kPass = 0, kCount, kSum, kAvg };

// One visible SELECT output column in the aggregate executor plan.
struct LineairDBAggOutput {
  Item *orig = nullptr;                 // SELECT output item
  LineairDBAggKind kind = LineairDBAggKind::kPass;
  Item *arg = nullptr;                  // aggregate argument expr (nullptr=COUNT*)
  Item_result rtype = STRING_RESULT;    // result/accumulation type
};

// Per-group accumulator used by both server-result finalization and fallback.
struct LineairDBAggAccumulator {
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

// HAVING predicate form supported by grouped-semijoin pushdown.
struct LineairDBHavingPredicate {
  Item_sum *aggregate = nullptr;
  LineairDBAggKind kind = LineairDBAggKind::kCount;
  Item *arg = nullptr;
  Item_result result_type = INT_RESULT;
  Item_func::Functype op = Item_func::EQ_FUNC;
  Item *constant = nullptr;
};

// Keep aggregate decimal inputs inside the server's __int128 accumulator.
constexpr uint kLineairDBAggregateDecimalPrecisionMax = 18;

// Wire marker also understood by server/rpc/aggregate_executor.cc.
constexpr uint32_t kLineairDBAggregateHavingFilterColumns =
    std::numeric_limits<uint32_t>::max();
}  // namespace

/**
 * @brief Build the visible SELECT-output plan for aggregate execution.
 *
 * Uses Query_block::fields because JOIN::fields can be rebound to an
 * aggregation temp table after optimization.
 */
static bool plan_aggregate_outputs(
    JOIN *join, std::vector<LineairDBAggOutput> *out) {
  if (join->query_block == nullptr) return false;
  for (Item *it : VisibleFields(join->query_block->fields)) {
    LineairDBAggOutput o;
    o.orig = it;
    if (it->type() == Item::SUM_FUNC_ITEM) {
      // Only bare aggregate items are supported here.
      Item_sum *s = down_cast<Item_sum *>(it);
      if (s->has_wf() || s->has_subquery()) return false;
      switch (s->sum_func()) {
        case Item_sum::COUNT_FUNC:
          o.kind = LineairDBAggKind::kCount;
          // COUNT(*) and COUNT(non-null const) count every input row.
          if (s->argument_count() > 0) {
            Item *a0 = s->arguments()[0];
            if (!a0->const_item() || a0->is_nullable() || a0->is_null())
              return false;
          }
          break;
        case Item_sum::SUM_FUNC: o.kind = LineairDBAggKind::kSum; break;
        case Item_sum::AVG_FUNC: o.kind = LineairDBAggKind::kAvg; break;
        default: return false;
      }
      o.rtype = s->result_type();
      if (o.kind != LineairDBAggKind::kCount && o.rtype != DECIMAL_RESULT &&
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
      o.kind = LineairDBAggKind::kPass;
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
 * row stream is owned by execute_aggregate_override.
 */
static bool is_aggregate_pushdown_shape(THD *thd, JOIN *join) {
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
  std::vector<LineairDBAggOutput> probe;
  if (!plan_aggregate_outputs(join, &probe)) return false;

  // Without GROUP BY, passthrough columns have no group row to bind to.
  if (gitems.empty())
    for (const LineairDBAggOutput &o : probe)
      if (o.kind == LineairDBAggKind::kPass) return false;

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
 * @brief Return true if a Field::val_str() value is safe decimal input.
 *
 * The server parses aggregate arguments from raw column bytes, so accepted
 * fields must serialize to decimal text with the same meaning SQL gives them.
 * Temporal text and wide DECIMAL values are rejected here rather than relying
 * on server-side overflow checks.
 */
static bool is_safe_decimal_aggregate_field(const Field *field) {
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
                 kLineairDBAggregateDecimalPrecisionMax;
    default:
      return false;
  }
}

/**
 * @brief Return a bare field item if it is valid decimal aggregate input.
 */
static Item_field *safe_bare_decimal_aggregate_arg(Item *arg) {
  if (arg == nullptr) return nullptr;
  Item *real = arg->real_item();
  if (real == nullptr || real->type() != Item::FIELD_ITEM) return nullptr;
  Item_field *field_arg = down_cast<Item_field *>(real);
  return is_safe_decimal_aggregate_field(field_arg->field) ? field_arg
                                                          : nullptr;
}

/**
 * @brief Allocate writable Item_cache cells for the already-described output.
 */
static bool make_aggregate_output_caches(JOIN *join,
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
 * @brief Serialize a supported aggregate argument expression for LineairDB.
 *
 * Supports column refs, integer constants, and +, -, *, and unary minus.
 */
namespace lineairdb {

bool serialize_aggregate_expression(
    const Item *it, LineairDB::Protocol::FilterExpr *out) {
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
        return serialize_aggregate_expression(fn->arguments()[0],
                                              out->add_children());
      }
      if (fn->argument_count() != 2) return false;
      return serialize_aggregate_expression(fn->arguments()[0],
                                            out->add_children()) &&
             serialize_aggregate_expression(fn->arguments()[1],
                                            out->add_children());
    }
    default:
      return false;
  }
}

}  // namespace lineairdb

/**
 * @brief Classify COUNT/SUM/AVG used by a supported HAVING predicate.
 */
static bool classify_having_aggregate(Item_sum *sum,
                                      LineairDBHavingPredicate *out) {
  if (sum == nullptr || out == nullptr) return false;
  if (sum->has_wf() || sum->has_subquery()) return false;
  switch (sum->sum_func()) {
    case Item_sum::COUNT_FUNC:
      out->kind = LineairDBAggKind::kCount;
      if (sum->argument_count() > 0) {
        Item *arg0 = sum->arguments()[0];
        if (!arg0->const_item() || arg0->is_nullable() || arg0->is_null()) {
          return false;
        }
      }
      break;
    case Item_sum::SUM_FUNC:
      out->kind = LineairDBAggKind::kSum;
      break;
    case Item_sum::AVG_FUNC:
      out->kind = LineairDBAggKind::kAvg;
      break;
    default:
      return false;
  }

  out->result_type = sum->result_type();
  if (out->kind != LineairDBAggKind::kCount &&
      out->result_type != DECIMAL_RESULT && out->result_type != INT_RESULT) {
    return false;
  }
  out->arg = sum->argument_count() > 0 ? sum->arguments()[0] : nullptr;
  if (out->arg != nullptr &&
      (out->arg->has_subquery() || out->arg->has_aggregation() ||
       out->arg->is_non_deterministic())) {
    return false;
  }
  if (out->kind != LineairDBAggKind::kCount) {
    Item_field *field_arg = safe_bare_decimal_aggregate_arg(out->arg);
    if (field_arg == nullptr) return false;
    out->arg = field_arg;
  }
  out->aggregate = sum;
  return true;
}

/**
 * @brief Parse HAVING of the form aggregate(column) compare constant.
 *
 * MySQL can add in2exists helper predicates to HAVING during subquery
 * optimization. Those generated predicates are ignored; any user-written
 * HAVING must be a single supported aggregate comparison.
 */
static bool parse_aggregate_having(Query_block *query_block,
                                   LineairDBHavingPredicate *out) {
  if (query_block == nullptr || out == nullptr) return false;
  *out = LineairDBHavingPredicate();
  Item *having = query_block->having_cond();
  if (having == nullptr) return true;

  if (having->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(having)->functype() ==
          Item_func::COND_AND_FUNC) {
    Item *user_having = nullptr;
    for (Item &condition : *down_cast<Item_cond *>(having)->argument_list()) {
      if (condition.created_by_in2exists()) continue;
      if (user_having != nullptr) return false;
      user_having = &condition;
    }
    if (user_having == nullptr) return true;
    having = user_having;
  } else if (having->created_by_in2exists()) {
    return true;
  }

  if (having->type() != Item::FUNC_ITEM) return false;
  Item_func *func = down_cast<Item_func *>(having);
  Item_func::Functype op = func->functype();
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
  if (func->argument_count() != 2) return false;

  Item *lhs = func->arguments()[0]->real_item();
  Item *rhs = func->arguments()[1]->real_item();
  Item *aggregate_side = nullptr;
  Item *constant_side = nullptr;
  bool aggregate_on_left = false;
  if (lhs->type() == Item::SUM_FUNC_ITEM && rhs->const_item()) {
    aggregate_side = lhs;
    constant_side = rhs;
    aggregate_on_left = true;
  } else if (rhs->type() == Item::SUM_FUNC_ITEM && lhs->const_item()) {
    aggregate_side = rhs;
    constant_side = lhs;
  } else {
    return false;
  }
  if (constant_side->has_subquery() ||
      constant_side->is_non_deterministic()) {
    return false;
  }
  const Item_result constant_type = constant_side->result_type();
  if (constant_type != INT_RESULT && constant_type != DECIMAL_RESULT) {
    return false;
  }
  if (constant_type == DECIMAL_RESULT &&
      constant_side->decimal_precision() >
          kLineairDBAggregateDecimalPrecisionMax) {
    return false;
  }
  if (!classify_having_aggregate(down_cast<Item_sum *>(aggregate_side), out)) {
    return false;
  }

  if (aggregate_on_left) {
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
  out->constant = constant_side;
  return true;
}

/**
 * @brief Build an AggregateSpec that also emits a HAVING aggregate value.
 */
static bool build_grouped_aggregate_spec(
    TABLE *table, bool read_only_no_validate,
    const std::vector<LineairDBAggOutput> &outputs,
    const std::vector<Item *> &group_items,
    const LineairDBHavingPredicate &having, int *having_aggregate_index,
    std::string *serialized_spec) {
  if (table == nullptr || having_aggregate_index == nullptr ||
      serialized_spec == nullptr) {
    return false;
  }

  *having_aggregate_index = -1;
  serialized_spec->clear();
  if (!read_only_no_validate) return false;
  for (Item *group_item : group_items) {
    if (group_item == nullptr || group_item->is_nullable()) return false;
  }

  const size_t output_count = outputs.size();
  for (size_t output = 0; output < output_count; ++output) {
    if (outputs[output].kind == LineairDBAggKind::kPass) {
      Field *field = down_cast<Item_field *>(outputs[output].orig)->field;
      bool found_group_column = false;
      for (size_t group = 0; group < group_items.size(); ++group) {
        if (down_cast<Item_field *>(group_items[group])->field == field) {
          found_group_column = true;
          break;
        }
      }
      if (!found_group_column) return false;
    }
  }

  LineairDB::Protocol::AggregateSpec spec;
  spec.set_num_columns(table->s->fields);
  for (Item *group_item : group_items) {
    spec.add_group_columns(
        down_cast<Item_field *>(group_item)->field->field_index());
  }

  for (size_t output = 0; output < output_count; ++output) {
    if (outputs[output].kind == LineairDBAggKind::kPass) continue;
    auto *aggregate = spec.add_aggs();
    if (outputs[output].kind == LineairDBAggKind::kCount) {
      aggregate->set_kind(LineairDB::Protocol::AggFunc::AGG_COUNT);
    } else {
      aggregate->set_kind(outputs[output].kind == LineairDBAggKind::kSum
                              ? LineairDB::Protocol::AggFunc::AGG_SUM
                              : LineairDB::Protocol::AggFunc::AGG_AVG);
      if (outputs[output].rtype != DECIMAL_RESULT ||
          outputs[output].arg == nullptr ||
          !lineairdb::serialize_aggregate_expression(
              outputs[output].arg, aggregate->mutable_arg())) {
        return false;
      }
    }
    aggregate->set_result_scale(0);
  }

  if (having.aggregate != nullptr) {
    auto *aggregate = spec.add_aggs();
    if (having.kind == LineairDBAggKind::kCount) {
      aggregate->set_kind(LineairDB::Protocol::AggFunc::AGG_COUNT);
    } else {
      aggregate->set_kind(having.kind == LineairDBAggKind::kSum
                              ? LineairDB::Protocol::AggFunc::AGG_SUM
                              : LineairDB::Protocol::AggFunc::AGG_AVG);
      if (having.result_type != DECIMAL_RESULT || having.arg == nullptr ||
          !lineairdb::serialize_aggregate_expression(
              having.arg, aggregate->mutable_arg())) {
        return false;
      }
    }
    aggregate->set_result_scale(0);
    *having_aggregate_index = spec.aggs_size() - 1;
  }

  spec.SerializeToString(serialized_spec);
  return true;
}

/**
 * @brief Serialize HAVING as a group-row predicate for the server aggregate.
 */
static bool build_aggregate_having_filter(
    const LineairDBHavingPredicate &having, uint32_t group_row_value_column,
    std::string *out) {
  if (out == nullptr || having.aggregate == nullptr ||
      having.constant == nullptr) {
    return false;
  }
  out->clear();

  // The grouped summary bridge currently consumes SUM/COUNT result columns.
  if (having.kind == LineairDBAggKind::kAvg) return false;

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

  my_decimal decimal_constant;
  my_decimal *constant = having.constant->val_decimal(&decimal_constant);
  if (constant == nullptr || having.constant->null_value) return false;
  String constant_text;
  if (my_decimal2string(E_DEC_FATAL_ERROR, constant, &constant_text) !=
      E_DEC_OK) {
    return false;
  }

  LineairDB::Protocol::PushedPredicate predicate;
  predicate.set_num_columns(kLineairDBAggregateHavingFilterColumns);
  auto *expr = predicate.mutable_expr();
  expr->set_op(op);
  auto *lhs = expr->add_children();
  lhs->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
  lhs->set_column_index(group_row_value_column);
  auto *rhs = expr->add_children();
  rhs->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
  rhs->set_string_val(
      std::string(constant_text.ptr(), constant_text.length()));
  predicate.SerializeToString(out);
  return true;
}

/**
 * @brief Return the handler table key used by read-plan steps.
 */
static std::string physical_table_key(const TABLE *table) {
  if (table == nullptr || table->s == nullptr) return {};
  const TABLE_SHARE *share = table->s;
  if (share->normalized_path.str == nullptr ||
      share->normalized_path.length == 0) {
    return {};
  }
  return std::string(share->normalized_path.str, share->normalized_path.length);
}

/**
 * @brief Unwrap a WHERE item when it represents an IN subquery predicate.
 */
static Item_in_subselect *as_where_in_subselect(Item *item) {
  if (item == nullptr) return nullptr;
  Item *current = item->real_item();
  if (current == nullptr) return nullptr;
  if (current->type() == Item::CACHE_ITEM) {
    current = down_cast<Item_cache *>(current)->get_example();
    if (current == nullptr) return nullptr;
    current = current->real_item();
    if (current == nullptr) return nullptr;
  }
  if (current->type() == Item::SUBSELECT_ITEM) {
    Item_subselect *subselect = down_cast<Item_subselect *>(current);
    if (subselect->substype() == Item_subselect::IN_SUBS) {
      return down_cast<Item_in_subselect *>(subselect);
    }
  }

  if (current->type() != Item::FUNC_ITEM) return nullptr;
  Item_func *func = down_cast<Item_func *>(current);
  if (func->argument_count() != 1 ||
      std::strcmp(func->func_name(), "<in_optimizer>") != 0) {
    return nullptr;
  }
  Item *arg = func->get_arg(0);
  if (arg == nullptr) return nullptr;
  arg = arg->real_item();
  if (arg == nullptr || arg->type() != Item::SUBSELECT_ITEM) return nullptr;
  Item_subselect *subselect = down_cast<Item_subselect *>(arg);
  if (subselect->substype() != Item_subselect::IN_SUBS) return nullptr;
  return down_cast<Item_in_subselect *>(subselect);
}

/**
 * @brief Return true if the IN predicate is a top-level WHERE conjunct.
 */
static bool is_outer_where_top_level_in(Item *where,
                                        const Item_in_subselect *subselect) {
  if (where == nullptr || subselect == nullptr) return false;
  if (as_where_in_subselect(where) == subselect) return true;
  if (where->type() != Item::COND_ITEM ||
      down_cast<Item_cond *>(where)->functype() !=
          Item_func::COND_AND_FUNC) {
    return false;
  }
  for (Item &arg : *down_cast<Item_cond *>(where)->argument_list()) {
    if (as_where_in_subselect(&arg) == subselect) return true;
  }
  return false;
}

/**
 * @brief Return true if table is one of the query block's leaf tables.
 */
static bool table_belongs_to_query_block(const Query_block *query_block,
                                         const TABLE *table) {
  if (query_block == nullptr || table == nullptr) return false;
  for (const Table_ref *table_ref = query_block->leaf_tables;
       table_ref != nullptr; table_ref = table_ref->next_leaf) {
    if (table_ref->table == table) return true;
  }
  return false;
}

/**
 * @brief Reject ambiguous physical-table matches in statement read-plan steps.
 */
static bool physical_table_is_unique_in_statement(THD *thd,
                                                  const TABLE *table) {
  if (thd == nullptr || thd->lex == nullptr || table == nullptr) return false;
  const std::string key = physical_table_key(table);
  if (key.empty()) return false;

  int count = 0;
  for (const Table_ref *table_ref = thd->lex->query_tables;
       table_ref != nullptr; table_ref = table_ref->next_global) {
    if (table_ref->table != nullptr &&
        physical_table_key(table_ref->table) == key) {
      if (++count > 1) return false;
    }
  }
  return count == 1;
}

/**
 * @brief Return true if a grouped-semijoin key can compare raw bytes safely.
 */
static bool grouped_semijoin_integer_key_type(const Field *field) {
  if (field == nullptr || field->result_type() != INT_RESULT) return false;
  switch (field->type()) {
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

/**
 * @brief Return true if SQL equality matches server byte membership checks.
 */
static bool grouped_semijoin_key_bytes_match_sql_equality(
    const Field *group_field, const Field *outer_field) {
  if (!grouped_semijoin_integer_key_type(group_field) ||
      !grouped_semijoin_integer_key_type(outer_field)) {
    return false;
  }
  if (group_field->is_unsigned() != outer_field->is_unsigned()) return false;

  // Field::val_str() zero-pads ZEROFILL integers to display width, so SQL-equal
  // values from different column widths can have different bytes.
  if (down_cast<const Field_num *>(group_field)->zerofill ||
      down_cast<const Field_num *>(outer_field)->zerofill) {
    return false;
  }
  return true;
}

/**
 * @brief Register eligible grouped IN-subquery pushdown state.
 */
static void try_register_grouped_semijoin(THD *thd, JOIN *join) {
  if (thd == nullptr || join == nullptr || thd->lex == nullptr) return;
  if (thd->lex->sql_command != SQLCOM_SELECT || thd->lex->is_explain()) return;

  Query_block *query_block = join->query_block;
  if (query_block == nullptr ||
      query_block->outer_query_block() == nullptr) {
    return;
  }
  Query_block *outer_query_block = query_block->outer_query_block();
  Query_expression *query_expression = query_block->master_query_expression();
  if (query_expression == nullptr || !query_expression->is_simple() ||
      query_expression->uncacheable != 0) {
    return;
  }
  if (query_expression->item == nullptr ||
      query_expression->item->substype() != Item_subselect::IN_SUBS) {
    return;
  }

  Item_in_subselect *in_subquery =
      down_cast<Item_in_subselect *>(query_expression->item);
  if (in_subquery->left_expr == nullptr) return;
  if (!is_outer_where_top_level_in(outer_query_block->where_cond(),
                                   in_subquery)) {
    return;
  }
  if (in_subquery->value_transform != Item::BOOL_IDENTITY &&
      in_subquery->value_transform != Item::BOOL_IS_TRUE) {
    return;
  }

  if (query_block->leaf_table_count != 1 || !query_block->is_grouped()) return;
  if (query_block->is_distinct() || query_block->has_limit() ||
      query_block->olap != UNSPECIFIED_OLAP_TYPE ||
      query_block->has_windows() || query_block->group_list.elements != 1 ||
      query_block->having_cond() == nullptr) {
    return;
  }
  if (query_block->where_cond() != nullptr) return;

  TABLE *inner_table = query_block->leaf_tables != nullptr
                           ? query_block->leaf_tables->table
                           : nullptr;
  if (inner_table == nullptr || inner_table->file == nullptr ||
      inner_table->file->ht != lineairdb_hton) {
    return;
  }
  ha_lineairdb *inner_handler = down_cast<ha_lineairdb *>(inner_table->file);
  if (!inner_handler->tx_ro_novalidate()) return;

  Item *group_item = *query_block->group_list.first->item;
  if (group_item->type() != Item::FIELD_ITEM || group_item->is_nullable()) {
    return;
  }
  Field *group_field = down_cast<Item_field *>(group_item)->field;
  if (group_field == nullptr || group_field->is_nullable()) return;

  std::vector<LineairDBAggOutput> outputs;
  if (!plan_aggregate_outputs(join, &outputs)) return;
  if (outputs.size() != 1 ||
      outputs[0].kind != LineairDBAggKind::kPass) {
    return;
  }
  if (down_cast<Item_field *>(outputs[0].orig)->field != group_field) return;

  LineairDBHavingPredicate having;
  if (!parse_aggregate_having(query_block, &having) ||
      having.aggregate == nullptr) {
    return;
  }

  std::vector<Item *> group_items{group_item};
  int having_aggregate_index = -1;
  std::string aggregate_spec;
  if (!build_grouped_aggregate_spec(
          inner_table, inner_handler->tx_ro_novalidate(), outputs, group_items,
          having, &having_aggregate_index, &aggregate_spec)) {
    return;
  }
  if (having_aggregate_index < 0) return;

  std::string having_filter;
  const uint32_t having_value_column =
      static_cast<uint32_t>(group_items.size() + 2 * having_aggregate_index);
  if (!build_aggregate_having_filter(having, having_value_column,
                                     &having_filter)) {
    return;
  }

  Item *left = in_subquery->left_expr->real_item();
  if (left->type() != Item::FIELD_ITEM) return;
  Field *outer_field = down_cast<Item_field *>(left)->field;
  if (outer_field == nullptr || outer_field->table == nullptr ||
      outer_field->table->file == nullptr ||
      outer_field->table->file->ht != lineairdb_hton) {
    return;
  }

  TABLE *outer_table = outer_field->table;
  if (outer_table == inner_table) return;
  if (!table_belongs_to_query_block(outer_query_block, outer_table)) return;
  if (!physical_table_is_unique_in_statement(thd, outer_table)) return;

  int probe_column = -1;
  for (uint field_index = 0; field_index < outer_table->s->fields;
       ++field_index) {
    if (outer_table->field[field_index] == outer_field) {
      probe_column = static_cast<int>(field_index);
      break;
    }
  }
  if (probe_column < 0) return;

  const Table_ref *outer_table_ref = outer_table->pos_in_table_list;
  if (outer_table_ref == nullptr ||
      outer_table_ref->is_inner_table_of_outer_join()) {
    return;
  }
  for (const Table_ref *embedding = outer_table_ref->embedding;
       embedding != nullptr; embedding = embedding->embedding) {
    if (embedding->is_sj_or_aj_nest()) return;
  }
  if (outer_table_ref->query_block == nullptr ||
      outer_table_ref->query_block->outer_query_block() != nullptr) {
    return;
  }
  if (outer_field->is_nullable()) return;
  if (group_field->type() != outer_field->type() ||
      group_field->pack_length() != outer_field->pack_length()) {
    return;
  }
  if (group_field->result_type() == STRING_RESULT &&
      group_field->charset() != outer_field->charset()) {
    return;
  }
  if (!grouped_semijoin_key_bytes_match_sql_equality(group_field,
                                                     outer_field)) {
    return;
  }

  const std::string inner_key = physical_table_key(inner_table);
  const std::string outer_key = physical_table_key(outer_table);
  if (inner_key.empty() || outer_key.empty()) return;

  if (inner_handler->tx_has_grouped_semijoin()) return;

  LineairDBTransaction::GroupedSemijoin grouped_semijoin;
  grouped_semijoin.inner_table_key = inner_key;
  grouped_semijoin.agg_spec = aggregate_spec;
  grouped_semijoin.having_filter = having_filter;
  grouped_semijoin.outer_table_key = outer_key;
  grouped_semijoin.outer_probe_column = static_cast<uint32_t>(probe_column);
  inner_handler->tx_register_grouped_semijoin(std::move(grouped_semijoin));

  if (having.kind != LineairDBAggKind::kSum || having.arg == nullptr) return;
  Item *sum_arg = having.arg->real_item();
  if (sum_arg->type() != Item::FIELD_ITEM || sum_arg->is_nullable()) return;
  Field *sum_field = down_cast<Item_field *>(sum_arg)->field;
  if (sum_field == nullptr || sum_field->table != inner_table ||
      sum_field == group_field || sum_field->is_nullable()) {
    return;
  }

  int sum_column = -1;
  int group_column = -1;
  for (uint field_index = 0; field_index < inner_table->s->fields;
       ++field_index) {
    if (inner_table->field[field_index] == sum_field) {
      sum_column = static_cast<int>(field_index);
    }
    if (inner_table->field[field_index] == group_field) {
      group_column = static_cast<int>(field_index);
    }
  }
  if (sum_column < 0 || group_column < 0) return;

  for (uint field_index = 0; field_index < inner_table->s->fields;
       ++field_index) {
    if (!bitmap_is_set(inner_table->read_set, field_index)) continue;
    if (static_cast<int>(field_index) != sum_column &&
        static_cast<int>(field_index) != group_column) {
      return;
    }
  }

  LineairDBTransaction::GroupedSummaryRegistration registration;
  registration.spec = std::move(aggregate_spec);
  registration.filter = std::move(having_filter);
  registration.template_cols.assign(inner_table->s->fields, "0");
  registration.group_col = static_cast<uint32_t>(group_column);
  registration.col_a = static_cast<uint32_t>(sum_column);
  registration.col_b = static_cast<uint32_t>(sum_column);
  registration.single_sum = true;
  inner_handler->tx_register_grouped_summary(std::move(registration));
}

/**
 * @brief Execute a supported single-table aggregate SELECT.
 *
 * The caller owns result metadata and EOF. This function only sends data rows.
 * It first tries server-side aggregation, then falls back to local aggregation.
 */
static bool execute_aggregate_override(JOIN *join, Query_result *query_result) {
  THD *thd = join->thd;
  Query_block *qb = join->query_block;
  TABLE *t = qb->leaf_tables->table;
  join->send_records = 0;

  std::vector<LineairDBAggOutput> outs;
  if (!plan_aggregate_outputs(join, &outs)) return true;  // whitelist already checked
  const size_t n = outs.size();

  std::vector<Item *> gitems;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next)
    gitems.push_back(*g->item);
  const bool implicit = gitems.empty();

  // Try server-side aggregation first.
  {
    bool can_use_server_aggregation = true;
    for (const LineairDBAggOutput &o : outs) {
      if (o.kind != LineairDBAggKind::kPass && o.kind != LineairDBAggKind::kCount &&
          o.kind != LineairDBAggKind::kSum && o.kind != LineairDBAggKind::kAvg) {
        can_use_server_aggregation = false;
      }
    }
    // Group rows do not carry a per-base-row validation footprint.
    if (!down_cast<ha_lineairdb *>(t->file)->tx_ro_novalidate())
      can_use_server_aggregation = false;
    // Server group-key decoding does not represent NULL separately yet.
    for (Item *gi : gitems)
      if (gi->is_nullable()) can_use_server_aggregation = false;
    std::vector<int> output_aggregate_index(n, -1), output_group_index(n, -1);
    if (can_use_server_aggregation) {
      int agg_pos = 0;
      for (size_t c = 0; c < n && can_use_server_aggregation; ++c) {
        if (outs[c].kind == LineairDBAggKind::kPass) {
          Field *of = down_cast<Item_field *>(outs[c].orig)->field;
          for (size_t g = 0; g < gitems.size(); ++g)
            if (down_cast<Item_field *>(gitems[g])->field == of) {
              output_group_index[c] = static_cast<int>(g);
              break;
            }
          if (output_group_index[c] < 0)
            can_use_server_aggregation = false;  // passthrough not a group col
        } else {
          output_aggregate_index[c] = agg_pos++;
        }
      }
    }
    if (can_use_server_aggregation) {
      LineairDB::Protocol::AggregateSpec spec;
      spec.set_num_columns(t->s->fields);
      for (Item *gi : gitems)
        spec.add_group_columns(down_cast<Item_field *>(gi)->field->field_index());
      for (size_t c = 0; c < n && can_use_server_aggregation; ++c) {
        if (outs[c].kind == LineairDBAggKind::kPass) continue;
        auto *af = spec.add_aggs();
        if (outs[c].kind == LineairDBAggKind::kCount) {
          af->set_kind(LineairDB::Protocol::AggFunc::AGG_COUNT);
        } else {
          af->set_kind(outs[c].kind == LineairDBAggKind::kSum
                           ? LineairDB::Protocol::AggFunc::AGG_SUM
                           : LineairDB::Protocol::AggFunc::AGG_AVG);
          // Server-side SUM/AVG supports exact decimal expressions only.
          if (outs[c].rtype != DECIMAL_RESULT || outs[c].arg == nullptr ||
              !lineairdb::serialize_aggregate_expression(outs[c].arg,
                                                         af->mutable_arg())) {
            can_use_server_aggregation = false;
            break;
          }
        }
        af->set_result_scale(0);
      }
      if (!can_use_server_aggregation) {
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

      std::map<std::string, std::vector<LineairDBAggAccumulator>> groups;
      std::string_view raw;
      while (hl->agg_next_raw(&raw)) {
        std::vector<std::string_view> fv;
        std::vector<bool> nul;
        // Malformed group rows cannot be rechecked from base rows here.
        if (!parse_fields(raw, &fv, &nul) ||
            fv.size() != 1 + ng + 2 * static_cast<size_t>(n_aggs)) {
          t->file->ha_rnd_end();
          hl->tx_clear_pushed_aggregate();
          my_error(ER_INTERNAL_ERROR, MYF(0), "LineairDB aggregate group row malformed");
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
        std::vector<LineairDBAggAccumulator> *grp;
        if (it == groups.end()) {
          auto &v = groups[key];
          v.resize(n);
          for (size_t c = 0; c < n; ++c)
            if (outs[c].kind == LineairDBAggKind::kPass) {
              std::string_view gv = fv[1 + output_group_index[c]];
              if (nul[1 + output_group_index[c]]) v[c].p_null = true;
              else
                v[c].p_str.copy(
                    gv.data(), gv.size(),
                    gitems[output_group_index[c]]->collation.collation);
            }
          grp = &v;
        } else {
          grp = &it->second;
        }
        for (size_t c = 0; c < n; ++c) {
          const LineairDBAggOutput &o = outs[c];
          if (o.kind == LineairDBAggKind::kPass) continue;
          const size_t vi = 1 + ng + 2 * output_aggregate_index[c];  // value column in fv
          const size_t ci = vi + 1;                    // count column in fv
          LineairDBAggAccumulator &a = (*grp)[c];
          if (o.kind == LineairDBAggKind::kCount) {
            if (vi < fv.size() && !nul[vi]) {
              std::string s(fv[vi]);
              a.cnt += std::strtoll(s.c_str(), nullptr, 10);
            }
          } else {
            // SUM/AVG group rows carry an exact decimal sum and non-null count.
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
      if (make_aggregate_output_caches(join, &row)) return true;
      const int div_inc = static_cast<int>(thd->variables.div_precincrement);
      for (auto &kv : groups) {
        std::vector<LineairDBAggAccumulator> &g = kv.second;
        for (size_t c = 0; c < n; ++c) {
          Item_cache *cache = down_cast<Item_cache *>(row[c]);
          const LineairDBAggOutput &o = outs[c];
          LineairDBAggAccumulator &a = g[c];
          if (o.kind == LineairDBAggKind::kPass) {
            if (a.p_null) { cache->store_null(); continue; }
            cache->null_value = false;
            down_cast<Item_cache_str *>(cache)->store_value(cache, a.p_str);
          } else if (o.kind == LineairDBAggKind::kCount) {
            cache->null_value = false;
            down_cast<Item_cache_int *>(cache)->store_value(cache, a.cnt);
          } else if (o.kind == LineairDBAggKind::kSum) {
            if (a.cnt == 0) { cache->store_null(); continue; }
            cache->null_value = false;
            down_cast<Item_cache_decimal *>(cache)->store_value(cache, &a.dec);
          } else {  // AVG
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

  std::map<std::string, std::vector<LineairDBAggAccumulator>> groups;
  std::vector<LineairDBAggAccumulator> *implicit_grp = nullptr;
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

    std::vector<LineairDBAggAccumulator> *grp;
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
          if (outs[c].kind != LineairDBAggKind::kPass) continue;
          Item *oi = outs[c].orig;
          LineairDBAggAccumulator &a = v[c];
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
      const LineairDBAggOutput &o = outs[c];
      if (o.kind == LineairDBAggKind::kPass) continue;
      LineairDBAggAccumulator &a = (*grp)[c];
      if (o.kind == LineairDBAggKind::kCount) { ++a.cnt; continue; }
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
  if (make_aggregate_output_caches(join, &row)) return true;
  const int div_inc = static_cast<int>(thd->variables.div_precincrement);

  for (auto &kv : groups) {  // std::map => ascending group-key order
    std::vector<LineairDBAggAccumulator> &g = kv.second;
    for (size_t c = 0; c < n; ++c) {
      Item_cache *cache = down_cast<Item_cache *>(row[c]);
      const LineairDBAggOutput &o = outs[c];
      LineairDBAggAccumulator &a = g[c];
      if (o.kind == LineairDBAggKind::kPass) {
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
      } else if (o.kind == LineairDBAggKind::kCount) {
        cache->null_value = false;
        down_cast<Item_cache_int *>(cache)->store_value(cache, a.cnt);
      } else if (o.kind == LineairDBAggKind::kSum) {
        // SUM over zero non-NULL inputs is NULL in SQL (not 0).
        if (a.cnt == 0) { cache->store_null(); continue; }
        cache->null_value = false;
        if (o.rtype == REAL_RESULT)
          down_cast<Item_cache_real *>(cache)->store_value(cache, a.dbl);
        else
          down_cast<Item_cache_decimal *>(cache)->store_value(cache, &a.dec);
      } else {  // AVG
        if (a.cnt == 0) { cache->store_null(); continue; }
        cache->null_value = false;
        if (o.rtype == REAL_RESULT) {
          down_cast<Item_cache_real *>(cache)->store_value(
              cache, a.dbl / static_cast<double>(a.cnt));
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

/**
 * @brief Return true when the chosen leaf is a full PRIMARY scan.
 *
 * @details The override reads the table by a primary full scan, and autogen
 * stages that primary range. Install it only for such a leaf; a secondary or
 * bounded leaf would be staged as a scan the override never reads, so prefetch
 * would miss and abort. Other leaves run on MySQL's normal executor.
 */
static bool leaf_is_full_primary_scan(AccessPath *root_path, JOIN *join) {
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

int lineairdb_push_to_engine(THD *thd, AccessPath *root_path, JOIN *join) {
  try_register_grouped_semijoin(thd, join);
  if (!is_aggregate_pushdown_shape(thd, join)) return 0;
  // The override reads a PRIMARY full scan; install it only when the plan chose
  // that scan, else its read misses the staged secondary and prefetch aborts.
  if (!leaf_is_full_primary_scan(root_path, join)) return 0;
  join->override_executor_func = &execute_aggregate_override;
  return 0;
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

/**
 * @brief Register a grouped aggregate leaf for synthetic summary-row serving.
 */
void ha_lineairdb::tx_register_grouped_summary(
    LineairDBTransaction::GroupedSummaryRegistration registration) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return;

  THD *thd = ha_thd();
  const uint64_t query_id =
      thd != nullptr ? static_cast<uint64_t>(thd->query_id) : 0;
  if (tx->autogen_query_id() != query_id) {
    tx->reset_autogen_for_statement(query_id);
  }
  tx->register_grouped_summary(table, std::move(registration));
}

/**
 * @brief Register a grouped semijoin reduction for this statement.
 */
void ha_lineairdb::tx_register_grouped_semijoin(
    LineairDBTransaction::GroupedSemijoin grouped_semijoin) {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return;

  THD *thd = ha_thd();
  const uint64_t query_id =
      thd != nullptr ? static_cast<uint64_t>(thd->query_id) : 0;
  if (tx->autogen_query_id() != query_id) {
    tx->reset_autogen_for_statement(query_id);
  }
  tx->register_grouped_semijoin(std::move(grouped_semijoin));
}

/**
 * @brief Return true if this statement already has grouped-semijoin state.
 */
bool ha_lineairdb::tx_has_grouped_semijoin() {
  auto tx = get_transaction(ha_thd());
  if (tx == nullptr) return false;

  THD *thd = ha_thd();
  const uint64_t query_id =
      thd != nullptr ? static_cast<uint64_t>(thd->query_id) : 0;
  if (tx->autogen_query_id() != query_id) return false;

  return tx->has_grouped_summary_registrations() ||
         !tx->grouped_semijoins().empty();
}

/**
 * @brief Return the transaction used by QEP read-plan autogen.
 */
LineairDBTransaction *ha_lineairdb::tx_for_autogen() {
  return get_transaction(ha_thd());
}

/**
 * @brief Materialize a skipped grouped summary leaf as synthetic handler rows.
 *
 * Autogen removes the base full-table scan for a recognized grouped summary
 * leaf, then this helper serves the server-produced group rows through the
 * normal handler cursor buffers that rnd_next() and index_next() consume.
 */
int ha_lineairdb::fill_grouped_summary_buffers(LineairDBTransaction *tx) {
  const LineairDBTransaction::GroupedSummaryRegistration *registration =
      tx->grouped_summary_registration(table);
  if (registration == nullptr) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "grouped summary scan skipped without registration");
    return HA_ERR_INTERNAL_ERROR;
  }

  std::vector<std::string> group_rows;
  if (const std::vector<std::string> *cached =
          tx->grouped_semijoin_group_rows(db_table_name)) {
    group_rows = *cached;
  } else {
    LineairDBProxy::ReadPlanStep step;
    step.table_name = db_table_name;
    step.is_scan = true;
    step.end_key_prefix = lineairdb_keyenc::scan_end_sentinel();
    step.aggregate_serialized = registration->spec;
    step.serialized_filter = registration->filter;
    if (!tx->execute_read_plan_raw({step}, &group_rows)) {
      my_error(ER_LOCK_DEADLOCK, MYF(0));
      return HA_ERR_LOCK_DEADLOCK;
    }
    tx->cache_grouped_semijoin_group_rows(db_table_name, group_rows);
  }

  reset_index_search_buffers();
  scanned_keys_.clear();
  scanned_values_.clear();
  scan_cache_.clear();
  buffer_position_ = 0;
  scan_exhausted_ = true;

  secondary_index_results_.reserve(group_rows.size());
  secondary_index_payloads_.reserve(group_rows.size());
  scanned_keys_.reserve(group_rows.size());
  scanned_values_.reserve(group_rows.size());

  const uint field_count = table->s->fields;
  if (registration->template_cols.size() != field_count ||
      registration->group_col >= field_count ||
      registration->col_a >= field_count ||
      (!registration->single_sum && registration->col_b >= field_count)) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "grouped summary registration does not match table shape");
    return HA_ERR_INTERNAL_ERROR;
  }

  std::vector<uchar> null_flags(table->s->null_bytes, 0);
  LineairDBField encoder;
  LineairDBField group_decoder;
  const size_t synthetic_key_size =
      ref_length > sizeof(uint16_t) ? ref_length - sizeof(uint16_t) : 0;
  if (synthetic_key_size < 2) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "grouped summary cursor ref is too small");
    return HA_ERR_INTERNAL_ERROR;
  }

  for (const std::string &group_row : group_rows) {
    group_decoder.make_mysql_table_row(
        reinterpret_cast<const std::byte *>(group_row.data()),
        group_row.size());
    if (group_decoder.get_row_size() < (registration->single_sum ? 2 : 3)) {
      my_error(ER_INTERNAL_ERROR, MYF(0), "malformed grouped summary row");
      return HA_ERR_INTERNAL_ERROR;
    }

    std::vector<std::string> row = registration->template_cols;
    row[registration->group_col] =
        std::string(group_decoder.get_column_of_row(0));
    row[registration->col_a] = std::string(group_decoder.get_column_of_row(1));
    if (!registration->single_sum) {
      row[registration->col_b] =
          std::string(group_decoder.get_column_of_row(2));
    }

    encoder.set_null_field(null_flags.data(), null_flags.size());
    std::string encoded_row = encoder.get_null_field();
    for (const std::string &field : row) {
      encoder.set_lineairdb_field(field.data(), field.size());
      encoded_row += encoder.get_lineairdb_field();
    }

    const size_t row_index = scanned_values_.size();
    const size_t ordinal_bytes = synthetic_key_size - 1;
    if (ordinal_bytes < sizeof(size_t) &&
        (row_index >> (ordinal_bytes * 8)) != 0) {
      my_error(ER_INTERNAL_ERROR, MYF(0),
               "grouped summary cursor key space exhausted");
      return HA_ERR_INTERNAL_ERROR;
    }

    // Grouped summary rows are aggregate output, so they have no storage
    // primary key. The handler cursor still needs a key for position(),
    // rnd_pos(), and scan_cache_; keep it within ref_length because MySQL
    // copies this value through the handler ref buffer.
    std::string synthetic_key(synthetic_key_size, '\0');
    synthetic_key[0] = '\x01';
    for (size_t byte = 1; byte < synthetic_key.size() &&
                          byte <= sizeof(size_t);
         ++byte) {
      synthetic_key[byte] =
          static_cast<char>((row_index >> ((byte - 1) * 8)) & 0xff);
    }

    scanned_keys_.push_back(synthetic_key);
    secondary_index_results_.push_back(synthetic_key);
    secondary_index_payloads_.push_back(encoded_row);
    scan_cache_[synthetic_key] = row_index;
    scanned_values_.emplace_back(
        reinterpret_cast<const std::byte *>(encoded_row.data()),
        reinterpret_cast<const std::byte *>(encoded_row.data()) +
            encoded_row.size());
  }

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
    if (!fetch_next_batch()) {
      scan_exhausted_ = true;
      return false;
    }
  }
  if (buffer_position_ >= scanned_values_.size()) return false;
  const auto &value = scanned_values_[buffer_position_];
  buffer_position_++;
  *out_value = std::string_view(reinterpret_cast<const char *>(value.data()),
                                value.size());
  current_position_++;
  return true;
}
