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
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "lineairdb_field_types.h"
#include "lineairdb_keyenc.hh"
#include "lineairdb_pushdown.hh"
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
#include "typelib.h"

// ---------------------------------------------------------------------------
// Predicate Pushdown: serialize MySQL Item tree → FilterExpr protobuf
// ---------------------------------------------------------------------------

const Item *ha_lineairdb::cond_push(const Item *cond) {
  DBUG_TRACE;
  pushed_filter_serialized_.clear();
  has_unpushed_filter_ = false;
  if (!cond || !table) return cond;

  LineairDB::Protocol::PushedPredicate predicate;
  predicate.set_num_columns(table->s->fields);
  if (!serialize_item(cond, predicate.mutable_expr())) {
    // Serialization failed: keep the full predicate for MySQL evaluation.
    has_unpushed_filter_ = true;
    return cond;
  }
  predicate.SerializeToString(&pushed_filter_serialized_);
  return cond;  // MySQL re-evaluates the predicate as a safety net.
}

/**
 * @brief Serialize a MySQL Item expression into a FilterExpr protobuf.
 *
 * @return false when the expression is unsupported by server-side filtering.
 */
bool serialize_item(const Item *item,
                    LineairDB::Protocol::FilterExpr *expr) {
  if (!item) return false;

  // Unwrap constant-propagation caches before reading their value.
  if (item->type() == Item::CACHE_ITEM) {
    const Item *example =
        down_cast<const Item_cache *>(item)->get_example();
    if (example != nullptr) return serialize_item(example, expr);
    return false;
  }

  // Fold constant expressions that are not already bare literals.
  if (item->const_item() && !item->has_subquery() &&
      item->type() != Item::INT_ITEM && item->type() != Item::REAL_ITEM &&
      item->type() != Item::STRING_ITEM &&
      item->type() != Item::DECIMAL_ITEM && item->type() != Item::NULL_ITEM &&
      item->type() != Item::COND_ITEM && item->type() != Item::FIELD_ITEM) {
    // Comparison and boolean functions must keep their expression shape.
    const bool is_structural_func = [&] {
      if (item->type() != Item::FUNC_ITEM) return false;
      switch (down_cast<const Item_func *>(item)->functype()) {
        case Item_func::EQ_FUNC:
        case Item_func::NE_FUNC:
        case Item_func::LT_FUNC:
        case Item_func::LE_FUNC:
        case Item_func::GT_FUNC:
        case Item_func::GE_FUNC:
        case Item_func::BETWEEN:
        case Item_func::IN_FUNC:
        case Item_func::LIKE_FUNC:
        case Item_func::ISNULL_FUNC:
        case Item_func::ISNOTNULL_FUNC:
        case Item_func::NOT_FUNC:
          return true;
        default:
          return false;
      }
    }();
    if (!is_structural_func) {
      Item *mut = const_cast<Item *>(item);
      // Stored temporal values compare as their string representation.
      if (mut->is_temporal()) {
        String tbuf;
        String *ts = mut->val_str(&tbuf);
        if (mut->null_value || ts == nullptr) {
          expr->set_op(LineairDB::Protocol::FilterExpr::CONST_NULL);
        } else {
          expr->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
          expr->set_string_val(ts->ptr(), ts->length());
        }
        return true;
      }
      // Serialize the folded value using MySQL's result type.
      switch (mut->result_type()) {
        case INT_RESULT: {
          const longlong v = mut->val_int();
          if (mut->null_value) {
            expr->set_op(LineairDB::Protocol::FilterExpr::CONST_NULL);
          } else if (mut->unsigned_flag) {
            expr->set_op(LineairDB::Protocol::FilterExpr::CONST_UINT);
            expr->set_uint_val(static_cast<ulonglong>(v));
          } else {
            expr->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
            expr->set_int_val(v);
          }
          return true;
        }
        case REAL_RESULT:
        case DECIMAL_RESULT: {
          const double v = mut->val_real();
          if (mut->null_value) {
            expr->set_op(LineairDB::Protocol::FilterExpr::CONST_NULL);
          } else {
            expr->set_op(LineairDB::Protocol::FilterExpr::CONST_DOUBLE);
            expr->set_double_val(v);
          }
          return true;
        }
        case STRING_RESULT: {
          String buf;
          String *s = mut->val_str(&buf);
          if (mut->null_value || s == nullptr) {
            expr->set_op(LineairDB::Protocol::FilterExpr::CONST_NULL);
          } else {
            expr->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
            expr->set_string_val(s->ptr(), s->length());
          }
          return true;
        }
        default:
          return false;
      }
    }
  }

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

      // Set compare_type based on MySQL field type
      switch (field->result_type()) {
        case INT_RESULT:
          if (field->is_unsigned())
            expr->set_compare_type(1);  // UNSIGNED_INT
          else
            expr->set_compare_type(0);  // SIGNED_INT
          break;
        case REAL_RESULT:
        case DECIMAL_RESULT:
          expr->set_compare_type(2);  // DOUBLE
          break;
        default:
          expr->set_compare_type(3);  // STRING
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
          expr->set_op(LineairDB::Protocol::FilterExpr::OP_IN);
          auto *in_func = down_cast<const Item_func_in *>(func);
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
          return false;  // unsupported function → skip PP
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
      return false;  // unsupported item type → skip PP
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

// True when WHERE is an AND tree of simple integer comparisons
static bool item_is_limit_safe_filter(const Item *item) {
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

/**
 * @brief Collect predicates that reference only one table from an AND tree.
 *
 * @details Cross-table and constant predicates are skipped. Dropping them only
 * relaxes the pushed filter, because MySQL still evaluates the full WHERE
 * later.
 */
static void collect_table_local_predicates(Item *it, table_map me,
                                           std::vector<Item *> *out) {
  if (it == nullptr) return;
  if (it->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(it)->functype() == Item_func::COND_AND_FUNC) {
    for (Item &child : *down_cast<Item_cond *>(it)->argument_list()) {
      collect_table_local_predicates(&child, me, out);
    }
  } else if (it->used_tables() == me) {
    out->push_back(it);
  }
}

/**
 * @brief Serialize the table-local necessary condition of an OR predicate.
 *
 * @details Every OR branch must constrain this table. If one branch does not,
 * pushing the OR would be stricter than the query, so this returns false.
 *
 * @return true when the OR has a safe table-local predicate.
 */
static bool serialize_or_necessary_condition(
    Item *or_item, table_map me, LineairDB::Protocol::FilterExpr *out) {
  if (or_item == nullptr || or_item->type() != Item::COND_ITEM ||
      down_cast<Item_cond *>(or_item)->functype() != Item_func::COND_OR_FUNC)
    return false;
  out->set_op(LineairDB::Protocol::FilterExpr::OP_OR);

  // Every OR branch must have a predicate for this table.
  for (Item &disj : *down_cast<Item_cond *>(or_item)->argument_list()) {
    std::vector<Item *> branch_predicates;
    collect_table_local_predicates(&disj, me, &branch_predicates);
    if (branch_predicates.empty()) return false;  // unconstrained branch

    // Multiple predicates in one branch stay grouped under AND.
    LineairDB::Protocol::FilterExpr branch;
    if (branch_predicates.size() == 1) {
      if (!serialize_item(branch_predicates[0], &branch)) return false;
    } else {
      branch.set_op(LineairDB::Protocol::FilterExpr::OP_AND);
      for (Item *predicate : branch_predicates)
        if (!serialize_item(predicate, branch.add_children())) return false;
    }
    *out->add_children() = std::move(branch);
  }
  return out->children_size() > 0;
}

bool build_single_table_filter(THD *thd, TABLE *table,
                               std::string *out_serialized) {
  if (out_serialized == nullptr) return false;
  out_serialized->clear();
  if (thd == nullptr || table == nullptr || table->s == nullptr) return false;
  if (table->pos_in_table_list == nullptr) return false;

  // Read the WHERE from the query block that owns this table reference.
  Query_block *qb = table->pos_in_table_list->query_block;
  if (qb == nullptr) return false;
  Item *where = qb->where_cond();
  if (where == nullptr) return false;

  const table_map me = table->pos_in_table_list->map();

  std::vector<LineairDB::Protocol::FilterExpr> serialized;

  // Split only the top-level AND; each conjunct can be pushed independently.
  std::vector<Item *> conjuncts;
  if (where->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(where)->functype() == Item_func::COND_AND_FUNC) {
    for (Item &c : *down_cast<Item_cond *>(where)->argument_list())
      conjuncts.push_back(&c);
  } else {
    conjuncts.push_back(where);
  }

  // Serialize table-local predicates. OR must be safe as a necessary condition.
  for (Item *c : conjuncts) {
    if (c->type() == Item::COND_ITEM &&
        down_cast<Item_cond *>(c)->functype() == Item_func::COND_OR_FUNC) {
      LineairDB::Protocol::FilterExpr or_expr;
      if (serialize_or_necessary_condition(c, me, &or_expr))
        serialized.push_back(std::move(or_expr));
    } else {
      std::vector<Item *> table_predicates;
      collect_table_local_predicates(c, me, &table_predicates);
      for (Item *predicate : table_predicates) {
        LineairDB::Protocol::FilterExpr expr;
        if (serialize_item(predicate, &expr))
          serialized.push_back(std::move(expr));
      }
    }
  }
  if (serialized.empty()) return false;

  // Combine all pushed pieces with AND and attach the table column count.
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

// Push the SELECT WHERE into this transaction if LIMIT will depend on it.
bool prepare_select_filter_for_tx(THD *thd, TABLE *table,
                                  LineairDBTransaction *tx,
                                  std::string *serialized_filter) {
  // Check the handler state needed to inspect the current SELECT.
  if (tx == nullptr) return false;                   // Missing transaction
  if (thd == nullptr) return false;                  // Missing session
  const bool has_table = (table != nullptr && table->s != nullptr);
  if (!has_table) return false;                      // Missing table
  if (thd->lex == nullptr) return false;             // Missing SQL state
  if (thd->lex->unit == nullptr) return false;       // Missing query unit

  // Read the current SELECT WHERE from MySQL's query block.
  Query_block *qb = thd->lex->unit->global_parameters();
  if (qb == nullptr) return false;                   // Missing query block

  const Item *where = qb->where_cond();
  if (where == nullptr) {
    if (serialized_filter != nullptr) serialized_filter->clear();
    tx->clear_pushed_filter();
    return true;                                     // No WHERE to push
  }

  // Push only predicates that can be evaluated on this table's rows.
  std::string encoded;
  if (!build_single_table_filter(thd, table, &encoded) || encoded.empty()) {
    if (serialized_filter != nullptr) serialized_filter->clear();
    tx->clear_pushed_filter();
    return false;                                    // MySQL must filter
  }

  // Attach the encoded filter to the transaction for the next scan RPC.
  if (serialized_filter != nullptr) {
    *serialized_filter = encoded;
  }
  tx->set_pushed_filter(encoded);

  // LIMIT pushdown is safe only when the table-local filter is the whole WHERE.
  if (table->pos_in_table_list == nullptr) return false;
  const table_map me = table->pos_in_table_list->map();
  return where->used_tables() == me && item_is_limit_safe_filter(where);
}
