#include "aggregate_pushdown.hh"

#include <cstdint>
#include <cstdlib>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lineairdb.pb.h"
#include "lineairdb_pushdown.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_func.h"
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
static bool serialize_aggregate_expression(
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
              !serialize_aggregate_expression(outs[c].arg, af->mutable_arg())) {
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
  if (!is_aggregate_pushdown_shape(thd, join)) return 0;
  // The override reads a PRIMARY full scan; install it only when the plan chose
  // that scan, else its read misses the staged secondary and prefetch aborts.
  if (!leaf_is_full_primary_scan(root_path, join)) return 0;
  join->override_executor_func = &execute_aggregate_override;
  return 0;
}

/**
 * @brief Expose the engine-pushdown hook to MySQL's optimizer.
 */
const handlerton *ha_lineairdb::hton_supporting_engine_pushdown() {
  return lineairdb_hton;
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
