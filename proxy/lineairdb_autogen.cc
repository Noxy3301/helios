#include "lineairdb_autogen.hh"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lineairdb_keyenc.hh"
#include "lineairdb_pushdown.hh"
#include "my_base.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/range_optimizer/range_optimizer.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_opt_exec_shared.h"
#include "sql/table.h"
#include "storage/lineairdb/ha_lineairdb.hh"

namespace {

struct UnsupportedQep {
  AccessPath::Type type{AccessPath::TABLE_SCAN};
  std::string reason;
};

const char *access_path_type_name(AccessPath::Type type) {
  switch (type) {
    case AccessPath::TABLE_SCAN: return "TABLE_SCAN";
    case AccessPath::INDEX_SCAN: return "INDEX_SCAN";
    case AccessPath::REF: return "REF";
    case AccessPath::REF_OR_NULL: return "REF_OR_NULL";
    case AccessPath::EQ_REF: return "EQ_REF";
    case AccessPath::PUSHED_JOIN_REF: return "PUSHED_JOIN_REF";
    case AccessPath::FULL_TEXT_SEARCH: return "FULL_TEXT_SEARCH";
    case AccessPath::CONST_TABLE: return "CONST_TABLE";
    case AccessPath::MRR: return "MRR";
    case AccessPath::FOLLOW_TAIL: return "FOLLOW_TAIL";
    case AccessPath::INDEX_RANGE_SCAN: return "INDEX_RANGE_SCAN";
    case AccessPath::INDEX_MERGE: return "INDEX_MERGE";
    case AccessPath::ROWID_INTERSECTION: return "ROWID_INTERSECTION";
    case AccessPath::ROWID_UNION: return "ROWID_UNION";
    case AccessPath::INDEX_SKIP_SCAN: return "INDEX_SKIP_SCAN";
    case AccessPath::GROUP_INDEX_SKIP_SCAN: return "GROUP_INDEX_SKIP_SCAN";
    case AccessPath::DYNAMIC_INDEX_RANGE_SCAN:
      return "DYNAMIC_INDEX_RANGE_SCAN";
    case AccessPath::TABLE_VALUE_CONSTRUCTOR:
      return "TABLE_VALUE_CONSTRUCTOR";
    case AccessPath::FAKE_SINGLE_ROW: return "FAKE_SINGLE_ROW";
    case AccessPath::ZERO_ROWS: return "ZERO_ROWS";
    case AccessPath::ZERO_ROWS_AGGREGATED: return "ZERO_ROWS_AGGREGATED";
    case AccessPath::MATERIALIZED_TABLE_FUNCTION:
      return "MATERIALIZED_TABLE_FUNCTION";
    case AccessPath::UNQUALIFIED_COUNT: return "UNQUALIFIED_COUNT";
    case AccessPath::NESTED_LOOP_JOIN: return "NESTED_LOOP_JOIN";
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      return "NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL";
    case AccessPath::BKA_JOIN: return "BKA_JOIN";
    case AccessPath::HASH_JOIN: return "HASH_JOIN";
    case AccessPath::FILTER: return "FILTER";
    case AccessPath::SORT: return "SORT";
    case AccessPath::AGGREGATE: return "AGGREGATE";
    case AccessPath::TEMPTABLE_AGGREGATE: return "TEMPTABLE_AGGREGATE";
    case AccessPath::LIMIT_OFFSET: return "LIMIT_OFFSET";
    case AccessPath::STREAM: return "STREAM";
    case AccessPath::MATERIALIZE: return "MATERIALIZE";
    case AccessPath::MATERIALIZE_INFORMATION_SCHEMA_TABLE:
      return "MATERIALIZE_INFORMATION_SCHEMA_TABLE";
    case AccessPath::APPEND: return "APPEND";
    case AccessPath::WINDOW: return "WINDOW";
    case AccessPath::WEEDOUT: return "WEEDOUT";
    case AccessPath::REMOVE_DUPLICATES: return "REMOVE_DUPLICATES";
    case AccessPath::REMOVE_DUPLICATES_ON_INDEX:
      return "REMOVE_DUPLICATES_ON_INDEX";
    case AccessPath::ALTERNATIVE: return "ALTERNATIVE";
    case AccessPath::CACHE_INVALIDATOR: return "CACHE_INVALIDATOR";
    case AccessPath::DELETE_ROWS: return "DELETE_ROWS";
    case AccessPath::UPDATE_ROWS: return "UPDATE_ROWS";
  }
  return "UNKNOWN";
}

bool raise_unsupported(THD *thd, const char *type_name,
                       const std::string &reason) {
  const LEX_CSTRING query = thd != nullptr ? thd->query() : LEX_CSTRING();
  const std::string sql =
      query.str != nullptr && query.length > 0
          ? std::string(query.str, query.length)
          : std::string();
  const long long query_id = thd != nullptr ? thd->query_id : 0;

  std::string msg = "LineairDB autogen read plan unsupported: type=";
  msg += type_name != nullptr ? type_name : "UNKNOWN";
  msg += " reason=";
  msg += reason;
  msg += " query_id=";
  msg += std::to_string(query_id);
  msg += " sql=";
  msg += sql;

  my_error(ER_NOT_SUPPORTED_YET, MYF(0), msg.c_str());
  return false;
}

bool raise_unsupported(THD *thd, AccessPath::Type type,
                       const std::string &reason) {
  return raise_unsupported(thd, access_path_type_name(type), reason);
}

void set_unsupported(AccessPath *p, const char *reason, bool *ok,
                     UnsupportedQep *unsupported) {
  if (ok != nullptr) *ok = false;
  if (unsupported != nullptr && unsupported->reason.empty()) {
    unsupported->type = p != nullptr ? p->type : AccessPath::TABLE_SCAN;
    unsupported->reason = reason != nullptr ? reason : "unsupported QEP node";
  }
}

key_part_map first_n_keyparts_map(uint key_parts) {
  key_part_map map = 0;
  for (uint i = 0; i < key_parts; ++i) map |= (key_part_map{1} << i);
  return map;
}

bool is_int32_key_field(const Field *f) {
  return f != nullptr && f->type() == MYSQL_TYPE_LONG &&
         f->pack_length() == 4 && !f->is_unsigned();
}

// Physical-table key from a TABLE's own share: "./<db>/<table>", matching the
// path ha_lineairdb::open() stores in db_table_name.
std::string physical_table_key(const TABLE *t) {
  if (t == nullptr || t->s == nullptr) return std::string();
  const TABLE_SHARE *s = t->s;
  std::string key = "./";
  if (s->db.str != nullptr && s->db.length > 0) {
    key.append(s->db.str, s->db.length);
  }
  key.push_back('/');
  if (s->table_name.str != nullptr && s->table_name.length > 0) {
    key.append(s->table_name.str, s->table_name.length);
  }
  return key;
}

int qep_table_field_index(TABLE *t, Field *f) {
  if (t == nullptr || t->s == nullptr || f == nullptr) return -1;
  for (uint i = 0; i < t->s->fields; ++i) {
    if (t->field[i] == f) return static_cast<int>(i);
  }
  return -1;
}

void collect_qep_leaves(AccessPath *p, std::vector<AccessPath *> *out,
                        bool *ok, UnsupportedQep *unsupported) {
  if (p == nullptr || out == nullptr || ok == nullptr || !*ok) return;
  switch (p->type) {
    case AccessPath::TABLE_SCAN:
    case AccessPath::INDEX_SCAN:
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::EQ_REF:
    case AccessPath::PUSHED_JOIN_REF:
    case AccessPath::CONST_TABLE:
    case AccessPath::INDEX_RANGE_SCAN:
    case AccessPath::MRR:
      // MRR is BKA's inner-table access: a ref-like (table, ref) leaf whose ref
      // keyparts bind to the outer table, so compile_ref_lookup turns it into a
      // for_each point/range probe.
      out->push_back(p);
      return;
    case AccessPath::NESTED_LOOP_JOIN:
      collect_qep_leaves(p->nested_loop_join().outer, out, ok, unsupported);
      collect_qep_leaves(p->nested_loop_join().inner, out, ok, unsupported);
      return;
    case AccessPath::BKA_JOIN:
      collect_qep_leaves(p->bka_join().outer, out, ok, unsupported);
      collect_qep_leaves(p->bka_join().inner, out, ok, unsupported);
      return;
    case AccessPath::HASH_JOIN:
      if ((p->hash_join().outer != nullptr &&
           p->hash_join().outer->parameter_tables != 0) ||
          (p->hash_join().inner != nullptr &&
           p->hash_join().inner->parameter_tables != 0)) {
        set_unsupported(p, "parameterized hash join side", ok, unsupported);
        return;
      }
      collect_qep_leaves(p->hash_join().outer, out, ok, unsupported);
      collect_qep_leaves(p->hash_join().inner, out, ok, unsupported);
      return;
    case AccessPath::FILTER:
      collect_qep_leaves(p->filter().child, out, ok, unsupported);
      return;
    case AccessPath::SORT:
      collect_qep_leaves(p->sort().child, out, ok, unsupported);
      return;
    case AccessPath::LIMIT_OFFSET:
      collect_qep_leaves(p->limit_offset().child, out, ok, unsupported);
      return;
    case AccessPath::AGGREGATE:
      collect_qep_leaves(p->aggregate().child, out, ok, unsupported);
      return;
    case AccessPath::STREAM:
      collect_qep_leaves(p->stream().child, out, ok, unsupported);
      return;
    case AccessPath::TEMPTABLE_AGGREGATE:
      // MySQL can run GROUP BY by filling an internal temp table and reading it
      // back. Prefetch only needs the LineairDB reads that feed that temp table;
      // the temp-table leaves themselves are skipped in the compile loop.
      collect_qep_leaves(p->temptable_aggregate().subquery_path, out, ok,
                         unsupported);
      collect_qep_leaves(p->temptable_aggregate().table_path, out, ok,
                         unsupported);
      return;
    case AccessPath::MATERIALIZE:
      // Derived tables and materialized subqueries have the same shape: the
      // source subqueries may read LineairDB, but the materialized table is
      // local to MySQL.
      for (const MaterializePathParameters::QueryBlock &qb :
           p->materialize().param->query_blocks) {
        collect_qep_leaves(qb.subquery_path, out, ok, unsupported);
      }
      collect_qep_leaves(p->materialize().table_path, out, ok, unsupported);
      return;
    case AccessPath::DELETE_ROWS:
      collect_qep_leaves(p->delete_rows().child, out, ok, unsupported);
      return;
    case AccessPath::UPDATE_ROWS:
      collect_qep_leaves(p->update_rows().child, out, ok, unsupported);
      return;
    case AccessPath::WEEDOUT:
      // Semijoin duplicate weedout dedups locally via handler rowids;
      // position()/rnd_pos() re-reads hit the staged row cache.
      collect_qep_leaves(p->weedout().child, out, ok, unsupported);
      return;
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      collect_qep_leaves(p->nested_loop_semijoin_with_duplicate_removal().outer,
                         out, ok, unsupported);
      collect_qep_leaves(p->nested_loop_semijoin_with_duplicate_removal().inner,
                         out, ok, unsupported);
      return;
    case AccessPath::ZERO_ROWS:
      // The optimizer proved this subtree returns nothing; no rows will be
      // read at runtime, so no prefetch step is needed.
      return;
    default:
      set_unsupported(p, "unsupported QEP node", ok, unsupported);
      return;
  }
}

bool qep_leaf_info(AccessPath *p, TABLE **tbl, Index_lookup **ref,
                   bool *is_full_scan, int *full_scan_index) {
  if (p == nullptr || tbl == nullptr || ref == nullptr ||
      is_full_scan == nullptr || full_scan_index == nullptr) {
    return false;
  }

  *tbl = nullptr;
  *ref = nullptr;
  *is_full_scan = false;
  *full_scan_index = -1;

  switch (p->type) {
    case AccessPath::TABLE_SCAN:
      *tbl = p->table_scan().table;
      *is_full_scan = true;
      return true;
    case AccessPath::INDEX_SCAN:
      *tbl = p->index_scan().table;
      *is_full_scan = true;
      *full_scan_index = p->index_scan().idx;
      return true;
    case AccessPath::REF:
      *tbl = p->ref().table;
      *ref = p->ref().ref;
      return true;
    case AccessPath::REF_OR_NULL:
      *tbl = p->ref_or_null().table;
      *ref = p->ref_or_null().ref;
      return true;
    case AccessPath::EQ_REF:
      *tbl = p->eq_ref().table;
      *ref = p->eq_ref().ref;
      return true;
    case AccessPath::PUSHED_JOIN_REF:
      *tbl = p->pushed_join_ref().table;
      *ref = p->pushed_join_ref().ref;
      return true;
    case AccessPath::CONST_TABLE:
      *tbl = p->const_table().table;
      *ref = p->const_table().ref;
      return true;
    case AccessPath::MRR:
      *tbl = p->mrr().table;
      *ref = p->mrr().ref;
      return true;
    case AccessPath::INDEX_RANGE_SCAN:
      if (p->index_range_scan().used_key_part == nullptr ||
          p->index_range_scan().used_key_part[0].field == nullptr) {
        return false;
      }
      *tbl = p->index_range_scan().used_key_part[0].field->table;
      return true;
    default:
      return false;
  }
}

bool append_bound_keypart(
    TABLE *target, Index_lookup *ref, uint keypart_idx,
    Field *source_field, int source_step,
    LineairDBProxy::ReadPlanStep *step, std::string *reason) {
  if (target == nullptr || target->s == nullptr || ref == nullptr ||
      step == nullptr || source_field == nullptr ||
      source_field->table == nullptr || ref->key < 0 ||
      ref->key >= static_cast<int>(target->s->keys)) {
    if (reason != nullptr) *reason = "invalid bound keypart metadata";
    return false;
  }

  Field *target_field = target->key_info[ref->key].key_part[keypart_idx].field;
  if (!is_int32_key_field(target_field)) {
    if (reason != nullptr) *reason = "non-INT4 join key";
    return false;
  }

  LineairDBProxy::ReadPlanKeyBinding binding;
  binding.source_step = static_cast<uint32_t>(source_step);

  TABLE *source_table = source_field->table;
  const uint source_pk = source_table->s->primary_key;
  int pk_pos = -1;
  int pk_parts = 0;
  if (source_pk != MAX_KEY) {
    KEY &source_key = source_table->key_info[source_pk];
    pk_parts = static_cast<int>(source_key.user_defined_key_parts);
    for (int i = 0; i < pk_parts; ++i) {
      if (source_key.key_part[i].field == source_field) {
        pk_pos = i;
        break;
      }
    }
  }

  if (pk_pos == 0 && pk_parts == 1) {
    if (!is_int32_key_field(source_field)) {
      if (reason != nullptr) *reason = "non-INT4 source key";
      return false;
    }
    binding.from_key = true;
  } else if (pk_pos >= 0) {
    KEY &source_key = source_table->key_info[source_pk];
    for (int i = 0; i <= pk_pos; ++i) {
      if (!is_int32_key_field(source_key.key_part[i].field)) {
        if (reason != nullptr) *reason = "non-INT4 source key slice";
        return false;
      }
    }

    binding.from_key = true;
    uint offset = 0;
    for (int i = 0; i < pk_pos; ++i) {
      offset += 4 + source_key.key_part[i].length;
    }
    binding.source_offset = offset;
    binding.source_length = 4 + source_key.key_part[pk_pos].length;
  } else {
    if (!is_int32_key_field(source_field)) {
      if (reason != nullptr) *reason = "non-INT4 source column";
      return false;
    }
    const int field_index = qep_table_field_index(source_table, source_field);
    if (field_index < 0) {
      if (reason != nullptr) *reason = "source field not in table";
      return false;
    }
    binding.source_column = field_index + 1;
    binding.column_as_int_key = true;
  }

  step->bindings.push_back(std::move(binding));
  return true;
}

bool compile_index_range_scan(AccessPath *leaf, TABLE *table,
                              LineairDBProxy::ReadPlanStep *step,
                              std::string *reason) {
  if (leaf == nullptr || table == nullptr || table->s == nullptr ||
      step == nullptr) {
    if (reason != nullptr) *reason = "invalid range-scan metadata";
    return false;
  }

  const auto &range_scan = leaf->index_range_scan();
  if (range_scan.geometry) {
    if (reason != nullptr) *reason = "geometry index range scan";
    return false;
  }
  if (range_scan.index >= table->s->keys) {
    if (reason != nullptr) *reason = "invalid range-scan index";
    return false;
  }
  if (range_scan.num_ranges != 1) {
    if (reason != nullptr) *reason = "multi-range index scan";
    return false;
  }
  if (range_scan.ranges == nullptr || range_scan.ranges[0] == nullptr) {
    if (reason != nullptr) *reason = "missing range bounds";
    return false;
  }

  QUICK_RANGE *range = range_scan.ranges[0];
  step->table_name = physical_table_key(table);
  step->is_scan = true;
  // Stage the canonical forward, unbounded shape (reverse_scan=false,
  // scan_limit=0); MySQL applies ORDER BY / LIMIT / WHERE above and the consumer
  // requests the same shape, so staged and consumed scans match. Pushing
  // direction/limit is filter-aware v2 work (the plan scan carries no WHERE).
  if (range_scan.index != table->s->primary_key) {
    step->index_name = table->key_info[range_scan.index].name;
  }

  if (range->flag & NO_MIN_RANGE) {
    step->key_prefix.clear();
  } else {
    step->key_prefix = lineairdb_keyenc::convert_key_to_ldbformat(
        table, range_scan.index, range->min_key, range->min_keypart_map);
    if (range->min_keypart_map != 0 && step->key_prefix.empty()) {
      if (reason != nullptr) *reason = "failed to encode range start key";
      return false;
    }
    // NEAR_MIN (exclusive lower): match the handler, which appends one '\0' for
    // HA_READ_AFTER_KEY. build_prefix_range_end would overshoot the handler's
    // start and miss the cache; '\0' can over-include a shared-prefix key, but
    // the WHERE re-check makes that a safe over-fetch.
    if (range->flag & NEAR_MIN) step->key_prefix.push_back('\0');
  }

  if (range->flag & NO_MAX_RANGE) {
    step->end_key_prefix = lineairdb_keyenc::scan_end_sentinel();
  } else {
    step->end_key_prefix = lineairdb_keyenc::convert_key_to_ldbformat(
        table, range_scan.index, range->max_key, range->max_keypart_map);
    if (range->max_keypart_map != 0 && step->end_key_prefix.empty()) {
      if (reason != nullptr) *reason = "failed to encode range end key";
      return false;
    }
    if (!(range->flag & NEAR_MAX)) {
      step->end_key_prefix =
          lineairdb_keyenc::build_prefix_range_end(step->end_key_prefix);
    }
  }

  return !step->table_name.empty();
}

bool compile_ref_lookup(
    TABLE *table, Index_lookup *ref,
    const std::unordered_map<TABLE *, int> &table_steps,
    LineairDBProxy::ReadPlanStep *step, std::string *reason) {
  if (table == nullptr || table->s == nullptr || ref == nullptr ||
      step == nullptr) {
    if (reason != nullptr) *reason = "invalid ref metadata";
    return false;
  }
  if (ref->key < 0 || ref->key >= static_cast<int>(table->s->keys)) {
    if (reason != nullptr) *reason = "invalid ref index";
    return false;
  }
  if (ref->key_parts == 0 || ref->key_buff == nullptr ||
      ref->items == nullptr) {
    if (reason != nullptr) *reason = "missing ref key buffer";
    return false;
  }

  KEY &key = table->key_info[ref->key];
  if (ref->key_parts > key.user_defined_key_parts) {
    if (reason != nullptr) *reason = "ref uses too many keyparts";
    return false;
  }

  step->table_name = physical_table_key(table);

  uint leading_constant_parts = 0;
  bool saw_binding = false;
  uint bound_parts = 0;

  // First pass: validate shape (constants strictly before bindings) and
  // collect the bound keyparts so we can pick a single iterating source.
  struct BoundPart {
    uint kp;
    Item_field *item;
  };
  std::vector<BoundPart> bound_items;
  for (uint kp = 0; kp < ref->key_parts; ++kp) {
    Item *item = ref->items[kp];
    if (item == nullptr) {
      if (reason != nullptr) *reason = "missing ref keypart item";
      return false;
    }
    item = item->real_item();
    if (item == nullptr) {
      if (reason != nullptr) *reason = "missing real ref keypart item";
      return false;
    }

    if (item->type() != Item::FIELD_ITEM) {
      if (!item->const_for_execution()) {
        if (reason != nullptr) *reason = "non-constant non-field keypart";
        return false;
      }
      if (saw_binding) {
        if (reason != nullptr) *reason = "constant keypart after binding";
        return false;
      }
      ++leading_constant_parts;
      continue;
    }

    saw_binding = true;
    Item_field *item_field = down_cast<Item_field *>(item);
    if (item_field->field == nullptr || item_field->field->table == nullptr) {
      if (reason != nullptr) *reason = "missing bound source field";
      return false;
    }
    bound_items.push_back({kp, item_field});
  }

  // Pick the iterator source. A keypart bound to a real earlier step iterates
  // directly. One bound to a materialized temp table is remapped via Item_equal
  // onto a real earlier step, staging only the leading key prefix; the dropped
  // trailing keyparts over-fetch a superset that the WHERE re-check trims.
  TABLE *iter_table = nullptr;
  int iter_step = -1;

  // Stage the probed table/index as a full range. Used when a temp-table-driven
  // probe has no salvageable leading prefix.
  auto stage_full_range = [&]() -> bool {
    const THD *leaf_thd = table->in_use;
    const bool plain_select =
        leaf_thd != nullptr && leaf_thd->lex != nullptr &&
        leaf_thd->lex->sql_command == SQLCOM_SELECT &&
        table->reginfo.lock_type <= TL_READ;
    if (!plain_select) {
      if (reason != nullptr) {
        *reason = "temp-table-driven probe outside plain SELECT";
      }
      return false;
    }
    step->bindings.clear();
    step->end_bindings.clear();
    step->for_each = false;
    step->is_scan = true;
    step->key_prefix.clear();
    step->end_key_prefix = lineairdb_keyenc::scan_end_sentinel();
    if (ref->key == static_cast<int>(table->s->primary_key)) {
      step->index_name.clear();
    } else {
      step->index_name = key.name;
    }
    return !step->table_name.empty();
  };

  // Resolve a bound keypart to a real earlier step: a direct staged source if
  // it has one, else the latest real step among its Item_equal members.
  auto resolve_real_step = [&](Item_field *bound, TABLE **out_table,
                               int *out_step) -> bool {
    auto direct = table_steps.find(bound->field->table);
    if (direct != table_steps.end()) {
      *out_table = bound->field->table;
      *out_step = direct->second;
      return true;
    }
    Item_equal *eq = bound->item_equal_all_join_nests != nullptr
                         ? bound->item_equal_all_join_nests
                         : bound->item_equal;
    if (eq == nullptr) return false;
    TABLE *best = nullptr;
    int best_step = -1;
    Item_equal::FieldProxy proxy(eq);
    for (Item_field &candidate : proxy) {
      if (candidate.field == nullptr) continue;
      auto real = table_steps.find(candidate.field->table);
      if (real != table_steps.end() && real->second > best_step) {
        best_step = real->second;
        best = candidate.field->table;
      }
    }
    if (best == nullptr) return false;
    *out_table = best;
    *out_step = best_step;
    return true;
  };

  // True when the keypart's field is on the iterator table or has an Item_equal
  // member there (so the second pass below can bind it to that iterator).
  auto remaps_onto = [&](Item_field *bound, TABLE *target) -> bool {
    if (bound->field->table == target) return true;
    Item_equal *eq = bound->item_equal_all_join_nests != nullptr
                         ? bound->item_equal_all_join_nests
                         : bound->item_equal;
    if (eq == nullptr) return false;
    Item_equal::FieldProxy proxy(eq);
    for (Item_field &candidate : proxy) {
      if (candidate.field != nullptr && candidate.field->table == target) {
        return true;
      }
    }
    return false;
  };

  bool saw_temp_source = false;
  for (const BoundPart &bp : bound_items) {
    if (table_steps.find(bp.item->field->table) != table_steps.end()) continue;
    TABLE *src_table = bp.item->field->table;
    if (src_table != nullptr && src_table->s != nullptr &&
        src_table->s->tmp_table != NO_TMP_TABLE) {
      saw_temp_source = true;
    } else {
      // Unknown source (neither a staged step nor a temp table): fail the read
      // plan as unsupported, rather than salvaging or full-ranging it.
      if (reason != nullptr) *reason = "bound source is not an earlier step";
      return false;
    }
  }

  if (!saw_temp_source) {
    // Every bound source is a real earlier step; iterate from the latest.
    for (const BoundPart &bp : bound_items) {
      const int source_step = table_steps.find(bp.item->field->table)->second;
      if (source_step > iter_step) {
        iter_step = source_step;
        iter_table = bp.item->field->table;
      }
    }
  } else {
    // The first keypart picks the iterator; keep later keyparts only while they
    // remap onto it. Drop the rest -- only a leading prefix can form a range.
    std::vector<BoundPart> leading;
    for (const BoundPart &bp : bound_items) {
      if (iter_table == nullptr) {
        TABLE *cand_table = nullptr;
        int cand_step = -1;
        if (!resolve_real_step(bp.item, &cand_table, &cand_step)) break;
        iter_table = cand_table;
        iter_step = cand_step;
      } else if (!remaps_onto(bp.item, iter_table)) {
        break;
      }
      leading.push_back(bp);
    }
    bound_items.swap(leading);
    if (bound_items.empty() && leading_constant_parts == 0) {
      return stage_full_range();
    }
  }

  // Append every bound key part against the chosen iterator source.
  for (const BoundPart &bp : bound_items) {
    Field *source_field = bp.item->field;
    if (source_field->table != iter_table) {
      // This key part came from another table; find its equal field on the
      // iterator table.
      Item_equal *eq = bp.item->item_equal_all_join_nests != nullptr
                           ? bp.item->item_equal_all_join_nests
                           : bp.item->item_equal;
      Field *remapped = nullptr;
      if (eq != nullptr) {
        Item_equal::FieldProxy proxy(eq);
        for (Item_field &candidate : proxy) {
          if (candidate.field != nullptr &&
              candidate.field->table == iter_table) {
            remapped = candidate.field;
            break;
          }
        }
      }
      if (remapped == nullptr) {
        if (saw_temp_source) return stage_full_range();
        if (reason != nullptr) {
          *reason = "for_each binding spans multiple source steps";
        }
        return false;
      }
      source_field = remapped;
    }
    if (!append_bound_keypart(table, ref, bp.kp, source_field, iter_step,
                              step, reason)) {
      if (saw_temp_source) return stage_full_range();
      return false;
    }
    ++bound_parts;
  }

  if (leading_constant_parts > 0) {
    step->key_prefix = lineairdb_keyenc::convert_key_to_ldbformat(
        table, ref->key, ref->key_buff,
        first_n_keyparts_map(leading_constant_parts));
    if (step->key_prefix.empty()) {
      if (reason != nullptr) *reason = "failed to encode constant key prefix";
      return false;
    }
  }

  const uint used_key_parts = leading_constant_parts + bound_parts;
  if (used_key_parts == 0) {
    if (reason != nullptr) *reason = "no compilable keyparts";
    return false;
  }

  const bool child_primary =
      ref->key == static_cast<int>(table->s->primary_key);
  const uint child_pk_parts =
      table->s->primary_key != MAX_KEY
          ? table->key_info[table->s->primary_key].user_defined_key_parts
          : 0;

  if (bound_parts > 0) {
    step->for_each = true;
    // Lossless only when every ref keypart was staged. A temp-table-driven
    // probe that dropped trailing keyparts (above) stages a wider superset the
    // WHERE re-check trims; existence_only must not cap such a probe.
    step->exact_keyed_probe = (used_key_parts == ref->key_parts);
    if (child_primary && used_key_parts >= child_pk_parts) {
      // Full-primary-key point probe per source row.
      step->is_scan = false;
      return !step->table_name.empty();
    }
    // Otherwise each source row drives a bounded range probe: a primary-prefix
    // scan for partial primary keys, or a secondary-index scan for secondary
    // refs. The server groups rows by deduplicated probe key.
    step->is_scan = true;
    if (!child_primary) step->index_name = key.name;
    return !step->table_name.empty();
  }

  step->key_prefix = lineairdb_keyenc::convert_key_to_ldbformat(
      table, ref->key, ref->key_buff, first_n_keyparts_map(used_key_parts));
  if (step->key_prefix.empty()) {
    if (reason != nullptr) *reason = "failed to encode constant key";
    return false;
  }

  if (child_primary && used_key_parts >= child_pk_parts) {
    step->is_scan = false;
  } else {
    step->is_scan = true;
    step->end_key_prefix =
        lineairdb_keyenc::build_prefix_range_end(step->key_prefix);
    if (!child_primary) step->index_name = key.name;
  }

  return !step->table_name.empty();
}

bool compile_leaf(AccessPath *leaf,
                  const std::unordered_map<TABLE *, int> &table_steps,
                  LineairDBProxy::ReadPlanStep *step, std::string *reason) {
  TABLE *table = nullptr;
  Index_lookup *ref = nullptr;
  bool full_scan = false;
  int full_scan_index = -1;
  if (!qep_leaf_info(leaf, &table, &ref, &full_scan, &full_scan_index)) {
    if (reason != nullptr) *reason = "unsupported QEP leaf";
    return false;
  }
  if (table == nullptr || table->s == nullptr) {
    if (reason != nullptr) *reason = "missing leaf table";
    return false;
  }
  if (table->s->tmp_table != NO_TMP_TABLE) {
    if (reason != nullptr) *reason = "temporary-table leaf";
    return false;
  }
  if (leaf->type == AccessPath::REF_OR_NULL) {
    if (reason != nullptr) *reason = "REF_OR_NULL null companion lookup";
    return false;
  }

  if (leaf->type == AccessPath::INDEX_RANGE_SCAN) {
    return compile_index_range_scan(leaf, table, step, reason);
  }
  if (full_scan) {
    // Plain SELECT primary scans consume the staged ["", sentinel) range.
    const THD *leaf_thd = table->in_use;
    const bool plain_select = leaf_thd != nullptr && leaf_thd->lex != nullptr &&
                              leaf_thd->lex->sql_command == SQLCOM_SELECT &&
                              table->reginfo.lock_type <= TL_READ;
    if (!plain_select) {
      if (reason != nullptr) *reason = "full table/index scan unsupported";
      return false;
    }
    step->table_name = physical_table_key(table);
    if (step->table_name.empty()) {
      if (reason != nullptr) *reason = "missing leaf table name";
      return false;
    }
    const bool primary_order =
        full_scan_index < 0 ||
        (table->s->primary_key != MAX_KEY &&
         full_scan_index == static_cast<int>(table->s->primary_key));
    step->is_scan = true;
    step->key_prefix.clear();
    step->end_key_prefix = lineairdb_keyenc::scan_end_sentinel();
    if (!primary_order) {
      // Full secondary INDEX_SCAN: stage the secondary range itself. Runtime
      // index_first/index_next consumes it, then base rows come from row cache.
      step->index_name = table->key_info[full_scan_index].name;
    }
    return true;
  }
  if (ref == nullptr) {
    if (reason != nullptr) *reason = "table access without ref or range bound";
    return false;
  }
  return compile_ref_lookup(table, ref, table_steps, step, reason);
}

// Translate one resolved handler IndexSearchPlan (point/prefix/range/
// index-first) into a single ReadPlanStep; reject reverse/unbounded access.
bool compile_index_search(TABLE *table, uint index,
                          const IndexSearchPlan &search,
                          LineairDBProxy::ReadPlanStep *step,
                          std::string *reason) {
  if (table == nullptr || table->s == nullptr || step == nullptr) {
    if (reason != nullptr) *reason = "invalid handler search metadata";
    return false;
  }
  if (index >= table->s->keys) {
    if (reason != nullptr) *reason = "invalid handler index";
    return false;
  }

  const bool is_primary = index == table->s->primary_key;
  if (search.is_primary != is_primary) {
    if (reason != nullptr) *reason = "handler primary-index mismatch";
    return false;
  }

  step->table_name = physical_table_key(table);
  if (step->table_name.empty()) {
    if (reason != nullptr) *reason = "missing handler table name";
    return false;
  }

  const auto set_scan = [&](const std::string &start,
                            const std::string &end) {
    step->is_scan = true;
    step->key_prefix = start;
    step->end_key_prefix =
        end.empty() ? lineairdb_keyenc::scan_end_sentinel() : end;
    if (!is_primary) step->index_name = table->key_info[index].name;
  };

  switch (search.op) {
    case IndexSearchOp::kUniquePoint:
      if (search.start_key_serialized.empty()) {
        if (reason != nullptr) *reason = "missing handler point key";
        return false;
      }
      if (is_primary) {
        step->is_scan = false;
        step->key_prefix = search.start_key_serialized;
      } else {
        set_scan(search.start_key_serialized,
                 lineairdb_keyenc::build_prefix_range_end(
                     search.start_key_serialized));
      }
      return true;

    case IndexSearchOp::kSameKeyMaterialize:
    case IndexSearchOp::kPrefixFirst:
      if (search.same_group_prefix_serialized.empty()) {
        if (reason != nullptr) *reason = "missing handler prefix bound";
        return false;
      }
      set_scan(search.same_group_prefix_serialized,
               search.same_group_end_serialized);
      return true;

    case IndexSearchOp::kRangeMaterialize: {
      if (search.start_key_serialized.empty()) {
        if (reason != nullptr) *reason = "missing handler range start";
        return false;
      }
      std::string start = search.start_key_serialized;
      if (search.find_flag == HA_READ_AFTER_KEY) start.push_back('\0');
      set_scan(start, search.end_key_serialized);
      return true;
    }

    case IndexSearchOp::kIndexFirst:
      if (search.end_key_serialized.empty()) {
        if (reason != nullptr) {
          *reason = "unbounded index-first/full scan unsupported";
        }
        return false;
      }
      set_scan("", search.end_key_serialized);
      return true;

    case IndexSearchOp::kPrevKey:
    case IndexSearchOp::kPrefixLast:
      if (reason != nullptr) *reason = "reverse handler access unsupported";
      return false;
  }

  if (reason != nullptr) *reason = "unsupported handler access";
  return false;
}

/**
 * @brief Compile one AccessPath tree into staged read-plan steps.
 *
 * @details The main statement tree and optional inner subquery trees share
 * `table_steps`, so correlated inner probes can bind to earlier outer steps.
 * `allow_limit_pushdown` is kept off for optional inner roots. `eq_edges`
 * collects FIELD-to-keypart edges used by semijoin planning.
 *
 * @note Failures are returned through `unsupported` instead of raising
 * immediately; `added_tables` lets optional callers roll back only this tree's
 * additions.
 */
bool compile_tree_leaves(
    THD *thd, AccessPath *root, bool allow_limit_pushdown,
    std::unordered_map<TABLE *, int> *table_steps,
    std::vector<LineairDBProxy::ReadPlanStep> *steps,
    std::vector<TABLE *> *added_tables,
    std::vector<std::pair<Field *, Field *>> *eq_edges,
    UnsupportedQep *unsupported) {
  std::vector<AccessPath *> leaves;
  bool ok = true;
  collect_qep_leaves(root, &leaves, &ok, unsupported);
  if (!ok) {
    return false;
  }
  if (leaves.empty()) {
    unsupported->type = root->type;
    unsupported->reason = "QEP has no table leaves";
    return false;
  }

  for (AccessPath *leaf : leaves) {
    TABLE *table = nullptr;
    Index_lookup *ref = nullptr;
    bool full_scan = false;
    int full_scan_index = -1;
    if (!qep_leaf_info(leaf, &table, &ref, &full_scan, &full_scan_index) ||
        table == nullptr) {
      unsupported->type = leaf->type;
      unsupported->reason = "unsupported QEP leaf";
      return false;
    }
    // Join edge for semijoin planning: source field -> probed keypart field.
    if (eq_edges != nullptr && ref != nullptr && ref->items != nullptr &&
        table->s != nullptr && ref->key >= 0 &&
        ref->key < static_cast<int>(table->s->keys)) {
      for (uint kp = 0; kp < ref->key_parts; ++kp) {
        Item *val = ref->items[kp];
        if (val == nullptr) continue;
        val = val->real_item();
        if (val->type() != Item::FIELD_ITEM) continue;
        Field *sf = down_cast<Item_field *>(val)->field;
        Field *tf = (kp < table->key_info[ref->key].user_defined_key_parts)
                        ? table->key_info[ref->key].key_part[kp].field
                        : nullptr;
        if (sf != nullptr && tf != nullptr) eq_edges->emplace_back(sf, tf);
      }
    }
    if (table->s != nullptr && table->s->tmp_table != NO_TMP_TABLE) {
      // Local MySQL temp tables are not stored in LineairDB, so there is
      // nothing to prefetch for this leaf.
      continue;
    }
    if (table_steps->find(table) != table_steps->end()) {
      unsupported->type = leaf->type;
      unsupported->reason = "duplicate QEP table leaf";
      return false;
    }

    LineairDBProxy::ReadPlanStep step;
    std::string reason;
    if (!compile_leaf(leaf, *table_steps, &step, &reason)) {
      unsupported->type = leaf->type;
      unsupported->reason = reason;
      return false;
    }

    // Push LIMIT into a single ASC REF scan only when LIMIT sits directly
    // above this leaf. count_all_rows and reject_multiple_rows both read past
    // LIMIT, so they cannot use a truncated staged entry.
    if (allow_limit_pushdown && root->type == AccessPath::LIMIT_OFFSET &&
        root->limit_offset().offset == 0 &&
        !root->limit_offset().count_all_rows &&
        !root->limit_offset().reject_multiple_rows &&
        root->limit_offset().child == leaf && leaves.size() == 1 &&
        leaf->type == AccessPath::REF && ref != nullptr && ref->key >= 0 &&
        table->s != nullptr && ref->key < static_cast<int>(table->s->keys) &&
        step.is_scan && !step.for_each && step.index_name.empty() &&
        step.scan_limit == 0) {
      const RangeScanLimit limit = range_scan_limit_for_order(
          thd, &table->key_info[ref->key], ref->key_parts,
          /*has_mysql_only_filter=*/false);
      if (limit.row_limit > 0 && !limit.reverse_scan) {
        step.scan_limit = static_cast<uint64_t>(limit.row_limit);
        step.reverse_scan = false;
      }
    }

    (*table_steps)[table] = static_cast<int>(steps->size());
    added_tables->push_back(table);
    steps->push_back(std::move(step));
  }

  return true;
}

/**
 * @brief Return true when semijoin membership reduction is result-preserving.
 *
 * @details The reduction is limited to plain inner joins. Outer joins and
 * semi/anti nests can need rows that fail the membership test and stay
 * rejected. Nested query blocks are allowed because the caller pairs source
 * and probe within a single query block.
 */
bool semijoin_safe_leaf(const TABLE *table) {
  if (table == nullptr) return false;
  const Table_ref *ref = table->pos_in_table_list;
  if (ref == nullptr) return false;
  if (ref->is_inner_table_of_outer_join()) return false;
  for (const Table_ref *embedding = ref->embedding; embedding != nullptr;
       embedding = embedding->embedding) {
    if (embedding->is_sj_or_aj_nest()) return false;
  }
  // A leaf in a nested query block is allowed: the reduction loop pairs source
  // and probe only within one query block, so membership stays a same-block
  // inner-join reachability set that cannot drop a needed row.
  if (ref->query_block == nullptr) return false;
  return true;
}

/**
 * @brief Check whether two join key fields can be compared as stored bytes.
 */
bool semijoin_keys_compatible(const Field *source, const Field *probe) {
  if (source == nullptr || probe == nullptr) return false;
  if (source->is_nullable() || probe->is_nullable()) return false;
  if (source->type() != probe->type()) return false;
  if (source->pack_length() != probe->pack_length()) return false;
  if (source->result_type() == STRING_RESULT &&
      source->charset() != probe->charset()) {
    return false;
  }
  return true;
}

/**
 * @brief Collect plan roots that hang off Item-held subqueries.
 *
 * @details The main AccessPath tree does not cover every read MySQL may run.
 * IN and correlated subqueries can live as separate Query_expressions under
 * Item conditions, so collect those roots and stage them separately.
 */
void collect_inner_unit_roots(Query_expression *unit,
                              std::vector<AccessPath *> *roots) {
  if (unit == nullptr) return;
  for (Query_block *qb = unit->first_query_block(); qb != nullptr;
       qb = qb->next_query_block()) {
    for (Query_expression *inner = qb->first_inner_query_expression();
         inner != nullptr; inner = inner->next_query_expression()) {
      AccessPath *root = inner->root_access_path();
      if (root == nullptr) {
        Query_block *inner_block = inner->first_query_block();
        if (inner_block != nullptr && inner_block->join != nullptr) {
          root = inner_block->join->root_access_path();
        }
      }
      if (root != nullptr) roots->push_back(root);
      collect_inner_unit_roots(inner, roots);
    }
  }
}

// Collect tables that are the bare, residual-free inner of an anti-join. Only
// such an inner is safe to cap at one row: a filter above the leaf could reject
// the first match while a later row qualifies. qep_leaf_info() is that test.
static void collect_existence_only_antijoin_inners(
    AccessPath *p, std::unordered_set<const TABLE *> *out) {
  if (p == nullptr) return;
  auto mark_if_bare = [&](AccessPath *inner) {
    TABLE *t = nullptr;
    Index_lookup *ref = nullptr;
    bool fs = false;
    int fsi = -1;
    if (inner != nullptr && qep_leaf_info(inner, &t, &ref, &fs, &fsi) &&
        t != nullptr) {
      out->insert(t);
    }
  };
  switch (p->type) {
    case AccessPath::NESTED_LOOP_JOIN:
      if (p->nested_loop_join().join_type == JoinType::ANTI)
        mark_if_bare(p->nested_loop_join().inner);
      collect_existence_only_antijoin_inners(p->nested_loop_join().outer, out);
      collect_existence_only_antijoin_inners(p->nested_loop_join().inner, out);
      return;
    case AccessPath::BKA_JOIN:
      if (p->bka_join().join_type == JoinType::ANTI)
        mark_if_bare(p->bka_join().inner);
      collect_existence_only_antijoin_inners(p->bka_join().outer, out);
      collect_existence_only_antijoin_inners(p->bka_join().inner, out);
      return;
    case AccessPath::HASH_JOIN:
      // A hash anti-join probes its build side as a whole, not once per outer
      // row; "one row per probe" does not apply -- just recurse.
      collect_existence_only_antijoin_inners(p->hash_join().outer, out);
      collect_existence_only_antijoin_inners(p->hash_join().inner, out);
      return;
    case AccessPath::FILTER:
      collect_existence_only_antijoin_inners(p->filter().child, out);
      return;
    case AccessPath::SORT:
      collect_existence_only_antijoin_inners(p->sort().child, out);
      return;
    case AccessPath::LIMIT_OFFSET:
      collect_existence_only_antijoin_inners(p->limit_offset().child, out);
      return;
    case AccessPath::AGGREGATE:
      collect_existence_only_antijoin_inners(p->aggregate().child, out);
      return;
    case AccessPath::STREAM:
      collect_existence_only_antijoin_inners(p->stream().child, out);
      return;
    case AccessPath::TEMPTABLE_AGGREGATE:
      collect_existence_only_antijoin_inners(
          p->temptable_aggregate().subquery_path, out);
      collect_existence_only_antijoin_inners(
          p->temptable_aggregate().table_path, out);
      return;
    case AccessPath::WEEDOUT:
      collect_existence_only_antijoin_inners(p->weedout().child, out);
      return;
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      collect_existence_only_antijoin_inners(
          p->nested_loop_semijoin_with_duplicate_removal().outer, out);
      collect_existence_only_antijoin_inners(
          p->nested_loop_semijoin_with_duplicate_removal().inner, out);
      return;
    default:
      return;
  }
}

}  // namespace

bool autogen_read_plan_from_qep(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::vector<LineairDBProxy::ReadPlanStep> *out,
    bool include_inner_units) {
  if (out == nullptr) {
    return raise_unsupported(thd, "NONE", "null output vector");
  }
  out->clear();

  if (root == nullptr) {
    return raise_unsupported(thd, "NONE", "missing JOIN root_access_path");
  }

  std::unordered_map<TABLE *, int> table_steps;
  std::vector<LineairDBProxy::ReadPlanStep> steps;
  std::vector<TABLE *> added_tables;
  UnsupportedQep unsupported;

  std::vector<std::pair<Field *, Field *>> eq_edges;
  if (!compile_tree_leaves(thd, root, /*allow_limit_pushdown=*/true,
                           &table_steps, &steps,
                           &added_tables, &eq_edges, &unsupported)) {
    return raise_unsupported(thd, unsupported.type, unsupported.reason);
  }

  if (include_inner_units && thd != nullptr && thd->lex != nullptr) {
    std::vector<AccessPath *> inner_roots;
    collect_inner_unit_roots(thd->lex->unit, &inner_roots);
    for (AccessPath *inner_root : inner_roots) {
      if (inner_root == root) continue;

      // Optional inner tree: keep the outer plan if this subquery shape is not
      // stageable, but remove any partial steps from the failed attempt.
      const size_t step_mark = steps.size();
      const size_t table_mark = added_tables.size();
      UnsupportedQep inner_unsupported;
      const size_t edge_mark = eq_edges.size();
      if (!compile_tree_leaves(thd, inner_root,
                               /*allow_limit_pushdown=*/false, &table_steps,
                               &steps, &added_tables, &eq_edges,
                               &inner_unsupported)) {
        steps.resize(step_mark);
        eq_edges.resize(edge_mark);
        for (size_t i = table_mark; i < added_tables.size(); ++i) {
          table_steps.erase(added_tables[i]);
        }
        added_tables.resize(table_mark);
      }
    }
  }

  if (steps.empty()) {
    return raise_unsupported(thd, root->type, "QEP has no stageable leaves");
  }

  // SharedScan dedup: fold byte-identical staged steps into one -- (a)
  // self-contained scans (a view read twice) and (b) for_each probes with
  // deep-equal bindings (a self-join or correlated subquery). Keep the
  // earliest, record the folded aliases (the filter/semijoin passes require
  // agreement), and remap later steps' source_step (like execute_read_plan).
  std::vector<std::vector<TABLE *>> step_aliases(steps.size());
  for (size_t i = 0; i < steps.size() && i < added_tables.size(); ++i) {
    if (added_tables[i] != nullptr) step_aliases[i].push_back(added_tables[i]);
  }
  {
    const auto foldable = [](const LineairDBProxy::ReadPlanStep &s) {
      return s.is_scan && s.scan_limit == 0 &&
             s.aggregate_serialized.empty() && s.semijoins.empty();
    };
    const auto same_binding = [](const LineairDBProxy::ReadPlanKeyBinding &a,
                                 const LineairDBProxy::ReadPlanKeyBinding &b) {
      return a.source_step == b.source_step && a.source_row == b.source_row &&
             a.source_offset == b.source_offset &&
             a.source_length == b.source_length &&
             a.use_midpoint == b.use_midpoint && a.from_key == b.from_key &&
             a.source_column == b.source_column &&
             a.column_as_int_key == b.column_as_int_key &&
             a.int_delta == b.int_delta;
    };
    const auto same_bindings =
        [&](const std::vector<LineairDBProxy::ReadPlanKeyBinding> &a,
            const std::vector<LineairDBProxy::ReadPlanKeyBinding> &b) {
          if (a.size() != b.size()) return false;
          for (size_t k = 0; k < a.size(); ++k)
            if (!same_binding(a[k], b[k])) return false;
          return true;
        };
    const auto same_step = [&](const LineairDBProxy::ReadPlanStep &a,
                               const LineairDBProxy::ReadPlanStep &b) {
      return a.table_name == b.table_name && a.index_name == b.index_name &&
             a.key_prefix == b.key_prefix &&
             a.end_key_prefix == b.end_key_prefix &&
             a.for_each == b.for_each && a.reverse_scan == b.reverse_scan &&
             a.serialized_filter == b.serialized_filter &&
             same_bindings(a.bindings, b.bindings) &&
             same_bindings(a.end_bindings, b.end_bindings);
    };
    std::vector<uint32_t> new_index(steps.size(), 0);
    std::vector<LineairDBProxy::ReadPlanStep> folded;
    std::vector<std::vector<TABLE *>> folded_aliases;
    folded.reserve(steps.size());
    folded_aliases.reserve(steps.size());
    bool any_fold = false;
    for (size_t j = 0; j < steps.size(); ++j) {
      int target = -1;
      if (foldable(steps[j])) {
        for (size_t i = 0; i < folded.size(); ++i) {
          if (foldable(folded[i]) && same_step(folded[i], steps[j])) {
            target = static_cast<int>(i);
            break;
          }
        }
      }
      if (target >= 0) {
        new_index[j] = static_cast<uint32_t>(target);
        for (TABLE *t : step_aliases[j]) folded_aliases[target].push_back(t);
        // Two probes can fold to the same shape yet differ in exactness (one
        // dropped a keypart). Keep the folded group exact only when every member
        // was; otherwise the cap could hit a widened probe.
        folded[target].exact_keyed_probe =
            folded[target].exact_keyed_probe && steps[j].exact_keyed_probe;
        any_fold = true;
      } else {
        new_index[j] = static_cast<uint32_t>(folded.size());
        folded.push_back(std::move(steps[j]));
        folded_aliases.push_back(std::move(step_aliases[j]));
      }
    }
    steps = std::move(folded);
    step_aliases = std::move(folded_aliases);
    if (any_fold) {
      for (auto &s : steps) {
        for (auto &b : s.bindings) b.source_step = new_index[b.source_step];
        for (auto &b : s.end_bindings)
          b.source_step = new_index[b.source_step];
        for (auto &sj : s.semijoins)
          sj.source_step = new_index[sj.source_step];
      }
      for (auto &kv : table_steps) {
        if (kv.second >= 0 && kv.second < static_cast<int>(new_index.size()))
          kv.second = static_cast<int>(new_index[kv.second]);
      }
    }
  }

  LineairDBTransaction *tx = nullptr;
  const auto find_tx = [&]() -> LineairDBTransaction * {
    if (tx != nullptr) return tx;
    for (const auto &aliases : step_aliases) {
      for (TABLE *t : aliases) {
        if (t == nullptr || t->file == nullptr || t->file->ht != lineairdb_hton)
          continue;
        tx = down_cast<ha_lineairdb *>(t->file)->tx_for_autogen();
        return tx;
      }
    }
    return nullptr;
  };

  // q18 GroupedSummary: if the handler registered this full-scan leaf for
  // synthetic serving, remove its base scan from staging and mark the TABLE*.
  // Guard on a live GS registration: otherwise this pass must not touch `steps`
  // (it moves every step into `kept` and only commits `kept` back when a drop
  // happened — running it for an unregistered query would leave `steps`
  // moved-from and corrupt the plan).
  if (allow_filter_pushdown) {
    LineairDBTransaction *qtx = find_tx();
    if (qtx != nullptr && qtx->has_gs_registrations()) {
      std::vector<bool> referenced(steps.size(), false);
      for (const auto &st : steps) {
        for (const auto &b : st.bindings)
          if (b.source_step < referenced.size())
            referenced[b.source_step] = true;
        for (const auto &b : st.end_bindings)
          if (b.source_step < referenced.size())
            referenced[b.source_step] = true;
        for (const auto &sj : st.semijoins)
          if (sj.source_step < referenced.size())
            referenced[sj.source_step] = true;
      }

      std::vector<uint32_t> new_index(steps.size(), 0);
      std::vector<bool> dropped(steps.size(), false);
      std::vector<LineairDBProxy::ReadPlanStep> kept;
      std::vector<std::vector<TABLE *>> kept_aliases;
      kept.reserve(steps.size());
      kept_aliases.reserve(step_aliases.size());
      bool any_drop = false;
      for (size_t i = 0; i < steps.size(); ++i) {
        bool drop = steps[i].is_scan && !steps[i].for_each &&
                    steps[i].key_prefix.empty() &&
                    steps[i].end_key_prefix ==
                        lineairdb_keyenc::scan_end_sentinel() &&
                    steps[i].serialized_filter.empty() &&
                    steps[i].scan_limit == 0 && !referenced[i] &&
                    i < step_aliases.size() && !step_aliases[i].empty() &&
                    steps[i].semijoins.empty();
        if (drop) {
          for (TABLE *at : step_aliases[i]) {
            if (at == nullptr || qtx->gs_registration(at) == nullptr) {
              drop = false;
              break;
            }
          }
        }
        if (drop) {
          for (TABLE *at : step_aliases[i]) qtx->mark_gs_skipped(at);
          dropped[i] = true;
          any_drop = true;
          continue;
        }
        new_index[i] = static_cast<uint32_t>(kept.size());
        kept.push_back(std::move(steps[i]));
        kept_aliases.push_back(std::move(step_aliases[i]));
      }
      // Always commit `kept` (a faithful move of the surviving steps): the loop
      // moved every step into it, so leaving `steps` here would be moved-from.
      // The index rebind only matters when a drop actually shifted ordinals.
      steps = std::move(kept);
      step_aliases = std::move(kept_aliases);
      if (any_drop) {
        for (auto &st : steps) {
          for (auto &b : st.bindings)
            if (b.source_step < new_index.size())
              b.source_step = new_index[b.source_step];
          for (auto &b : st.end_bindings)
            if (b.source_step < new_index.size())
              b.source_step = new_index[b.source_step];
          for (auto &sj : st.semijoins)
            if (sj.source_step < new_index.size())
              sj.source_step = new_index[sj.source_step];
        }
        for (auto it = table_steps.begin(); it != table_steps.end();) {
          const int idx = it->second;
          if (idx >= 0 && idx < static_cast<int>(dropped.size()) &&
              dropped[idx]) {
            it = table_steps.erase(it);
          } else {
            if (idx >= 0 && idx < static_cast<int>(new_index.size()))
              it->second = static_cast<int>(new_index[idx]);
            ++it;
          }
        }
      }
    }
  }

  // Attach scan filters once the plan is known. A rejected key caches as a
  // table-level not-found entry shared by every step on the table, so skip a
  // multi-step table; on a folded step attach only when EVERY alias builds the
  // SAME predicate (then a dropped row is one each alias's own WHERE discards).
  if (allow_filter_pushdown) {
    std::unordered_map<std::string, int> table_step_count;
    for (const auto &s : steps) table_step_count[s.table_name]++;
    for (size_t i = 0; i < steps.size(); ++i) {
      auto &s = steps[i];
      if (!s.is_scan) continue;
      if (table_step_count[s.table_name] != 1) continue;
      const std::vector<TABLE *> &aliases = step_aliases[i];
      if (aliases.empty()) continue;
      std::string table_filter;
      bool agree = true;
      for (size_t a = 0; a < aliases.size(); ++a) {
        std::string f;
        if (aliases[a] == nullptr ||
            !build_single_table_filter(thd, aliases[a], &f) || f.empty()) {
          agree = false;
          break;
        }
        if (a == 0) {
          table_filter = std::move(f);
        } else if (f != table_filter) {
          agree = false;
          break;
        }
      }
      if (agree) s.serialized_filter = std::move(table_filter);
    }
  }

  // Semijoin reduction: for high-fanout probes, use an earlier filtered
  // source step as a membership set and skip probe rows that cannot join.
  if (allow_filter_pushdown && !eq_edges.empty()) {
    // Build join-key equivalence classes from FIELD=keypart edges.
    std::unordered_map<Field *, Field *> uf;
    std::function<Field *(Field *)> find_root = [&](Field *x) -> Field * {
      auto it = uf.find(x);
      if (it == uf.end()) { uf[x] = x; return x; }
      if (it->second == x) return x;
      Field *r = find_root(it->second);
      uf[x] = r;
      return r;
    };
    for (auto &e : eq_edges) uf[find_root(e.first)] = find_root(e.second);

    // Per-alias chooser: a qualifying membership source for probe_t's step, or
    // false if none. Candidates are earlier steps in the same query block.
    const auto choose_semijoin =
        [&](TABLE *probe_t, size_t probe_step,
            LineairDBProxy::ReadPlanStep::Semijoin *out_sj) -> bool {
      auto &ps = steps[probe_step];
      if (!semijoin_safe_leaf(probe_t)) return false;
      if (probe_t->s == nullptr) return false;

      // Find the field whose value each probe row will join on.
      Field *probe_field = nullptr;
      if (!ps.index_name.empty()) {
        for (uint k = 0; k < probe_t->s->keys; ++k) {
          if (ps.index_name == probe_t->key_info[k].name) {
            probe_field = probe_t->key_info[k].key_part[0].field;
            break;
          }
        }
      } else if (probe_t->s->primary_key != MAX_KEY) {
        probe_field =
            probe_t->key_info[probe_t->s->primary_key].key_part[0].field;
      }
      if (probe_field == nullptr || uf.find(probe_field) == uf.end())
        return false;
      Field *cls = find_root(probe_field);

      for (auto &kvp2 : table_steps) {
        TABLE *src_t = kvp2.first;
        const int src_step = kvp2.second;
        if (src_step >= static_cast<int>(probe_step) || src_step < 0 ||
            src_step >= static_cast<int>(steps.size()))
          continue;
        if (!semijoin_safe_leaf(src_t)) continue;
        if (src_t->pos_in_table_list->query_block !=
            probe_t->pos_in_table_list->query_block)
          continue;
        if (!steps[src_step].is_scan) continue;

        // Source membership must come from rows already filtered, or from a
        // source_filter carried by the semijoin.
        std::string sf_filter;
        const bool src_prefiltered =
            steps[src_step].is_scan && !steps[src_step].for_each &&
            !steps[src_step].serialized_filter.empty();
        if (!src_prefiltered &&
            (!build_single_table_filter(thd, src_t, &sf_filter) ||
             sf_filter.empty()))
          continue;  // no selective predicate -> not a useful source
        if (src_t->s == nullptr || src_t->s->primary_key == MAX_KEY) continue;
        Field *src_pk =
            src_t->key_info[src_t->s->primary_key].key_part[0].field;
        if (uf.find(src_pk) == uf.end() || find_root(src_pk) != cls) continue;
        if (!semijoin_keys_compatible(src_pk, probe_field)) continue;

        const int sc = qep_table_field_index(src_t, src_pk);
        const int pc = qep_table_field_index(probe_t, probe_field);
        if (sc < 0 || pc < 0) continue;
        out_sj->source_step = static_cast<uint32_t>(src_step);
        out_sj->source_column = static_cast<uint32_t>(sc);
        out_sj->probe_column = static_cast<uint32_t>(pc);
        out_sj->source_filter = src_prefiltered ? std::string() : sf_filter;
        return true;  // one semijoin source per probe step
      }
      return false;
    };

    // A folded probe serves several aliases, so a membership reduction is sound
    // only when EVERY alias picks the SAME semijoin -- a row one alias prunes
    // must be one every alias's join would drop. Any mismatch vetoes.
    for (size_t pi = 0; pi < steps.size(); ++pi) {
      auto &ps = steps[pi];
      if (!(ps.for_each && ps.is_scan)) continue;  // FER/FES high-fanout only
      if (pi >= step_aliases.size()) continue;
      const std::vector<TABLE *> &aliases = step_aliases[pi];
      if (aliases.empty()) continue;
      LineairDBProxy::ReadPlanStep::Semijoin chosen;
      bool unanimous = true;
      for (size_t a = 0; a < aliases.size(); ++a) {
        LineairDBProxy::ReadPlanStep::Semijoin cand;
        if (aliases[a] == nullptr || !choose_semijoin(aliases[a], pi, &cand)) {
          unanimous = false;
          break;
        }
        if (a == 0) {
          chosen = cand;
        } else if (!(chosen.source_step == cand.source_step &&
                     chosen.source_column == cand.source_column &&
                     chosen.probe_column == cand.probe_column &&
                     chosen.source_filter == cand.source_filter)) {
          unanimous = false;
          break;
        }
      }
      if (unanimous) ps.semijoins.push_back(std::move(chosen));
    }
  }

  // Mark anti-join inner probes to cap at the first match. Only on the
  // no-validate read path: a capped, incomplete range cannot pass commit-time
  // validation, the same reason filter and projection pushdown gate on it.
  if (allow_filter_pushdown) {
    std::unordered_set<const TABLE *> existence_only_inners;
    collect_existence_only_antijoin_inners(root, &existence_only_inners);
    // A folded probe stands for several aliases. Cap it only when all of them
    // are residual-free anti-join inners and the probe is an exact lookup, the
    // same all-must-agree rule the filter and semijoin passes use.
    for (size_t i = 0; i < steps.size(); ++i) {
      auto &s = steps[i];
      if (!(s.for_each && s.exact_keyed_probe)) continue;
      if (i >= step_aliases.size()) continue;
      const std::vector<TABLE *> &aliases = step_aliases[i];
      if (aliases.empty()) continue;
      bool all_inner = true;
      for (TABLE *t : aliases) {
        if (t == nullptr || existence_only_inners.count(t) == 0) {
          all_inner = false;
          break;
        }
      }
      if (all_inner) s.existence_only = true;
    }
  }

  // q18 grouped-semijoin: prepend a HAVING-filtered aggregate over lineitem
  // and semijoin the plain outer orders scan against its group keys. This runs
  // after the real-QEP leaf mutation passes: the aggregate step reuses the
  // physical table name but emits group rows, so it must not participate in
  // alias/drop/table_steps decisions meant for base-table leaves.
  if (allow_filter_pushdown) {
    LineairDBTransaction *qtx = find_tx();
    if (qtx != nullptr && !qtx->grouped_semijoins().empty()) {
      const bool gdbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
      for (const auto &gs : qtx->grouped_semijoins()) {
        int outer_idx = -1;
        for (size_t i = 0; i < steps.size(); ++i) {
          const auto &s = steps[i];
          if (s.is_scan && !s.for_each && s.table_name == gs.outer_table_key &&
              s.aggregate_serialized.empty() && s.index_name.empty() &&
              s.key_prefix.empty() &&
              s.end_key_prefix == lineairdb_keyenc::scan_end_sentinel() &&
              s.scan_limit == 0 && s.semijoins.empty()) {
            outer_idx = static_cast<int>(i);
            break;
          }
        }
        if (outer_idx < 0) {
          if (gdbg)
            std::fprintf(stderr, "[GSEMI] no plain outer scan for %s\n",
                         gs.outer_table_key.c_str());
          continue;
        }

        LineairDBProxy::ReadPlanStep agg;
        agg.table_name = gs.inner_table_key;
        agg.is_scan = true;
        agg.for_each = false;
        agg.end_key_prefix = lineairdb_keyenc::scan_end_sentinel();
        agg.aggregate_serialized = gs.agg_spec;
        agg.serialized_filter = gs.having_filter;

        steps.insert(steps.begin(), std::move(agg));
        step_aliases.insert(step_aliases.begin(), {});
        for (auto &s : steps) {
          for (auto &b : s.bindings) b.source_step += 1;
          for (auto &b : s.end_bindings) b.source_step += 1;
          for (auto &sj : s.semijoins) sj.source_step += 1;
        }
        for (auto &kv : table_steps) {
          if (kv.second >= 0) kv.second += 1;
        }

        LineairDBProxy::ReadPlanStep::Semijoin sj;
        sj.source_step = 0;
        sj.source_column = 0;
        sj.probe_column = gs.outer_probe_column;
        steps[outer_idx + 1].semijoins.push_back(std::move(sj));
        if (gdbg)
          std::fprintf(stderr,
                       "[GSEMI] agg step for %s + semijoin on %s step=%d\n",
                       gs.inner_table_key.c_str(), gs.outer_table_key.c_str(),
                       outer_idx + 1);
      }
    }
  }

  *out = std::move(steps);
  return true;
}

void plan_projection_pushdown(
    THD *thd, std::vector<LineairDBProxy::ReadPlanStep> *steps,
    std::unordered_map<std::string, std::vector<uint32_t>> *kept_out) {
  kept_out->clear();
  if (thd == nullptr || thd->lex == nullptr || steps == nullptr) return;

  std::unordered_map<std::string, std::vector<bool>> union_rs;
  std::unordered_map<std::string, uint32_t> fields_of;
  std::unordered_set<std::string> gcol_tables;
  const auto source_is_aggregate = [&](uint32_t source_step) {
    return source_step < steps->size() &&
           !(*steps)[source_step].aggregate_serialized.empty();
  };

  // Merge read_set by physical table; one projection layout is shared by all
  // aliases that read the same LineairDB table.
  for (auto *tl = thd->lex->query_tables; tl != nullptr; tl = tl->next_global) {
    if (tl->table == nullptr || tl->table->s == nullptr) continue;
    TABLE *t = tl->table;
    const std::string n = physical_table_key(t);
    const uint32_t nf = t->s->fields;
    fields_of[n] = nf;
    auto &u = union_rs[n];
    if (u.size() < nf) u.resize(nf, false);
    for (uint f = 0; f < nf; ++f) {
      if (t->field[f]->is_gcol()) gcol_tables.insert(n);
      if (bitmap_is_set(t->read_set, f)) u[f] = true;
    }
  }

  // Keep columns needed by value bindings. Column-form bindings can be
  // remapped to the trimmed layout; byte-slice bindings need the source row
  // to stay full because their offsets are not column-aware.
  std::unordered_set<std::string> unsafe_src;
  const auto force_binding_col =
      [&](const LineairDBProxy::ReadPlanKeyBinding &b) {
        if (b.from_key || b.source_step >= steps->size()) return;
        if (source_is_aggregate(b.source_step)) return;
        const std::string &src = (*steps)[b.source_step].table_name;
        auto fo = fields_of.find(src);
        if (fo == fields_of.end()) {
          unsafe_src.insert(src);
          return;
        }
        if (b.source_column > 0) {
          const uint32_t fi = static_cast<uint32_t>(b.source_column - 1);
          if (fi >= fo->second) {
            unsafe_src.insert(src);
            return;
          }
          auto &u = union_rs[src];
          if (u.size() < fo->second) u.resize(fo->second, false);
          u[fi] = true;  // keep the binding's source column through the trim
        } else {
          unsafe_src.insert(src);  // byte-slice form: not remappable
        }
      };
  for (const auto &s : *steps) {
    for (const auto &b : s.bindings) force_binding_col(b);
    for (const auto &b : s.end_bindings) force_binding_col(b);
  }

  // Semijoin source rows must keep the column used for membership. A
  // source_filter uses full-row ordinals, so that source ships full rows.
  for (const auto &s : *steps) {
    for (const auto &sj : s.semijoins) {
      if (sj.source_step >= steps->size()) continue;
      if (source_is_aggregate(sj.source_step)) continue;
      const std::string &src = (*steps)[sj.source_step].table_name;
      auto fo = fields_of.find(src);
      if (fo == fields_of.end()) continue;
      if (!sj.source_filter.empty()) {
        unsafe_src.insert(src);  // full-row filter indices -> ship full
      } else if (sj.source_column < fo->second) {
        auto &u = union_rs[src];
        if (u.size() < fo->second) u.resize(fo->second, false);
        u[sj.source_column] = true;
      }
    }
  }

  // Pick projected tables and build full ordinal -> projected position maps.
  // Positions are 1-based because source_column==0 means byte-slice binding.
  std::unordered_map<std::string, std::vector<uint32_t>> full_to_proj;
  for (auto &kv : union_rs) {
    const std::string &n = kv.first;
    if (gcol_tables.count(n) != 0 || unsafe_src.count(n) != 0) continue;
    const uint32_t nf = fields_of[n];
    std::vector<uint32_t> kept;
    for (uint32_t f = 0; f < nf; ++f) {
      if (kv.second[f]) kept.push_back(f);
    }
    if (kept.empty() || kept.size() == nf) continue;  // no benefit
    std::vector<uint32_t> f2p(nf, 0);
    for (uint32_t k = 0; k < kept.size(); ++k) f2p[kept[k]] = k + 1;
    full_to_proj.emplace(n, std::move(f2p));
    kept_out->emplace(n, std::move(kept));
  }

  // Remap column-form bindings from original column ordinal to projected
  // ordinal, matching the row layout the server will actually ship.
  const auto remap_binding = [&](LineairDBProxy::ReadPlanKeyBinding &b) {
    if (b.from_key || b.source_column <= 0 || b.source_step >= steps->size())
      return;
    if (source_is_aggregate(b.source_step)) return;
    auto it = full_to_proj.find((*steps)[b.source_step].table_name);
    if (it == full_to_proj.end()) return;  // source ships full rows
    const uint32_t fi = static_cast<uint32_t>(b.source_column - 1);
    const uint32_t pos = (fi < it->second.size()) ? it->second[fi] : 0;
    if (pos > 0) b.source_column = static_cast<int32_t>(pos);
  };
  for (auto &s : *steps) {
    for (auto &b : s.bindings) remap_binding(b);
    for (auto &b : s.end_bindings) remap_binding(b);
  }

  // Remap each semijoin's source_column to its projected position when the
  // source step is projected (pre-filtered branch; force-kept above, so the
  // packed position is nonzero). Full-shipped sources keep full ordinals.
  for (auto &s : *steps) {
    for (auto &sj : s.semijoins) {
      if (sj.source_step >= steps->size()) continue;
      if (source_is_aggregate(sj.source_step)) continue;
      auto it = full_to_proj.find((*steps)[sj.source_step].table_name);
      if (it == full_to_proj.end()) continue;
      const uint32_t fi = sj.source_column;
      const uint32_t pos = (fi < it->second.size()) ? it->second[fi] : 0;
      if (pos > 0) sj.source_column = pos - 1;  // 1-based f2p -> 0-based packed
    }
  }

  for (auto &s : *steps) {
    if (!s.aggregate_serialized.empty()) continue;
    auto it = kept_out->find(s.table_name);
    if (it == kept_out->end()) continue;
    s.projection = it->second;
    s.projection_num_columns = fields_of[s.table_name];
  }
}

// Produce a one-step prefetch plan from the handler access, raising
// ER_NOT_SUPPORTED on an unsupported shape.
bool autogen_read_plan_from_index_search(
    THD *thd, TABLE *table, uint index, const IndexSearchPlan &search,
    std::vector<LineairDBProxy::ReadPlanStep> *out) {
  if (out == nullptr) {
    return raise_unsupported(thd, "HANDLER", "null output vector");
  }
  out->clear();

  LineairDBProxy::ReadPlanStep step;
  std::string reason;
  if (!compile_index_search(table, index, search, &step, &reason)) {
    return raise_unsupported(thd, "HANDLER", reason);
  }

  out->push_back(std::move(step));
  return true;
}
