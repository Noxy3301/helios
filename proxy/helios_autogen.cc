#include "helios_autogen.hh"


#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "key_pack.hh"
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
#include "storage/helios/ha_helios.hh"

extern handlerton *helios_hton;

namespace {

key_part_map first_n_keyparts_map(uint key_parts) {
  key_part_map map = 0;
  for (uint i = 0; i < key_parts; ++i) map |= (key_part_map{1} << i);
  return map;
}

bool is_int32_key_field(const Field *f) {
  return f != nullptr && f->type() == MYSQL_TYPE_LONG &&
         f->pack_length() == 4 && !f->is_unsigned();
}

// Handler table key from the TABLE share path. MySQL passes the same normalized
// path to ha_helios::open(), and the handler stores it as db_table_name.
std::string physical_table_key(const TABLE *t) {
  if (t == nullptr || t->s == nullptr) return std::string();
  const TABLE_SHARE *s = t->s;
  if (s->normalized_path.str == nullptr || s->normalized_path.length == 0)
    return std::string();
  return std::string(s->normalized_path.str, s->normalized_path.length);
}

int qep_table_field_index(TABLE *t, Field *f) {
  if (t == nullptr || t->s == nullptr || f == nullptr) return -1;
  for (uint i = 0; i < t->s->fields; ++i) {
    if (t->field[i] == f) return static_cast<int>(i);
  }
  return -1;
}

void collect_qep_leaves(AccessPath *p, std::vector<AccessPath *> *out,
                        bool *ok) {
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
    case AccessPath::UNQUALIFIED_COUNT:
      // COUNT(*) without WHERE. The node carries no table parameters (the
      // table is implicit from the JOIN); compile_tree_leaves resolves it
      // from the owning query block.
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
      if ((p->hash_join().outer != nullptr &&
           p->hash_join().outer->parameter_tables != 0) ||
          (p->hash_join().inner != nullptr &&
           p->hash_join().inner->parameter_tables != 0)) {
        *ok = false;
        return;
      }
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
    case AccessPath::STREAM:
      collect_qep_leaves(p->stream().child, out, ok);
      return;
    case AccessPath::TEMPTABLE_AGGREGATE:
      // MySQL can run GROUP BY by filling an internal temp table and reading it
      // back. Prefetch only needs the Helios reads that feed that temp table;
      // the temp-table leaves themselves are skipped in the compile loop.
      collect_qep_leaves(p->temptable_aggregate().subquery_path, out, ok);
      collect_qep_leaves(p->temptable_aggregate().table_path, out, ok);
      return;
    case AccessPath::MATERIALIZE:
      // Derived tables and materialized subqueries have the same shape: the
      // source subqueries may read Helios, but the materialized table is
      // local to MySQL.
      for (const MaterializePathParameters::QueryBlock &qb :
           p->materialize().param->query_blocks) {
        collect_qep_leaves(qb.subquery_path, out, ok);
      }
      collect_qep_leaves(p->materialize().table_path, out, ok);
      return;
    case AccessPath::DELETE_ROWS:
      collect_qep_leaves(p->delete_rows().child, out, ok);
      return;
    case AccessPath::UPDATE_ROWS:
      collect_qep_leaves(p->update_rows().child, out, ok);
      return;
    case AccessPath::WEEDOUT:
      // Semijoin duplicate weedout dedups locally via handler rowids;
      // position()/rnd_pos() re-reads hit the transaction row cache.
      collect_qep_leaves(p->weedout().child, out, ok);
      return;
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      collect_qep_leaves(p->nested_loop_semijoin_with_duplicate_removal().outer,
                         out, ok);
      collect_qep_leaves(p->nested_loop_semijoin_with_duplicate_removal().inner,
                         out, ok);
      return;
    case AccessPath::ZERO_ROWS:
      // The optimizer proved this subtree returns nothing; no rows will be
      // read at runtime, so no prefetch step is needed.
      return;
    default:
      *ok = false;
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
    HeliosProxy::ReadPlanStep *step) {
  if (target == nullptr || target->s == nullptr || ref == nullptr ||
      step == nullptr || source_field == nullptr ||
      source_field->table == nullptr || ref->key < 0 ||
      ref->key >= static_cast<int>(target->s->keys)) {
    return false;
  }

  Field *target_field = target->key_info[ref->key].key_part[keypart_idx].field;
  if (!is_int32_key_field(target_field)) {
    return false;
  }

  HeliosProxy::ReadPlanKeyBinding binding;
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
      return false;
    }
    binding.from_key = true;
  } else if (pk_pos >= 0) {
    KEY &source_key = source_table->key_info[source_pk];
    for (int i = 0; i <= pk_pos; ++i) {
      if (!is_int32_key_field(source_key.key_part[i].field)) {
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
      return false;
    }
    const int field_index = qep_table_field_index(source_table, source_field);
    if (field_index < 0) {
      return false;
    }
    binding.source_column = field_index + 1;
    binding.column_as_int_key = true;
  }

  step->bindings.push_back(std::move(binding));
  return true;
}

bool compile_index_range_scan(AccessPath *leaf, TABLE *table,
                              HeliosProxy::ReadPlanStep *step) {
  if (leaf == nullptr || table == nullptr || table->s == nullptr ||
      step == nullptr) {
    return false;
  }

  const auto &range_scan = leaf->index_range_scan();
  if (range_scan.geometry) {
    return false;
  }
  if (range_scan.index >= table->s->keys) {
    return false;
  }
  if (range_scan.num_ranges != 1) {
    return false;
  }
  if (range_scan.ranges == nullptr || range_scan.ranges[0] == nullptr) {
    return false;
  }

  QUICK_RANGE *range = range_scan.ranges[0];
  step->table_name = physical_table_key(table);
  step->is_scan = true;
  // Ask for the canonical forward, unbounded shape; MySQL applies ORDER BY /
  // LIMIT / WHERE above and the consumer requests the same shape.
  if (range_scan.index != table->s->primary_key) {
    step->index_name = table->key_info[range_scan.index].name;
  }

  if (range->flag & NO_MIN_RANGE) {
    step->key_prefix.clear();
  } else {
    step->key_prefix = key_pack::pack_key(
        table, range_scan.index, range->min_key, range->min_keypart_map);
    if (range->min_keypart_map != 0 && step->key_prefix.empty()) {
      return false;
    }
    // NEAR_MIN (exclusive lower): append one '\0' as the handler does for
    // HA_READ_AFTER_KEY. It can over-include a shared-prefix key, which the
    // WHERE re-check trims.
    if (range->flag & NEAR_MIN) step->key_prefix.push_back('\0');
  }

  if (range->flag & NO_MAX_RANGE) {
    step->end_key_prefix = key_pack::scan_end_sentinel();
  } else {
    step->end_key_prefix = key_pack::pack_key(
        table, range_scan.index, range->max_key, range->max_keypart_map);
    if (range->max_keypart_map != 0 && step->end_key_prefix.empty()) {
      return false;
    }
    if (!(range->flag & NEAR_MAX)) {
      step->end_key_prefix =
          key_pack::build_prefix_range_end(step->end_key_prefix);
    }
  }

  return !step->table_name.empty();
}

bool compile_ref_lookup(
    TABLE *table, Index_lookup *ref,
    const std::unordered_map<TABLE *, int> &table_steps,
    HeliosProxy::ReadPlanStep *step) {
  if (table == nullptr || table->s == nullptr || ref == nullptr ||
      step == nullptr) {
    return false;
  }
  if (ref->key < 0 || ref->key >= static_cast<int>(table->s->keys)) {
    return false;
  }
  if (ref->key_parts == 0 || ref->key_buff == nullptr ||
      ref->items == nullptr) {
    return false;
  }

  KEY &key = table->key_info[ref->key];
  if (ref->key_parts > key.user_defined_key_parts) {
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
      return false;
    }
    item = item->real_item();
    if (item == nullptr) {
      return false;
    }

    if (item->type() != Item::FIELD_ITEM) {
      if (!item->const_for_execution()) {
        return false;
      }
      if (saw_binding) {
        return false;
      }
      ++leading_constant_parts;
      continue;
    }

    saw_binding = true;
    Item_field *item_field = down_cast<Item_field *>(item);
    if (item_field->field == nullptr || item_field->field->table == nullptr) {
      return false;
    }
    bound_items.push_back({kp, item_field});
  }

  // A keypart bound to a real earlier step iterates directly; one bound to a
  // materialized temp table is remapped via Item_equal onto a real step,
  // reading only the leading key prefix and over-fetching the rest.
  TABLE *iter_table = nullptr;
  int iter_step = -1;

  // Read the probed table/index as a full range. Used when a temp-table-driven
  // probe has no salvageable leading prefix.
  auto use_full_range = [&]() -> bool {
    const THD *leaf_thd = table->in_use;
    const bool plain_select =
        leaf_thd != nullptr && leaf_thd->lex != nullptr &&
        leaf_thd->lex->sql_command == SQLCOM_SELECT &&
        table->reginfo.lock_type <= TL_READ;
    if (!plain_select) {
      return false;
    }
    step->bindings.clear();
    step->end_bindings.clear();
    step->for_each = false;
    step->is_scan = true;
    step->key_prefix.clear();
    step->end_key_prefix = key_pack::scan_end_sentinel();
    if (ref->key == static_cast<int>(table->s->primary_key)) {
      step->index_name.clear();
    } else {
      step->index_name = key.name;
    }
    return !step->table_name.empty();
  };

  // Resolve a bound keypart to a real earlier step: a direct source step if
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
      // Unknown source (neither a plan step nor a temp table): fail the read
      // plan as unsupported, rather than salvaging or full-ranging it.
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
      return use_full_range();
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
        if (saw_temp_source) return use_full_range();
        return false;
      }
      source_field = remapped;
    }
    if (!append_bound_keypart(table, ref, bp.kp, source_field, iter_step,
                              step)) {
      if (saw_temp_source) return use_full_range();
      return false;
    }
    ++bound_parts;
  }

  if (leading_constant_parts > 0) {
    step->key_prefix =
        key_pack::pack_key(table, ref->key, ref->key_buff,
                           first_n_keyparts_map(leading_constant_parts));
    if (step->key_prefix.empty()) {
      return false;
    }
  }

  const uint used_key_parts = leading_constant_parts + bound_parts;
  if (used_key_parts == 0) {
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

  step->key_prefix = key_pack::pack_key(
      table, ref->key, ref->key_buff, first_n_keyparts_map(used_key_parts));
  if (step->key_prefix.empty()) {
    return false;
  }

  if (child_primary && used_key_parts >= child_pk_parts) {
    step->is_scan = false;
  } else {
    step->is_scan = true;
    step->end_key_prefix =
        key_pack::build_prefix_range_end(step->key_prefix);
    if (!child_primary) step->index_name = key.name;
  }

  return !step->table_name.empty();
}

bool compile_leaf(AccessPath *leaf,
                  const std::unordered_map<TABLE *, int> &table_steps,
                  HeliosProxy::ReadPlanStep *step) {
  TABLE *table = nullptr;
  Index_lookup *ref = nullptr;
  bool full_scan = false;
  int full_scan_index = -1;
  if (!qep_leaf_info(leaf, &table, &ref, &full_scan, &full_scan_index)) {
    return false;
  }
  if (table == nullptr || table->s == nullptr) {
    return false;
  }
  if (table->s->tmp_table != NO_TMP_TABLE) {
    return false;
  }
  if (leaf->type == AccessPath::REF_OR_NULL) {
    return false;
  }

  if (leaf->type == AccessPath::INDEX_RANGE_SCAN) {
    return compile_index_range_scan(leaf, table, step);
  }
  if (full_scan) {
    // Plain SELECT primary scans consume the cached ["", sentinel) range.
    const THD *leaf_thd = table->in_use;
    const bool plain_select = leaf_thd != nullptr && leaf_thd->lex != nullptr &&
                              leaf_thd->lex->sql_command == SQLCOM_SELECT &&
                              table->reginfo.lock_type <= TL_READ;
    if (!plain_select) {
      return false;
    }
    step->table_name = physical_table_key(table);
    if (step->table_name.empty()) {
      return false;
    }
    const bool primary_order =
        full_scan_index < 0 ||
        (table->s->primary_key != MAX_KEY &&
         full_scan_index == static_cast<int>(table->s->primary_key));
    step->is_scan = true;
    step->key_prefix.clear();
    step->end_key_prefix = key_pack::scan_end_sentinel();
    if (!primary_order) {
      // Full secondary INDEX_SCAN: read the secondary range itself. Runtime
      // index_first/index_next consumes it, then base rows come from row cache.
      step->index_name = table->key_info[full_scan_index].name;
    }
    return true;
  }
  if (ref == nullptr) {
    return false;
  }
  return compile_ref_lookup(table, ref, table_steps, step);
}

// Translate one resolved handler IndexSearchPlan (point/prefix/range/
// index-first) into a single ReadPlanStep; reject reverse/unbounded access.
bool compile_index_search(TABLE *table, uint index,
                          const IndexSearchPlan &search,
                          HeliosProxy::ReadPlanStep *step) {
  if (table == nullptr || table->s == nullptr || step == nullptr) {
    return false;
  }
  if (index >= table->s->keys) {
    return false;
  }

  const bool is_primary = index == table->s->primary_key;
  if (search.is_primary != is_primary) {
    return false;
  }

  step->table_name = physical_table_key(table);
  if (step->table_name.empty()) {
    return false;
  }

  const auto set_scan = [&](const std::string &start,
                            const std::string &end) {
    step->is_scan = true;
    step->key_prefix = start;
    step->end_key_prefix =
        end.empty() ? key_pack::scan_end_sentinel() : end;
    if (!is_primary) step->index_name = table->key_info[index].name;
  };

  switch (search.op) {
    case IndexSearchOp::kUniquePoint:
      if (search.packed_start_key.empty()) {
        return false;
      }
      if (is_primary) {
        step->is_scan = false;
        step->key_prefix = search.packed_start_key;
      } else {
        set_scan(search.packed_start_key,
                 key_pack::build_prefix_range_end(
                     search.packed_start_key));
      }
      return true;

    case IndexSearchOp::kSameKey:
    case IndexSearchOp::kPrefixFirst:
      if (search.packed_same_key_prefix.empty()) {
        return false;
      }
      set_scan(search.packed_same_key_prefix,
               search.packed_same_key_end);
      return true;

    case IndexSearchOp::kRangeScan: {
      if (search.packed_start_key.empty()) {
        return false;
      }
      std::string start = search.packed_start_key;
      if (search.find_flag == HA_READ_AFTER_KEY) start.push_back('\0');
      set_scan(start, search.packed_end_key);
      return true;
    }

    case IndexSearchOp::kIndexFirst:
      if (search.packed_end_key.empty()) {
        return false;
      }
      set_scan("", search.packed_end_key);
      return true;

    case IndexSearchOp::kPrevKey:
    case IndexSearchOp::kPrefixLast:
      return false;
  }

  return false;
}

// True when `node` appears in the AccessPath tree rooted at `root`. The
// walk reuses collect_qep_leaves; a subtree it rejects is reported as not
// containing the node, which makes the caller fail loudly.
bool plan_tree_contains(AccessPath *root, const AccessPath *node) {
  if (root == nullptr || node == nullptr) return false;
  if (root == node) return true;
  std::vector<AccessPath *> leaves;
  bool ok = true;
  collect_qep_leaves(root, &leaves, &ok);
  for (const AccessPath *collected : leaves) {
    if (collected == node) return true;
  }
  return false;
}

// The query block whose plan contains `node`. The node may sit below wrapper
// paths (FILTER, LIMIT_OFFSET), so this tests containment, not the plan root.
Query_block *query_block_containing_plan_node(Query_expression *unit,
                                              const AccessPath *node) {
  if (unit == nullptr || node == nullptr) return nullptr;
  for (Query_block *qb = unit->first_query_block(); qb != nullptr;
       qb = qb->next_query_block()) {
    if (qb->join != nullptr &&
        plan_tree_contains(qb->join->root_access_path(), node)) {
      return qb;
    }
    for (Query_expression *inner = qb->first_inner_query_expression();
         inner != nullptr; inner = inner->next_query_expression()) {
      if (Query_block *found =
              query_block_containing_plan_node(inner, node)) {
        return found;
      }
    }
  }
  return nullptr;
}

// Compile the scan behind a bare COUNT(*). UNQUALIFIED_COUNT carries no table
// parameters, so the step must follow the access MySQL counts through:
// ha_records() for JT_ALL, ha_records(index) otherwise.
bool compile_unqualified_count(
    THD *thd, AccessPath *leaf,
    std::unordered_map<TABLE *, int> *table_steps,
    std::vector<HeliosProxy::ReadPlanStep> *steps,
    std::vector<TABLE *> *added_tables) {
  Query_block *qb =
      (thd != nullptr && thd->lex != nullptr)
          ? query_block_containing_plan_node(thd->lex->unit, leaf)
          : nullptr;
  TABLE *table = nullptr;
  if (qb != nullptr) {
    for (Table_ref *tr = qb->leaf_tables; tr != nullptr; tr = tr->next_leaf) {
      if (tr->table == nullptr) continue;
      if (table != nullptr) {
        table = nullptr;  // more than one leaf table: not this shape
        break;
      }
      table = tr->table;
    }
  }
  if (table == nullptr || table->s == nullptr ||
      table->s->tmp_table != NO_TMP_TABLE || table->file == nullptr ||
      table->file->ht != helios_hton ||
      table->reginfo.lock_type > TL_READ) {
    return false;
  }
  if (table_steps->find(table) != table_steps->end()) {
    return false;
  }

  // Mirror get_exact_record_count(). ha_helios reports a non-clustered
  // primary, so only JT_ALL and index()==primary count through the cached
  // primary range; another index choice reads that secondary range.
  const QEP_TAB *qt = nullptr;
  if (qb->join != nullptr && qb->join->qep_tab != nullptr &&
      qb->join->primary_tables > 0) {
    qt = &qb->join->qep_tab[0];
  }
  if (qt == nullptr || qt->table() != table) {
    return false;
  }
  std::string index_name;
  if (qt->type() != JT_ALL) {
    const uint count_index = qt->index();
    if (count_index >= table->s->keys) {
      return false;
    }
    if (count_index != table->s->primary_key) {
      index_name = table->key_info[count_index].name;
    }
  }

  HeliosProxy::ReadPlanStep step;
  step.table_name = physical_table_key(table);
  if (step.table_name.empty()) {
    return false;
  }
  step.is_scan = true;
  step.key_prefix.clear();
  step.end_key_prefix = key_pack::scan_end_sentinel();
  step.index_name = std::move(index_name);
  (*table_steps)[table] = static_cast<int>(steps->size());
  added_tables->push_back(table);
  steps->push_back(std::move(step));
  return true;
}

/**
 * @brief Compile one AccessPath tree into read-plan steps.
 *
 * @details The main statement tree and optional inner subquery trees share
 * `table_steps`, so correlated inner probes can bind to earlier outer steps.
 * `allow_limit_pushdown` is kept off for optional inner roots.
 *
 * @note `added_tables` lets optional callers roll back only this tree's
 * additions.
 */
bool compile_tree_leaves(
    THD *thd, AccessPath *root, bool allow_limit_pushdown,
    std::unordered_map<TABLE *, int> *table_steps,
    std::vector<HeliosProxy::ReadPlanStep> *steps,
    std::vector<TABLE *> *added_tables) {
  std::vector<AccessPath *> leaves;
  bool ok = true;
  collect_qep_leaves(root, &leaves, &ok);
  if (!ok) {
    return false;
  }
  if (leaves.empty()) {
    return false;
  }

  for (AccessPath *leaf : leaves) {
    if (leaf->type == AccessPath::UNQUALIFIED_COUNT) {
      if (!compile_unqualified_count(thd, leaf, table_steps, steps,
                                     added_tables)) {
        return false;
      }
      continue;
    }
    TABLE *table = nullptr;
    Index_lookup *ref = nullptr;
    bool full_scan = false;
    int full_scan_index = -1;
    if (!qep_leaf_info(leaf, &table, &ref, &full_scan, &full_scan_index) ||
        table == nullptr) {
      return false;
    }
    if ((table->s != nullptr && table->s->tmp_table != NO_TMP_TABLE) ||
        table->file == nullptr || table->file->ht != helios_hton) {
      // Only Helios base tables hold cached rows; MySQL temp tables and
      // tables of another engine have nothing to prefetch for this leaf.
      continue;
    }
    if (table_steps->find(table) != table_steps->end()) {
      return false;
    }

    HeliosProxy::ReadPlanStep step;
    if (!compile_leaf(leaf, *table_steps, &step)) {
      return false;
    }

    // Push LIMIT into a single ASC REF scan only when LIMIT sits directly
    // above this leaf. count_all_rows and reject_multiple_rows both read past
    // LIMIT, so they cannot use a truncated cache entry.
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
 * @brief Collect plan roots that hang off Item-held subqueries.
 *
 * @details The main AccessPath tree does not cover every read MySQL may run.
 * IN and correlated subqueries can live as separate Query_expressions under
 * Item conditions, so collect those roots and compile them separately.
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

}  // namespace

bool compile_read_plan_from_qep(
    THD *thd, AccessPath *root,
    std::vector<HeliosProxy::ReadPlanStep> *out,
    bool include_inner_units) {
  if (out == nullptr) {
    return false;  // null output vector
  }
  out->clear();

  if (root == nullptr) {
    return false;  // missing JOIN root_access_path
  }

  std::unordered_map<TABLE *, int> table_steps;
  std::vector<HeliosProxy::ReadPlanStep> steps;
  std::vector<TABLE *> added_tables;

  if (!compile_tree_leaves(thd, root, /*allow_limit_pushdown=*/true,
                           &table_steps, &steps, &added_tables)) {
    return false;  // a leaf shape no step covers
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
      if (!compile_tree_leaves(thd, inner_root,
                               /*allow_limit_pushdown=*/false, &table_steps,
                               &steps, &added_tables)) {
        steps.resize(step_mark);
        for (size_t i = table_mark; i < added_tables.size(); ++i) {
          table_steps.erase(added_tables[i]);
        }
        added_tables.resize(table_mark);
      }
    }
  }

  if (steps.empty()) {
    return false;  // the QEP has no leaf a read plan covers
  }

  // Fold byte-identical steps into the earliest one (a view read twice,
  // or for_each probes with deep-equal bindings) and remap the later steps'
  // source_step.
  std::vector<std::vector<TABLE *>> step_aliases(steps.size());
  for (size_t i = 0; i < steps.size() && i < added_tables.size(); ++i) {
    if (added_tables[i] != nullptr) step_aliases[i].push_back(added_tables[i]);
  }
  {
    const auto foldable = [](const HeliosProxy::ReadPlanStep &s) {
      return s.is_scan && s.scan_limit == 0;
    };
    const auto same_binding = [](const HeliosProxy::ReadPlanKeyBinding &a,
                                 const HeliosProxy::ReadPlanKeyBinding &b) {
      return a.source_step == b.source_step && a.source_row == b.source_row &&
             a.source_offset == b.source_offset &&
             a.source_length == b.source_length &&
             a.use_midpoint == b.use_midpoint && a.from_key == b.from_key &&
             a.source_column == b.source_column &&
             a.column_as_int_key == b.column_as_int_key &&
             a.int_delta == b.int_delta;
    };
    const auto same_bindings =
        [&](const std::vector<HeliosProxy::ReadPlanKeyBinding> &a,
            const std::vector<HeliosProxy::ReadPlanKeyBinding> &b) {
          if (a.size() != b.size()) return false;
          for (size_t k = 0; k < a.size(); ++k)
            if (!same_binding(a[k], b[k])) return false;
          return true;
        };
    const auto same_step = [&](const HeliosProxy::ReadPlanStep &a,
                               const HeliosProxy::ReadPlanStep &b) {
      return a.table_name == b.table_name && a.index_name == b.index_name &&
             a.key_prefix == b.key_prefix &&
             a.end_key_prefix == b.end_key_prefix &&
             a.for_each == b.for_each && a.reverse_scan == b.reverse_scan &&
             same_bindings(a.bindings, b.bindings) &&
             same_bindings(a.end_bindings, b.end_bindings);
    };
    std::vector<uint32_t> new_index(steps.size(), 0);
    std::vector<HeliosProxy::ReadPlanStep> folded;
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
      }
      for (auto &kv : table_steps) {
        if (kv.second >= 0 && kv.second < static_cast<int>(new_index.size()))
          kv.second = static_cast<int>(new_index[kv.second]);
      }
    }
  }

  *out = std::move(steps);
  return true;
}

// Produce a one-step read plan from the handler access; a shape it cannot
// compile returns false and its reads take the row path.
bool compile_read_plan_for_single_table_dml(
    THD *thd, TABLE *table, uint index, const IndexSearchPlan &search,
    std::vector<HeliosProxy::ReadPlanStep> *out) {
  if (out == nullptr) {
    return false;  // null output vector
  }
  out->clear();

  HeliosProxy::ReadPlanStep step;
  if (!compile_index_search(table, index, search, &step)) {
    return false;  // a handler shape no step covers
  }

  out->push_back(std::move(step));
  return true;
}
