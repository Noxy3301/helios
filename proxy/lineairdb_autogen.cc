#include "lineairdb_autogen.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lineairdb_keyenc.hh"
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
      // for_each point probe (FER/FES still rejected there).
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
      // GROUP BY via a local temp table: only the feeding subquery touches
      // LineairDB tables; the table_path reads the temp table (its leaves are
      // skipped as tmp-table leaves by the compile loop).
      collect_qep_leaves(p->temptable_aggregate().subquery_path, out, ok,
                         unsupported);
      collect_qep_leaves(p->temptable_aggregate().table_path, out, ok,
                         unsupported);
      return;
    case AccessPath::MATERIALIZE:
      // Derived table / subquery materialization: prefetch the operand
      // subqueries; the materialized temp table itself is local.
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

  // The server iterates one source step per for_each probe, so every bound
  // keypart must read from the same source table. The optimizer may reference
  // fields from different tables of the join prefix (multi-equality
  // propagation, e.g. lineitem probed by (partsupp.ps_partkey,
  // supplier.s_suppkey)); pick the most recent step among the bound sources
  // and remap other tables' fields onto it through their multiple equalities.
  TABLE *iter_table = nullptr;
  int iter_step = -1;
  for (const BoundPart &bp : bound_items) {
    auto source = table_steps.find(bp.item->field->table);
    if (source == table_steps.end()) {
      TABLE *src_table = bp.item->field->table;
      if (src_table->s != nullptr &&
          src_table->s->tmp_table != NO_TMP_TABLE) {
        // Probe values come from a local materialized temp table (derived
        // table / view): they are unknown at staging time. Stage the whole
        // probed table instead — point probes then hit the row cache and
        // range probes are covered by the full staged range. (Same answer as
        // the phase-7 "temp source -> full scan" finding.)
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
        // The runtime probes whatever index the ref chose, so the staged
        // full scan must live on that same index: a primary full scan
        // cannot serve secondary-cache lookups (Codex P1).
        if (ref->key == static_cast<int>(table->s->primary_key)) {
          step->index_name.clear();
        } else {
          step->index_name = table->key_info[ref->key].name;
        }
        return !step->table_name.empty();
      }
      if (reason != nullptr) *reason = "bound source is not an earlier step";
      return false;
    }
    if (source->second > iter_step) {
      iter_step = source->second;
      iter_table = bp.item->field->table;
    }
  }

  for (const BoundPart &bp : bound_items) {
    Field *source_field = bp.item->field;
    if (source_field->table != iter_table) {
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
        if (reason != nullptr) {
          *reason = "for_each binding spans multiple source steps";
        }
        return false;
      }
      source_field = remapped;
    }
    if (!append_bound_keypart(table, ref, bp.kp, source_field, iter_step,
                              step, reason)) {
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
    if (child_primary && used_key_parts >= child_pk_parts) {
      // Full-primary-key point probe per source row.
      step->is_scan = false;
      return !step->table_name.empty();
    }
    // FER (primary partial key) / FES (secondary key): the server executes one
    // bounded prefix scan per deduplicated source row and returns the rows in
    // per-probe groups; the proxy stages one scan-cache entry per probe.
    step->is_scan = true;
    if (!child_primary) step->index_name = key.name;
    return !step->table_name.empty();
  }

  step->key_prefix = lineairdb_keyenc::convert_key_to_ldbformat(
      table, ref->key, ref->key_buff, first_n_keyparts_map(ref->key_parts));
  if (step->key_prefix.empty()) {
    if (reason != nullptr) *reason = "failed to encode constant key";
    return false;
  }

  if (child_primary && ref->key_parts >= child_pk_parts) {
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
    // A primary full scan of a plain SELECT stages the whole table as the
    // range ["", sentinel): the row path (rnd_next / index_first) consumes
    // exactly that range from the scan cache and commit revalidates the same
    // bounds. Locked reads (FOR UPDATE/SHARE) and DML keep the old rejection:
    // their row set must come from the locking read path.
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
      // Full secondary INDEX_SCAN: stage the whole secondary index; the
      // runtime walks it via index_first/index_next, which consume the staged
      // secondary range and batch-fetch the base rows from the row cache.
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

}  // namespace

bool autogen_read_plan_from_qep(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::vector<LineairDBProxy::ReadPlanStep> *out) {
  if (out == nullptr) {
    return raise_unsupported(thd, "NONE", "null output vector");
  }
  out->clear();

  if (root == nullptr) {
    return raise_unsupported(thd, "NONE", "missing JOIN root_access_path");
  }

  std::vector<AccessPath *> leaves;
  bool ok = true;
  UnsupportedQep unsupported;
  collect_qep_leaves(root, &leaves, &ok, &unsupported);
  if (!ok) {
    return raise_unsupported(thd, unsupported.type, unsupported.reason);
  }
  if (leaves.empty()) {
    return raise_unsupported(thd, root->type, "QEP has no table leaves");
  }

  std::unordered_map<TABLE *, int> table_steps;
  std::vector<LineairDBProxy::ReadPlanStep> steps;
  steps.reserve(leaves.size());

  for (AccessPath *leaf : leaves) {
    TABLE *table = nullptr;
    Index_lookup *ref = nullptr;
    bool full_scan = false;
    int full_scan_index = -1;
    if (!qep_leaf_info(leaf, &table, &ref, &full_scan, &full_scan_index) ||
        table == nullptr) {
      return raise_unsupported(thd, leaf->type, "unsupported QEP leaf");
    }
    if (table->s != nullptr && table->s->tmp_table != NO_TMP_TABLE) {
      // Local temp table (materialized derived table / GROUP BY buffer):
      // reading it needs no prefetch step.
      continue;
    }
    if (table_steps.find(table) != table_steps.end()) {
      return raise_unsupported(thd, leaf->type, "duplicate QEP table leaf");
    }

    LineairDBProxy::ReadPlanStep step;
    std::string reason;
    if (!compile_leaf(leaf, table_steps, &step, &reason)) {
      return raise_unsupported(thd, leaf->type, reason);
    }

    // LIMIT pushdown into staging (TPC-C Delivery: `... ORDER BY pk ASC
    // LIMIT 1` otherwise stages a whole district queue per RPC). Gate: the
    // root is LIMIT_OFFSET(offset=0) whose child IS this single REF leaf —
    // no FILTER/SORT between means the WHERE is fully absorbed into the key
    // prefix, so a server-side limit cannot cut rows MySQL still needed.
    // ASC only (forward entries); the consumer marks the served result
    // truncated and aborts loudly if anything reads past the staged limit.
    if (root->type == AccessPath::LIMIT_OFFSET &&
        root->limit_offset().offset == 0 &&
        root->limit_offset().child == leaf && leaves.size() == 1 &&
        leaf->type == AccessPath::REF && ref != nullptr &&
        ref->key >= 0 && ref->key < static_cast<int>(table->s->keys) &&
        step.is_scan && !step.for_each && step.index_name.empty() &&
        step.scan_limit == 0) {
      const RangeScanLimit lim = range_scan_limit_for_order(
          thd, &table->key_info[ref->key], ref->key_parts,
          /*has_mysql_only_filter=*/false);
      if (lim.row_limit > 0 && !lim.reverse_scan) {
        step.scan_limit = static_cast<uint64_t>(lim.row_limit);
        step.reverse_scan = false;
      }
    }

    // Attach the table-local cond_push() filter to every scan step (full
    // scans, secondary scans, and FER/FES probe groups) so the server drops
    // non-matching rows before transfer. Only when commit-time validation is
    // off (the replay could not reproduce a filtered key set); MySQL
    // re-evaluates the condition either way.
    if (allow_filter_pushdown && step.is_scan && table->file != nullptr) {
      step.serialized_filter =
          down_cast<ha_lineairdb *>(table->file)->pushed_filter_for_autogen();
    }

    table_steps[table] = static_cast<int>(steps.size());
    steps.push_back(std::move(step));
  }

  *out = std::move(steps);
  return true;
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
