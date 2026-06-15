#include "lineairdb_autogen.hh"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "helios_gate.hh"
#include "lineairdb_keyenc.hh"
#include "lineairdb_pushdown.hh"
#include "lineairdb_transaction.hh"
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

// Phase-18 membership-staging: collect tables that are the SOLE, residual-free
// inner of an ANTI-JOIN (NOT EXISTS / NOT IN). For an anti-join the executor
// only ever asks "does a match exist?" and never reads inner columns, so the
// server may ship ONE row per probe (existence) instead of every match — but
// ONLY when the anti-join inner is a bare table access with no FILTER/SORT
// above the leaf. A residual the executor re-checks (q21's NOT EXISTS l3:
// l_suppkey<>l1.l_suppkey AND l_receiptdate>l_commitdate) could reject the
// server's first row while a later one qualifies, turning a real match into a
// false "not exists". qep_leaf_info() returns true ONLY for a bare leaf access
// path (FILTER/SORT/etc. return false), so it is exactly the residual-free
// test. Partial recursion is sound: an unhandled node just yields no marking.
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
      // row, so "one row per probe" does not apply — just recurse.
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

// Compile every leaf of one plan tree into `steps`, sharing `table_steps`
// across trees so later trees can bind probes to earlier steps. On failure
// sets unsupported and returns false WITHOUT raising; the caller decides
// whether the failure is fatal (main tree) or skippable (inner unit).
// added_tables records this call's table_steps insertions for rollback.
static bool compile_tree_leaves(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::unordered_map<TABLE *, int> *table_steps,
    std::vector<LineairDBProxy::ReadPlanStep> *steps,
    std::vector<TABLE *> *added_tables,
    std::vector<std::pair<Field *, Field *>> *eq_edges,
    UnsupportedQep *unsupported) {
  std::vector<AccessPath *> leaves;
  bool ok = true;
  collect_qep_leaves(root, &leaves, &ok, unsupported);

  // Phase-18 membership-staging (q22): tables whose only role is an anti-join
  // existence probe with no executor-side residual. Default ON gate; HELIOS_Q22_MEMBERSHIP=0
  // disables (off-switch / A-B).
  //
  // REQUIRES ro_novalidate (allow_filter_pushdown == tx->ro_novalidate()): the
  // existence cap ships ONE row per probe, an INCOMPLETE secondary-range result.
  // Under commit-time validation, validate_secondary_key_list re-scans the full
  // range and demands an exact key-list match, so the capped staging would
  // DETERMINISTICALLY mismatch and abort (livelock) — exactly why pushed filters
  // and projection are also ro_novalidate-gated (a replay cannot reproduce a
  // reduced result set). Gating here scopes existence_only to the no-validate
  // read path where the reduction is sound. (Review: Phase-18 Claude found the
  // deterministic-abort bug when this was unconditionally default-on.)
  static const char *q22_env = std::getenv("HELIOS_Q22_MEMBERSHIP");
  static const bool q22_membership_on = q22_env == nullptr || q22_env[0] != '0';
  std::unordered_set<const TABLE *> existence_only_inners;
  if (q22_membership_on && allow_filter_pushdown) {
    collect_existence_only_antijoin_inners(root, &existence_only_inners);
  }
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
    // Join equality edges for the semijoin planner: ref->items[kp] (a source
    // field) joins this leaf's index key_part[kp] field.
    if (eq_edges != nullptr && ref != nullptr && ref->items != nullptr &&
        ref->key >= 0 && ref->key < static_cast<int>(table->s->keys)) {
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
      // Local temp table (materialized derived table / GROUP BY buffer):
      // reading it needs no prefetch step.
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

    // LIMIT pushdown into staging (TPC-C Delivery: `... ORDER BY pk ASC
    // LIMIT 1` otherwise stages a whole district queue per RPC). Gate: the
    // root is LIMIT_OFFSET(offset=0) whose child IS this single REF leaf —
    // no FILTER/SORT between means the WHERE is fully absorbed into the key
    // prefix, so a server-side limit cannot cut rows MySQL still needed.
    // ASC only (forward entries); the consumer marks the served result
    // truncated and aborts loudly if anything reads past the staged limit.
    // count_all_rows (SQL_CALC_FOUND_ROWS) keeps reading past LIMIT to count
    // the rest; reject_multiple_rows (scalar subquery LIMIT) reads one row
    // beyond to detect cardinality errors. Both need rows past the staged
    // limit, so a truncated staging would abort a valid statement (Codex F2).
    if (root->type == AccessPath::LIMIT_OFFSET &&
        root->limit_offset().offset == 0 &&
        !root->limit_offset().count_all_rows &&
        !root->limit_offset().reject_multiple_rows &&
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

    // Phase-18 membership-staging (q22): a for_each probe of a residual-free
    // anti-join inner otherwise stages every matching inner row (q22: all 1.5M
    // orders by o_custkey, 100MB) when one existence marker per probe key
    // suffices. Cap at the first match per probe. Restricted to for_each steps
    // (the over-fetching shape) and to the tables proven residual-free above.
    if (step.for_each && existence_only_inners.count(table) != 0) {
      step.existence_only = true;
    }

    (*table_steps)[table] = static_cast<int>(steps->size());
    added_tables->push_back(table);
    steps->push_back(std::move(step));
  }
  return true;
}

// P0 semijoin correctness whitelist. A semijoin membership-reduction ("drop
// probe rows whose key is absent from the source set") is result-preserving
// ONLY between two leaves joined by a plain INNER equi-join in the SAME
// top-level query block. For anti-join (NOT IN / NOT EXISTS) the absent rows
// are exactly the ones to KEEP, so the reduction corrupts results (old
// branch: q22 over-count, q21 -> 0 rows). Reject any leaf that is:
//   (a) the inner side of an outer join (NULL-extended rows must survive),
//   (b) embedded in a semi-join or anti-join nest, or
//   (c) not in the statement's top-level query block.
static bool helios_sj_safe_leaf(const TABLE *t) {
  if (t == nullptr) return false;
  const Table_ref *tr = t->pos_in_table_list;
  if (tr == nullptr) return false;
  if (tr->is_inner_table_of_outer_join()) return false;      // (a)
  for (const Table_ref *emb = tr->embedding; emb != nullptr;  // (b)
       emb = emb->embedding) {
    if (emb->is_sj_or_aj_nest()) return false;
  }
  const Query_block *qb = tr->query_block;  // (c)
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
      src->charset() != probe->charset()) {
    return false;
  }
  return true;
}

// Collect plan roots of every inner query expression below `unit`,
// recursively: dependent scalar subqueries and in_optimizer materialized IN
// hang off Item conditions, so their plan trees never appear in the main
// AccessPath tree's child pointers.
static void collect_inner_unit_roots(Query_expression *unit,
                                     std::vector<AccessPath *> *roots) {
  if (unit == nullptr) return;
  for (Query_block *qb = unit->first_query_block(); qb != nullptr;
       qb = qb->next_query_block()) {
    for (Query_expression *inner = qb->first_inner_query_expression();
         inner != nullptr; inner = inner->next_query_expression()) {
      AccessPath *r = inner->root_access_path();
      if (r == nullptr) {
        // Materialized IN-subqueries keep their plan on the subquery JOIN;
        // the unit-level root is only populated for some shapes (same
        // fallback as table_unit_plan_root in lineairdb_prefetch.cc).
        Query_block *iqb = inner->first_query_block();
        if (iqb != nullptr && iqb->join != nullptr) {
          r = iqb->join->root_access_path();
        }
      }
      if (r != nullptr) roots->push_back(r);
      collect_inner_unit_roots(inner, roots);
    }
  }
}

bool autogen_read_plan_from_qep(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::vector<LineairDBProxy::ReadPlanStep> *out,
    bool include_inner_units, LineairDBTransaction *tx) {
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
  if (!compile_tree_leaves(thd, root, allow_filter_pushdown, &table_steps,
                           &steps, &added_tables, &eq_edges, &unsupported)) {
    return raise_unsupported(thd, unsupported.type, unsupported.reason);
  }

  // Stage Item-embedded subquery plans too: a plan that keeps an IN-subquery
  // in in_optimizer form (q18) or probes inside a dependent scalar subquery
  // (q20) executes those trees at runtime, and their leaves are invisible to
  // the main-tree walk above. Compiled into the SAME step list so correlated
  // probes bind to outer steps as for_each. A failed inner unit rolls back
  // and is non-fatal: an unstaged subquery that never executes costs
  // nothing, and one that does execute misses and aborts exactly as before.
  if (include_inner_units && thd != nullptr && thd->lex != nullptr) {
    std::vector<AccessPath *> inner_roots;
    collect_inner_unit_roots(thd->lex->unit, &inner_roots);
    for (AccessPath *inner_root : inner_roots) {
      if (inner_root == root) continue;
      const size_t step_mark = steps.size();
      const size_t table_mark = added_tables.size();
      UnsupportedQep inner_unsupported;
      if (!compile_tree_leaves(thd, inner_root, allow_filter_pushdown,
                               &table_steps, &steps, &added_tables, &eq_edges,
                               &inner_unsupported)) {
        steps.resize(step_mark);
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

  // SharedScan dedup: identical SELF-CONTAINED scan steps (same table/index/
  // bounds, no bindings, no for_each, no LIMIT) are staged ONCE. A view that
  // is materialized twice (q15's revenue0) compiles two byte-identical full
  // scans of the base table — without folding, every row ships twice AND the
  // duplicate step count blocks the single-step filter pushdown below. The
  // fold keeps the EARLIEST step and records every folded alias so the filter
  // pass can verify all aliases agree on the pushed predicate. Later-step
  // index references (bindings/semijoins source_step) are remapped, the same
  // pattern as the covered-step drop in execute_read_plan.
  std::vector<std::vector<TABLE *>> step_aliases(steps.size());
  for (size_t i = 0; i < steps.size() && i < added_tables.size(); ++i) {
    if (added_tables[i] != nullptr) step_aliases[i].push_back(added_tables[i]);
  }
  {
    // Two flavors fold: (a) self-contained scans (no bindings, no LIMIT —
    // the q15 view-clone case), and (b) for_each probe steps whose bindings
    // are deep-equal (q21's self-join compiles THREE identical
    // probe-lineitem-by-l_orderkey fetches off the same source rows; the
    // predicates that differ between the aliases are MySQL-side). Probe
    // bindings compare by value INCLUDING source_step, which at this point
    // is the pre-remap index — equal references mean the same source.
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
             a.for_each == b.for_each &&
             a.reverse_scan == b.reverse_scan &&
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
        for (TABLE *t : step_aliases[j])
          folded_aliases[target].push_back(t);
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
        for (auto &sj : s.semijoins) sj.source_step = new_index[sj.source_step];
      }
      for (auto &kv : table_steps) {
        if (kv.second >= 0 && kv.second < static_cast<int>(new_index.size()))
          kv.second = static_cast<int>(new_index[kv.second]);
      }
    }
  }

  // GroupedSummary skip (Phase-16 entry 25): a leaf registered for the
  // handler-local grouped-summary scan must NOT have its base rows staged —
  // the handler fetches server group rows itself and serves synthetic rows.
  // Only unreferenced plain scans whose EVERY alias is GS-registered drop;
  // the decision is recorded on the tx so the handler activates exactly when
  // the step was really removed (otherwise the staged raw scan serves).
  if (allow_filter_pushdown && tx != nullptr) {
    std::vector<bool> referenced(steps.size(), false);
    for (const auto &st : steps) {
      for (const auto &b : st.bindings)
        if (b.source_step < referenced.size()) referenced[b.source_step] = true;
      for (const auto &b : st.end_bindings)
        if (b.source_step < referenced.size()) referenced[b.source_step] = true;
    }
    std::vector<uint32_t> new_index(steps.size(), 0);
    std::vector<bool> dropped(steps.size(), false);
    std::vector<LineairDBProxy::ReadPlanStep> kept;
    std::vector<std::vector<TABLE *>> kept_aliases;
    bool any_drop = false;
    for (size_t i = 0; i < steps.size(); ++i) {
      // GS synthetic rows are served ONLY on the FULL-scan path (rnd_init ->
      // gs_fill_buffers, and index_read_map only when key == nullptr). That
      // covers a PRIMARY full scan AND a full SECONDARY index scan (q18's inner
      // `Index scan using l_ok`: index_first drives it with key==nullptr). The
      // gating shape is therefore "unbounded full scan", NOT "index_name empty":
      // key_prefix empty && end_key_prefix == sentinel && no pushed filter.
      // M5(q18): the old `index_name.empty()` (commit ceb5a50) was over-broad --
      // it kept q18's full l_ok secondary scan staged (24s) when the optimizer
      // chose l_ok over PRIMARY, bypassing GS. A keyed/range SECONDARY scan
      // (q15's view body over an l_sd RANGE: non-empty key_prefix or non-sentinel
      // end) MUST stay staged -- its keyed index_read (key!=nullptr) never takes
      // the GS branch, so dropping it would orphan the lookup (ceb5a50's case).
      // The full-scan shape keeps q15 staged while letting q18's l_ok be served
      // by GS; gs_fill_buffers aggregates the FULL leaf under reg->filter (empty
      // for q18), producing the identical group set the dropped scan would have.
      // AUDIT NOTE (phase22 logic audit): the LOAD-BEARING safety net is the
      // per-alias gs_registration()!=nullptr guard below + the strict GS-shape
      // gates (single non-nullable group col, single SUM(a*(1-b)), RO-novalidate)
      // -- NOT the shape test alone. serialized_filter/semijoins are attached by
      // LATER passes so they are always empty here (forward-guards, currently
      // inert -- kept so a future pass-reorder cannot silently drop a filtered/
      // semijoin step). A both-unbounded INDEX_RANGE_SCAN (NO_MIN+NO_MAX) also
      // matches key_prefix-empty + sentinel-end, but is harmless: it too is
      // consumed with key==nullptr (read_range_first -> index_first) so GS serves
      // it correctly. The discriminator vs q15 is the keyed/bounded range
      // (non-empty key_prefix OR non-sentinel end), which stays staged.
      bool drop = steps[i].is_scan && !steps[i].for_each &&
                  steps[i].key_prefix.empty() &&
                  steps[i].end_key_prefix ==
                      lineairdb_keyenc::scan_end_sentinel() &&
                  steps[i].serialized_filter.empty() &&
                  steps[i].scan_limit == 0 && !referenced[i] &&
                  i < step_aliases.size() && !step_aliases[i].empty() &&
                  steps[i].semijoins.empty();
      if (drop) {
        for (TABLE *at : step_aliases[i])
          if (at == nullptr || tx->gs_registration(at) == nullptr) {
            drop = false;
            break;
          }
      }
      if (drop) {
        for (TABLE *at : step_aliases[i]) tx->mark_gs_skipped(at);
        dropped[i] = true;
        any_drop = true;
        continue;
      }
      new_index[i] = static_cast<uint32_t>(kept.size());
      kept.push_back(std::move(steps[i]));
      kept_aliases.push_back(std::move(step_aliases[i]));
    }
    if (any_drop) {
      steps = std::move(kept);
      step_aliases = std::move(kept_aliases);
      for (auto &st : steps) {
        for (auto &b : st.bindings) b.source_step = new_index[b.source_step];
        for (auto &b : st.end_bindings)
          b.source_step = new_index[b.source_step];
        for (auto &sj : st.semijoins) sj.source_step = new_index[sj.source_step];
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
    } else {
      steps = std::move(kept);
      step_aliases = std::move(kept_aliases);
    }
  }

  // Scan filter pushdown (post-pass; ro_novalidate only). A table-local WHERE
  // filter is attached ONLY when the physical table backs exactly ONE plan
  // step: with multiple steps (self-join aliases, temp-source full-scan
  // coverage fallbacks) the table-name-keyed caches cross-serve entries, and
  // a row dropped by one alias's filter would silently vanish from another
  // alias's reads (q2/q20: point probes into the temp-fallback part scan).
  // With a single step, every consumer of the table sees rows filtered by
  // that alias's OWN WHERE conjunct — rows MySQL would discard anyway. A
  // step folded from several aliases qualifies only when EVERY alias builds
  // the SAME serialized predicate (identical view clones): then a dropped
  // row is one each alias's own WHERE discards.
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

  // Inner-unit aggregate stamping (Phase-16): a materialized uncorrelated
  // subquery whose JOIN the override executor took over registered its
  // Phase-B AggregateSpec (+ the exact WHERE it requires) on the tx at
  // optimize time. Stamp the spec onto the matching staged scan step so the
  // server returns GROUP rows — but ONLY when the step's attached filter is
  // byte-identical to the registered one (a missing/different filter would
  // make the server aggregate rows the inner WHERE excludes). The decision is
  // recorded on the tx: the executor consumes group rows for stamped tables
  // and falls back to proxy-side (Phase A) aggregation over raw rows
  // otherwise. Steps referenced by bindings stay raw: a binding reads the
  // source's shipped row positionally, which a group row would break.
  if (allow_filter_pushdown && tx != nullptr) {
    static const bool aggdbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
    std::vector<bool> referenced(steps.size(), false);
    for (const auto &s : steps) {
      for (const auto &b : s.bindings)
        if (b.source_step < referenced.size()) referenced[b.source_step] = true;
      for (const auto &b : s.end_bindings)
        if (b.source_step < referenced.size()) referenced[b.source_step] = true;
    }
    for (size_t i = 0; i < steps.size(); ++i) {
      auto &s = steps[i];
      if (!s.is_scan || s.for_each || !s.index_name.empty() ||
          s.scan_limit != 0 || s.reverse_scan || referenced[i] ||
          !s.bindings.empty() || !s.end_bindings.empty() ||
          !s.aggregate_serialized.empty()) {
        if (aggdbg && s.is_scan && !s.for_each &&
            tx->inner_aggregate(s.table_name) != nullptr)
          std::fprintf(stderr,
                       "[AGGSTAMP] skip step=%zu tbl=%s idx=%s lim=%llu rev=%d "
                       "ref=%d nbind=%zu\n",
                       i, s.table_name.c_str(), s.index_name.c_str(),
                       (unsigned long long)s.scan_limit, (int)s.reverse_scan,
                       (int)referenced[i], s.bindings.size());
        continue;
      }
      const auto *reg = tx->inner_aggregate(s.table_name);
      if (reg == nullptr) continue;
      // Ownership guard (Codex P1-2): EVERY alias folded into this step must
      // be a leaf of a registered inner unit. A raw same-table consumer that
      // happened to fold with the aggregate's scan (identical bytes) would
      // otherwise lose its raw staged range when the stamp flips the step to
      // group rows.
      bool all_consumers = i < step_aliases.size() && !step_aliases[i].empty();
      if (all_consumers) {
        for (TABLE *at : step_aliases[i]) {
          if (at == nullptr || reg->leaves.count(at) == 0) {
            all_consumers = false;
            break;
          }
        }
      }
      if (!all_consumers) {
        if (aggdbg)
          std::fprintf(stderr, "[AGGSTAMP] non-consumer alias tbl=%s\n",
                       s.table_name.c_str());
        continue;
      }
      if (s.serialized_filter != reg->filter) {
        if (aggdbg)
          std::fprintf(stderr,
                       "[AGGSTAMP] filter mismatch tbl=%s step=%zuB reg=%zuB\n",
                       s.table_name.c_str(), s.serialized_filter.size(),
                       reg->filter.size());
        continue;
      }
      s.aggregate_serialized = reg->spec;
      s.projection.clear();
      s.projection_num_columns = 0;
      tx->mark_inner_agg_stamped(s.table_name);
      if (aggdbg)
        std::fprintf(stderr, "[AGGSTAMP] stamped tbl=%s step=%zu\n",
                     s.table_name.c_str(), i);
    }
  }

  // Phase-9 semijoin reduction (gated HELIOS_ENABLE_SEMIJOIN): for each
  // FER/FES high-fanout probe step, if its probe join key is in the same
  // equality class (union-find over the collected join edges) as an EARLIER
  // step whose table carries a selective single-table predicate, attach a
  // SemijoinFilter: the server ships only probe rows whose key appears among
  // the (predicate-passing) source rows. Results are unchanged for plain
  // inner equi-joins (helios_sj_safe_leaf / key-compat guards).
  // Phase-22: default-ON (net-win on TPC-H, q21 SIP; TPC-C parity in isolation
  // tests). Disabled by HELIOS_ENABLE_SEMIJOIN=0.
  static const bool semijoin_on = helios::gate_default_on("HELIOS_ENABLE_SEMIJOIN");
  if (semijoin_on && allow_filter_pushdown && !eq_edges.empty()) {
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

    // Per-alias chooser: the best membership source for `probe_t`'s probe
    // step, or false when none qualifies.
    const auto choose_semijoin =
        [&](TABLE *probe_t, size_t probe_step,
            LineairDBProxy::ReadPlanStep::Semijoin *out_sj) -> bool {
      auto &ps = steps[probe_step];
      if (!helios_sj_safe_leaf(probe_t)) return false;
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
        if (!helios_sj_safe_leaf(src_t)) continue;
        if (src_t->pos_in_table_list->query_block !=
            probe_t->pos_in_table_list->query_block)
          continue;
        // The source must carry a selective single-table predicate. If its
        // scan is already filtered at fetch, its shipped rows ARE the reduced
        // set (no source_filter needed). Otherwise the predicate rides in the
        // semijoin and the server collects keys only from rows passing it,
        // while the source step still ships in full (it may be a join inner
        // the executor point-probes for every outer row).
        // An aggregate-stamped step ships GROUP rows — its shipped values are
        // not base rows, so it can never be a membership source.
        if (!steps[src_step].aggregate_serialized.empty()) continue;
        std::string sf_filter;
        const bool src_prefiltered =
            steps[src_step].is_scan && !steps[src_step].for_each &&
            !steps[src_step].serialized_filter.empty();
        if (!src_prefiltered &&
            (!build_single_table_filter(thd, src_t, &sf_filter) ||
             sf_filter.empty()))
          continue;  // no selective predicate -> not a useful source
        if (src_t->s->primary_key == MAX_KEY) continue;
        Field *src_pk =
            src_t->key_info[src_t->s->primary_key].key_part[0].field;
        if (uf.find(src_pk) == uf.end() || find_root(src_pk) != cls) continue;
        if (!helios_sj_keys_compatible(src_pk, probe_field)) continue;
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

    // A FOLDED probe step serves several aliases: a membership reduction is
    // sound only when EVERY alias independently selects the SAME semijoin —
    // a row dropped by one alias's reduction must be a row every alias's own
    // join would discard. Any alias without a (matching) choice vetoes.
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

  // Phase-17 q18 grouped-semijoin: for each registered
  //   outer.col IN (SELECT gcol FROM T GROUP BY gcol HAVING agg>const)
  // prepend a server-side AGGREGATE step over T (group rows survive the
  // server-side HAVING -> ~57 keys) and attach a semijoin on the outer scan
  // step against that step's group column (field ordinal 0). The outer is then
  // staged reduced to the qualifying orders, so the for_each probes hanging off
  // it (customer/lineitem) fetch only those. Result-preserving: the membership
  // set equals the IN set (positive, not anti-join). MySQL still materializes
  // the subquery itself by scanning T's raw rows (a separate step), so this
  // only prunes the OUTER staging (Phase A; the inner removal is Phase B).
  if (allow_filter_pushdown && tx != nullptr &&
      !tx->grouped_semijoins().empty()) {
    static const bool gdbg = std::getenv("HELIOS_FE_DEBUG") != nullptr;
    for (const auto &gs : tx->grouped_semijoins()) {
      // Find a plain outer scan step on the outer table (not for_each, not
      // already aggregate-stamped, no existing semijoin) to reduce.
      int outer_idx = -1;
      for (size_t i = 0; i < steps.size(); ++i) {
        const auto &s = steps[i];
        if (s.is_scan && !s.for_each && s.table_name == gs.outer_table_key &&
            s.aggregate_serialized.empty() && s.index_name.empty() &&
            s.semijoins.empty()) {
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
      // Build the AGGREGATE step over T (full primary scan + the HAVING spec).
      LineairDBProxy::ReadPlanStep agg;
      agg.table_name = gs.inner_table_key;
      agg.is_scan = true;
      agg.for_each = false;
      agg.end_key_prefix = lineairdb_keyenc::scan_end_sentinel();
      agg.aggregate_serialized = gs.agg_spec;
      // Insert at the FRONT so the server has its group rows in previous_results
      // before the (later) outer step's semijoin reads them. Shift every
      // existing source_step reference by +1.
      steps.insert(steps.begin(), std::move(agg));
      for (auto &s : steps) {
        for (auto &b : s.bindings) b.source_step += 1;
        for (auto &b : s.end_bindings) b.source_step += 1;
        for (auto &sj : s.semijoins) sj.source_step += 1;
      }
      LineairDBProxy::ReadPlanStep::Semijoin sj;
      sj.source_step = 0;            // the agg step we just prepended
      sj.source_column = 0;          // group col 0 = gcol (field ordinal 1)
      sj.probe_column = gs.outer_probe_column;
      steps[outer_idx + 1].semijoins.push_back(std::move(sj));
      if (gdbg)
        std::fprintf(stderr,
                     "[GSEMI] agg step for %s + semijoin on %s (step %d) "
                     "probe_col=%u\n",
                     gs.inner_table_key.c_str(), gs.outer_table_key.c_str(),
                     outer_idx + 1, gs.outer_probe_column);
    }
  }

  *out = std::move(steps);
  return true;
}

// Projection pushdown planning (v1, ported from the old branch minus the
// semijoin reconciliation it carried there): per PHYSICAL table ("./db/tbl",
// cross-db safe), UNION the read_set of every alias so a self-joined table
// ships one uniform column set that is a superset of each alias's needs.
// Excluded: generated-column tables (server can't recompute), tables whose
// shipped rows feed value-form bindings (the server slices their VALUES
// positionally; v1 ships those full instead of remapping), and the
// no-benefit cases (no columns / all columns read).
void plan_projection_pushdown(
    THD *thd, std::vector<LineairDBProxy::ReadPlanStep> *steps,
    std::unordered_map<std::string, std::vector<uint32_t>> *kept_out) {
  kept_out->clear();
  if (thd == nullptr || thd->lex == nullptr || steps == nullptr) return;

  std::unordered_map<std::string, std::vector<bool>> union_rs;
  std::unordered_map<std::string, uint32_t> fields_of;
  std::unordered_set<std::string> gcol_tables;
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

  // Value-form bindings read the SOURCE step's shipped row positionally
  // (option-2, ported from the old branch):
  //  - column form (source_column > 0): the server extracts column
  //    (source_column - 1) from the value. PROJECTABLE: force that column
  //    into the source table's kept set, then remap source_column to its
  //    projected position below. (Without this, lineitem ships all 16
  //    columns on q5/q7/q8/q14/q21 because l_partkey/l_suppkey probes are
  //    value columns — the single largest transfer left.)
  //  - byte-slice form (source_column == 0): offsets into the raw value are
  //    not remappable -> the source ships full.
  std::unordered_set<std::string> unsafe_src;
  const auto force_binding_col =
      [&](const LineairDBProxy::ReadPlanKeyBinding &b) {
        if (b.from_key || b.source_step >= steps->size()) return;
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

  // Semijoin reconciliation: the server reads sj.source_column from the
  // SOURCE step's SHIPPED rows. With a source_filter (full-row column
  // indices) the source must ship FULL rows; without one (pre-filtered
  // source) keep it projected but force-keep the membership column and remap
  // its ordinal to the packed position below. The PROBE side is untouched:
  // fe_reject runs on the probe's untrimmed row server-side (trim happens at
  // emission), so probe_column stays a full ordinal.
  for (const auto &s : *steps) {
    for (const auto &sj : s.semijoins) {
      if (sj.source_step >= steps->size()) continue;
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

  // Decide kept per eligible table and build full-ordinal -> projected
  // position maps (1-based; 0 = not kept) for the binding remap.
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

  // Remap column-form bindings whose source table is now projected: the
  // server reads (source_column - 1) from the SHIPPED (trimmed) value, so
  // the index must point at the projected position. The column was forced
  // into kept above, so its position is nonzero.
  const auto remap_binding = [&](LineairDBProxy::ReadPlanKeyBinding &b) {
    if (b.from_key || b.source_column <= 0 || b.source_step >= steps->size())
      return;
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
      auto it = full_to_proj.find((*steps)[sj.source_step].table_name);
      if (it == full_to_proj.end()) continue;
      const uint32_t fi = sj.source_column;
      const uint32_t pos = (fi < it->second.size()) ? it->second[fi] : 0;
      if (pos > 0) sj.source_column = pos - 1;  // 1-based f2p -> 0-based packed
    }
  }

  for (auto &s : *steps) {
    // Aggregate-stamped steps ship group rows computed from FULL rows (the
    // spec addresses original field ordinals); never trim them.
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
