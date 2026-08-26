#include "storage/lineairdb/ha_lineairdb.hh"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "lineairdb_keyenc.hh"
#include "my_dbug.h"
#include "sql/field.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

// Optimizer statistics and cost-model entry points. These methods keep MySQL's
// table/index cardinality estimates aligned with LineairDB row counts, NDV
// stats, histograms, and batched remote-access costs.

static std::vector<std::pair<std::string, uint32_t>> index_ndv_descriptors(
    TABLE *table) {
  std::vector<std::pair<std::string, uint32_t>> descs;
  if (table == nullptr || table->s == nullptr)
    return descs;

  descs.reserve(table->s->keys);
  for (uint i = 0; i < table->s->keys; ++i) {
    KEY *key = table->key_info + i;
    const bool is_primary = (i == table->s->primary_key);
    descs.emplace_back(is_primary ? std::string()
                                  : std::string(key->name ? key->name : ""),
                       key->user_defined_key_parts);
  }
  return descs;
}

// Decode a MySQL range endpoint through key_restore() so Field owns signedness
// and key-format decoding. Unsupported shapes fall back to coarse estimates.
static bool decode_int_keypart(TABLE *table, KEY *key, uint part,
                               const key_range *range, longlong *out_value) {
  if (table == nullptr || key == nullptr || range == nullptr ||
      range->key == nullptr || out_value == nullptr) {
    return false;
  }
  if (part >= key->user_defined_key_parts) return false;

  uint required = 0;
  for (uint i = 0; i <= part; ++i) {
    required += key->key_part[i].store_length;
  }
  if (range->length < required) return false;

  const KEY_PART_INFO &kp = key->key_part[part];
  Field *field = kp.field;
  if (field == nullptr || field->result_type() != INT_RESULT) return false;
  if (field->is_nullable()) return false;
  if (kp.key_part_flag & HA_REVERSE_SORT) return false;
  if (table->record[0] == nullptr || table->record[1] == nullptr) return false;

  uchar *scratch = table->record[1];
  key_restore(scratch, range->key, key, range->length);
  const ptrdiff_t delta = static_cast<ptrdiff_t>(scratch - table->record[0]);
  field->move_field_offset(delta);
  *out_value = field->val_int();
  field->move_field_offset(-delta);
  return true;
}

bool ha_lineairdb::seed_row_count_from_cache(LineairDBProxy *proxy) {
  if (proxy == nullptr || share == nullptr || db_table_name.empty())
    return false;

  const auto &stats_cache = proxy->cached_table_stats();
  auto it = stats_cache.find(db_table_name);
  if (it == stats_cache.end() || it->second <= 0)
    return false;

  share->stats_base_records.store(static_cast<uint64_t>(it->second),
                                  std::memory_order_relaxed);
  for (auto &shard : share->rowcount_shards)
    shard.delta.store(0, std::memory_order_relaxed);
  return true;
}

void ha_lineairdb::load_index_stats_from_cache(LineairDBProxy *proxy) {
  if (proxy == nullptr || share == nullptr)
    return;

  const uint64_t records =
      share->stats_base_records.load(std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock(share->index_ndv_mu_);
  share->index_ndv_.clear();
  share->index_hist_.clear();
  for (const auto &entry : proxy->last_index_ndv()) {
    if (entry.second.available)
      share->index_ndv_[entry.first] = entry.second.values;
  }
  for (const auto &entry : proxy->last_index_hist()) {
    const auto &hist = entry.second;
    if (!hist.available || hist.bounds.empty() ||
        hist.bounds.size() != hist.cum.size())
      continue;

    bool monotone = true;
    for (size_t i = 1; i < hist.cum.size(); ++i) {
      if (hist.cum[i] < hist.cum[i - 1] ||
          hist.bounds[i] < hist.bounds[i - 1]) {
        monotone = false;
        break;
      }
    }
    if (!monotone)
      continue;

    LineairDB_share::RangeHist range_hist;
    range_hist.bounds = hist.bounds;
    range_hist.cum = hist.cum;
    share->index_hist_[entry.first] = std::move(range_hist);
  }
  share->index_ndv_records_.store(records, std::memory_order_relaxed);
  share->index_ndv_loaded_.store(true, std::memory_order_relaxed);
}

void ha_lineairdb::mark_stale_index_ndv_for_select() {
  if (!srv_stats_drift_refresh)
    return;

  if (share == nullptr ||
      !share->index_ndv_loaded_.load(std::memory_order_relaxed))
    return;

  THD *thd = ha_thd();
  const bool is_select = thd != nullptr && thd->lex != nullptr &&
                         thd->lex->sql_command == SQLCOM_SELECT;
  if (!is_select)
    return;

  const uint64_t at_fetch =
      share->index_ndv_records_.load(std::memory_order_relaxed);
  const uint64_t now =
      share->stats_base_records.load(std::memory_order_relaxed);
  const uint64_t hi = std::max(at_fetch, now);
  const uint64_t lo = std::min(at_fetch, now);
  // Refetch when the gap is more than 20% of the larger row count.
  if (hi == 0 || (hi - lo) * 5 <= hi)
    return;

  share->index_ndv_force_refresh_.store(true, std::memory_order_relaxed);
  share->index_ndv_loaded_.store(false, std::memory_order_relaxed);
}

void ha_lineairdb::seed_optimizer_stats() {
  if (share == nullptr || db_table_name.empty())
    return;

  const bool need_rowcount =
      share->stats_base_records.load(std::memory_order_relaxed) == 0;
  mark_stale_index_ndv_for_select();
  const bool need_ndv =
      !share->index_ndv_loaded_.load(std::memory_order_relaxed);
  const bool force_ndv =
      share->index_ndv_force_refresh_.load(std::memory_order_relaxed);
  if (!need_rowcount && !need_ndv && !force_ndv)
    return;

  THD *thd = ha_thd();
  if (thd == nullptr)
    return;

  userThread = thd;
  LineairDBProxy *proxy = get_proxy();
  if (proxy == nullptr)
    return;

  // Existing path: use BEGIN/END piggyback stats when available.
  const bool seeded = seed_row_count_from_cache(proxy);

  // Cold optimizer paths can reach info() before tx_begin.
  if (!seeded || need_ndv || force_ndv) {
    bool fetched = false;
    bool requested_ndv = false;
    if (need_ndv || force_ndv) {
      const auto descs = index_ndv_descriptors(table);
      // Consume ANALYZE's force flag only when issuing the NDV RPC.
      const bool force = share->index_ndv_force_refresh_.exchange(
          false, std::memory_order_relaxed);
      fetched = proxy->fetch_table_stats(db_table_name, descs, force);
      requested_ndv = true;
    } else {
      fetched = proxy->fetch_table_stats();
    }
    if (fetched) {
      seed_row_count_from_cache(proxy);
      if (requested_ndv)
        load_index_stats_from_cache(proxy);
    }
  }
}

int ha_lineairdb::info(uint flag) {
  DBUG_TRACE;

  if (table == nullptr || table->s == nullptr) {
    if (stats.records < 2)
      stats.records = 2;
    return 0;
  }

  // print_error asks which key a duplicate landed on through this flag.
  if (flag & HA_STATUS_ERRKEY) {
    errkey = duplicate_key_index_;
  }

  if (flag & (HA_STATUS_VARIABLE | HA_STATUS_CONST)) {
    seed_optimizer_stats();

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

    THD *thd = ha_thd();
    if (thd != nullptr) {
      LineairDBTransaction *active_tx = active_transaction(thd);
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

int ha_lineairdb::analyze(THD *, HA_CHECK_OPT *) {
  DBUG_TRACE;

  if (share != nullptr) {
    share->stats_base_records.store(0, std::memory_order_relaxed);
    for (auto &shard : share->rowcount_shards)
      shard.delta.store(0, std::memory_order_relaxed);
    share->index_ndv_loaded_.store(false, std::memory_order_relaxed);
    share->index_ndv_force_refresh_.store(true, std::memory_order_relaxed);
  }

  info(HA_STATUS_VARIABLE | HA_STATUS_CONST);
  return HA_ADMIN_OK;
}

void ha_lineairdb::set_generic_rec_per_key(KEY *key, uint key_parts,
                                           bool is_primary) {
  bool is_unique = (key->flags & HA_NOSAME);

  // Prefer server-measured NDV; otherwise use a uniform-distribution fallback.
  std::vector<uint64_t> ndv;
  if (share != nullptr &&
      share->index_ndv_loaded_.load(std::memory_order_relaxed)) {
    const std::string index_name =
        is_primary ? std::string() : std::string(key->name ? key->name : "");
    std::lock_guard<std::mutex> lock(share->index_ndv_mu_);
    auto it = share->index_ndv_.find(index_name);
    if (it != share->index_ndv_.end() && it->second.size() >= key_parts)
      ndv = it->second;
  }
  const bool has_ndv = ndv.size() >= key_parts;

  // How much each additional key part narrows the result set (fallback path).
  double per_part = std::max(
      2.0, std::pow(static_cast<double>(stats.records), 1.0 / key_parts));

  for (uint j = 0; j < key_parts; j++) {
    ulong rpk;
    if ((is_primary || is_unique) && j == key_parts - 1) {
      rpk = 1;
    } else if (has_ndv && ndv[j] > 0) {
      const uint64_t records = static_cast<uint64_t>(stats.records);
      const uint64_t distinct = ndv[j];
      rpk = static_cast<ulong>(
          std::max<uint64_t>(1, (records + distinct - 1) / distinct));
    } else {
      // per_part^(j+1) is the total divisor for j+1 key parts.
      double selectivity = std::pow(per_part, static_cast<double>(j + 1));
      rpk = static_cast<ulong>(
          std::max(1.0, static_cast<double>(stats.records) / selectivity));
    }
    key->rec_per_key[j] = rpk;
    key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk));
  }
}

ha_rows ha_lineairdb::records_in_range(uint inx, key_range *min_key,
                                       key_range *max_key) {
  DBUG_TRACE;

  if (table == nullptr || table->s == nullptr) {
    return 10;
  }

  KEY *key = table->key_info + inx;
  if (key == nullptr) {
    return 10;
  }

  ha_rows total_records = stats.records;
  if (total_records < 2)
    total_records = 2;

  if (min_key == nullptr && max_key == nullptr) {
    return total_records;
  }

  uint key_parts_used = 0;
  if (min_key != nullptr) {
    key_parts_used = calculate_key_parts_from_length(key, min_key->length);
  } else if (max_key != nullptr) {
    key_parts_used = calculate_key_parts_from_length(key, max_key->length);
  }

  if ((key->flags & HA_NOSAME) &&
      key_parts_used == key->user_defined_key_parts) {
    return 1;
  }

  if (key_parts_used == 0) {
    return total_records;
  }

  uint eq_parts = 0;
  if (min_key != nullptr && max_key != nullptr) {
    // Compare endpoints part-by-part to find the equality prefix before the
    // first range condition.
    uint cmp_len = std::min(min_key->length, max_key->length);
    uint consumed = 0;
    for (uint p = 0; p < key->user_defined_key_parts && consumed < cmp_len;
         p++) {
      uint part_len = key->key_part[p].store_length;
      if (consumed + part_len > cmp_len)
        break;
      if (memcmp(min_key->key + consumed, max_key->key + consumed, part_len) ==
          0) {
        eq_parts++;
        consumed += part_len;
      } else {
        break;
      }
    }
  } else if (min_key != nullptr) {
    if (min_key->flag == HA_READ_KEY_EXACT ||
        min_key->flag == HA_READ_KEY_OR_NEXT) {
      eq_parts = key_parts_used;
    }
  }

  ha_rows estimate;
  if (eq_parts > 0) {
    // Use rec_per_key at the equality depth; refine a trailing integer range
    // when the endpoint can be decoded safely.
    uint rpk_idx = eq_parts - 1;
    if (rpk_idx < key->user_defined_key_parts) {
      estimate = static_cast<ha_rows>(key->rec_per_key[rpk_idx]);
    } else {
      estimate = 1;
    }

    if (eq_parts < key_parts_used) {
      longlong lo = 0;
      longlong hi = 0;
      if (eq_parts < key->user_defined_key_parts &&
          key->rec_per_key[eq_parts] > 0 &&
          decode_int_keypart(table, key, eq_parts, min_key, &lo) &&
          decode_int_keypart(table, key, eq_parts, max_key, &hi) &&
          hi >= lo) {
        const double range_vals =
            (hi > lo) ? static_cast<double>(hi - lo) : 1.0;
        double est =
            range_vals * static_cast<double>(key->rec_per_key[eq_parts]);

        const double cap = static_cast<double>(key->rec_per_key[rpk_idx]);
        if (cap >= 1.0 && est > cap) est = cap;
        if (est < 1.0) est = 1.0;
        estimate = static_cast<ha_rows>(est);
      } else {
        estimate = std::max(static_cast<ha_rows>(1), estimate / 2);
      }
    }
  } else {
    if (min_key != nullptr && max_key != nullptr) {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 20);
    } else {
      estimate = std::max(static_cast<ha_rows>(2), total_records / 10);
    }

    // Leading-key range histogram is a floor over the heuristic. Missing or
    // malformed stats keep the fallback estimate.
    const bool is_primary = table->s != nullptr && inx == table->s->primary_key;
    const std::string index_name =
        is_primary ? std::string() : std::string(key->name ? key->name : "");
    if (share != nullptr) {
      std::lock_guard<std::mutex> lock(share->index_ndv_mu_);
      auto hist_it = share->index_hist_.find(index_name);
      if (hist_it != share->index_hist_.end()) {
        const LineairDB_share::RangeHist &hist = hist_it->second;
        if (!hist.bounds.empty() && hist.bounds.size() == hist.cum.size()) {
          auto rank_le = [&](const std::string &encoded_key) -> double {
            if (encoded_key >= hist.bounds.back())
              return static_cast<double>(hist.cum.back());
            auto it =
                std::upper_bound(hist.bounds.begin(), hist.bounds.end(),
                                 encoded_key);
            if (it == hist.bounds.begin())
              return 0.0;
            const size_t pos =
                static_cast<size_t>((it - hist.bounds.begin()) - 1);
            return static_cast<double>(hist.cum[pos]);
          };

          constexpr key_part_map kLeadingPart = 1;
          bool enc_ok = true;
          double lo = 0.0;
          double hi = static_cast<double>(hist.cum.back());
          if (min_key != nullptr) {
            std::string encoded = lineairdb_keyenc::convert_key_to_ldbformat(
                table, inx, min_key->key, kLeadingPart);
            if (encoded.empty())
              enc_ok = false;
            else
              lo = rank_le(encoded);
          }
          if (enc_ok && max_key != nullptr) {
            std::string encoded = lineairdb_keyenc::convert_key_to_ldbformat(
                table, inx, max_key->key, kLeadingPart);
            if (encoded.empty())
              enc_ok = false;
            else
              hi = rank_le(encoded);
          }
          const double hist_est = enc_ok ? (hi - lo) : -1.0;
          if (hist_est >= 1.0 &&
              static_cast<ha_rows>(hist_est) > estimate) {
            estimate = static_cast<ha_rows>(hist_est);
          }
        }
      }
    }
  }

  if (estimate < 1)
    estimate = 1;

  return estimate;
}

bool ha_lineairdb::should_charge_materialization_cost(
    uint index, double rows [[maybe_unused]]) const {
  const TABLE *t = table;
  if (t == nullptr || t->in_use == nullptr) return true;

  if (t->s != nullptr && t->s->primary_key != MAX_KEY &&
      index == t->s->primary_key) {
    return false;
  }

  return true;
}
