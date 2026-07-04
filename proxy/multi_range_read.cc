#include "storage/lineairdb/ha_lineairdb.hh"

#include <string>
#include <utility>
#include <vector>

#include "lineairdb_prefetch.hh"
#include "sql/table.h"

// Handler MRR/read_range entry points. The custom path batches primary-key
// point lookups into one LineairDB RPC; unsupported ranges fall back to MySQL's
// default DS-MRR implementation.

// True only for MySQL's standard forward index-range sequence. Reverse ranges
// and BKA callbacks do not match the forward-staged prefetch cache.
static bool lineairdb_is_forward_index_range_sequence(RANGE_SEQ_IF *seq) {
  extern range_seq_t quick_range_seq_init(void *, uint, uint);
  return seq != nullptr && seq->init == quick_range_seq_init;
}

ha_rows ha_lineairdb::multi_range_read_info_const(
    uint keyno, RANGE_SEQ_IF *seq, void *seq_init_param, uint n_ranges,
    uint *bufsz, uint *flags, bool *force_default_mrr, Cost_estimate *cost) {
  ha_rows rows = handler::multi_range_read_info_const(
      keyno, seq, seq_init_param, n_ranges, bufsz, flags, force_default_mrr,
      cost);
  if (rows == HA_POS_ERROR) return rows;

  // Custom batch MRR only in batched mode; prefetch serves reads from the cache.
  if (!predict_prefetch_mode(ha_thd()) && keyno == table->s->primary_key) {
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
  if (!predict_prefetch_mode(ha_thd()) && keyno == table->s->primary_key) {
    *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
    *bufsz = 0;
    if (cost) {
      cost->reset();
      cost->add_io(1.0);
    }
  }
  return rows;
}

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
    const bool legacy_dml = prefetch_needs_legacy_dml_handler(ha_thd(), tx);
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
      if (int err = maybe_prefetch_for_statement(ha_thd(), tx, table))
        return err;
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

  range_seq_t seq_ctx = seq->init(seq_init_param, n_ranges, mode);
  KEY_MULTI_RANGE range;
  std::vector<std::string> batch_keys;
  std::vector<char *> range_infos;

  const uint pk_parts = table->key_info[active_index].user_defined_key_parts;
  const key_part_map full_key_map =
      (pk_parts < sizeof(key_part_map) * 8)
          ? ((static_cast<key_part_map>(1) << pk_parts) - 1)
          : ~static_cast<key_part_map>(0);

  while (seq->next(seq_ctx, &range) == 0) {
    // Only batch full-key point lookups. Partial-key ranges and inequality
    // scans must use DS-MRR because batch_read only accepts exact keys.
    if (!(range.range_flag & EQ_RANGE) ||
        (range.start_key.keypart_map & full_key_map) != full_key_map) {
      mrr_use_batch_ = false;
      m_ds_mrr.init(table);
      return m_ds_mrr.dsmrr_init(seq, seq_init_param, n_ranges,
                                 mode | HA_MRR_USE_DEFAULT_IMPL, buf);
    }
    std::string ldb_key = convert_key_to_ldbformat(
        range.start_key.key, range.start_key.keypart_map);
    batch_keys.push_back(ldb_key);
    range_infos.push_back(range.ptr);
  }

  mrr_use_batch_ = true;
  mrr_buffer_.clear();
  mrr_buffer_pos_ = 0;

  tx->choose_table(db_table_name);

  if (batch_keys.empty()) return 0;

  auto results = tx->batch_read(batch_keys);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

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
