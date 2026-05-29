#include "lineairdb_transaction.hh"
#include "mem_probe.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

#include <thread>
#include <unordered_set>
#include <cstdio>
#include <cstdlib>
#include <cstring>  // std::strcmp (HELIOS_BORROW_SERVE / range-hash gates)

namespace {

std::string next_lexicographic_key(std::string key) {
  for (size_t i = key.size(); i-- > 0;) {
    auto byte = static_cast<unsigned char>(key[i]);
    if (byte != 0xFF) {
      key[i] = static_cast<char>(byte + 1);
      key.resize(i + 1);
      return key;
    }
  }
  return {};
}

bool range_validation_is_logical(
    const LineairDBProxy::RangeValidationEntry& entry) {
  return !entry.end_key.empty();
}

bool range_validation_can_be_sliced(
    const std::vector<LineairDBProxy::RangeValidationEntry>& ranges,
    const std::vector<LineairDBProxy::IndexValidationEntry>& indexes) {
  if (!indexes.empty()) return false;
  // Physical entries (end_key.empty(), Codex helios PHYSICAL_OCC) carry no
  // result_keys — slicing is trivially a no-op on them and the captured
  // (owner,node_ptr,version) covers the wider range that holds the probe.
  // So they are sliceable. Logical entries still need at least one entry
  // with a key list to be useful for negative-membership; we keep the
  // existing "must have entries" guard.
  return !ranges.empty();
}

std::string trace_count_event(const char* kind, const std::string& table_name,
                              size_t count) {
  return std::string(kind) + ":" + table_name + ":n=" +
         std::to_string(count);
}

std::string trace_plan_scan_event(const std::string& table_name,
                                  const std::string& index_name,
                                  size_t keys, size_t values,
                                  uint64_t limit, bool for_each) {
  std::string event = "plan_fetch:S:" + table_name;
  if (!index_name.empty()) event += ":" + index_name;
  event += ":keys=" + std::to_string(keys);
  event += ":vals=" + std::to_string(values);
  if (limit > 0) event += ":lim=" + std::to_string(limit);
  if (for_each) event += ":each";
  return event;
}

}  // namespace

LineairDBTransaction::LineairDBTransaction(THD* thd, 
                                            LineairDBProxy* lineairdb_proxy,
                                            handlerton* lineairdb_hton,
                                            bool isFence) 
    : tx_id(-1), 
      lineairdb_proxy(lineairdb_proxy),
      thread(thd), 
      isTransaction(false), 
      hton(lineairdb_hton),
      isFence(isFence),
      is_aborted_(false)
    {}

std::string LineairDBTransaction::get_selected_table_name() { return db_table_key; }

void LineairDBTransaction::choose_table(std::string db_table_name) {
  if (db_table_key != db_table_name) {
    pushed_filter_.clear();
  }
  db_table_key = db_table_name;
}

bool LineairDBTransaction::table_is_not_chosen() {
  if (db_table_key.size() == 0) {
    LOG_WARNING("Database and Table is not chosen in LineairDBTransaction");
    return true;
  }
  return false;
}

const std::pair<const std::byte *const, const size_t>
LineairDBTransaction::read(std::string key) {
  if (table_is_not_chosen()) return std::pair<const std::byte *const, const size_t>{nullptr, 0};

  // Phase-3b: fire staged oneshot plan on first cache-targeting access
  execute_pending_oneshot_plan();

  // Silo-style local view: own writes are visible before remote reads
  if (auto entry = lookup_local_write_set(db_table_key, key)) {
    rpc_trace_.record_local_view("read_write_hit");
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Repeat exact-key reads can use the local read set
  if (auto entry = lookup_local_read_set(db_table_key, key)) {
    rpc_trace_.record_local_view("read_cache_hit");
    activate_local_read(*entry);
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Positive covering: a row prefetched by a FER/FES sub-scan lives ONLY in a
  // range entry (push_local_range_scan never calls record_local_read), so an
  // exact-PK point read would false-miss it here and abort. (Q2's correlated
  // MIN joins part->partsupp by a ps_partkey prefix sub-scan, then the handler
  // issues a full-PK point read on a matched partsupp row.) Serve the row's
  // value from the range entry and record its per-row TID as a commit
  // obligation so a concurrent value UPDATE is still detected (physical node
  // validation alone misses value-only changes).
  if (oneshot_mode_) {
    if (auto hit = lookup_positive_covering_range_row(db_table_key, key)) {
      rpc_trace_.record_local_view("read_range_hit");
      const uint64_t tid = hit->entry->row_tids[hit->row_idx];
      record_stateless_read(db_table_key, key, true, tid);
      last_read_value_ = hit->entry->rows[hit->row_idx].second;
      return {reinterpret_cast<const std::byte*>(last_read_value_.data()),
              last_read_value_.size()};
    }
  }

  // Negative caching: if a completed, unlimited PK range scan already
  // covers this key and its authoritative pre-filter key set does NOT contain
  // it, the row is provably absent — answer not-found locally instead of an
  // RPC. (Q2's correlated MIN subquery point-reads partsupp by (ps_partkey,
  // ps_suppkey) for many suppliers that don't stock the part; the FER prefix
  // scan already proved those rows absent.) The covering range's phantom
  // validation is activated so a concurrent INSERT still aborts at commit.
  if (oneshot_mode_) {
    if (const LocalRangeScanEntry* cov =
            find_negative_covering_range_scan(db_table_key, key)) {
      rpc_trace_.record_local_view("read_negative_hit");
      activate_range_validation(cov->range_versions, cov->index_reads);
      record_local_read(db_table_key, key, false, "");  // non-validating; OCC via range
      return std::pair<const std::byte *const, const size_t>{nullptr, 0};
    }
  }

  // Normal path misses go to the server; oneshot plans must prefetch them
  rpc_trace_.record_local_view("read_miss:" + db_table_key);
  if (oneshot_mode_) {
    // Isolated bench: abort on prefetch miss instead of the per-probe stateless
    // fallback (see note_oneshot_miss). Diagnoses which probe the plan failed
    // to cover.
    if (note_oneshot_miss("read", db_table_key, key))
      return std::pair<const std::byte *const, const size_t>{nullptr, 0};
    auto sr = lineairdb_proxy->tx_stateless_read(db_table_key, key);
    if (!sr.ok) {
      rpc_trace_.record_local_view("abort_oneshot_read_fallback_rpc");
      is_aborted_ = true;
      return std::pair<const std::byte *const, const size_t>{nullptr, 0};
    }
    record_local_read(db_table_key, key, sr.found, sr.value, sr.tid, true);
    // (d/P1) Validate this fallback read at commit. record_local_read only
    // enters stateless_read_set_ (the commit-validated set) when the entry is
    // re-read (activate_local_read); a single read would otherwise go
    // unvalidated. Record it directly so its TID is checked at commit.
    record_stateless_read(db_table_key, key, sr.found, sr.tid);
    if (!sr.found) {
      return std::pair<const std::byte *const, const size_t>{nullptr, 0};
    }
    last_read_value_ = sr.value;
    return {reinterpret_cast<const std::byte *>(last_read_value_.data()),
            last_read_value_.size()};
  }

  ensure_started_for_normal_rpc();  // (d/P1-a) oneshot may be off w/ tx unstarted
  last_read_value_ = lineairdb_proxy->tx_read(this, key);
  if (last_read_value_.empty()) {
    record_local_read(db_table_key, key, false, ""); // value unused when not found
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  record_local_read(db_table_key, key, true, last_read_value_);

  return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
}

std::vector<std::pair<bool, std::string>>
LineairDBTransaction::batch_read(const std::vector<std::string>& keys) {
  if (table_is_not_chosen()) return {};

  // Phase-3b: fire staged oneshot plan before consulting the cache. The PK
  // MRR / secondary-payload batching paths land here without going through
  // read()/get_matching_keys_*, so without this hook the plan stays pending
  // and every cache lookup misses, aborting the transaction.
  execute_pending_oneshot_plan();

  std::vector<std::pair<bool, std::string>> pairs;
  pairs.resize(keys.size());

  std::vector<std::string> rpc_keys;
  std::vector<size_t> rpc_positions;
  rpc_keys.reserve(keys.size());
  rpc_positions.reserve(keys.size());

  // Resolve keys covered by the local read/write sets first
  for (size_t i = 0; i < keys.size(); ++i) {
    if (auto entry = lookup_local_write_set(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("batch_write_hit");
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    if (auto entry = lookup_local_read_set(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("batch_cache_hit");
      activate_local_read(*entry);
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    // Positive covering: serve a FER/FES-prefetched row from its range entry
    // (see read()), recording the per-row TID obligation. Without this, batched
    // / MRR PK reads hit the same false miss read() guards against.
    if (oneshot_mode_) {
      if (auto hit = lookup_positive_covering_range_row(db_table_key, keys[i])) {
        rpc_trace_.record_local_view("batch_range_hit");
        const uint64_t tid = hit->entry->row_tids[hit->row_idx];
        record_stateless_read(db_table_key, keys[i], true, tid);
        pairs[i] = {true, hit->entry->rows[hit->row_idx].second};
        continue;
      }
      // Negative covering: prove the batched key absent from a covering range
      // (mirrors read()); else absent MRR probes would false-miss and abort
      // (Codex review). Activate the covering range's validation so a concurrent
      // INSERT of this key still aborts at commit.
      if (const LocalRangeScanEntry* cov =
              find_negative_covering_range_scan(db_table_key, keys[i])) {
        rpc_trace_.record_local_view("batch_negative_hit");
        activate_range_validation(cov->range_versions, cov->index_reads);
        record_local_read(db_table_key, keys[i], false, "");
        pairs[i] = {false, ""};
        continue;
      }
    }
    rpc_trace_.record_local_view("batch_miss");
    rpc_positions.push_back(i);
    rpc_keys.push_back(keys[i]);
  }

  // Oneshot plans must fetch every key up front; misses mean the plan is short
  if (oneshot_mode_ && !rpc_keys.empty()) {
    // Isolated bench: abort on prefetch miss instead of the stateless batch
    // fallback.
    if (note_oneshot_miss("batch_read", db_table_key, rpc_keys.front()))
      return pairs;
    std::vector<LineairDBProxy::StatelessReadKey> srk;
    srk.reserve(rpc_keys.size());
    for (const auto& k : rpc_keys) srk.push_back({db_table_key, k});
    auto res = lineairdb_proxy->tx_stateless_batch_read(srk);
    if (res.size() != rpc_keys.size()) {
      rpc_trace_.record_local_view("abort_oneshot_batch_fallback_rpc");
      is_aborted_ = true;
      return pairs;
    }
    for (size_t i = 0; i < res.size(); ++i) {
      const size_t pos = rpc_positions[i];
      pairs[pos] = {res[i].found, res[i].value};
      record_local_read(db_table_key, rpc_keys[i], res[i].found, res[i].value,
                        res[i].tid, true);
      // (d/P1) validate each fallback read at commit (see read()).
      record_stateless_read(db_table_key, rpc_keys[i], res[i].found, res[i].tid);
    }
    return pairs;
  }

  // Fetch only cache misses; tx_batch_read() returns rows in rpc_keys order
  //   Example: keys=[A,B,C], B is local -> rpc_keys=[A,C],
  //            rpc_positions=[0,2], so RPC results fill pairs[0] and pairs[2].
  if (!rpc_keys.empty()) {
    ensure_started_for_normal_rpc();  // (d/P1-a) oneshot may be off w/ tx unstarted
    auto results = lineairdb_proxy->tx_batch_read(this, rpc_keys);
    if (results.size() != rpc_keys.size()) {
      rpc_trace_.record_local_view("abort_batch_size_mismatch");
      is_aborted_ = true;
      return pairs;
    }
    for (size_t i = 0; i < results.size(); ++i) {
      // Map each RPC result back to the original keys[] position
      const size_t pos = rpc_positions[i];
      pairs[pos] = {results[i].found, std::move(results[i].value)};
      record_local_read(db_table_key, keys[pos], pairs[pos].first, pairs[pos].second);
    }
  }
  return pairs;
}

void LineairDBTransaction::prefetch_stateless_reads(
    const std::vector<LineairDBProxy::StatelessReadKey>& reads) {
  if (!oneshot_mode_ || reads.empty()) return;

  std::vector<LineairDBProxy::StatelessReadKey> rpc_reads;
  rpc_reads.reserve(reads.size());
  std::unordered_set<std::string> seen;
  seen.reserve(reads.size());

  // Keep the plan prefetch to rows not already covered by the local view
  for (const auto& read : reads) {
    const std::string seen_key = read.table_name + '\0' + read.key;
    if (!seen.insert(seen_key).second) continue;
    if (lookup_local_write_set(read.table_name, read.key)) continue;
    if (lookup_local_read_set(read.table_name, read.key)) continue;
    rpc_reads.push_back(read);
  }

  if (rpc_reads.empty()) return;

  auto results = lineairdb_proxy->tx_stateless_batch_read(rpc_reads);
  if (results.size() != rpc_reads.size()) {
    rpc_trace_.record_local_view("abort_prefetch_size_mismatch");
    is_aborted_ = true;
    return;
  }

  // Store prefetched rows in the local cache; validate them only if MySQL reads them
  for (size_t i = 0; i < rpc_reads.size(); ++i) {
    const auto& read = rpc_reads[i];
    auto& result = results[i];
    if (!result.ok) {
      rpc_trace_.record_local_view("abort_prefetch_rpc");
      is_aborted_ = true;
      return;
    }
    if (result.found) {
      record_local_read(read.table_name, read.key, true, result.value,
                        result.tid, true);
    } else {
      record_local_read(read.table_name, read.key, false, "",
                        result.tid, true); // value unused when not found
    }
  }
}

void LineairDBTransaction::stage_oneshot_plan(
    std::vector<LineairDBProxy::ReadPlanStep> steps) {
  if (!oneshot_mode_ || steps.empty()) return;
  pending_oneshot_plan_steps_ = std::move(steps);
}

void LineairDBTransaction::execute_pending_oneshot_plan() {
  if (pending_oneshot_plan_steps_.empty()) return;
  auto steps = std::move(pending_oneshot_plan_steps_);
  pending_oneshot_plan_steps_.clear();
  // Drop into the existing execute_read_plan path — that one attaches the
  // current pushed_filter_ to primary-PK S: steps before sending, which is
  // exactly the point of the deferred-execution refactor.
  execute_read_plan(steps);
}

void LineairDBTransaction::execute_read_plan(
    const std::vector<LineairDBProxy::ReadPlanStep>& steps) {
  if (!oneshot_mode_ || steps.empty()) return;

  // Phase-3b: copy steps so we can stamp the current pushed_filter_ onto each
  // primary-PK S: step. cond_push runs before MySQL hits the first scan, and
  // rnd_init / index_init have already propagated pushed_filter_serialized_
  // to the tx via set_pushed_filter() by the time this method fires (because
  // the plan is now staged and only executed from the first read/scan call).
  // Per Codex review: pushed_filter_ was serialized for whatever handler
  // last called set_pushed_filter(), so we may only stamp it onto an S: step
  // whose table_name matches the current scan's table (db_table_key). For a
  // multi-table plan (e.g. lineitem + part) the filter is for one of them;
  // stamping it on every primary scan step would let a predicate on lineitem
  // columns reject part rows by index, producing incorrect results.
  //
  // Phase-3c: this stamping path is now LIVE for TPC-H SELECT full scans —
  // rnd_init derives a single-table predicate via build_single_table_filter()
  // and set_pushed_filter(), so S: steps actually ship a filter to the server.
  // Range validation stays sound because the server builds the logical
  // result_keys from the PRE-filter full range (database_impl.h:480) and
  // lookup_local_range_scan() preserves that full key set (never narrows it to
  // filter-passing rows). Remaining caveats, benign for read-only TPC-H but to
  // close before HTAP / concurrent writes:
  //   (P1) CLOSED (c): the server now ships filter-rejected rows' (key, tid) in
  //        StepResult.filtered_keys/tids and the proxy records them as
  //        validating stateless reads below, so a concurrent UPDATE flipping a
  //        rejected row into the predicate before commit changes its TID and
  //        aborts. (Both the full-S filter and the FER/FES fe_reject paths.)
  //   (P2) local_range_scans_ cache key does not include the filter, so a later
  //        same-range scan in the same tx with a different predicate could hit
  //        the cached entry. TPC-H issues one scan per table per query, so this
  //        does not arise today; include filter_serialized in the key to close.
  std::vector<LineairDBProxy::ReadPlanStep> steps_with_filter;
  steps_with_filter.reserve(steps.size());
  for (const auto& step : steps) {
    steps_with_filter.push_back(step);
    auto& s = steps_with_filter.back();
    const bool stamp_eligible = s.is_scan && !s.for_each &&
                                s.index_name.empty() &&
                                !pushed_filter_.empty() &&
                                s.table_name == db_table_key;
    if (stamp_eligible) {
      s.filter_serialized = pushed_filter_;
    }
  }

  rpc_trace_.record_local_view("plan_request:steps=" +
                               std::to_string(steps_with_filter.size()));
  // HELIOS_TIMEPROF: per-phase latency breakdown. ns timestamps around each
  // major proxy-side phase so a profile can attribute time to RPC / ingest /
  // MySQL handler work / commit. Aggregated counter is printed at commit.
  const bool timeprof = std::getenv("HELIOS_TIMEPROF") != nullptr;
  auto tp_now = []() {
    timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return static_cast<uint64_t>(t.tv_sec) * 1000000000ull + t.tv_nsec;
  };
  const uint64_t tp_rpc_start = timeprof ? tp_now() : 0;
  auto result =
      lineairdb_proxy->tx_execute_read_plan(steps_with_filter, ro_novalidate_);
  const uint64_t tp_rpc_end = timeprof ? tp_now() : 0;
  if (!result.ok || result.steps.size() != steps.size()) {
    rpc_trace_.record_local_view("abort_read_plan_rpc");
    is_aborted_ = true;
    return;
  }
  // helios 2-RPC PHYSICAL OCC (Step 5): non-zero => server retained range/
  // index OCC under this key. We just stash it; at commit we echo it.
  tx_occ_key_ = result.tx_occ_key;
  if (timeprof) {
    tp_rpc_execute_ns_ += (tp_rpc_end - tp_rpc_start);
    ++tp_rpc_execute_count_;
    std::fprintf(stderr,
        "[TIMEPROF] rpc_exec ok=%d tx_occ_key=%llu steps=%zu\n",
        result.ok ? 1 : 0, (unsigned long long)result.tx_occ_key,
        result.steps.size());
    std::fflush(stderr);
  }
  const uint64_t tp_ingest_start = timeprof ? tp_now() : 0;

  const bool memprof = std::getenv("HELIOS_MEMPROF") != nullptr;
  const size_t je_pre_ingest = memprof ? helios_mem::je_allocated() : 0;
  for (size_t i = 0; i < result.steps.size() && i < steps.size(); ++i) {
    const auto& step = steps[i];
    auto& step_result = result.steps[i];  // local result: move-from at ingest

    if (!step.is_scan && !step.for_each) {
      rpc_trace_.record_local_view(trace_count_event(
          step_result.found ? "plan_fetch:R:hit" : "plan_fetch:R:miss",
          step.table_name, 1));
      if (step_result.found) {
        record_local_read(step.table_name, step_result.actual_key, true,
                          step_result.value, step_result.tid, true);
      } else {
        record_local_read(step.table_name, step_result.actual_key, false, "",
                          step_result.tid, true);
      }
      continue;
    }

    rpc_trace_.record_local_view(trace_plan_scan_event(
        step.table_name, step.index_name, step_result.scan_keys.size(),
        step_result.scan_values.size(), step.scan_limit, step.for_each));

    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint64_t> row_tids;
    rows.reserve(step_result.scan_keys.size());
    row_tids.reserve(step_result.scan_keys.size());
    // Only the non-for_each PRIMARY path consumes `rows` (push_local_range_scan
    // below). For a secondary scan, scan_keys is moved wholesale into
    // cached.primary_keys later, so it must stay intact here (Codex P1: moving it
    // into the unused `rows` would install moved-from strings as primary keys).
    const bool build_primary_rows = !step.for_each && step.index_name.empty();
    // Step2c: a FULL-COVER primary scan (start_key=="", finite end) stores every
    // found row in the range entry (`rows`) AND used to ALSO copy it into
    // local_read_set_ — a redundant second materialization (Q21 step0: 6M rows,
    // ~1GB, ~part of the 8.4s ingest). For these rows local_read_set_ is dead:
    //   - serving: a later exact-PK read()/batch_read() is answered by
    //     lookup_positive_covering_range_row, which DISCOVERS the entry via the
    //     "" full-cover bucket (the for_each sub-scan path already relies on this
    //     and never calls record_local_read).
    //   - OCC: positive-covering records the per-row TID on use; the scan's own
    //     range validation (range_versions / range-hash digest) covers the range.
    // Codex GO is gated to the full-cover case ONLY: positive-covering is not a
    // general interval lookup, so a BOUNDED primary range [A,Z) containing K is
    // NOT rediscoverable (unless A=="" / A==K / A∈keypart_prefixes(K)). Bounded,
    // reverse, tid-less, filtered-by-empty-tid, and secondary scans keep the
    // local_read_set_ copy. Not-found rows always keep it (negative covering is
    // only conditionally sound). (Codex review 2026-05-29.)
    const bool fullcover_skip_eligible =
        build_primary_rows && !step.reverse_scan &&
        step_result.actual_start_key.empty() &&
        !step_result.actual_end_key.empty();
    for (size_t j = 0; j < step_result.scan_keys.size(); ++j) {
      std::string& key = step_result.scan_keys[j];
      const uint64_t tid =
          j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
      const bool found =
          j < step_result.scan_values.size() && !step_result.scan_values[j].empty();
      if (found) {
        std::string& value = step_result.scan_values[j];
        const bool skip_local_readset =
            fullcover_skip_eligible && j < step_result.scan_tids.size() &&
            key_is_in_range(key, step_result.actual_start_key,
                            step_result.actual_end_key);
        // record_local_read copies key+value into local_read_set_; for the
        // full-cover-safe case we skip that copy (the row lives in the range
        // entry, served via positive-covering + range-validation OCC). Then
        // (primary path only) move the key/value out of the discarded RPC result
        // into the cache row — avoids a further materialization of every row.
        if (!skip_local_readset)
          record_local_read(step.table_name, key, true, value, tid, true);
        if (build_primary_rows) {
          rows.emplace_back(std::move(key), std::move(value));
          row_tids.push_back(tid);
        }
      } else {
        record_local_read(step.table_name, key, false, std::string(), tid, true);
      }
    }

    // (c) Validate rows the server's pushed filter REJECTED. They exist in the
    // scanned range but were dropped from the result; record each (key, tid) as
    // a validating read so a concurrent UPDATE that flips a rejected row's
    // predicate column INTO the matched set before commit is detected (its TID
    // changes → abort). Logical range key-list validation alone misses such
    // value-only changes. (P1 closed.)
    if (!step_result.filtered_keys.empty())
      rpc_trace_.record_local_view(trace_count_event(
          "filtered_validate", step.table_name, step_result.filtered_keys.size()));
    for (size_t j = 0; j < step_result.filtered_keys.size(); ++j) {
      const uint64_t ftid = j < step_result.filtered_tids.size()
                                ? step_result.filtered_tids[j]
                                : 0;
      record_stateless_read(step.table_name, step_result.filtered_keys[j], true,
                            ftid);
    }

    if (step.for_each) {
      // FER/FES join prefetch: one cache entry per source-row sub-scan so each
      // join probe hits the O(1) start-key index. (Plain FE point reads carry
      // no subscans and were already recorded above.)
      if (std::getenv("HELIOS_FE_DEBUG"))
        std::fprintf(stderr,
            "[INGEST] tbl=%s subscans=%zu flat_keys=%zu before_idx=%zu\n",
            step.table_name.c_str(), step_result.subscans.size(),
            step_result.scan_keys.size(), range_scan_index_.size());
      for (auto& sub : step_result.subscans) {
        if (step.index_name.empty()) {
          std::vector<std::pair<std::string, std::string>> srows;
          std::vector<uint64_t> stids;
          srows.reserve(sub.scan_keys.size());
          stids.reserve(sub.scan_keys.size());
          for (size_t k = 0; k < sub.scan_keys.size(); ++k) {
            if (k >= sub.scan_values.size() || sub.scan_values[k].empty())
              continue;
            // Move the decoded key/value out of the RPC result (a local that is
            // discarded after ingest) straight into the cache row — avoids a
            // second materialization of every for_each row (19.5M for Q21 SF1).
            srows.emplace_back(std::move(sub.scan_keys[k]),
                               std::move(sub.scan_values[k]));
            stids.push_back(k < sub.scan_tids.size() ? sub.scan_tids[k] : 0);
          }
          push_local_range_scan({step.table_name, sub.start_key, sub.end_key,
                                 false, 0, std::move(srows), std::move(stids),
                                 std::move(sub.range_versions), {},
                                 step.filter_serialized});
        } else {
          LocalSecondaryScanEntry cached;
          cached.table_name = step.table_name;
          cached.index_name = step.index_name;
          cached.start_key = sub.start_key;
          cached.end_key = sub.end_key;
          cached.reverse_scan = false;
          cached.row_limit = 0;
          cached.secondary_keys = std::move(sub.secondary_keys);
          cached.primary_keys = std::move(sub.scan_keys);
          cached.range_versions = std::move(sub.range_versions);
          push_local_secondary_scan(std::move(cached));
        }
      }
      continue;
    }

    if (step.index_name.empty()) {
      // Mark the entry with the predicate the server applied (plan-level filter
      // on the step, or the driver filter late-stamped from pushed_filter_), so
      // range_entry_matches won't serve these pruned rows to a different probe.
      std::string eff_filter = step.filter_serialized;
      if (eff_filter.empty() && step.is_scan && !step.for_each &&
          step.table_name == db_table_key && !pushed_filter_.empty())
        eff_filter = pushed_filter_;
      push_local_range_scan(
          {step.table_name, step_result.actual_start_key,
           step_result.actual_end_key, step.reverse_scan, step.scan_limit,
           std::move(rows), std::move(row_tids),
           std::move(step_result.range_versions),
           std::move(step_result.index_reads), std::move(eff_filter)});
    } else {
      LocalSecondaryScanEntry cached;
      cached.table_name = step.table_name;
      cached.index_name = step.index_name;
      cached.start_key = step_result.actual_start_key;
      cached.end_key = step_result.actual_end_key;
      cached.reverse_scan = step.reverse_scan;
      cached.row_limit = step.scan_limit;
      cached.secondary_keys = std::move(step_result.secondary_keys);
      cached.primary_keys = std::move(step_result.scan_keys);
      cached.range_versions = std::move(step_result.range_versions);
      cached.index_reads = std::move(step_result.index_reads);
      push_local_secondary_scan(std::move(cached));
    }
  }
  if (memprof) {
    // ingest copied the rows from ReadPlanResult(#5) into the local cache(#6);
    // the delta is #6 (the copy the join replays against; #5 frees on return).
    const size_t je_post = helios_mem::je_allocated();
    std::fprintf(stderr,
        "[MEMPROF-proxy] #6 cache ingest_delta=%.2fGB | je_now(#5+#6)=%.2fGB\n",
        (double)(je_post - je_pre_ingest) / 1e9, je_post / 1e9);
    std::fflush(stderr);
  }
  if (timeprof) {
    const uint64_t tp_ingest_end = tp_now();
    tp_ingest_ns_ += (tp_ingest_end - tp_ingest_start);
    ++tp_ingest_count_;
  }
}

bool LineairDBTransaction::batch_write(
    const std::string& table_name,
    const std::vector<LineairDBProxy::BatchOp>& ops) {
  if (oneshot_mode_) {
    for (auto op : ops) {
      if (op.table_name.empty()) op.table_name = table_name;
      if (op.type == LineairDBProxy::BatchOp::Type::Write) {
        record_local_write(op.table_name, op.key, true, op.value);
      } else if (op.type == LineairDBProxy::BatchOp::Type::Delete) {
        record_local_write(op.table_name, op.key, false, ""); // value unused when not found
      } else if (op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexWrite ||
                 op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexDelete) {
        drop_local_secondary_scans(op.table_name, op.index_name);
      }
      write_buffer_ops_.push_back(std::move(op));
    }
    return true;
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  return lineairdb_proxy->tx_batch_write(this, table_name, ops);
}

std::vector<std::string>
LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};
  if (!fallback_to_normal_transaction("get_all_keys")) return {};
  flush_write_buffer_for_table(db_table_key);

  auto key_value_pairs = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, "");

  std::vector<std::string> keyList;
  for (const auto& kv : key_value_pairs) {
    keyList.push_back(kv.key);
  }

  return keyList;
}

std::vector<std::string>
LineairDBTransaction::get_matching_keys(std::string first_key_part) {
  if (table_is_not_chosen()) return {};
  if (!fallback_to_normal_transaction("get_matching_keys")) return {};
  flush_write_buffer_for_table(db_table_key);

  auto key_value_pairs = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, first_key_part);

  std::vector<std::string> keyList;
  for (const auto& kv : key_value_pairs) {
    keyList.push_back(kv.key);
  }

  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;
  if (oneshot_mode_) {
    buffer_write(db_table_key, key, value);
    return true;
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  const bool ok = lineairdb_proxy->tx_write(this, key, value);
  if (ok) record_local_write(db_table_key, key, true, value);
  return ok;
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;
  if (oneshot_mode_) {
    buffer_delete(db_table_key, key);
    return true;
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  const bool ok = lineairdb_proxy->tx_delete(this, key);
  if (ok) record_local_write(db_table_key, key, false, ""); // value unused when not found
  return ok;
}

// Secondary index operations

std::vector<std::string>
LineairDBTransaction::read_secondary_index(std::string index_name,
                                           std::string secondary_key) {
  if (table_is_not_chosen()) return {};
  if (oneshot_mode_) {
    const std::string end_key = next_lexicographic_key(secondary_key);
    if (end_key.empty()) {
      rpc_trace_.record_local_view("abort_secondary_key_end");
      is_aborted_ = true;
      return {};
    }
    return get_matching_primary_keys_in_range(index_name, secondary_key,
                                              end_key);
  }

  if (!fallback_to_normal_transaction("read_secondary_index")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_read_secondary_index(this, index_name, secondary_key);
}

bool LineairDBTransaction::write_secondary_index(std::string index_name,
                                                 std::string secondary_key,
                                                 const std::string primary_key) {
  if (table_is_not_chosen()) return false;
  if (oneshot_mode_) {
    buffer_write_secondary_index(db_table_key, index_name, secondary_key,
                                 primary_key);
    return true;
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  return lineairdb_proxy->tx_write_secondary_index(this, index_name, secondary_key, primary_key);
}

bool LineairDBTransaction::delete_secondary_index(std::string index_name,
                                                  std::string secondary_key,
                                                  const std::string primary_key) {
  if (table_is_not_chosen()) return false;
  if (oneshot_mode_) {
    buffer_delete_secondary_index(db_table_key, index_name, secondary_key,
                                  primary_key);
    return true;
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  return lineairdb_proxy->tx_delete_secondary_index(this, index_name, secondary_key, primary_key);
}

bool LineairDBTransaction::update_secondary_index(std::string index_name,
                                                  std::string old_secondary_key,
                                                  std::string new_secondary_key,
                                                  const std::string primary_key) {
  if (table_is_not_chosen()) return false;
  if (oneshot_mode_) {
    buffer_delete_secondary_index(db_table_key, index_name, old_secondary_key,
                                  primary_key);
    buffer_write_secondary_index(db_table_key, index_name, new_secondary_key,
                                 primary_key);
    return true;
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  return lineairdb_proxy->tx_update_secondary_index(this, index_name, old_secondary_key, new_secondary_key, primary_key);
}

// Primary key scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_keys_in_range(std::string start_key,
                                                 std::string end_key) {
  if (table_is_not_chosen()) return {};
  // Phase-3b: deferred plan exec on first scan; relies on pushed_filter_ being
  // set by rnd_init / index_init before MySQL hits the scan path.
  execute_pending_oneshot_plan();
  if (oneshot_mode_) {
    if (end_key.empty()) end_key.assign(16, '\xff');  // (d/P1-c)
    if (auto cached =
            lookup_local_range_scan(db_table_key, start_key, end_key, false, 0)) {
      // Step2a: `cached` is a local optional destroyed at scope end, so move its
      // rows into the working vector instead of a second full deep copy. For the
      // step0 full-cover scan (Q21: 6M rows) this removes one 6M-row copy. The
      // OCC recording below reads `rows` (not cached->rows) and uses
      // cached->row_tids / cached->range_versions which are NOT moved.
      std::vector<std::pair<std::string, std::string>> rows =
          std::move(cached->rows);
      rpc_trace_.record_local_view(
          trace_count_event("use_pk_key_scan", db_table_key, rows.size()));
      // OCC recording is idempotent per probe (immutable cache); skip on a
      // repeat serve of the same probe to avoid re-hashing result_keys.
      if (!probe_occ_already_recorded(db_table_key, "", start_key, end_key, 0,
                                      false)) {
        // Per-row stateless read recording (REINSTATED: user 2026-05-29
        // explicitly rejected Step C's value-update detection gap as
        // unacceptable). The commit-RPC cost of sending millions of TIDs is
        // accepted to keep physical OCC sound under concurrent UPDATE: node
        // version doesn't bump on value update, so without a per-row TID
        // record at commit a value flip would go undetected.
        // Phase-6 range-hash OCC: for a read-only SELECT (rangehash_eligible_)
        // serving from a FULL-COVER entry (start_key==""), skip per-row read
        // recording — the server revalidates this range via its retained
        // footprint digest at commit (correct value-update detection, O(1)
        // wire). All other cases keep per-row recording.
        const bool rh_skip = rangehash_eligible_ && cached->start_key.empty();
        if (!rh_skip)
        for (size_t i = 0; i < rows.size() && i < cached->row_tids.size(); ++i) {
          record_stateless_read(db_table_key, rows[i].first, true,
                                cached->row_tids[i]);
        }
        // Activate validation against the pre-merge key list in
        // cached->range_versions: server-side re-walk at commit cannot see
        // this tx's pending writes, so validating against the post-merge
        // view would false-abort on every own-insert / own-delete in range.
        activate_range_validation(cached->range_versions, cached->index_reads);
      }

      merge_pending_rows_into_range_scan(rows, start_key, end_key, false);
      std::vector<std::string> keys;
      keys.reserve(rows.size());
      for (const auto& row : rows) keys.push_back(row.first);
      return keys;
    }

    // Cache miss in oneshot mode. Previously fell back to a value-scan path
    // (which itself may go stateless). User 2026-05-29: silent stateless
    // fallback hides planner gaps; abort loudly so the missing prefetch
    // surfaces. The note_oneshot_miss helper sets is_aborted_ and logs a
    // [ONESHOT-MISS] line so the failing probe is debuggable. Set
    // HELIOS_ALLOW_ONESHOT_FALLBACK=1 to restore the old fallback path.
    if (has_oneshot_local_state()) {
      if (std::getenv("HELIOS_ALLOW_ONESHOT_FALLBACK") != nullptr) {
        rpc_trace_.record_local_view("oneshot_pk_key_scan_fallback");
        auto pairs = get_matching_keys_and_values_in_range(start_key, end_key, 0, false);
        std::vector<std::string> keys;
        keys.reserve(pairs.size());
        for (auto& kv : pairs) keys.push_back(kv.first);
        return keys;
      }
      (void)note_oneshot_miss("pk_key_scan", db_table_key, start_key);
      return {};
    }
    // clean tx: fall through to normal-transaction switch below
  }

  if (!fallback_to_normal_transaction("get_matching_keys_in_range")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_keys_in_range(this, start_key, end_key);
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_in_range(std::string start_key,
                                                            std::string end_key,
                                                            uint64_t row_limit,
                                                            bool reverse_scan) {
  if (table_is_not_chosen()) return {};
  execute_pending_oneshot_plan();  // Phase-3b: lazy plan exec
  if (oneshot_mode_) {
    // (d/P1-c) Normalize an open-ended upper bound to the max sentinel BEFORE
    // the cache lookup: an empty end_key sorts as the smallest string, so the
    // cover test (end_key <= e.end_key) would wrongly treat any finite cached
    // range as covering [start, +inf) and return an incomplete slice. The
    // sentinel makes both the cover test and the stateless RPC use +inf.
    if (end_key.empty()) end_key.assign(16, '\xff');
    // (d/P1-b) A LIMIT-bounded cache entry holds only N rows; if this tx has
    // buffered writes/deletes in the range, merging them could leave a hole the
    // cache can't backfill (a deleted cached row's successor was never fetched).
    // Bypass the cache in that case and take the stateless path (unlimited scan
    // → merge → truncate). Unlimited cache entries (the common prefetch case)
    // are unaffected.
    const bool limited_with_writes =
        row_limit > 0 && has_pending_ops_for_table(db_table_key);
    std::optional<LocalRangeScanEntry> cached;
    if (!limited_with_writes)
      cached = lookup_local_range_scan(db_table_key, start_key, end_key,
                                       reverse_scan, row_limit);
    if (cached) {
      // Step2a: move the sliced rows out of the local optional `cached` instead
      // of a second full deep copy (`pairs = cached->rows`). The OCC loop below
      // now reads `pairs` (the moved-to vector); cached->row_tids /
      // cached->range_versions / cached->start_key are NOT moved and stay valid.
      std::vector<std::pair<std::string, std::string>> pairs =
          std::move(cached->rows);
      // OCC recording is idempotent per probe; skip on a repeat serve.
      if (!probe_occ_already_recorded(db_table_key, "", start_key, end_key,
                                      row_limit, reverse_scan)) {
        // Per-row stateless read recording (REINSTATED — Step C reverted
        // per user 2026-05-29). See get_matching_keys_in_range above.
        // Phase-6 range-hash OCC: skip for read-only full-cover serve.
        const bool rh_skip2 = rangehash_eligible_ && cached->start_key.empty();
        if (!rh_skip2)
        for (size_t i = 0;
             i < pairs.size() && i < cached->row_tids.size(); ++i) {
          record_stateless_read(db_table_key, pairs[i].first, true,
                                cached->row_tids[i]);
        }
        // See get_matching_keys_in_range above for the rationale.
        activate_range_validation(cached->range_versions, cached->index_reads);
      }

      merge_pending_rows_into_range_scan(pairs, start_key, end_key,
                                         reverse_scan);
      if (row_limit > 0 && pairs.size() > row_limit) {
        pairs.resize(static_cast<size_t>(row_limit));
      }
      rpc_trace_.record_local_view(
          trace_count_event("use_pk_value_scan", db_table_key, pairs.size()));
      return pairs;
    }

    // Cache miss. If this oneshot tx already accumulated local state, it cannot
    // re-begin as a normal tx, so read this uncovered scan statelessly (correct
    // range OCC, only the prefetch win is lost). If the tx is still CLEAN (e.g.
    // a MIN/MAX or index-only query that never went through the auto-gen hook,
    // so no plan was staged), fall through to switch to the normal scan-capable
    // path instead of a stateless RPC. (d)
    if (has_oneshot_local_state()) {
      // Isolated bench: abort on prefetch miss instead of the stateless range
      // fallback.
      if (note_oneshot_miss("pk_value_scan", db_table_key, start_key)) return {};
      rpc_trace_.record_local_view("oneshot_pk_value_scan_fallback");
      // end_key is already normalized to the max sentinel above (P1-c).
      // The stateless scan cannot apply the pushed WHERE filter (only the
      // normal RPC carries it). A pushed LIMIT assumed server-side filtering,
      // so an unfiltered limited scan could return fewer WHERE-passing rows
      // than requested. With own writes to merge, a limited server scan also
      // can't form the correct post-merge window. In either case fetch the
      // full range and let MySQL apply WHERE + LIMIT locally. (d/P1-b, P1-LIMIT)
      const bool merge_writes = has_pending_ops_for_table(db_table_key);
      const bool has_filter = !pushed_filter_.empty();
      const uint64_t eff_limit =
          ((merge_writes || has_filter) && row_limit > 0) ? 0 : row_limit;
      auto sr = lineairdb_proxy->tx_stateless_range_scan(
          db_table_key, start_key, end_key, eff_limit, reverse_scan);
      if (!sr.ok) {
        rpc_trace_.record_local_view("abort_oneshot_range_fallback_rpc");
        is_aborted_ = true;
        return {};
      }
      std::vector<std::pair<std::string, std::string>> pairs;
      pairs.reserve(sr.rows.size());
      for (auto& row : sr.rows) {
        record_stateless_read(db_table_key, row.key, row.found, row.tid);
        if (row.found) pairs.emplace_back(row.key, row.value);
      }
      activate_range_validation(sr.range_versions, sr.index_reads);
      // (d/P1-b) reflect this tx's own buffered writes/deletes in the range.
      if (merge_writes)
        merge_pending_rows_into_range_scan(pairs, start_key, end_key, reverse_scan);
      // Only truncate to the caller's LIMIT when no pushed filter remains to be
      // applied by MySQL (otherwise MySQL must see all candidates).
      if (row_limit > 0 && !has_filter && pairs.size() > row_limit) {
        pairs.resize(static_cast<size_t>(row_limit));
      }
      return pairs;
    }
    // clean tx: fall through to normal-transaction switch below
  }

  if (!fallback_to_normal_transaction("get_matching_keys_and_values_in_range")) return {};
  const bool can_merge_local_rows = (row_limit == 0 && pushed_filter_.empty());
  // LIMIT / pushed filter scans must see only server-filtered rows
  if (!can_merge_local_rows) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_in_range(
      this, start_key, end_key, row_limit, reverse_scan);

  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& kv : results) {
    pairs.emplace_back(kv.key, kv.value);
  }
  // Merge unflushed own writes after the server has validated the range
  if (can_merge_local_rows) {
    merge_pending_rows_into_range_scan(pairs, start_key, end_key, reverse_scan);
  }
  return pairs;
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_from_prefix(std::string prefix) {
  if (table_is_not_chosen()) return {};
  execute_pending_oneshot_plan();  // Phase-3b: lazy plan exec
  if (oneshot_mode_) {
    std::string prefix_end;
    if (prefix.empty()) {
      // Full-table scan: end_key cannot be derived from prefix (next_lex of ""
      // is ""), so use a max sentinel. Plan-side S: with no bounds must use
      // the same sentinel (see parse_plan_steps in ha_lineairdb.cc).
      prefix_end.assign(16, '\xff');
    } else {
      prefix_end = next_lexicographic_key(prefix);
      if (prefix_end.empty()) {
        rpc_trace_.record_local_view("abort_pk_prefix_end");
        is_aborted_ = true;
        return {};
      }
    }
    return get_matching_keys_and_values_in_range(prefix, prefix_end);
  }

  if (!fallback_to_normal_transaction("get_matching_keys_and_values_from_prefix")) return {};
  const bool can_merge_local_rows = pushed_filter_.empty();
  // Pushed filter scans must see only server-filtered rows
  if (!can_merge_local_rows) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, prefix);

  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& kv : results) {
    pairs.emplace_back(kv.key, kv.value);
  }
  // Merge unflushed own writes after the server has validated the prefix
  if (can_merge_local_rows) {
    merge_pending_rows_into_prefix_scan(pairs, prefix);
  }
  return pairs;
}

// ---- Phase-7 Step3: borrowed-span serve (InnoDB fetch-cache analog) --------
LineairDBTransaction::BorrowedScan
LineairDBTransaction::borrow_fullcover_pk_scan() {
  BorrowedScan h;
  // Opt-out: HELIOS_BORROW_SERVE=0 falls back to the copy path (for A/B).
  static const char* bs_env = std::getenv("HELIOS_BORROW_SERVE");
  if (bs_env != nullptr && std::strcmp(bs_env, "0") == 0) return h;
  if (table_is_not_chosen()) return h;
  execute_pending_oneshot_plan();
  if (!oneshot_mode_ || is_aborted_) return h;
  // Own-write merge cannot reflect into a borrowed (read-only) span; the copy
  // path handles writes via merge_pending_rows_into_range_scan.
  if (has_pending_ops_for_table(db_table_key)) return h;

  // Find a TRUE full-cover unfiltered unlimited forward primary entry (the
  // step0 S: scan), registered under the empty start_key bucket. Mirrors the
  // full-cover gate in lookup_local_range_scan / slice_range_entry_fast.
  static const std::string kFullEnd16(16, '\xff');
  auto fit = range_scan_index_.find(std::string());
  if (fit == range_scan_index_.end()) return h;
  for (auto rit = fit->second.rbegin(); rit != fit->second.rend(); ++rit) {
    const LocalRangeScanEntry& e = local_range_scans_[*rit];
    if (e.table_name != db_table_key) continue;
    if (!e.start_key.empty()) continue;
    if (e.reverse_scan) continue;
    if (e.row_limit != 0) continue;
    if (!e.filter_serialized.empty()) continue;
    if (e.end_key != kFullEnd16) continue;
    if (e.row_tids.size() != e.rows.size()) continue;  // need 1:1 TIDs for OCC
    // Record OCC obligations for the whole range ONCE (identical to the copy
    // path in get_matching_keys_and_values_in_range): per-row TIDs unless this
    // is a read-only range-hash full-cover serve, plus range validation.
    if (!probe_occ_already_recorded(db_table_key, "", std::string(), kFullEnd16,
                                    0, false)) {
      const bool rh_skip = rangehash_eligible_ && e.start_key.empty();
      if (!rh_skip)
        for (size_t i = 0; i < e.rows.size() && i < e.row_tids.size(); ++i)
          record_stateless_read(db_table_key, e.rows[i].first, true,
                                e.row_tids[i]);
      activate_range_validation(e.range_versions, e.index_reads);
    }
    h.ok = true;
    h.entry_idx = *rit;
    h.count = e.rows.size();
    rpc_trace_.record_local_view(
        trace_count_event("borrow_fullcover_serve", db_table_key, h.count));
    return h;
  }
  return h;
}

const std::string* LineairDBTransaction::borrowed_value(const BorrowedScan& h,
                                                        size_t pos) const {
  if (!h.ok || h.entry_idx >= local_range_scans_.size()) return nullptr;
  const LocalRangeScanEntry& e = local_range_scans_[h.entry_idx];
  if (pos >= e.rows.size()) return nullptr;
  return &e.rows[pos].second;
}

const std::string* LineairDBTransaction::borrowed_key(const BorrowedScan& h,
                                                      size_t pos) const {
  if (!h.ok || h.entry_idx >= local_range_scans_.size()) return nullptr;
  const LocalRangeScanEntry& e = local_range_scans_[h.entry_idx];
  if (pos >= e.rows.size()) return nullptr;
  return &e.rows[pos].first;
}

const std::string* LineairDBTransaction::borrowed_value_for_key(
    const BorrowedScan& h, const std::string& pk) const {
  if (!h.ok || h.entry_idx >= local_range_scans_.size()) return nullptr;
  const LocalRangeScanEntry& e = local_range_scans_[h.entry_idx];
  // rows are sorted ascending by key (entry invariant; the borrow gate forbids
  // reverse), so binary-search for the exact PK (rnd_pos re-read).
  auto key_less = [](const std::pair<std::string, std::string>& r,
                     const std::string& k) { return r.first < k; };
  auto it = std::lower_bound(e.rows.begin(), e.rows.end(), pk, key_less);
  if (it == e.rows.end() || it->first != pk) return nullptr;
  return &it->second;
}

std::optional<std::string>
LineairDBTransaction::fetch_last_key_in_range(const std::string &start_key,
                                              const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (oneshot_mode_ && has_oneshot_local_state()) {
    // (d) Don't abort under oneshot state: derive the max key from the
    // cache-or-stateless range scan (correct range OCC), then pick the max.
    auto pairs = get_matching_keys_and_values_in_range(start_key, end_key, 0, false);
    const std::string* mx = nullptr;
    for (auto& kv : pairs)
      if (mx == nullptr || kv.first > *mx) mx = &kv.first;
    if (mx == nullptr) return std::nullopt;
    return *mx;
  }
  if (!fallback_to_normal_transaction("fetch_last_key_in_range")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_key_in_range(this, start_key, end_key);
}

std::optional<std::string>
LineairDBTransaction::fetch_first_key_with_prefix(const std::string &prefix,
                                                  const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  if (oneshot_mode_ && has_oneshot_local_state()) {
    auto pairs = get_matching_keys_and_values_in_range(prefix, prefix_end, 0, false);
    const std::string* mn = nullptr;
    for (auto& kv : pairs)
      if (mn == nullptr || kv.first < *mn) mn = &kv.first;
    if (mn == nullptr) return std::nullopt;
    return *mn;
  }
  if (!fallback_to_normal_transaction("fetch_first_key_with_prefix")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_first_key_with_prefix(this, prefix, prefix_end);
}

std::optional<std::string>
LineairDBTransaction::fetch_next_key_with_prefix(const std::string &last_key,
                                                 const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  if (oneshot_mode_ && has_oneshot_local_state()) {
    // Smallest key strictly greater than last_key within [last_key, prefix_end).
    auto pairs = get_matching_keys_and_values_in_range(last_key, prefix_end, 0, false);
    const std::string* mn = nullptr;
    for (auto& kv : pairs)
      if (kv.first > last_key && (mn == nullptr || kv.first < *mn)) mn = &kv.first;
    if (mn == nullptr) return std::nullopt;
    return *mn;
  }
  if (!fallback_to_normal_transaction("fetch_next_key_with_prefix")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_next_key_with_prefix(this, last_key, prefix_end);
}

// Secondary index scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_in_range(std::string index_name,
                                                         std::string start_key,
                                                         std::string end_key) {
  if (table_is_not_chosen()) return {};
  execute_pending_oneshot_plan();  // Phase-3b: lazy plan exec
  if (oneshot_mode_) {
    // (d/P1-c) normalize open-ended end_key before the cache lookup too, so the
    // secondary cover test doesn't treat a finite cached range as covering +inf.
    if (end_key.empty()) end_key.assign(16, '\xff');
    if (has_pending_secondary_ops_for_index(db_table_key, index_name)) {
      rpc_trace_.record_local_view("abort_secondary_scan_after_secondary_write:" +
                                   index_name);
      is_aborted_ = true;
      return {};
    }

    if (auto cached = lookup_local_secondary_scan(
            db_table_key, index_name, start_key, end_key, false, 0)) {
      rpc_trace_.record_local_view("use_si_scan:" + db_table_key + ":" +
                                   index_name + ":n=" +
                                   std::to_string(cached->primary_keys.size()));
      // OCC recording idempotent per probe; skip re-activation (and its
      // result_keys re-hash) on a repeat serve. This is the NLJ inner-probe hot
      // path (e.g. lineitem-by-l_partkey for Q9).
      if (!probe_occ_already_recorded(db_table_key, index_name, start_key,
                                      end_key, 0, false))
        activate_range_validation(cached->range_versions, cached->index_reads);
      // Step2a: `cached` is a local optional; move its primary_keys out instead
      // of copying on return (FES inner-probe hot path, e.g. Q9).
      return std::move(cached->primary_keys);
    }

    // Cache miss. With local state → stateless secondary scan (correct OCC);
    // clean → fall through to the normal scan-capable path. (d, see
    // get_matching_keys_and_values_in_range for rationale.)
    if (has_oneshot_local_state()) {
      // Isolated bench: abort on prefetch miss instead of the stateless
      // secondary fallback (this is the lineitem-by-l_partkey join probe path).
      if (note_oneshot_miss(("secondary_scan:" + index_name).c_str(),
                            db_table_key, start_key))
        return {};
      rpc_trace_.record_local_view("oneshot_secondary_scan_fallback:" + index_name);
      // end_key already normalized to the max sentinel above (P1-c). Own
      // secondary writes are excluded by the has_pending_secondary_ops guard.
      auto sr = lineairdb_proxy->tx_stateless_secondary_range_scan(
          db_table_key, index_name, start_key, end_key, 0, false);
      if (!sr.ok) {
        rpc_trace_.record_local_view("abort_oneshot_secondary_fallback_rpc");
        is_aborted_ = true;
        return {};
      }
      std::vector<std::string> primary_keys;
      primary_keys.reserve(sr.rows.size());
      for (auto& row : sr.rows) {
        record_stateless_read(db_table_key, row.primary_key, row.found, row.tid);
        primary_keys.push_back(row.primary_key);
      }
      activate_range_validation(sr.range_versions, sr.index_reads);
      return primary_keys;
    }
    // clean tx: fall through to normal-transaction switch below
  }

  if (!fallback_to_normal_transaction("get_matching_primary_keys_in_range")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_primary_keys_in_range(this, index_name, start_key, end_key);
}

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_from_prefix(std::string index_name,
                                                            std::string prefix) {
  if (table_is_not_chosen()) return {};
  if (oneshot_mode_) {
    const std::string prefix_end = next_lexicographic_key(prefix);
    if (prefix_end.empty()) {
      rpc_trace_.record_local_view("abort_secondary_prefix_end");
      is_aborted_ = true;
      return {};
    }
    return get_matching_primary_keys_in_range(index_name, prefix, prefix_end);
  }

  if (!fallback_to_normal_transaction("get_matching_primary_keys_from_prefix")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_primary_keys_from_prefix(this, index_name, prefix);
}

std::optional<std::string>
LineairDBTransaction::fetch_last_primary_key_in_secondary_range(const std::string &index_name,
                                                                const std::string &start_key,
                                                                const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (oneshot_mode_ && has_oneshot_local_state()) {
    // (d) get_matching_primary_keys_in_range returns primary keys in ascending
    // secondary-key order (cache-or-stateless, correct OCC); the last is the
    // primary key of the entry with the max secondary key in range.
    auto pks = get_matching_primary_keys_in_range(index_name, start_key, end_key);
    if (pks.empty()) return std::nullopt;
    return pks.back();
  }
  if (!fallback_to_normal_transaction("fetch_last_primary_key_in_secondary_range")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_primary_key_in_secondary_range(this, index_name, start_key, end_key);
}

std::optional<SecondaryIndexEntry>
LineairDBTransaction::fetch_last_secondary_entry_in_range(const std::string &index_name,
                                                          const std::string &start_key,
                                                          const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (oneshot_mode_ && has_oneshot_local_state()) {
    // (d) Max secondary key (+ its primary keys) in range, from the cached
    // secondary scan if present, else a stateless secondary range scan.
    // (d/P1-c) normalize open-ended end_key before both lookup and stateless.
    const std::string eff_end =
        end_key.empty() ? std::string(16, '\xff') : end_key;
    if (auto cached = lookup_local_secondary_scan(db_table_key, index_name,
                                                  start_key, eff_end, false, 0)) {
      if (!probe_occ_already_recorded(db_table_key, index_name, start_key,
                                      eff_end, 0, false))
        activate_range_validation(cached->range_versions, cached->index_reads);
      if (cached->secondary_keys.empty()) return std::nullopt;
      // secondary_keys/primary_keys are parallel, ascending by secondary key.
      const std::string& maxsk = cached->secondary_keys.back();
      SecondaryIndexEntry entry;
      entry.secondary_key = maxsk;
      for (size_t i = 0; i < cached->secondary_keys.size(); ++i)
        if (cached->secondary_keys[i] == maxsk &&
            i < cached->primary_keys.size())
          entry.primary_keys.push_back(cached->primary_keys[i]);
      return entry;
    }
    if (note_oneshot_miss(("secondary_entry:" + index_name).c_str(),
                          db_table_key, start_key))
      return std::nullopt;
    rpc_trace_.record_local_view("oneshot_secondary_entry_fallback:" + index_name);
    auto sr = lineairdb_proxy->tx_stateless_secondary_range_scan(
        db_table_key, index_name, start_key, eff_end, 0, false);
    if (!sr.ok) {
      rpc_trace_.record_local_view("abort_oneshot_secondary_entry_fallback_rpc");
      is_aborted_ = true;
      return std::nullopt;
    }
    activate_range_validation(sr.range_versions, sr.index_reads);
    std::optional<std::string> maxsk;
    for (auto& row : sr.rows) {
      record_stateless_read(db_table_key, row.primary_key, row.found, row.tid);
      if (!maxsk || row.secondary_key > *maxsk) maxsk = row.secondary_key;
    }
    if (!maxsk) return std::nullopt;
    SecondaryIndexEntry entry;
    entry.secondary_key = *maxsk;
    for (auto& row : sr.rows)
      if (row.secondary_key == *maxsk) entry.primary_keys.push_back(row.primary_key);
    return entry;
  }
  if (!fallback_to_normal_transaction("fetch_last_secondary_entry_in_range")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_secondary_entry_in_range(this, index_name, start_key, end_key);
}

// Row count delta tracking

void LineairDBTransaction::add_rowcount_delta(LineairDB_share *share,
                                              const std::string &table_name,
                                              int64_t delta) {
  if (share == nullptr || delta == 0) return;

  for (auto &entry : rowcount_deltas_) {
    if (entry.share == share) {
      entry.delta += delta;
      return;
    }
  }

  rowcount_deltas_.push_back({share, table_name, delta});
}

int64_t
LineairDBTransaction::peek_rowcount_delta(const LineairDB_share *share) const {
  if (share == nullptr) return 0;

  for (const auto &entry : rowcount_deltas_) {
    if (entry.share == share)
      return entry.delta;
  }

  return 0;
}

void LineairDBTransaction::buffer_write(const std::string& table_name,
                                        const std::string& key,
                                        const std::string& value) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::Write;
  op.key = key;
  op.value = value;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  record_local_write(table_name, key, true, value);

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

void LineairDBTransaction::buffer_write_secondary_index(const std::string& table_name,
                                                        const std::string& index_name,
                                                        const std::string& secondary_key,
                                                        const std::string& primary_key) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexWrite;
  op.index_name = index_name;
  op.secondary_key = secondary_key;
  op.primary_key = primary_key;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  drop_local_secondary_scans(table_name, index_name);

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

void LineairDBTransaction::buffer_delete(const std::string& table_name,
                                         const std::string& key) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::Delete;
  op.key = key;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  record_local_write(table_name, key, false, ""); // value unused when not found

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

void LineairDBTransaction::buffer_delete_secondary_index(
    const std::string& table_name,
    const std::string& index_name,
    const std::string& secondary_key,
    const std::string& primary_key) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexDelete;
  op.index_name = index_name;
  op.secondary_key = secondary_key;
  op.primary_key = primary_key;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  drop_local_secondary_scans(table_name, index_name);

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

bool LineairDBTransaction::flush_write_buffer() {
  if (write_buffer_ops_.empty()) return true;
  if (oneshot_mode_) return true;
  if (is_aborted_) {
    write_buffer_ops_.clear();
    return false;
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  bool ok = lineairdb_proxy->tx_batch_write(this, "", write_buffer_ops_);
  write_buffer_ops_.clear();
  return ok;
}

bool LineairDBTransaction::flush_write_buffer_for_table(
    const std::string& table_name) {
  if (write_buffer_ops_.empty()) return true;
  if (oneshot_mode_) return true;
  if (is_aborted_) {
    write_buffer_ops_.clear();
    return false;
  }

  bool has_table_ops = false;
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name == table_name) {
      has_table_ops = true;
      break;
    }
  }
  if (!has_table_ops) return true;

  std::vector<LineairDBProxy::BatchOp> flush_ops;
  std::vector<LineairDBProxy::BatchOp> keep_ops;
  flush_ops.reserve(write_buffer_ops_.size());
  keep_ops.reserve(write_buffer_ops_.size());

  for (auto& op : write_buffer_ops_) {
    if (op.table_name == table_name) {
      flush_ops.push_back(std::move(op));
    } else {
      keep_ops.push_back(std::move(op));
    }
  }

  ensure_started_for_normal_rpc();  // (d/P1-a)
  bool ok = lineairdb_proxy->tx_batch_write(this, table_name, flush_ops);
  if (!ok) {
    write_buffer_ops_.clear();
    return false;
  }

  write_buffer_ops_ = std::move(keep_ops);
  return true;
}

std::optional<LineairDBTransaction::LocalRowEntry>
LineairDBTransaction::lookup_local_write_set(
    const std::string& table_name, const std::string& key) const {
  for (auto it = local_write_set_.rbegin(); it != local_write_set_.rend(); ++it) {
    if (it->table_name == table_name && it->key == key) {
      return *it;
    }
  }
  return std::nullopt;
}

const LineairDBTransaction::LocalRowEntry*
LineairDBTransaction::lookup_local_read_set(
    const std::string& table_name, const std::string& key) const {
  // #1: probe with the reusable scratch key (no per-call string alloc) and
  // return a pointer (no LocalRowEntry/value copy). Hot path: index_read_map
  // FE point reads (Q21 = millions of probes).
  fill_local_read_key(lrk_scratch_, table_name, key);
  auto it = local_read_set_.find(lrk_scratch_);
  if (it == local_read_set_.end()) return nullptr;
  return &it->second;
}

void LineairDBTransaction::drop_local_read(const std::string& table_name,
                                           const std::string& key) {
  fill_local_read_key(lrk_scratch_, table_name, key);
  local_read_set_.erase(lrk_scratch_);
}

bool LineairDBTransaction::key_is_in_range(const std::string& key,
                                           const std::string& start_key,
                                           const std::string& end_key) const {
  // LineairDB ranges are [start_key, end_key)
  if (key < start_key) return false;
  if (!end_key.empty() && key >= end_key) return false;
  return true;
}

bool LineairDBTransaction::key_starts_with(const std::string& key,
                                           const std::string& prefix) const {
  // Prefix scans use the encoded primary-key prefix
  if (key.size() < prefix.size()) return false;
  return key.compare(0, prefix.size(), prefix) == 0;
}

void LineairDBTransaction::remove_scan_row(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& key) const {
  // Local write/delete replaces any server row with the same key
  for (auto it = rows.begin(); it != rows.end(); ++it) {
    if (it->first == key) {
      rows.erase(it);
      return;
    }
  }
}

void LineairDBTransaction::insert_scan_row_in_order(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& key, const std::string& value,
    bool reverse_scan) const {
  // Keep the materialized scan result in key order
  for (auto it = rows.begin(); it != rows.end(); ++it) {
    if ((!reverse_scan && key < it->first) || (reverse_scan && key > it->first)) {
      rows.insert(it, {key, value});
      return;
    }
  }
  rows.emplace_back(key, value);
}

void LineairDBTransaction::merge_pending_rows_into_range_scan(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& start_key, const std::string& end_key,
    bool reverse_scan) const {
  // Server scan validates the range; proxy only adds its unflushed row ops
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name != db_table_key) continue;
    if (op.type != LineairDBProxy::BatchOp::Type::Write &&
        op.type != LineairDBProxy::BatchOp::Type::Delete) {
      continue;
    }
    if (!key_is_in_range(op.key, start_key, end_key)) continue;

    remove_scan_row(rows, op.key);
    if (op.type == LineairDBProxy::BatchOp::Type::Write) {
      insert_scan_row_in_order(rows, op.key, op.value, reverse_scan);
    }
  }
}

void LineairDBTransaction::merge_pending_rows_into_prefix_scan(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& prefix) const {
  // Prefix scans are ASC, so inserted local rows keep ASC key order
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name != db_table_key) continue;
    if (op.type != LineairDBProxy::BatchOp::Type::Write &&
        op.type != LineairDBProxy::BatchOp::Type::Delete) {
      continue;
    }
    if (!key_starts_with(op.key, prefix)) continue;

    remove_scan_row(rows, op.key);
    if (op.type == LineairDBProxy::BatchOp::Type::Write) {
      insert_scan_row_in_order(rows, op.key, op.value, false);
    }
  }
}

bool LineairDBTransaction::has_pending_ops_for_table(
    const std::string& table_name) const {
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name == table_name) return true;
  }
  return false;
}

bool LineairDBTransaction::has_pending_secondary_ops_for_index(
    const std::string& table_name,
    const std::string& index_name) const {
  for (const auto& op : write_buffer_ops_) {
    const bool same_index =
        op.table_name == table_name && op.index_name == index_name;
    if (!same_index) continue;

    if (op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexWrite ||
        op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexDelete) {
      return true;
    }
  }
  return false;
}

void LineairDBTransaction::drop_local_secondary_scans(
    const std::string& table_name,
    const std::string& index_name) {
  std::vector<LocalSecondaryScanEntry> kept;
  kept.reserve(local_secondary_scans_.size());

  // A buffered SI write/delete can make old scan results incomplete
  for (const auto& entry : local_secondary_scans_) {
    if (entry.table_name == table_name && entry.index_name == index_name) {
      continue;
    }
    kept.push_back(entry);
  }
  local_secondary_scans_.swap(kept);
  // Rebuild the O(1) start-key index since entry positions changed.
  secondary_scan_index_.clear();
  for (size_t i = 0; i < local_secondary_scans_.size(); ++i) {
    std::string ikey = local_secondary_scans_[i].index_name;
    ikey.push_back('\0');
    ikey.append(local_secondary_scans_[i].start_key);
    secondary_scan_index_[ikey].push_back(i);
  }
}

void LineairDBTransaction::record_local_write(const std::string& table_name,
                                              const std::string& key,
                                              bool found,
                                              const std::string& value) {
  // A later write/delete replaces any cached read for the same key
  drop_local_read(table_name, key);

  for (auto& entry : local_write_set_) {
    if (entry.table_name == table_name && entry.key == key) {
      entry.found = found;
      entry.value = value;
      return;
    }
  }
  local_write_set_.push_back({table_name, key, found, value});
}

void LineairDBTransaction::record_local_read(const std::string& table_name,
                                             const std::string& key,
                                             bool found,
                                             const std::string& value,
                                             uint64_t tid,
                                             bool validate_on_use) {
  local_read_set_[make_local_read_key(table_name, key)] =
      LocalRowEntry{table_name, key, found, value, tid, validate_on_use};
}

void LineairDBTransaction::record_stateless_read(const std::string& table_name,
                                                 const std::string& key,
                                                 bool found,
                                                 uint64_t tid) {
  // read-only no-validation: no commit validation runs, so per-row TID
  // recording is pure overhead -> skip entirely. (docs/phase7_readonly_novalidate.md)
  if (ro_novalidate_) return;
  // #1: dedup-probe with the reusable scratch key (no per-call alloc). Hot path:
  // millions of repeated point reads dedup to a few unique keys, so almost every
  // call is a find-hit that now allocates nothing. Only a genuinely-new key
  // copies the scratch into an owning map key below.
  fill_local_read_key(srr_scratch_, table_name, key);

  auto it = stateless_read_index_.find(srr_scratch_);
  if (it != stateless_read_index_.end()) {
    auto& entry = stateless_read_set_[it->second];
    // (d/P1) The same key was already observed in this transaction. Versions are
    // stable per value, so re-reading an unchanged key yields the same TID. A
    // DIFFERENT (tid, found) means the row changed under us between two reads in
    // the same tx — a non-serializable read skew. Silently overwriting the
    // earlier TID would drop its commit validation, so abort instead. (Equal
    // observations are a harmless dedup no-op.)
    if (entry.tid != tid || entry.found != found) {
      rpc_trace_.record_local_view("abort_stateless_read_tid_conflict");
      is_aborted_ = true;
    }
    return;
  }
  stateless_read_index_[srr_scratch_] = stateless_read_set_.size();  // copy: new key
  stateless_read_set_.push_back({table_name, key, tid, found});
}

void LineairDBTransaction::activate_local_read(const LocalRowEntry& entry) {
  if (!entry.validate_on_use) return;
  rpc_trace_.record_local_view(
      trace_count_event("use_point_read", entry.table_name, 1));
  record_stateless_read(entry.table_name, entry.key, entry.found, entry.tid);
}

// FNV-1a folding helpers — hash ALL fields the exact comparison below uses,
// including result_keys / result_primary_keys CONTENTS (the server's logical
// range validation compares those, so two same-range entries with different
// key lists are DISTINCT and must both be kept). Length-mixing avoids the
// '\0'-separator ambiguity of binary keys. (Codex review.)
static inline void hash_bytes(uint64_t& h, const char* p, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    h ^= static_cast<unsigned char>(p[i]);
    h *= 0x100000001b3ULL;
  }
}
static inline void hash_str(uint64_t& h, const std::string& s) {
  h ^= s.size(); h *= 0x100000001b3ULL;
  hash_bytes(h, s.data(), s.size());
}
static inline void hash_u64(uint64_t& h, uint64_t v) {
  hash_bytes(h, reinterpret_cast<const char*>(&v), sizeof(v));
}
static bool range_entry_equal(const LineairDBProxy::RangeValidationEntry& a,
                              const LineairDBProxy::RangeValidationEntry& b) {
  return a.table_name == b.table_name && a.index_name == b.index_name &&
         a.owner_ptr == b.owner_ptr && a.node_ptr == b.node_ptr &&
         a.version == b.version && a.start_key == b.start_key &&
         a.end_key == b.end_key && a.row_limit == b.row_limit &&
         a.reverse_scan == b.reverse_scan && a.result_keys == b.result_keys &&
         a.result_primary_keys == b.result_primary_keys;
}
static bool index_entry_equal(const LineairDBProxy::IndexValidationEntry& a,
                              const LineairDBProxy::IndexValidationEntry& b) {
  return a.table_name == b.table_name && a.index_name == b.index_name &&
         a.key == b.key && a.tid == b.tid && a.found == b.found;
}

void LineairDBTransaction::activate_range_validation(
    const std::vector<LineairDBProxy::RangeValidationEntry>& ranges,
    const std::vector<LineairDBProxy::IndexValidationEntry>& indexes) {
  // read-only no-validation: the commit RPC is skipped, so accumulating range
  // validation entries is pure overhead -> skip. (docs/phase7_readonly_novalidate.md)
  if (ro_novalidate_) return;
  // HELIOS_TIMEPROF accumulator.
  struct TpGuard {
    LineairDBTransaction* tx;
    uint64_t t0;
    bool on;
    TpGuard(LineairDBTransaction* t) : tx(t), t0(0),
        on(std::getenv("HELIOS_TIMEPROF") != nullptr) {
      if (on) { timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
                t0 = uint64_t(ts.tv_sec)*1000000000ull + ts.tv_nsec; }
    }
    ~TpGuard() {
      if (on) { timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
                tx->tp_activate_rv_ns_ += (uint64_t(ts.tv_sec)*1000000000ull + ts.tv_nsec) - t0;
                ++tx->tp_activate_rv_count_; }
    }
  } tp_guard(this);
  // Dedup is O(1) amortized: hash all compared fields, then on a hash hit run
  // the EXACT field-by-field comparison (range_entry_equal / index_entry_equal,
  // identical to the old linear scan's same_node && same_logical). Distinct
  // result_keys contents hash differently, so no distinct entry is dropped
  // (no under-validation); duplicates collapse as before. Hashing is O(entry
  // size) ⇒ O(total rows) overall, replacing the previous O(N^2) linear scan.
  for (const auto& range : ranges) {
    uint64_t h = 1469598103934665603ULL;
    hash_str(h, range.table_name);
    hash_str(h, range.index_name);
    hash_u64(h, range.owner_ptr);
    hash_u64(h, range.node_ptr);
    hash_u64(h, range.version);
    hash_str(h, range.start_key);
    hash_str(h, range.end_key);
    hash_u64(h, range.row_limit);
    hash_u64(h, range.reverse_scan ? 1 : 0);
    hash_u64(h, range.result_keys.size());
    for (const auto& k : range.result_keys) hash_str(h, k);
    hash_u64(h, range.result_primary_keys.size());
    for (const auto& k : range.result_primary_keys) hash_str(h, k);
    auto& bucket = range_validation_buckets_[h];
    bool found = false;
    for (size_t idx : bucket)
      if (range_entry_equal(range_validation_set_[idx], range)) { found = true; break; }
    if (!found) {
      bucket.push_back(range_validation_set_.size());
      range_validation_set_.push_back(range);
    }
  }

  for (const auto& index : indexes) {
    uint64_t h = 1469598103934665603ULL;
    hash_str(h, index.table_name);
    hash_str(h, index.index_name);
    hash_str(h, index.key);
    hash_u64(h, index.tid);
    hash_u64(h, index.found ? 1 : 0);
    auto& bucket = index_validation_buckets_[h];
    bool found = false;
    for (size_t idx : bucket)
      if (index_entry_equal(index_validation_set_[idx], index)) { found = true; break; }
    if (!found) {
      bucket.push_back(index_validation_set_.size());
      index_validation_set_.push_back(index);
    }
  }
}

bool LineairDBTransaction::note_oneshot_miss(const char* what,
                                            const std::string& table,
                                            const std::string& key) {
  // Isolated bench: a oneshot prefetch miss must NOT silently degrade into a
  // per-probe stateless RPC (that breaks the 2-RPC contract and floods the wire
  // with noise that hides where time goes). Abort loudly so misses are visible
  // and the query fails fast instead of crawling. Logging is unconditional and
  // flushed (mysqld stderr is block-buffered).
  std::string h;
  const size_t n = key.size() < 24 ? key.size() : 24;
  static const char* hx = "0123456789abcdef";
  for (size_t i = 0; i < n; ++i) {
    unsigned char c = static_cast<unsigned char>(key[i]);
    h.push_back(hx[c >> 4]);
    h.push_back(hx[c & 0xf]);
  }
  std::fprintf(stderr, "[ONESHOT-MISS] %s tbl=%s keylen=%zu keyhex=%s -> ABORT\n",
               what, table.c_str(), key.size(), h.c_str());
  std::fflush(stderr);
  rpc_trace_.record_local_view(std::string("oneshot_miss_abort:") + what + ":" +
                               table);
  is_aborted_ = true;
  return true;
}

bool LineairDBTransaction::probe_occ_already_recorded(
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key,
    uint64_t row_limit, bool reverse_scan) {
  // Length-prefix each field so distinct probes can't alias even when keys
  // contain 0x00 (a NUL separator would be ambiguous — same reasoning as the
  // FER/FES dedup key on the server). Build is O(field sizes), far cheaper than
  // re-hashing the cached range's full result_keys list on every serve.
  std::string k;
  k.reserve(table_name.size() + index_name.size() + start_key.size() +
            end_key.size() + 24);
  auto add = [&](const std::string& s) {
    uint32_t n = static_cast<uint32_t>(s.size());
    k.append(reinterpret_cast<const char*>(&n), sizeof(n));
    k.append(s);
  };
  add(table_name);
  add(index_name);
  add(start_key);
  add(end_key);
  // Full 8 bytes for row_limit + a separate byte for reverse_scan: packing them
  // into one shifted integer would drop row_limit's high bit (N collides with
  // N+2^63). (Codex review.)
  k.append(reinterpret_cast<const char*>(&row_limit), sizeof(row_limit));
  k.push_back(reverse_scan ? '\x01' : '\x00');
  // Include the active pushed predicate: the same physical range served under a
  // different filter is a DISTINCT OCC obligation, so it must not be skipped as
  // an already-recorded probe. Within one atomic statement pushed_filter_ is
  // constant (no behavior change); it matters only across statements of a
  // multi-statement tx that filter the same range differently. (Codex P1.)
  add(pushed_filter_);
  return !recorded_probe_keys_.insert(std::move(k)).second;
}

void LineairDBTransaction::push_local_range_scan(LocalRangeScanEntry entry) {
  const std::string key = entry.start_key;
  // Phase-1A: TRUE full-cover unfiltered S: entries (e.g. Q21 step0 lineitem)
  // serve inner FER probes via slice_range_entry_fast which leaves the
  // slice's range_versions EMPTY. The full-cover entry's own range_versions
  // (covering the whole table) must already be in range_validation_set_ at
  // first slice serve, so we eagerly activate at ingest. Other (filtered /
  // limited / FER per-probe) entries keep lazy activation.
  static const std::string kFullEnd16(16, '\xff');
  const bool eager_eligible =
      key.empty() && entry.end_key == kFullEnd16 &&
      entry.filter_serialized.empty() && entry.row_limit == 0 &&
      !entry.range_versions.empty();
  if (eager_eligible) {
    activate_range_validation(entry.range_versions, entry.index_reads);
  }
  local_range_scans_.push_back(std::move(entry));
  range_scan_index_[key].push_back(local_range_scans_.size() - 1);
}

void LineairDBTransaction::push_local_secondary_scan(
    LocalSecondaryScanEntry entry) {
  std::string key = entry.index_name;
  key.push_back('\0');
  key.append(entry.start_key);
  local_secondary_scans_.push_back(std::move(entry));
  secondary_scan_index_[key].push_back(local_secondary_scans_.size() - 1);
}

// Shared body that tests whether a cached primary range entry can serve a
// requested [start_key,end_key) probe, returning the (sliced) entry if so.
static bool range_entry_matches(
    const LineairDBTransaction::LocalRangeScanEntry& e,
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key, bool reverse_scan, uint64_t row_limit,
    bool (*can_slice)(const std::vector<LineairDBProxy::RangeValidationEntry>&,
                      const std::vector<LineairDBProxy::IndexValidationEntry>&)) {
  const bool same_table = e.table_name == table_name;
  const bool same_direction = e.reverse_scan == reverse_scan;
  const bool same_limit = e.row_limit == row_limit;
  const bool covers_range = e.start_key <= start_key && end_key <= e.end_key;
  const bool exact_range = e.start_key == start_key && e.end_key == end_key;
  // A FILTERED entry holds only predicate-matching rows; serving it to a
  // covering/sub-range probe (e.g. via the full-cover bucket) would return a
  // pruned row set the probe doesn't expect. Restrict filtered entries to an
  // exact-range hit (the scan that produced them). Unfiltered entries (the
  // common prefetch case) are unaffected. (Codex P1.)
  // RESIDUAL (latent, not reachable in single-statement atomic SQL): an exact
  // [start,end) re-scan of the SAME range with a DIFFERENT predicate in a
  // multi-statement transaction could still hit this filtered entry. A full
  // filter-identity match (compare e.filter_serialized to the probe's requested
  // filter) was considered but NOT applied: an FER/FES sub-scan's own pushed
  // filter need not byte-equal the serve-site handler's pushed_filter_, so the
  // strict match risks a false miss->abort for those probes. Revisit if
  // multi-statement same-range-different-filter prefetch becomes real.
  if (!e.filter_serialized.empty() && !exact_range) return false;
  // Codex P1 #4 fix: a `[wide, LIMIT N]` cached entry holds the FIRST N
  // rows of the wider range, not the N rows the narrower probe would have
  // returned. Slicing is sound only when the cache is unlimited
  // (row_limit == 0). For limited entries require exact_range.
  return same_table && same_direction && same_limit && covers_range &&
         (exact_range ||
          (e.row_limit == 0 && can_slice(e.range_versions, e.index_reads)));
}

// Phase-1A fast slice for TRUE full-cover unfiltered entries.
//
// Why: the existing slice_range_entry does `cached = src` (deep copy of the
// whole entry, including range_versions with all result_keys) then linearly
// filters rows. For Q21 SF=1 step0 lineitem = 6M rows; an inner FER probe
// expects ~4 matching rows. Doing 12M inner probes through that path = 36T
// ops. Catastrophic.
//
// What: binary_search the sorted `src.rows` for [start, end), assign just the
// matching slice. range_versions / index_reads / filter_serialized are left
// EMPTY on purpose:
//   - the SOURCE entry's range_versions are eagerly activated when it is
//     pushed into local_range_scans_ (see push_local_range_scan below), so
//     the OCC commit-time replay already covers the full range
//   - the caller's `activate_range_validation(slice.range_versions, ...)` on
//     an empty vector is a no-op (no result_keys to rehash). Returning the
//     full src.range_versions would re-hash all 6M result_keys per probe,
//     which made Phase-1A v1 time out at 200s.
//
// Safety: callers gate on the source entry being a TRUE full-cover (empty
// start_key, no filter, no row_limit). See lookup_local_range_scan below.
// Other slice cases continue to use slice_range_entry's verbatim path.
static LineairDBTransaction::LocalRangeScanEntry slice_range_entry_fast(
    const LineairDBTransaction::LocalRangeScanEntry& src,
    const std::string& start_key, const std::string& end_key) {
  LineairDBTransaction::LocalRangeScanEntry out;
  out.table_name = src.table_name;
  out.start_key = start_key;
  out.end_key = end_key.empty() ? src.end_key : end_key;
  out.reverse_scan = src.reverse_scan;
  out.row_limit = src.row_limit;
  auto key_less = [](const std::pair<std::string, std::string>& r,
                     const std::string& k) { return r.first < k; };
  auto lo = std::lower_bound(src.rows.begin(), src.rows.end(), start_key, key_less);
  auto hi = std::lower_bound(src.rows.begin(), src.rows.end(), out.end_key, key_less);
  out.rows.assign(lo, hi);
  if (!src.row_tids.empty() && src.row_tids.size() == src.rows.size()) {
    const size_t i0 = static_cast<size_t>(lo - src.rows.begin());
    const size_t i1 = static_cast<size_t>(hi - src.rows.begin());
    out.row_tids.assign(src.row_tids.begin() + i0, src.row_tids.begin() + i1);
  }
  // out.range_versions / out.index_reads / out.filter_serialized stay empty.
  return out;
}

// Slice a matched primary range entry down to [start_key,end_key). For an
// exact-range hit this is a verbatim copy (result_keys preserved); for a
// narrower slice it keeps only rows/keys inside the requested window but never
// drops filter-rejected keys from range_versions.result_keys (those keep the
// server's pre-filter full key set so commit's logical revalidation matches).
static LineairDBTransaction::LocalRangeScanEntry slice_range_entry(
    const LineairDBTransaction::LocalRangeScanEntry& src,
    const std::string& start_key, const std::string& end_key) {
  LineairDBTransaction::LocalRangeScanEntry cached = src;
  const bool exact_range =
      src.start_key == start_key && src.end_key == end_key;
  const std::string eff_end = end_key.empty() ? src.end_key : end_key;
  cached.start_key = start_key;
  cached.end_key = eff_end;
  std::vector<std::pair<std::string, std::string>> rows;
  std::vector<uint64_t> row_tids;
  rows.reserve(cached.rows.size());
  row_tids.reserve(cached.row_tids.size());
  for (size_t i = 0; i < cached.rows.size(); ++i) {
    const auto& row = cached.rows[i];
    if (row.first >= start_key && row.first < eff_end) {
      rows.push_back(row);
      if (i < cached.row_tids.size()) row_tids.push_back(cached.row_tids[i]);
    }
  }
  cached.rows = std::move(rows);
  cached.row_tids = std::move(row_tids);
  for (auto& range : cached.range_versions) {
    if (!range_validation_is_logical(range)) continue;
    if (!exact_range) {
      std::vector<std::string> sliced_keys;
      std::vector<std::string> sliced_pks;
      sliced_keys.reserve(range.result_keys.size());
      for (size_t k = 0; k < range.result_keys.size(); ++k) {
        const std::string& rk = range.result_keys[k];
        if (rk >= start_key && rk < eff_end) {
          sliced_keys.push_back(rk);
          if (k < range.result_primary_keys.size()) {
            sliced_pks.push_back(range.result_primary_keys[k]);
          }
        }
      }
      range.result_keys = std::move(sliced_keys);
      range.result_primary_keys = std::move(sliced_pks);
    }
    range.start_key = start_key;
    range.end_key = eff_end;
  }
  return cached;
}

static std::string fe_hex(const std::string& s) {
  static const char* h = "0123456789abcdef";
  std::string o;
  for (unsigned char c : s) { o.push_back(h[c >> 4]); o.push_back(h[c & 15]); }
  return o;
}

// Encoded key-part prefixes of `key`, shortest-first, EXCLUDING the full key.
// A FER/FES prefetch entry is keyed by N key parts, but MySQL's runtime probe
// for a composite index can carry MORE parts (e.g. lineitem l_partkey prefetch
// keyed by [l_partkey], probed by [l_partkey][l_suppkey]). The longer probe is
// covered by the shorter prefetched range, so on an exact-start-key index miss
// we retry with each prefix to find the covering entry in O(#keyparts). Each
// int key part is [0x00 marker][0x10 INT][2-byte len][len bytes] (header 4 +
// value). Parsing stops at the first non-int / malformed part (string parts and
// the like simply yield no extra prefixes — safe, just no O(1) prefix hit).
static std::vector<std::string> keypart_prefixes(const std::string& key) {
  std::vector<std::string> out;
  size_t off = 0;
  while (off + 4 <= key.size()) {
    const unsigned char marker = static_cast<unsigned char>(key[off]);
    const unsigned char type = static_cast<unsigned char>(key[off + 1]);
    if (marker != 0x00 || type != 0x10) break;  // only int parts understood
    const size_t len = (static_cast<unsigned char>(key[off + 2]) << 8) |
                       static_cast<unsigned char>(key[off + 3]);
    const size_t part = 4 + len;
    if (off + part > key.size()) break;
    off += part;
    if (off < key.size()) out.push_back(key.substr(0, off));  // exclude full key
  }
  return out;
}

std::optional<LineairDBTransaction::LocalRangeScanEntry>
LineairDBTransaction::lookup_local_range_scan(
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key, bool reverse_scan, uint64_t row_limit) const {
  // O(1) exact-start-key path: FER prefetch registers one entry per probe.
  auto idx_it = range_scan_index_.find(start_key);
  if (std::getenv("HELIOS_FE_DEBUG") && !range_scan_index_.empty()) {
    std::string sample;
    if (!range_scan_index_.empty())
      sample = fe_hex(range_scan_index_.begin()->first);
    std::fprintf(stderr,
        "[LURS] tbl=%s start=%s end_sz=%zu rlim=%llu idxhit=%d nidx=%zu "
        "sample_entry_start=%s\n",
        table_name.c_str(), fe_hex(start_key).c_str(), end_key.size(),
        (unsigned long long)row_limit,
        idx_it != range_scan_index_.end() ? 1 : 0, range_scan_index_.size(),
        sample.c_str());
  }
  if (idx_it != range_scan_index_.end()) {
    for (auto rit = idx_it->second.rbegin(); rit != idx_it->second.rend();
         ++rit) {
      const auto& cand = local_range_scans_[*rit];
      if (range_entry_matches(cand, table_name, start_key, end_key,
                              reverse_scan, row_limit,
                              &range_validation_can_be_sliced)) {
        return slice_range_entry(cand, start_key, end_key);
      }
    }
  }
  // O(#keyparts) prefix probe: a composite probe ([p0][p1]) may be covered by a
  // prefetch entry keyed by a shorter prefix ([p0]). Try each key-part prefix.
  for (const std::string& pfx : keypart_prefixes(start_key)) {
    auto pit = range_scan_index_.find(pfx);
    if (pit == range_scan_index_.end()) continue;
    for (auto rit = pit->second.rbegin(); rit != pit->second.rend(); ++rit) {
      const auto& cand = local_range_scans_[*rit];
      if (range_entry_matches(cand, table_name, start_key, end_key, reverse_scan,
                              row_limit, &range_validation_can_be_sliced))
        return slice_range_entry(cand, start_key, end_key);
    }
  }
  // Full-cover bucket: a full-table S: scan is registered under start_key "".
  // It can cover ANY [start,end) probe, but the exact/keypart probes above key
  // on the probe's own start, so they never find it. Check it explicitly here
  // (O(#full scans), tiny) BEFORE the linear-cap bailout — otherwise, once many
  // per-probe FER/FES entries exist (>cap), a probe coverable by the full scan
  // would falsely miss and (with miss->abort) abort a correct query. (Codex.)
  //
  // Phase-1A: when the source is a TRUE full-cover (empty start_key,
  // unfiltered, unlimited), route to slice_range_entry_fast which avoids the
  // O(N) deep-copy + linear filter. Required when the inner FER step is
  // dropped by the planner and N inner probes fall through to step0's 6M-row
  // entry (otherwise: 36T ops per Q21).
  {
    auto fit = range_scan_index_.find(std::string());
    if (fit != range_scan_index_.end()) {
      for (auto rit = fit->second.rbegin(); rit != fit->second.rend(); ++rit) {
        const auto& cand = local_range_scans_[*rit];
        if (range_entry_matches(cand, table_name, start_key, end_key,
                                reverse_scan, row_limit,
                                &range_validation_can_be_sliced)) {
          const bool full_cover_unfiltered =
              cand.start_key.empty() && cand.filter_serialized.empty() &&
              cand.row_limit == 0;
          if (full_cover_unfiltered)
            return slice_range_entry_fast(cand, start_key, end_key);
          return slice_range_entry(cand, start_key, end_key);
        }
      }
    }
  }
  // Linear fallback only for small caches (full-scan-slice plans); skipping it
  // when many per-probe entries exist avoids O(N^2). A miss returns nullopt and
  // the caller does a stateless RPC.
  if (local_range_scans_.size() > kScanCacheLinearScanCap) return std::nullopt;
  for (auto it = local_range_scans_.rbegin();
       it != local_range_scans_.rend(); ++it) {
    if (range_entry_matches(*it, table_name, start_key, end_key, reverse_scan,
                            row_limit, &range_validation_can_be_sliced)) {
      return slice_range_entry(*it, start_key, end_key);
    }
  }
  return std::nullopt;
}

const LineairDBTransaction::LocalRangeScanEntry*
LineairDBTransaction::find_negative_covering_range_scan(
    const std::string& table_name, const std::string& key) const {
  // Above this many pre-filter keys we skip negative caching (the linear
  // membership scan that proves absence would be too costly); the stateless
  // fallback below stays correct, only the prefetch win is lost.
  static constexpr size_t kNegativeMembershipCap = 8192;

  auto entry_proves_absent =
      [&](size_t idx) -> const LocalRangeScanEntry* {
    const LocalRangeScanEntry& e = local_range_scans_[idx];
    if (e.table_name != table_name) return nullptr;
    if (e.row_limit != 0) return nullptr;   // limited scan: range not read in full
    if (e.end_key.empty()) return nullptr;  // empty end_key unreliable for phantom
    if (!key_is_in_range(key, e.start_key, e.end_key)) return nullptr;
    // Prove absence against the LOGICAL pre-filter key set (range_versions.
    // result_keys), NOT the post-filter cached rows: a row dropped by a pushed
    // filter still exists on the server, so "in range but not in rows" is not
    // absence. (Codex review.)
    bool any_logical = false;
    for (const auto& rv : e.range_versions) {
      if (!range_validation_is_logical(rv)) continue;
      any_logical = true;
      if (rv.result_keys.size() > kNegativeMembershipCap) return nullptr;
      for (const auto& rk : rv.result_keys)
        if (rk == key) return nullptr;  // present (maybe filtered) -> not absent
    }
    if (any_logical) return &e;
    // No logical key set (PHYSICAL OCC mode, the default: scans emit only
    // node-version entries, no result_keys). For an UNFILTERED, fully-read
    // range, `rows` IS the complete set of existing keys in [start,end), so a
    // key in range but absent from rows is provably absent — and the entry's
    // node-version range_versions still abort at commit if a concurrent INSERT
    // adds the key (the caller activates them). Filtered entries are excluded:
    // a row dropped by the pushed filter still exists on the server, so
    // absence-from-rows would be unsound there (Codex review). row_limit==0 was
    // already required above. Binary search (rows are key-sorted) — O(log n),
    // so no membership cap is needed (unlike the logical linear scan).
    if (!e.filter_serialized.empty()) return nullptr;
    if (e.reverse_scan) return nullptr;  // ascending-rows assumption (see lookup_positive_covering_range_row)
    if (e.range_versions.empty()) return nullptr;  // no phantom guard -> unsafe
    auto key_less = [](const std::pair<std::string, std::string>& r,
                       const std::string& k) { return r.first < k; };
    auto lb = std::lower_bound(e.rows.begin(), e.rows.end(), key, key_less);
    if (lb != e.rows.end() && lb->first == key) return nullptr;  // present
    return &e;  // absent
  };

  // Candidate start-keys (deduped): the key itself ([key,next(key)) and
  // single-PK point scans), each key-part prefix (FER prefix scans), and ""
  // (full-table S: scans, which register under an empty start_key).
  std::vector<std::string> cands;
  cands.push_back(key);
  for (auto& p : keypart_prefixes(key)) cands.push_back(p);
  cands.emplace_back();
  std::unordered_set<std::string> seen;
  for (const auto& c : cands) {
    if (!seen.insert(c).second) continue;
    auto it = range_scan_index_.find(c);
    if (it == range_scan_index_.end()) continue;
    for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit)
      if (const LocalRangeScanEntry* e = entry_proves_absent(*rit)) return e;
  }
  // Small-cache linear fallback for entries not keyed by any candidate start.
  if (local_range_scans_.size() <= kScanCacheLinearScanCap)
    for (size_t i = local_range_scans_.size(); i-- > 0;)
      if (const LocalRangeScanEntry* e = entry_proves_absent(i)) return e;
  return nullptr;
}

std::optional<LineairDBTransaction::PositiveRangeHit>
LineairDBTransaction::lookup_positive_covering_range_row(
    const std::string& table_name, const std::string& key) const {
  // Binary-search a covering range entry's sorted `rows` for the EXACT key.
  // A hit means the row was prefetched and is provably present; we never infer
  // absence here (that is find_negative_covering_range_scan's job, which is
  // sound about filtered/physical entries). Filtered/limited entries are fine
  // for a positive hit: a row that survives into `rows` is a real present row.
  auto probe_entry = [&](size_t idx) -> std::optional<PositiveRangeHit> {
    const LocalRangeScanEntry& e = local_range_scans_[idx];
    if (e.table_name != table_name) return std::nullopt;
    // Binary search assumes ascending rows; a reverse-scan entry may store them
    // descending. FER sub-scans are always forward (ingest hardcodes false), so
    // skipping reverse entries costs nothing here and stays safe.
    if (e.reverse_scan) return std::nullopt;
    if (!key_is_in_range(key, e.start_key, e.end_key)) return std::nullopt;
    // Need a per-row TID for each row to record the commit obligation; without a
    // 1:1 tid array we cannot validate the served value, so decline (fall back
    // to the RPC/abort path rather than serve an unvalidatable row).
    if (e.row_tids.size() != e.rows.size()) return std::nullopt;
    auto key_less = [](const std::pair<std::string, std::string>& r,
                       const std::string& k) { return r.first < k; };
    auto it = std::lower_bound(e.rows.begin(), e.rows.end(), key, key_less);
    if (it == e.rows.end() || it->first != key) return std::nullopt;
    return PositiveRangeHit{&e, static_cast<size_t>(it - e.rows.begin())};
  };

  // Same candidate starts as the negative cache: the key itself (point/single-PK
  // scans), each key-part prefix (FER/FES prefix sub-scans — the common case),
  // and "" (full-table S: scans).
  std::vector<std::string> cands;
  cands.push_back(key);
  for (auto& p : keypart_prefixes(key)) cands.push_back(p);
  cands.emplace_back();
  std::unordered_set<std::string> seen;
  for (const auto& c : cands) {
    if (!seen.insert(c).second) continue;
    auto it = range_scan_index_.find(c);
    if (it == range_scan_index_.end()) continue;
    for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit)
      if (auto hit = probe_entry(*rit)) return hit;
  }
  // Small-cache linear fallback for entries not keyed by any candidate start
  // (mirrors find_negative_covering_range_scan).
  if (local_range_scans_.size() <= kScanCacheLinearScanCap)
    for (size_t i = local_range_scans_.size(); i-- > 0;)
      if (auto hit = probe_entry(i)) return hit;
  return std::nullopt;
}

static bool secondary_entry_matches(
    const LineairDBTransaction::LocalSecondaryScanEntry& e,
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key,
    uint64_t row_limit,
    bool (*can_slice)(const std::vector<LineairDBProxy::RangeValidationEntry>&,
                      const std::vector<LineairDBProxy::IndexValidationEntry>&)) {
  const bool same_index =
      e.table_name == table_name && e.index_name == index_name;
  const bool same_limit = e.row_limit == row_limit;
  const bool covers_range = e.start_key <= start_key && end_key <= e.end_key;
  const bool exact_range = e.start_key == start_key && e.end_key == end_key;
  // Codex P1 #4 fix (mirror of range_entry_matches): limited cached entry
  // can only be reused on an exact-range match; slicing it loses the
  // "first N of the wider range" semantics.
  return same_index && same_limit && covers_range &&
         (exact_range ||
          (e.row_limit == 0 && can_slice(e.range_versions, e.index_reads)));
}

static LineairDBTransaction::LocalSecondaryScanEntry slice_secondary_entry(
    const LineairDBTransaction::LocalSecondaryScanEntry& src,
    const std::string& start_key, const std::string& end_key) {
  LineairDBTransaction::LocalSecondaryScanEntry cached = src;
  // Open-ended scan (empty end_key) means "to the cached entry's end" — mirror
  // slice_range_entry, else `key < ""` rejects every row (Codex P1).
  const std::string eff_end = end_key.empty() ? src.end_key : end_key;
  cached.start_key = start_key;
  cached.end_key = eff_end;
  if (cached.secondary_keys.size() == cached.primary_keys.size()) {
    std::vector<std::string> secondary_keys;
    std::vector<std::string> primary_keys;
    secondary_keys.reserve(cached.secondary_keys.size());
    primary_keys.reserve(cached.primary_keys.size());
    for (size_t i = 0; i < cached.secondary_keys.size(); ++i) {
      if (cached.secondary_keys[i] >= start_key &&
          cached.secondary_keys[i] < eff_end) {
        secondary_keys.push_back(cached.secondary_keys[i]);
        primary_keys.push_back(cached.primary_keys[i]);
      }
    }
    cached.secondary_keys = std::move(secondary_keys);
    cached.primary_keys = std::move(primary_keys);
  }
  for (auto& range : cached.range_versions) {
    if (!range_validation_is_logical(range)) continue;
    range.start_key = start_key;
    range.end_key = eff_end;
    range.result_keys = cached.secondary_keys;
    range.result_primary_keys = cached.primary_keys;
  }
  return cached;
}

std::optional<LineairDBTransaction::LocalSecondaryScanEntry>
LineairDBTransaction::lookup_local_secondary_scan(
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key,
    bool reverse_scan, uint64_t row_limit) const {
  (void)reverse_scan; // SI cache keeps the query-specific order from the plan

  // O(1) exact-start-key path: FES prefetch registers one entry per probe.
  std::string ikey = index_name;
  ikey.push_back('\0');
  ikey.append(start_key);
  auto idx_it = secondary_scan_index_.find(ikey);
  if (std::getenv("HELIOS_FE_DEBUG") && !secondary_scan_index_.empty()) {
    std::string sample = fe_hex(secondary_scan_index_.begin()->first);
    std::fprintf(stderr,
        "[LUSS] tbl=%s idx=%s start=%s end_sz=%zu idxhit=%d nidx=%zu "
        "sample=%s\n", table_name.c_str(), index_name.c_str(),
        fe_hex(start_key).c_str(), end_key.size(),
        idx_it != secondary_scan_index_.end() ? 1 : 0,
        secondary_scan_index_.size(), sample.c_str());
  }
  if (idx_it != secondary_scan_index_.end()) {
    for (auto rit = idx_it->second.rbegin(); rit != idx_it->second.rend();
         ++rit) {
      const auto& cand = local_secondary_scans_[*rit];
      if (secondary_entry_matches(cand, table_name, index_name, start_key,
                                  end_key, row_limit,
                                  &range_validation_can_be_sliced)) {
        return slice_secondary_entry(cand, start_key, end_key);
      }
    }
  }
  // O(#keyparts) prefix probe: a composite-index probe ([p0][p1]) covered by a
  // prefetch entry keyed by a shorter prefix ([p0]).
  for (const std::string& pfx : keypart_prefixes(start_key)) {
    std::string pk = index_name;
    pk.push_back('\0');
    pk.append(pfx);
    auto pit = secondary_scan_index_.find(pk);
    if (pit == secondary_scan_index_.end()) continue;
    for (auto rit = pit->second.rbegin(); rit != pit->second.rend(); ++rit) {
      const auto& cand = local_secondary_scans_[*rit];
      if (secondary_entry_matches(cand, table_name, index_name, start_key,
                                  end_key, row_limit,
                                  &range_validation_can_be_sliced))
        return slice_secondary_entry(cand, start_key, end_key);
    }
  }
  // Full-cover bucket: a full secondary scan is registered under
  // (index_name + '\0' + ""). Check it before the linear-cap bailout so a
  // covering full scan still serves a probe when many per-probe entries exist.
  // (Codex; mirrors lookup_local_range_scan.)
  {
    std::string fk = index_name;
    fk.push_back('\0');
    auto fit = secondary_scan_index_.find(fk);
    if (fit != secondary_scan_index_.end()) {
      for (auto rit = fit->second.rbegin(); rit != fit->second.rend(); ++rit) {
        const auto& cand = local_secondary_scans_[*rit];
        if (secondary_entry_matches(cand, table_name, index_name, start_key,
                                    end_key, row_limit,
                                    &range_validation_can_be_sliced))
          return slice_secondary_entry(cand, start_key, end_key);
      }
    }
  }
  if (local_secondary_scans_.size() > kScanCacheLinearScanCap)
    return std::nullopt;
  for (auto it = local_secondary_scans_.rbegin();
       it != local_secondary_scans_.rend(); ++it) {
    if (secondary_entry_matches(*it, table_name, index_name, start_key, end_key,
                                row_limit, &range_validation_can_be_sliced)) {
      return slice_secondary_entry(*it, start_key, end_key);
    }
  }
  return std::nullopt;
}

bool LineairDBTransaction::has_oneshot_local_state() const {
  return !stateless_read_set_.empty() || !write_buffer_ops_.empty() ||
         !rowcount_deltas_.empty() || !local_read_set_.empty() ||
         !local_write_set_.empty() || !range_validation_set_.empty() ||
         !index_validation_set_.empty() || !local_range_scans_.empty() ||
         !local_secondary_scans_.empty();
}

bool LineairDBTransaction::fallback_to_normal_transaction(const char* reason) {
  if (!oneshot_mode_) {
    // oneshot may have been disabled mid-statement by maybe_auto_stage (no
    // plan) AFTER begin already deferred the server tx (oneshot defers
    // tx_begin to the prefetch RPC). A normal scan would then run with
    // tx_id == -1 and the commit aborts (observed: MIN/MAX index access).
    // Begin the server tx on demand so the normal path has a real tx. (d)
    if (is_not_started()) begin_transaction();
    return !is_aborted_;
  }

  // A clean transaction can still switch to the normal scan-capable path
  const bool has_oneshot_state = has_oneshot_local_state();
  if (!has_oneshot_state) {
    oneshot_mode_ = false;
    oneshot_registered_ = false;
    begin_transaction();
    return !is_aborted_;
  }

  // Mixing stateless point reads with scans would need phantom tracking
  LOG_WARNING("Oneshot fallback blocked by prior local state: %s", reason);
  rpc_trace_.record_local_view(std::string("abort_fallback_") + reason);
  is_aborted_ = true;
  return false;
}

bool LineairDBTransaction::oneshot_validate_and_commit() {
  bool was_aborted = is_aborted_;

  std::vector<LineairDBProxy::StatelessReadKey> reads;
  std::vector<uint64_t> read_tids;
  std::vector<bool> read_found;
  if (!was_aborted) {
    reads.reserve(stateless_read_set_.size());
    read_tids.reserve(stateless_read_set_.size());
    read_found.reserve(stateless_read_set_.size());
    for (const auto& entry : stateless_read_set_) {
      reads.push_back({entry.table_name, entry.key});
      read_tids.push_back(entry.tid);
      read_found.push_back(entry.found);
    }
  }

  std::vector<std::pair<std::string, int64_t>> server_deltas;
  if (!was_aborted && !rowcount_deltas_.empty()) {
    server_deltas.reserve(rowcount_deltas_.size());
    for (const auto& entry : rowcount_deltas_) {
      if (entry.share != nullptr && entry.delta != 0)
        server_deltas.emplace_back(entry.table_name, entry.delta);
    }
  }

  bool committed = false;
  std::string abort_reason;
  const bool timeprof = std::getenv("HELIOS_TIMEPROF") != nullptr;
  auto tp_now = []() {
    timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return static_cast<uint64_t>(t.tv_sec) * 1000000000ull + t.tv_nsec;
  };
  // read-only no-validation: skip the entire commit-validation RPC (2-RPC ->
  // 1-RPC). Only when there are genuinely no buffered writes (the gate ensures
  // SELECT, but assert here for safety: if a write somehow buffered, fall
  // through to the normal validating commit). (docs/phase7_readonly_novalidate.md)
  if (!was_aborted && ro_novalidate_) {
    if (write_buffer_ops_.empty()) {
      committed = true;  // read-only: no validation needed, skip commit RPC
    } else {
      // A write buffered under ro_novalidate_: the read/range OCC obligations
      // were already skipped, so this write cannot be validly committed. The
      // SELECT-only gate should make this impossible; hard-abort if it ever
      // happens. (Codex 2026-05-29.)
      committed = false;
      rpc_trace_.record_local_view("abort_ro_novalidate_unexpected_write");
    }
  } else if (!was_aborted) {
    const uint64_t t0 = timeprof ? tp_now() : 0;
    // Phase-6 range-hash OCC: only for a read-only txn (no buffered writes)
    // that was eligible. The server then revalidates full-cover ranges via
    // retained footprint digests instead of the per-row reads we skipped.
    const bool use_range_hash =
        rangehash_eligible_ && write_buffer_ops_.empty();
    committed = lineairdb_proxy->tx_validate_and_commit(
        reads, read_tids, read_found, range_validation_set_,
        index_validation_set_,
        write_buffer_ops_, server_deltas, isFence, &abort_reason,
        tx_occ_key_, use_range_hash);
    if (timeprof) {
      tp_commit_rpc_ns_ += (tp_now() - t0);
      ++tp_commit_rpc_count_;
    }
    if (!committed && !abort_reason.empty()) {
      rpc_trace_.record_local_view("abort_validate_" + abort_reason);
    }
  }
  if (timeprof) {
    auto ms = [](uint64_t ns){ return ns / 1000000.0; };
    std::fprintf(stderr,
        "[TIMEPROF] tx=%p oneshot=1 rpc_exec=%.1fms(%u) ingest=%.1fms(%u) "
        "act_rv=%.1fms(%u) commit=%.1fms(%u) committed=%d aborted=%d\n",
        (void*)this,
        ms(tp_rpc_execute_ns_), tp_rpc_execute_count_,
        ms(tp_ingest_ns_), tp_ingest_count_,
        ms(tp_activate_rv_ns_), tp_activate_rv_count_,
        ms(tp_commit_rpc_ns_), tp_commit_rpc_count_,
        committed ? 1 : 0, was_aborted ? 1 : 0);
    std::fflush(stderr);
  }
  if (htp_enabled()) {
    auto ms = [](uint64_t ns){ return ns / 1000000.0; };
    std::fprintf(stderr,
        "[HTIMEPROF] tx=%p "
        "rnd_init=%.2fms(%u) rnd_next=%.2fms(%u) rnd_pos=%.2fms(%u) "
        "idx_init=%.2fms(%u) idx_read=%.2fms(%u) idx_next=%.2fms(%u) "
        "idx_next_same=%.2fms(%u) idx_first=%.2fms(%u) idx_last=%.2fms(%u) "
        "idx_prev=%.2fms(%u) "
        "ext_lock=%.2fms(%u) start_stmt=%.2fms(%u) store_lock=%.2fms(%u) "
        "set_fields=%.2fms(%u)\n",
        (void*)this,
        ms(htp_.rnd_init_ns), htp_.rnd_init_n,
        ms(htp_.rnd_next_ns), htp_.rnd_next_n,
        ms(htp_.rnd_pos_ns), htp_.rnd_pos_n,
        ms(htp_.index_init_ns), htp_.index_init_n,
        ms(htp_.index_read_map_ns), htp_.index_read_map_n,
        ms(htp_.index_next_ns), htp_.index_next_n,
        ms(htp_.index_next_same_ns), htp_.index_next_same_n,
        ms(htp_.index_first_ns), htp_.index_first_n,
        ms(htp_.index_last_ns), htp_.index_last_n,
        ms(htp_.index_prev_ns), htp_.index_prev_n,
        ms(htp_.external_lock_ns), htp_.external_lock_n,
        ms(htp_.start_stmt_ns), htp_.start_stmt_n,
        ms(htp_.store_lock_ns), htp_.store_lock_n,
        ms(htp_.set_fields_ns), htp_.set_fields_n);
    std::fflush(stderr);
  }
  if (!committed) {
    thd_mark_transaction_to_rollback(thread, 1);
  }

  if (!was_aborted && committed && !rowcount_deltas_.empty()) {
    const uint64_t tid = static_cast<uint64_t>(thread->thread_id());
    const size_t shard =
        static_cast<size_t>(tid) & (LineairDB_share::kRowCountShards - 1);

    for (const auto& entry : rowcount_deltas_) {
      if (entry.share == nullptr || entry.delta == 0)
        continue;

      entry.share->rowcount_shards[shard].delta.fetch_add(
          entry.delta, std::memory_order_relaxed);
    }
  }

  if (rpc_trace_.active()) {
    RpcTraceLogger::instance().log_line(
        rpc_trace_.finalize_jsonl(committed && !was_aborted));
  }
  lineairdb_proxy->set_current_trace(nullptr);

  delete this;
  return committed;
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  rpc_trace_.start(-1, std::this_thread::get_id());
  lineairdb_proxy->set_current_trace(&rpc_trace_);

  if (oneshot_mode_) {
    oneshot_registered_ = true;
    is_aborted_ = false;
    if (thd_is_transaction()) {
      isTransaction = true;
      register_transaction_to_mysql();
    }
    else {
      register_single_statement_to_mysql();
    }
    return;
  }

  tx_id = lineairdb_proxy->tx_begin_transaction();
  // TODO: maybe need error handling when tx_id == -1
  assert(tx_id != -1);
  rpc_trace_.set_tx_id(tx_id);
  is_aborted_ = false;

  if (thd_is_transaction()) {
    isTransaction = true;
    register_transaction_to_mysql();
  }
  else {
    register_single_statement_to_mysql();
  }
}

void LineairDBTransaction::set_status_to_abort() {
  if (oneshot_mode_ || tx_id == -1) {
    is_aborted_ = true;
    return;
  }
  // Skip TX_ABORT RPC if the server already knows (is_aborted_ was set from an RPC response).
  if (!is_aborted_) {
    lineairdb_proxy->tx_abort(tx_id);
  }
  is_aborted_ = true;
}

bool LineairDBTransaction::end_transaction() {
  if (oneshot_mode_) {
    return oneshot_validate_and_commit();
  }

  assert(tx_id != -1);
  flush_write_buffer();
  bool was_aborted = is_aborted_;

  // Build row-delta pairs for the server (table_name, delta).
  std::vector<std::pair<std::string, int64_t>> server_deltas;
  if (!was_aborted && !rowcount_deltas_.empty()) {
    server_deltas.reserve(rowcount_deltas_.size());
    for (const auto &entry : rowcount_deltas_) {
      if (entry.share != nullptr && entry.delta != 0)
        server_deltas.emplace_back(entry.table_name, entry.delta);
    }
  }

  bool committed = lineairdb_proxy->db_end_transaction(tx_id, isFence, server_deltas);
  if (!committed) {
    thd_mark_transaction_to_rollback(thread, 1);
  }

  // Flush committed row-count deltas to local shards (for this proxy's info()).
  if (!was_aborted && committed && !rowcount_deltas_.empty()) {
    const uint64_t tid = static_cast<uint64_t>(thread->thread_id());
    const size_t shard =
        static_cast<size_t>(tid) & (LineairDB_share::kRowCountShards - 1);

    for (const auto &entry : rowcount_deltas_) {
      if (entry.share == nullptr || entry.delta == 0)
        continue;

      entry.share->rowcount_shards[shard].delta.fetch_add(
          entry.delta, std::memory_order_relaxed);
    }
  }

  if (isFence && !was_aborted && committed) {
    lineairdb_proxy->db_fence();
  }

  if (rpc_trace_.active()) {
    RpcTraceLogger::instance().log_line(
        rpc_trace_.finalize_jsonl(committed && !was_aborted));
  }
  lineairdb_proxy->set_current_trace(nullptr);

  delete this;
  return committed;
}

void LineairDBTransaction::fence() const { lineairdb_proxy->db_fence(); }




bool LineairDBTransaction::thd_is_transaction() const {
  return ::thd_test_options(thread, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN | OPTION_TABLE_LOCK);
}

void LineairDBTransaction::register_transaction_to_mysql() {
  const ulonglong threadID = static_cast<ulonglong>(thread->thread_id());
  ::trans_register_ha(thread, isTransaction, hton, &threadID);
}

void LineairDBTransaction::register_single_statement_to_mysql() {
  register_transaction_to_mysql();
}
