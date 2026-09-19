#include "parallel_scan.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>
#include <vector>

#include "key_pack.hh"

namespace {

// One worker's share of the scan.
struct RefChunkOut {
    std::vector<std::string> scan_keys;
    std::vector<std::string> scan_values;
    std::vector<uint64_t> tids;
    bool serial_scan_required = false;
};

// Gathers [chunk_start, chunk_end) of the step's table into out. A worker
// thread releases its epoch before returning; the caller's thread does not.
void run_range(helios::storage::Database* db,
               const Helios::Protocol::TxExecuteReadPlan::PlanStep& step,
               const std::string& chunk_start, const std::string& chunk_end,
               RefChunkOut& out, bool release_epoch) {
    auto refs = db->ScanPax(step.table_name(), chunk_start, chunk_end,
                            step.scan_limit(), step.reverse_scan());
    if (!refs.ok) {
        out.serial_scan_required = true;
        if (release_epoch) db->ReleaseThreadEpoch();
        return;
    }

    for (auto& row_ref : refs.rows) {
        const auto* group =
            static_cast<const helios::storage::pax::PaxGroup*>(row_ref.group);
        std::string value;
        value.resize(row_ref.row_size);
        const size_t gathered = group->GatherRow(
            row_ref.slot, reinterpret_cast<std::byte*>(value.data()),
            row_ref.row_size);
        if (gathered != row_ref.row_size) value.resize(gathered);

        // The cells just read are valid only if the row TID still matches
        // the ref-scan observation. Otherwise re-read a stable row copy.
        if (helios::storage::CurrentTid(row_ref) != row_ref.tid) {
            auto reread = db->Read(step.table_name(), row_ref.key, nullptr);
            if (!reread.found) continue;
            out.scan_keys.push_back(std::move(row_ref.key));
            out.scan_values.push_back(std::move(reread.value));
            out.tids.push_back(reread.tid);
            continue;
        }

        out.scan_keys.push_back(std::move(row_ref.key));
        out.scan_values.push_back(std::move(value));
        out.tids.push_back(row_ref.tid);
    }

    if (release_epoch) db->ReleaseThreadEpoch();
}

}  // namespace

bool parallel_primary_pax_row_ref_scan(
    helios::storage::Database* db,
    const Helios::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    Helios::Protocol::TxExecuteReadPlan::StepResult* step_result) {
    if (db == nullptr) return false;

    std::vector<RefChunkOut> chunks;
    bool ran_parallel = false;
    if (step.scan_limit() == 0 && !step.reverse_scan()) {
        const unsigned nproc = std::thread::hardware_concurrency();
        const unsigned max_threads = std::min<unsigned>(nproc ? nproc : 4, 8);
        const std::vector<uint32_t> key_only_columns;
        auto first = db->Scan(step.table_name(), start_key, end_key, 1, false,
                              &key_only_columns);
        auto last = db->Scan(step.table_name(), start_key, end_key, 1, true,
                             &key_only_columns);
        int64_t lo = 0;
        int64_t hi = 0;
        if (max_threads > 1 && first.ok && !first.rows.empty() && last.ok &&
            !last.rows.empty() &&
            unpack_leading_int_key(first.rows.front().key, lo) &&
            unpack_leading_int_key(last.rows.front().key, hi) && hi > lo) {
          const uint64_t span = static_cast<uint64_t>(hi - lo) + 1;
          constexpr uint64_t kMinParallelRows = 500000;
          constexpr uint64_t kMorselRows = 128000;
          if (span >= kMinParallelRows) {
            const unsigned worker_count =
                static_cast<unsigned>(std::min<uint64_t>(
                    (span + kMorselRows - 1) / kMorselRows, max_threads));
            if (worker_count > 1) {
              std::vector<std::string> starts(worker_count);
              std::vector<std::string> ends(worker_count);
              for (unsigned i = 0; i < worker_count; ++i) {
                const int64_t begin_value =
                    lo + static_cast<int64_t>((span * i) / worker_count);
                const int64_t end_value =
                    (i + 1 == worker_count)
                        ? hi + 1
                        : lo + static_cast<int64_t>((span * (i + 1)) /
                                                    worker_count);
                starts[i] =
                    i == 0 ? start_key : pack_int_key_part(begin_value);
                ends[i] = i + 1 == worker_count
                              ? end_key
                              : pack_int_key_part(end_value);
              }

              chunks = std::vector<RefChunkOut>(worker_count);
              std::vector<std::thread> workers;
              workers.reserve(worker_count);
              for (unsigned worker_index = 0; worker_index < worker_count;
                   ++worker_index) {
                workers.emplace_back(run_range, db, std::cref(step),
                                     std::cref(starts[worker_index]),
                                     std::cref(ends[worker_index]),
                                     std::ref(chunks[worker_index]), true);
              }
              for (auto &worker : workers) worker.join();
              ran_parallel = true;
            }
          }
        }
    }
    if (!ran_parallel) {
        chunks = std::vector<RefChunkOut>(1);
        run_range(db, step, start_key, end_key, chunks[0], false);
    }

    for (const auto& chunk : chunks) {
        if (chunk.serial_scan_required) return false;
    }

    uint64_t emitted = 0;
    for (auto& chunk : chunks) {
        for (size_t i = 0; i < chunk.scan_keys.size(); ++i) {
            step_result->add_scan_keys(std::move(chunk.scan_keys[i]));
            step_result->add_scan_values(std::move(chunk.scan_values[i]));
            step_result->add_scan_tids(chunk.tids[i]);
            if (step.scan_limit() > 0 && ++emitted >= step.scan_limit()) {
                return true;
            }
        }
    }
    return true;
}
