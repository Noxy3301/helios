#include "query_block_executor.hh"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

#include "predicate_evaluator.hh"

namespace query_block {
namespace {

namespace pb = LineairDB::Protocol;
using LineairDB::Pax::PaxGroup;
using LineairDB::Pax::PaxStore;

void set_failure(pb::TxExecuteQueryBlock::Response* response,
                 const std::string& message) {
    if (response == nullptr) return;
    response->set_ok(false);
    response->set_error(message);
    response->clear_rows();
}

// Materialized operator output. Each logical tuple is represented as one PAX row
// reference per participating table.
struct NodeResult {
    std::vector<uint32_t> tables;
    std::vector<std::vector<uint64_t>> refs;

    size_t rows() const { return refs.empty() ? 0 : refs[0].size(); }

    int table_pos(uint32_t table_idx) const {
        for (size_t idx = 0; idx < tables.size(); ++idx) {
            if (tables[idx] == table_idx) return static_cast<int>(idx);
        }
        return -1;
    }
};

class Executor {
 public:
    Executor(LineairDB::Database* db,
             const pb::TxExecuteQueryBlock::Request& request)
        : db_(db), request_(request) {}

    bool Run() {
        if (!PrepareTables()) return false;
        if (!ValidateNodeOrder()) return false;
        if (!RunNodes()) return false;
        if (!TablesAreStillQuiet()) return fail("concurrent modification");
        return fail("query-block output is not implemented");
    }

    const std::string& error() const { return error_; }

 private:
    bool fail(std::string message) {
        if (error_.empty()) error_ = std::move(message);
        return false;
    }

    bool PrepareTables() {
        if (db_ == nullptr) return fail("database is unavailable");
        if (request_.tables_size() == 0) {
            return fail("query block has no tables");
        }

        stores_.assign(request_.tables_size(), nullptr);
        write_counter_snapshots_.assign(request_.tables_size(), {});
        for (int table_idx = 0; table_idx < request_.tables_size();
             ++table_idx) {
            PaxStore* store =
                db_->GetPaxStore(request_.tables(table_idx).table_name());
            if (store == nullptr) return fail("table has no PAX store");
            if (store->overflow_count() > 0) {
                return fail("table has heap fallback rows");
            }

            stores_[table_idx] = store;

            // Capture each allocated group's writer counter before operators
            // read strip cells. A changed counter rejects the whole request.
            std::vector<uint64_t>& snapshot =
                write_counter_snapshots_[table_idx];
            snapshot.resize(store->group_count());
            for (size_t group_idx = 0; group_idx < snapshot.size();
                 ++group_idx) {
                PaxGroup* group = store->group(group_idx);
                snapshot[group_idx] =
                    group == nullptr
                        ? 0
                        : group->write_counter.load(std::memory_order_acquire);
            }
        }
        return true;
    }

    bool ValidateNodeOrder() {
        if (request_.nodes_size() == 0) return fail("empty query block");

        for (int node_idx = 0; node_idx < request_.nodes_size(); ++node_idx) {
            const pb::QueryBlockNode& node = request_.nodes(node_idx);
            if (node.has_scan()) {
                if (node.scan().table_idx() >=
                    static_cast<uint32_t>(request_.tables_size())) {
                    return fail("scan table out of range");
                }
                continue;
            }
            if (node.has_join()) {
                if (node.join().build() >= static_cast<uint32_t>(node_idx) ||
                    node.join().probe() >= static_cast<uint32_t>(node_idx)) {
                    return fail("join child order");
                }
                continue;
            }
            if (node.has_aggregate()) {
                if (node.aggregate().input() >=
                    static_cast<uint32_t>(node_idx)) {
                    return fail("aggregate child order");
                }
                continue;
            }
            return fail("unknown query-block node");
        }
        return true;
    }

    bool RunScan(const pb::QueryBlockScan& scan, NodeResult* output) {
        if (scan.table_idx() >= static_cast<uint32_t>(request_.tables_size())) {
            return fail("scan table out of range");
        }

        PaxStore* store = stores_[scan.table_idx()];
        const size_t group_count = store->group_count();
        output->tables = {scan.table_idx()};
        output->refs.assign(1, {});
        if (group_count == 0) return true;

        const bool has_filter = scan.has_filter() && scan.filter().has_expr();
        const unsigned worker_count = static_cast<unsigned>(
            std::min<size_t>(WorkerCount(), group_count));
        std::vector<std::vector<uint64_t>> local_refs(worker_count);
        std::vector<char> worker_failed(worker_count, 0);
        std::vector<std::thread> workers;
        workers.reserve(worker_count);

        for (unsigned worker_idx = 0; worker_idx < worker_count; ++worker_idx) {
            workers.emplace_back([&, worker_idx] {
                PredicateEvaluator evaluator;
                std::vector<uint64_t>& refs = local_refs[worker_idx];
                for (size_t group_idx = worker_idx; group_idx < group_count;
                     group_idx += worker_count) {
                    PaxGroup* group = store->group(group_idx);
                    if (group == nullptr) continue;

                    // Turn one 64-slot visibility word into row references for
                    // the live slots that survive the optional predicate.
                    for (uint32_t base = 0; base < PaxGroup::kRows;
                         base += 64) {
                        uint64_t visible_bits = 0;
                        for (uint32_t bit = 0; bit < 64; ++bit) {
                            if (group->IsVisible(base + bit)) {
                                visible_bits |= uint64_t{1} << bit;
                            }
                        }

                        while (visible_bits != 0) {
                            const uint32_t bit = static_cast<uint32_t>(
                                __builtin_ctzll(visible_bits));
                            visible_bits &= visible_bits - 1;
                            const uint32_t slot = base + bit;
                            if (has_filter) {
                                if (!evaluator.set_row_from_pax(
                                        *group, slot,
                                        scan.filter().num_columns())) {
                                    worker_failed[worker_idx] = 1;
                                    return;
                                }
                                if (!evaluator.evaluate(scan.filter().expr())) {
                                    continue;
                                }
                            }
                            refs.push_back(group_idx * PaxGroup::kRows + slot);
                        }
                    }
                }
            });
        }

        for (std::thread& worker : workers) worker.join();
        for (char failed : worker_failed) {
            if (failed) return fail("scan filter cannot read PAX columns");
        }

        size_t total_refs = 0;
        for (const std::vector<uint64_t>& refs : local_refs) {
            total_refs += refs.size();
        }
        output->refs[0].reserve(total_refs);
        for (const std::vector<uint64_t>& refs : local_refs) {
            output->refs[0].insert(output->refs[0].end(), refs.begin(),
                                   refs.end());
        }
        return true;
    }

    bool RunNodes() {
        results_.resize(request_.nodes_size());
        for (int node_idx = 0; node_idx < request_.nodes_size(); ++node_idx) {
            const pb::QueryBlockNode& node = request_.nodes(node_idx);
            if (node.has_scan()) {
                if (!RunScan(node.scan(), &results_[node_idx])) return false;
                continue;
            }
            if (node.has_join()) {
                return fail("query-block join is not implemented");
            }
            if (node.has_aggregate()) {
                return fail("query-block aggregate is not implemented");
            }
            return fail("unknown query-block node");
        }
        return true;
    }

    bool TablesAreStillQuiet() const {
        for (size_t table_idx = 0; table_idx < stores_.size(); ++table_idx) {
            PaxStore* store = stores_[table_idx];
            const std::vector<uint64_t>& snapshot =
                write_counter_snapshots_[table_idx];
            for (size_t group_idx = 0; group_idx < snapshot.size();
                 ++group_idx) {
                PaxGroup* group = store->group(group_idx);
                const uint64_t current =
                    group == nullptr
                        ? 0
                        : group->write_counter.load(std::memory_order_acquire);
                if (current != snapshot[group_idx]) return false;
            }
        }
        return true;
    }

    unsigned WorkerCount() const {
        const unsigned hardware_threads = std::thread::hardware_concurrency();
        return std::min<unsigned>(hardware_threads == 0 ? 8 : hardware_threads,
                                  32);
    }

    LineairDB::Database* db_;
    const pb::TxExecuteQueryBlock::Request& request_;
    std::string error_;
    std::vector<PaxStore*> stores_;
    std::vector<std::vector<uint64_t>> write_counter_snapshots_;
    std::vector<NodeResult> results_;
};

const std::string& default_error(const Executor& executor) {
    static const std::string kDefaultError = "query-block execution failed";
    return executor.error().empty() ? kDefaultError : executor.error();
}

}  // namespace

void ExecuteQueryBlock(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteQueryBlock::Request& request,
    LineairDB::Protocol::TxExecuteQueryBlock::Response* response) {
    if (response == nullptr) return;
    response->Clear();

    try {
        Executor executor(db, request);
        if (!executor.Run()) {
            set_failure(response, default_error(executor));
            return;
        }
        response->set_ok(true);
    } catch (const std::exception& e) {
        set_failure(response, e.what());
    } catch (...) {
        set_failure(response, "query-block execution failed");
    }
}

}  // namespace query_block
