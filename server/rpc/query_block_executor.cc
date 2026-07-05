#include "query_block_executor.hh"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

#include "decimal_arithmetic.hh"
#include "predicate_evaluator.hh"
#include "row_codec.hh"

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

    PaxRowRef ToRowRef(uint32_t table_idx, uint64_t ref) const {
        PaxStore* store = stores_[table_idx];
        return PaxRowRef{
            store->group(ref / PaxGroup::kRows),
            static_cast<uint32_t>(ref % PaxGroup::kRows),
        };
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

    struct GroupState {
        std::vector<std::string> keys;
        std::vector<uint64_t> counts;
        std::vector<DecimalValue> decimals;
        std::vector<std::string> strings;
        std::vector<bool> has_value;
    };

    using GroupMap = std::unordered_map<std::string, GroupState>;

    bool AccumulateRange(const pb::QueryBlockAggregate& aggregate,
                         const NodeResult& input, size_t begin, size_t end,
                         GroupMap* groups) {
        const int group_count = aggregate.group_columns_size();
        const int aggregate_count = aggregate.aggs_size();

        std::vector<int> group_positions(group_count, -1);
        for (int group_idx = 0; group_idx < group_count; ++group_idx) {
            group_positions[group_idx] =
                input.table_pos(aggregate.group_columns(group_idx).table_idx());
            if (group_positions[group_idx] < 0) {
                return fail("group table is not in aggregate input");
            }
        }

        std::vector<int> aggregate_positions(aggregate_count, -1);
        for (int aggregate_idx = 0; aggregate_idx < aggregate_count;
             ++aggregate_idx) {
            const pb::QueryBlockAggFunc& function =
                aggregate.aggs(aggregate_idx);
            if (function.has_arg() || function.has_filter()) {
                aggregate_positions[aggregate_idx] =
                    input.table_pos(function.arg_table());
                if (aggregate_positions[aggregate_idx] < 0) {
                    return fail("aggregate table is not in aggregate input");
                }
            }
        }

        PredicateEvaluator evaluator;
        std::string key_buffer;
        std::vector<std::string_view> group_values(group_count);
        for (size_t row_idx = begin; row_idx < end; ++row_idx) {
            key_buffer.clear();
            for (int group_idx = 0; group_idx < group_count; ++group_idx) {
                const pb::QueryBlockColumnRef& column =
                    aggregate.group_columns(group_idx);
                const uint64_t ref =
                    input.refs[group_positions[group_idx]][row_idx];
                group_values[group_idx] =
                    extract_value_column(ToRowRef(column.table_idx(), ref),
                                         column.column());
                const uint32_t length =
                    static_cast<uint32_t>(group_values[group_idx].size());
                key_buffer.append(reinterpret_cast<const char*>(&length),
                                  sizeof(length));
                key_buffer.append(group_values[group_idx].data(),
                                  group_values[group_idx].size());
            }

            auto group_it = groups->find(key_buffer);
            GroupState* state = nullptr;
            if (group_it == groups->end()) {
                GroupState new_state;
                new_state.keys.resize(group_count);
                for (int group_idx = 0; group_idx < group_count; ++group_idx) {
                    new_state.keys[group_idx] =
                        std::string(group_values[group_idx]);
                }
                new_state.counts.assign(aggregate_count, 0);
                new_state.decimals.assign(aggregate_count, DecimalValue{});
                new_state.strings.assign(aggregate_count, {});
                new_state.has_value.assign(aggregate_count, false);
                state = &groups->emplace(std::move(key_buffer),
                                         std::move(new_state))
                             .first->second;
                key_buffer.clear();
            } else {
                state = &group_it->second;
            }

            for (int aggregate_idx = 0; aggregate_idx < aggregate_count;
                 ++aggregate_idx) {
                const pb::QueryBlockAggFunc& function =
                    aggregate.aggs(aggregate_idx);
                const int input_pos = aggregate_positions[aggregate_idx];
                const uint64_t ref =
                    input_pos >= 0 ? input.refs[input_pos][row_idx] : 0;

                if (function.has_filter() && function.filter().has_expr()) {
                    PaxRowRef row = ToRowRef(function.arg_table(), ref);
                    if (row.group == nullptr ||
                        !evaluator.set_row_from_pax(
                            *row.group, row.slot,
                            function.filter().num_columns())) {
                        return fail("aggregate filter cannot read PAX columns");
                    }
                    if (!evaluator.evaluate(function.filter().expr())) {
                        continue;
                    }
                }

                switch (function.kind()) {
                    case pb::QueryBlockAggFunc::COUNT:
                        state->counts[aggregate_idx] += 1;
                        break;
                    case pb::QueryBlockAggFunc::SUM:
                    case pb::QueryBlockAggFunc::AVG: {
                        DecimalValue value = evaluate_decimal_expression(
                            function.arg(),
                            ToRowRef(function.arg_table(), ref));
                        if (value.is_null) break;
                        add_decimal_value(state->decimals[aggregate_idx],
                                          value);
                        state->counts[aggregate_idx] += 1;
                        break;
                    }
                    case pb::QueryBlockAggFunc::MIN:
                    case pb::QueryBlockAggFunc::MAX: {
                        const bool wants_max =
                            function.kind() == pb::QueryBlockAggFunc::MAX;
                        if (function.cmp_kind() == 1) {
                            std::string_view value = extract_value_column(
                                ToRowRef(function.arg_table(), ref),
                                function.arg().column_index());
                            if (!state->has_value[aggregate_idx] ||
                                (wants_max
                                     ? value >
                                           std::string_view(
                                               state->strings[aggregate_idx])
                                     : value <
                                           std::string_view(
                                               state->strings[aggregate_idx]))) {
                                state->strings[aggregate_idx] =
                                    std::string(value);
                            }
                        } else {
                            DecimalValue value = evaluate_decimal_expression(
                                function.arg(),
                                ToRowRef(function.arg_table(), ref));
                            if (value.is_null) break;
                            if (!state->has_value[aggregate_idx] ||
                                (wants_max
                                     ? compare_decimal_values(
                                           value,
                                           state->decimals[aggregate_idx]) > 0
                                     : compare_decimal_values(
                                           value,
                                           state->decimals[aggregate_idx]) < 0)) {
                                state->decimals[aggregate_idx] = value;
                            }
                        }
                        state->has_value[aggregate_idx] = true;
                        break;
                    }
                    default:
                        return fail("unsupported aggregate function");
                }
            }
        }
        return true;
    }

    static void MergeGroups(GroupMap* destination, GroupMap* source,
                            const pb::QueryBlockAggregate& aggregate) {
        if (destination->empty()) {
            *destination = std::move(*source);
            return;
        }

        for (auto& entry : *source) {
            auto destination_it = destination->find(entry.first);
            if (destination_it == destination->end()) {
                destination->emplace(entry.first, std::move(entry.second));
                continue;
            }

            GroupState& destination_state = destination_it->second;
            GroupState& source_state = entry.second;
            for (int aggregate_idx = 0;
                 aggregate_idx < aggregate.aggs_size(); ++aggregate_idx) {
                const pb::QueryBlockAggFunc& function =
                    aggregate.aggs(aggregate_idx);
                switch (function.kind()) {
                    case pb::QueryBlockAggFunc::COUNT:
                        destination_state.counts[aggregate_idx] +=
                            source_state.counts[aggregate_idx];
                        break;
                    case pb::QueryBlockAggFunc::SUM:
                    case pb::QueryBlockAggFunc::AVG:
                        if (source_state.counts[aggregate_idx] > 0) {
                            add_decimal_value(
                                destination_state.decimals[aggregate_idx],
                                source_state.decimals[aggregate_idx]);
                            destination_state.counts[aggregate_idx] +=
                                source_state.counts[aggregate_idx];
                        }
                        break;
                    case pb::QueryBlockAggFunc::MIN:
                    case pb::QueryBlockAggFunc::MAX: {
                        if (!source_state.has_value[aggregate_idx]) break;
                        const bool wants_max =
                            function.kind() == pb::QueryBlockAggFunc::MAX;
                        if (function.cmp_kind() == 1) {
                            if (!destination_state.has_value[aggregate_idx] ||
                                (wants_max
                                     ? source_state.strings[aggregate_idx] >
                                           destination_state
                                               .strings[aggregate_idx]
                                     : source_state.strings[aggregate_idx] <
                                           destination_state
                                               .strings[aggregate_idx])) {
                                destination_state.strings[aggregate_idx] =
                                    std::move(
                                        source_state.strings[aggregate_idx]);
                            }
                        } else if (
                            !destination_state.has_value[aggregate_idx] ||
                            (wants_max
                                 ? compare_decimal_values(
                                       source_state.decimals[aggregate_idx],
                                       destination_state
                                           .decimals[aggregate_idx]) > 0
                                 : compare_decimal_values(
                                       source_state.decimals[aggregate_idx],
                                       destination_state
                                           .decimals[aggregate_idx]) < 0)) {
                            destination_state.decimals[aggregate_idx] =
                                source_state.decimals[aggregate_idx];
                        }
                        destination_state.has_value[aggregate_idx] = true;
                        break;
                    }
                    default:
                        break;
                }
            }
        }
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
