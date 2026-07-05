#include "query_block_executor.hh"

#include <atomic>
#include <cstdint>
#include <exception>
#include <string>
#include <utility>
#include <vector>

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

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

class Executor {
 public:
    Executor(LineairDB::Database* db,
             const pb::TxExecuteQueryBlock::Request& request)
        : db_(db), request_(request) {}

    bool Run() {
        if (!PrepareTables()) return false;
        if (!ValidateNodeOrder()) return false;
        if (!TablesAreStillQuiet()) return fail("concurrent modification");
        return fail("query-block operators are not implemented");
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

    LineairDB::Database* db_;
    const pb::TxExecuteQueryBlock::Request& request_;
    std::string error_;
    std::vector<PaxStore*> stores_;
    std::vector<std::vector<uint64_t>> write_counter_snapshots_;
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
