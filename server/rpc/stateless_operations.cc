#include "lineairdb_rpc.hh"

#include <string>
#include <utility>
#include <vector>

#include "lineairdb.pb.h"

// Stateless read and client-driven OCC commit handlers: these act on the
// database directly and carry no server-side transaction state.

void LineairDBRpc::handleTxStatelessRead(const std::string& message,
                                         std::string& result) {
    LineairDB::Protocol::TxStatelessRead::Request request;
    LineairDB::Protocol::TxStatelessRead::Response response;

    request.ParseFromString(message);

    auto read_result =
        db_manager_->get_database()->StatelessRead(request.table_name(),
                                                   request.key());
    response.set_found(read_result.found);
    response.set_tid(read_result.tid);
    if (read_result.found) {
        response.set_value(std::move(read_result.value));
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessBatchRead(const std::string& message,
                                              std::string& result) {
    LineairDB::Protocol::TxStatelessBatchRead::Request request;
    LineairDB::Protocol::TxStatelessBatchRead::Response response;

    request.ParseFromString(message);

    std::vector<std::pair<std::string, std::string>> keys;
    keys.reserve(request.ops_size());
    for (const auto& op : request.ops()) {
        keys.emplace_back(op.table_name(), op.key());
    }

    auto read_results = db_manager_->get_database()->StatelessBatchRead(keys);
    for (auto& read_result : read_results) {
        auto* out = response.add_results();
        out->set_found(read_result.found);
        out->set_tid(read_result.tid);
        if (read_result.found) {
            out->set_value(std::move(read_result.value));
        }
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxValidateAndCommit(const std::string& message,
                                             std::string& result) {
    LineairDB::Protocol::TxValidateAndCommit::Request request;
    LineairDB::Protocol::TxValidateAndCommit::Response response;

    request.ParseFromString(message);

    std::vector<LineairDB::ExternalReadEntry> reads;
    reads.reserve(request.reads_size());
    for (const auto& read : request.reads()) {
        reads.push_back({read.table_name(), read.key(), read.tid(),
                         read.found()});
    }

    std::vector<LineairDB::ExternalWriteEntry> writes;
    writes.reserve(request.writes_size() + request.deletes_size());
    for (const auto& write : request.writes()) {
        writes.push_back({write.table_name(), write.key(), write.value(),
                          write.is_delete(), write.is_insert()});
    }
    for (const auto& del : request.deletes()) {
        writes.push_back({del.table_name(), del.key(), "", true, false});
    }

    std::vector<LineairDB::ExternalSecondaryIndexEntry> si_ops;
    si_ops.reserve(request.secondary_index_ops_size());
    for (const auto& op : request.secondary_index_ops()) {
        si_ops.push_back({op.table_name(), op.index_name(),
                          op.secondary_key(), op.primary_key(),
                          op.is_delete()});
    }

    std::vector<LineairDB::ExternalRangeReadEntry> range_reads;
    range_reads.reserve(request.range_reads_size());
    for (const auto& range : request.range_reads()) {
        LineairDB::ExternalRangeReadEntry entry;
        entry.table_name = range.table_name();
        entry.index_name = range.index_name();
        entry.start_key = range.start_key();
        entry.end_key = range.end_key();
        entry.row_limit = range.row_limit();
        entry.reverse_scan = range.reverse_scan();
        entry.result_keys.reserve(range.result_keys_size());
        for (const auto& key : range.result_keys()) {
            entry.result_keys.push_back(key);
        }
        entry.result_primary_keys.reserve(range.result_primary_keys_size());
        for (const auto& key : range.result_primary_keys()) {
            entry.result_primary_keys.push_back(key);
        }
        range_reads.push_back(std::move(entry));
    }

    std::string abort_detail;
    const bool committed =
        db_manager_->get_database()->ValidateAndCommit(reads, writes, si_ops,
                                                       range_reads,
                                                       &abort_detail);
    response.set_committed(committed);
    if (!committed && !abort_detail.empty()) {
        response.set_abort_detail(abort_detail);
        if (abort_detail == LineairDB::kDuplicateKeyAbortReason) {
            response.set_abort_reason(
                LineairDB::Protocol::ABORT_REASON_DUPLICATE_PRIMARY_KEY);
        }
    }

    if (committed && request.row_deltas_size() > 0) {
        row_counts_->apply_deltas(request.row_deltas());
    }
    if (committed && request.fence()) {
        db_manager_->get_database()->Fence();
    }

    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    result = response.SerializeAsString();
}
