#include "lineairdb_rpc.hh"

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lineairdb.pb.h"
#include "rpc_timing.hh"

namespace {

// bound = kCommitResponseFixedOverheadBytes + kAbortReasonHeadroomBytes +
//         Sum_touched(name_len + kTableStatFieldOverheadBytes + kTableStatRowCountMaxBytes)
constexpr size_t kCommitResponseFixedOverheadBytes = 32;
constexpr size_t kTableStatFieldOverheadBytes = 16;
constexpr size_t kTableStatRowCountMaxBytes = 10;
constexpr size_t kAbortReasonHeadroomBytes = 256;

size_t estimate_commit_response_bytes(
    const std::unordered_set<std::string>& touched_tables) {
    size_t bound = kCommitResponseFixedOverheadBytes + kAbortReasonHeadroomBytes;
    for (const auto& table_name : touched_tables) {
        bound += table_name.size() + kTableStatFieldOverheadBytes +
                 kTableStatRowCountMaxBytes;
    }
    return bound;
}

}  // namespace

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
    request.ParseFromString(message);
    handleTxValidateAndCommit(request, result);
}

void LineairDBRpc::handleTxValidateAndCommit(
    const LineairDB::Protocol::TxValidateAndCommit::Request& request, std::string& result) {
    LineairDB::Protocol::TxValidateAndCommit::Response response;

    // Tables this commit's response may name; computed before install to bound the envelope.
    std::unordered_set<std::string> touched_tables;
    for (const auto& read : request.reads()) {
        touched_tables.insert(read.table_name());
    }
    for (const auto& write : request.writes()) {
        touched_tables.insert(write.table_name());
    }
    for (const auto& del : request.deletes()) {
        touched_tables.insert(del.table_name());
    }
    for (const auto& op : request.secondary_index_ops()) {
        touched_tables.insert(op.table_name());
    }
    for (const auto& row_delta : request.row_deltas()) {
        touched_tables.insert(row_delta.table_name());
    }

    // Rejects before install so an oversized response cannot follow a durable write.
    if (estimate_commit_response_bytes(touched_tables) > kMaxRpcPayloadBytes) {
        response.set_committed(false);
        response.set_abort_reason("response envelope too large");
        result = response.SerializeAsString();
        return;
    }

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
                          write.is_delete()});
    }
    for (const auto& del : request.deletes()) {
        writes.push_back({del.table_name(), del.key(), "", true});
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

    std::string abort_reason;
    const bool committed =
        db_manager_->get_database()->ValidateAndCommit(reads, writes, si_ops,
                                                       range_reads,
                                                       &abort_reason);
    response.set_committed(committed);
    if (!committed && !abort_reason.empty()) {
        // Headroom reserved by the envelope bound above assumes at most
        // this many bytes; truncate rather than let a storage-layer message
        // grow the response past what was reserved.
        if (abort_reason.size() > kAbortReasonHeadroomBytes) {
            abort_reason.resize(kAbortReasonHeadroomBytes);
        }
        response.set_abort_reason(abort_reason);
    }

    if (committed && request.row_deltas_size() > 0) {
        row_counts_->apply_deltas(request.row_deltas());
    }
    if (committed && request.fence()) {
        db_manager_->get_database()->Fence();
    }

    // Only the touched set may be attached -- this is the envelope reserved
    // above. Tables with no known count are simply omitted.
    for (const auto& [name, count] : row_counts_->snapshot_for(touched_tables)) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    // Report the touched-only stats sub-size for the opt-in RPC timing dump,
    // including each repeated-field entry's tag and length-delimiter framing.
    uint64_t stats_bytes = 0;
    for (const auto& ts : response.table_stats()) {
        const uint64_t entry_bytes = ts.ByteSizeLong();
        uint64_t len_prefix = 1;
        for (uint64_t v = entry_bytes; v >= 0x80; v >>= 7) len_prefix++;
        stats_bytes += 1 + len_prefix + entry_bytes;
    }
    rpc_timing::note_commit_stats_bytes(stats_bytes);

    result = response.SerializeAsString();
}

// Reactor fast-path entry: request is already parsed (classify_rpc), so this
// times/records via the typed rpc_timing overload instead of the string
// path's parse-then-time_call.
void LineairDBRpc::handle_validate_and_commit(
    const LineairDB::Protocol::TxValidateAndCommit::Request& request, size_t request_bytes,
    std::string& result) {
    rpc_timing::time_call_parsed(MessageType::TX_VALIDATE_AND_COMMIT, request, request_bytes, result,
                                 [&] { handleTxValidateAndCommit(request, result); });
    // Mirrors handle_rpc's TX_VALIDATE_AND_COMMIT arm: close this thread's
    // masstree RCU critical section after the self-contained call.
    if (db_manager_) {
        auto db = db_manager_->get_database();
        if (db) db->ReleaseMasstreeThreadEpoch();
    }
}
