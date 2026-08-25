#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <vector>

#include "lineairdb_proxy.hh"
#include "lineairdb_transaction.hh"
#include "rpc_trace.hh"
#include "../common/log.h"

namespace {

void encode_range_read_entry(const LineairDBProxy::RangeReadEntry& entry,
                             LineairDB::Protocol::RangeReadEntry* out) {
    out->set_table_name(entry.table_name);
    out->set_index_name(entry.index_name);
    out->set_start_key(entry.start_key);
    out->set_end_key(entry.end_key);
    out->set_row_limit(entry.row_limit);
    out->set_reverse_scan(entry.reverse_scan);
    for (const auto& key : entry.result_keys) out->add_result_keys(key);
    for (const auto& key : entry.result_primary_keys) {
        out->add_result_primary_keys(key);
    }
}

}  // namespace


LineairDBProxy::LineairDBProxy(const std::string& host, int port)
    : socket_fd_(-1), connected_(false), host_(host), port_(port) {
    LOG_INFO("LineairDBProxy(%p): connecting to %s:%d",
             static_cast<const void*>(this), host_.c_str(), port_);
    if (!connect(host_, port_)) {
        std::cerr << "Failed to connect to LineairDB service at " << host_ << ":" << port_ << std::endl;
    }
}

LineairDBProxy::~LineairDBProxy() {
    LOG_INFO("LineairDBProxy(%p): destructor, connected=%s",
             static_cast<const void*>(this), connected_ ? "true" : "false");
    disconnect();
}

bool LineairDBProxy::connect(const std::string& host, int port) {
    if (connected_) {
        disconnect();
    }

    // create TCP socket
    socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        return false;
    }

    // set up server address
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    // connect
    if (::connect(socket_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    // Disable Nagle's algorithm for low-latency RPC
    int flag = 1;
    setsockopt(socket_fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    connected_ = true;
    host_ = host;
    port_ = port;
    return true;
}

void LineairDBProxy::disconnect() {
    if (socket_fd_ >= 0) {
        LOG_INFO("LineairDBProxy(%p): disconnecting socket_fd=%d",
                 static_cast<const void*>(this), socket_fd_);
        close(socket_fd_);
        socket_fd_ = -1;
    }
    connected_ = false;
}

bool LineairDBProxy::is_connected() const {
    return connected_;
}

bool LineairDBProxy::fetch_table_stats(
    const std::string& ndv_table,
    const std::vector<std::pair<std::string, uint32_t>>& ndv_indexes,
    bool force_ndv) {
    if (!connected_) return false;

    LineairDB::Protocol::GetTableStats::Request request;
    LineairDB::Protocol::GetTableStats::Response response;
    if (!ndv_table.empty()) {
        request.set_ndv_table(ndv_table);
        request.set_ndv_force_recompute(force_ndv);
        for (const auto& [index_name, key_parts] : ndv_indexes) {
            auto* desc = request.add_ndv_indexes();
            desc->set_index_name(index_name);
            desc->set_num_key_parts(key_parts);
        }
    }

    if (!send_protobuf_message(request, response,
                               MessageType::TX_GET_TABLE_STATS)) {
        return false;
    }

    table_stats_cache_.clear();
    for (const auto& ts : response.table_stats()) {
        table_stats_cache_[ts.table_name()] = ts.row_count();
    }
    last_index_ndv_.clear();
    last_index_hist_.clear();
    for (const auto& in : response.index_ndv()) {
        IndexNdvResult result;
        result.available = in.available();
        result.values.assign(in.ndv().begin(), in.ndv().end());
        last_index_ndv_[in.index_name()] = std::move(result);

        if (in.hist_available() && in.hist_bounds_size() > 0 &&
            in.hist_bounds_size() == in.hist_cum_size()) {
            IndexHistResult hist;
            hist.available = true;
            hist.bounds.assign(in.hist_bounds().begin(), in.hist_bounds().end());
            hist.cum.assign(in.hist_cum().begin(), in.hist_cum().end());
            last_index_hist_[in.index_name()] = std::move(hist);
        }
    }
    return true;
}

int64_t LineairDBProxy::tx_begin_transaction() {
    LOG_DEBUG("CLIENT: tx_begin_transaction called");
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return -1;
    }

    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;
    LOG_DEBUG("CLIENT: Created begin transaction request");

    if (!send_protobuf_message(request, response, MessageType::TX_BEGIN_TRANSACTION)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return -1;
    }

    // Cache table row counts from server for optimizer stats.
    table_stats_cache_.clear();
    for (const auto& ts : response.table_stats()) {
        table_stats_cache_[ts.table_name()] = ts.row_count();
    }

    LOG_DEBUG("CLIENT: tx_begin_transaction completed, tx_id: %ld, table_stats: %zu",
              response.transaction_id(), table_stats_cache_.size());
    return response.transaction_id();
}

void LineairDBProxy::tx_abort(int64_t tx_id) {
    LOG_DEBUG("CLIENT: tx_abort called with tx_id=%ld", tx_id);
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return;
    }

    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;

    request.set_transaction_id(tx_id);
    LOG_DEBUG("CLIENT: Created abort request");

    if (!send_protobuf_message(request, response, MessageType::TX_ABORT)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return;
    }

    LOG_DEBUG("CLIENT: tx_abort completed");
}

std::string LineairDBProxy::tx_read(LineairDBTransaction* tx, const std::string& key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_read called with tx_id=%ld, key=%s", tx_id, key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return "";
    }

    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_key(key);
    LOG_DEBUG("CLIENT: Created read request");

    if (!send_protobuf_message(request, response, MessageType::TX_READ)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return "";
    }

    // Update transaction abort status
    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_read completed, found: %s", response.found() ? "true" : "false");
    return response.found() ? response.value() : "";
}

bool LineairDBProxy::tx_write(LineairDBTransaction* tx, const std::string& key, const std::string& value) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_write called with tx_id=%ld, key=%s, value=%s", tx_id, key.c_str(), value.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_key(key);
    request.set_value(value);
    LOG_DEBUG("CLIENT: Created write request");

    if (!send_protobuf_message(request, response, MessageType::TX_WRITE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    // Update transaction abort status
    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_write completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

bool LineairDBProxy::tx_delete(LineairDBTransaction* tx, const std::string& key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_delete called with tx_id=%ld, key=%s", tx_id, key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::TxDelete::Request request;
    LineairDB::Protocol::TxDelete::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_key(key);

    if (!send_protobuf_message(request, response, MessageType::TX_DELETE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_delete completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

std::vector<LineairDBProxy::BatchReadResult> LineairDBProxy::tx_batch_read(
    LineairDBTransaction* tx, const std::vector<std::string>& keys) {
    int64_t tx_id = tx->get_tx_id();
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        tx->mark_transport_error();
        return {};
    }

    LineairDB::Protocol::TxBatchRead::Request request;
    LineairDB::Protocol::TxBatchRead::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    for (const auto& key : keys) {
        request.add_keys(key);
    }

    if (!send_protobuf_message(request, response, MessageType::TX_BATCH_READ)) {
        LOG_ERROR("RPC failed: Failed to send batch_read message to server");
        tx->mark_transport_error();
        return {};
    }

    tx->set_aborted(response.is_aborted());

    std::vector<BatchReadResult> results;
    results.reserve(response.results_size());
    for (const auto& r : response.results()) {
        results.push_back({r.found(), r.found() ? r.value() : ""});
    }

    return results;
}

bool LineairDBProxy::tx_batch_write(LineairDBTransaction* tx,
                                    const std::string& table_name,
                                    const std::vector<BatchOp>& ops) {
    int64_t tx_id = tx->get_tx_id();
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::TxBatchWrite::Request request;
    LineairDB::Protocol::TxBatchWrite::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(table_name);

    for (const auto& batch_op : ops) {
        auto* op = request.add_ops();
        switch (batch_op.type) {
            case BatchOp::Type::Write:
                op->set_type(LineairDB::Protocol::BATCH_OP_WRITE);
                op->set_key(batch_op.key);
                op->set_value(batch_op.value);
                op->set_table_name(batch_op.table_name);
                break;
            case BatchOp::Type::Delete:
                op->set_type(LineairDB::Protocol::BATCH_OP_DELETE);
                op->set_key(batch_op.key);
                op->set_table_name(batch_op.table_name);
                break;
            case BatchOp::Type::SecondaryIndexWrite:
                op->set_type(LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_WRITE);
                op->set_index_name(batch_op.index_name);
                op->set_secondary_key(batch_op.secondary_key);
                op->set_primary_key(batch_op.primary_key);
                op->set_table_name(batch_op.table_name);
                break;
            case BatchOp::Type::SecondaryIndexDelete:
                op->set_type(LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_DELETE);
                op->set_index_name(batch_op.index_name);
                op->set_secondary_key(batch_op.secondary_key);
                op->set_primary_key(batch_op.primary_key);
                op->set_table_name(batch_op.table_name);
                break;
        }
    }

    if (!send_protobuf_message(request, response, MessageType::TX_BATCH_WRITE)) {
        LOG_ERROR("RPC failed: Failed to send batch_write message to server");
        return false;
    }

    tx->set_aborted(response.is_aborted());
    return response.success();
}

LineairDBProxy::StatelessReadResult LineairDBProxy::tx_stateless_read(
    const std::string& table_name, const std::string& key) {
    StatelessReadResult result;
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return result;
    }

    LineairDB::Protocol::TxStatelessRead::Request request;
    LineairDB::Protocol::TxStatelessRead::Response response;
    request.set_table_name(table_name);
    request.set_key(key);

    if (!send_protobuf_message(request, response,
                               MessageType::TX_STATELESS_READ)) {
        LOG_ERROR("RPC failed: Failed to send stateless_read message to server");
        return result;
    }

    result.ok = true;
    result.found = response.found();
    result.tid = response.tid();
    if (result.found) result.value = response.value();
    return result;
}

std::vector<LineairDBProxy::StatelessReadResult>
LineairDBProxy::tx_stateless_batch_read(
    const std::vector<StatelessReadKey>& keys) {
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return {};
    }

    LineairDB::Protocol::TxStatelessBatchRead::Request request;
    LineairDB::Protocol::TxStatelessBatchRead::Response response;
    for (const auto& key : keys) {
        auto* op = request.add_ops();
        op->set_table_name(key.table_name);
        op->set_key(key.key);
    }

    if (!send_protobuf_message(request, response,
                               MessageType::TX_STATELESS_BATCH_READ)) {
        LOG_ERROR("RPC failed: Failed to send stateless_batch_read message to server");
        return {};
    }

    std::vector<StatelessReadResult> results;
    results.reserve(response.results_size());
    for (const auto& r : response.results()) {
        StatelessReadResult result;
        result.ok = true;
        result.found = r.found();
        result.value = r.found() ? r.value() : "";
        result.tid = r.tid();
        results.push_back(std::move(result));
    }
    return results;
}

bool LineairDBProxy::tx_execute_duckdb_query(
    const LineairDB::Protocol::TxExecuteDuckdbQuery::Request& request,
    LineairDB::Protocol::TxExecuteDuckdbQuery::Response* response) {
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }
    if (response == nullptr) {
        LOG_ERROR("RPC failed: resolved duckdb response is null");
        return false;
    }
    if (!send_protobuf_message(request, *response,
                               MessageType::TX_EXECUTE_DUCKDB_QUERY)) {
        LOG_ERROR("RPC failed: resolved duckdb message");
        return false;
    }
    return true;
}

LineairDBProxy::ReadPlanResult LineairDBProxy::tx_execute_read_plan(
    const std::vector<ReadPlanStep>& steps) {
    ReadPlanResult result;
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return result;
    }

    LineairDB::Protocol::TxExecuteReadPlan::Request request;
    for (const auto& step : steps) {
        auto* out = request.add_steps();
        out->set_table_name(step.table_name);
        out->set_key_prefix(step.key_prefix);
        out->set_end_key_prefix(step.end_key_prefix);
        out->set_is_scan(step.is_scan);
        out->set_scan_limit(step.scan_limit);
        out->set_index_name(step.index_name);
        out->set_for_each(step.for_each);
        out->set_reverse_scan(step.reverse_scan);
        for (const auto& binding : step.bindings) {
            auto* b = out->add_bindings();
            b->set_source_step(binding.source_step);
            b->set_source_row(binding.source_row);
            b->set_source_offset(binding.source_offset);
            b->set_source_length(binding.source_length);
            b->set_use_midpoint(binding.use_midpoint);
            b->set_from_key(binding.from_key);
            b->set_source_column(binding.source_column);
            b->set_column_as_int_key(binding.column_as_int_key);
            b->set_int_delta(binding.int_delta);
        }
        for (const auto& binding : step.end_bindings) {
            auto* b = out->add_end_bindings();
            b->set_source_step(binding.source_step);
            b->set_source_row(binding.source_row);
            b->set_source_offset(binding.source_offset);
            b->set_source_length(binding.source_length);
            b->set_use_midpoint(binding.use_midpoint);
            b->set_from_key(binding.from_key);
            b->set_source_column(binding.source_column);
            b->set_column_as_int_key(binding.column_as_int_key);
            b->set_int_delta(binding.int_delta);
        }
    }

    // TxExecuteReadPlan responses can exceed protobuf's ~2GB message limit, so
    // the server returns this response as flat binary.
    std::string raw;
    if (!send_protobuf_recv_binary(request, raw,
                                   MessageType::TX_EXECUTE_READ_PLAN)) {
        LOG_ERROR("RPC failed: Failed to send execute read plan message to server");
        return result;
    }

    struct Reader {
        const char* p;
        const char* end;
        bool ok = true;
        Reader(const char* data, size_t n) : p(data), end(data + n) {}
        uint8_t u8() {
            if (end - p < 1) {
                ok = false;
                return 0;
            }
            return static_cast<uint8_t>(*p++);
        }
        uint64_t u64() {
            if (end - p < 8) {
                ok = false;
                return 0;
            }
            uint64_t v;
            std::memcpy(&v, p, 8);
            p += 8;
            return v;
        }
        std::string bytes() {
            const uint64_t n = u64();
            if (!ok || static_cast<uint64_t>(end - p) < n) {
                ok = false;
                return {};
            }
            std::string out(p, n);
            p += n;
            return out;
        }
    };

    // Native-endian bytes spell "LDBFLATP" (LineairDB flat payload).
    static constexpr uint64_t kFlatMagic = 0x5054414C4642444Cull;
    static constexpr uint8_t kFlatVersion = 2;
    Reader r(raw.data(), raw.size());
    if (r.u64() != kFlatMagic || r.u8() != kFlatVersion) {
        LOG_ERROR("RPC failed: bad flat read-plan response header");
        return result;
    }
    const bool resp_ok = r.u8() != 0;
    const uint64_t count = r.u64();
    if (!r.ok) return result;
    result.ok = resp_ok;
    if (!resp_ok) return result;

    // Cap reserves by the wire size: every encoded element costs at least one
    // byte, so a corrupt count cannot force a huge allocation.
    const auto cap = [&raw](uint64_t n) {
        return static_cast<size_t>(std::min<uint64_t>(n, raw.size()));
    };
    result.steps.reserve(cap(count));
    for (uint64_t i = 0; i < count && r.ok; ++i) {
        ReadPlanStepResult out;
        out.found = r.u8() != 0;
        out.tid = r.u64();
        out.value = r.bytes();
        out.actual_key = r.bytes();
        out.actual_start_key = r.bytes();
        out.actual_end_key = r.bytes();
        uint64_t n = r.u64();
        out.scan_keys.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.scan_keys.push_back(r.bytes());
        n = r.u64();
        out.scan_values.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.scan_values.push_back(r.bytes());
        n = r.u64();
        out.scan_tids.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.scan_tids.push_back(r.u64());
        n = r.u64();
        out.secondary_keys.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.secondary_keys.push_back(r.bytes());
        n = r.u64();
        out.group_sizes.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.group_sizes.push_back(static_cast<uint32_t>(r.u64()));
        n = r.u64();
        out.group_start_keys.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.group_start_keys.push_back(r.bytes());
        n = r.u64();
        out.group_end_keys.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.group_end_keys.push_back(r.bytes());
        n = r.u64();
        out.filtered_keys.reserve(cap(n));
        for (uint64_t j = 0; j < n && r.ok; ++j)
            out.filtered_keys.push_back(r.bytes());
        result.steps.push_back(std::move(out));
    }
    if (!r.ok) {
        LOG_ERROR("RPC failed: truncated flat read-plan response");
        result.ok = false;
        result.steps.clear();
    }

    return result;
}

bool LineairDBProxy::tx_validate_and_commit(
    const std::vector<StatelessReadKey>& reads,
    const std::vector<uint64_t>& read_tids,
    const std::vector<bool>& read_found,
    const std::vector<RangeReadEntry>& range_reads,
    const std::vector<BatchOp>& ops,
    const std::vector<std::pair<std::string, int64_t>>& row_deltas,
    bool isFence,
    std::string* abort_reason,
    bool* transport_error) {
    if (abort_reason != nullptr) abort_reason->clear();
    if (transport_error != nullptr) *transport_error = false;
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        if (transport_error != nullptr) *transport_error = true;
        return false;
    }
    if (reads.size() != read_tids.size() || reads.size() != read_found.size()) {
        LOG_ERROR("validate_and_commit: read metadata size mismatch");
        return false;
    }

    LineairDB::Protocol::TxValidateAndCommit::Request request;
    LineairDB::Protocol::TxValidateAndCommit::Response response;
    request.set_fence(isFence);

    for (size_t i = 0; i < reads.size(); ++i) {
        auto* read = request.add_reads();
        read->set_table_name(reads[i].table_name);
        read->set_key(reads[i].key);
        read->set_tid(read_tids[i]);
        read->set_found(read_found[i]);
    }

    for (const auto& entry : range_reads) {
        auto* range = request.add_range_reads();
        encode_range_read_entry(entry, range);
    }

    for (const auto& batch_op : ops) {
        switch (batch_op.type) {
            case BatchOp::Type::Write: {
                auto* write = request.add_writes();
                write->set_table_name(batch_op.table_name);
                write->set_key(batch_op.key);
                write->set_value(batch_op.value);
                write->set_is_delete(false);
                break;
            }
            case BatchOp::Type::Delete: {
                auto* write = request.add_writes();
                write->set_table_name(batch_op.table_name);
                write->set_key(batch_op.key);
                write->set_is_delete(true);
                break;
            }
            case BatchOp::Type::SecondaryIndexWrite: {
                auto* si = request.add_secondary_index_ops();
                si->set_table_name(batch_op.table_name);
                si->set_index_name(batch_op.index_name);
                si->set_secondary_key(batch_op.secondary_key);
                si->set_primary_key(batch_op.primary_key);
                si->set_is_delete(false);
                break;
            }
            case BatchOp::Type::SecondaryIndexDelete: {
                auto* si = request.add_secondary_index_ops();
                si->set_table_name(batch_op.table_name);
                si->set_index_name(batch_op.index_name);
                si->set_secondary_key(batch_op.secondary_key);
                si->set_primary_key(batch_op.primary_key);
                si->set_is_delete(true);
                break;
            }
        }
    }

    for (const auto& [table, delta] : row_deltas) {
        auto* rd = request.add_row_deltas();
        rd->set_table_name(table);
        rd->set_delta(delta);
    }

    if (!send_protobuf_message(request, response,
                               MessageType::TX_VALIDATE_AND_COMMIT)) {
        LOG_ERROR("RPC failed: Failed to send validate_and_commit message to server");
        if (transport_error != nullptr) *transport_error = true;
        return false;
    }

    table_stats_cache_.clear();
    for (const auto& ts : response.table_stats()) {
        table_stats_cache_[ts.table_name()] = ts.row_count();
    }
    if (!response.committed() && abort_reason != nullptr) {
        *abort_reason = response.abort_reason();
    }
    return response.committed();
}

std::vector<std::string> LineairDBProxy::tx_read_secondary_index(LineairDBTransaction* tx,
                                                                  const std::string& index_name,
                                                                  const std::string& secondary_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_read_secondary_index called with tx_id=%ld, index=%s, key=%s",
              tx_id, index_name.c_str(), secondary_key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return {};
    }

    LineairDB::Protocol::TxReadSecondaryIndex::Request request;
    LineairDB::Protocol::TxReadSecondaryIndex::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_secondary_key(secondary_key);

    if (!send_protobuf_message(request, response, MessageType::TX_READ_SECONDARY_INDEX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return {};
    }

    tx->set_aborted(response.is_aborted());

    std::vector<std::string> values;
    for (const auto& v : response.values()) {
        values.emplace_back(v);
    }

    LOG_DEBUG("CLIENT: tx_read_secondary_index completed, found %zu values", values.size());
    return values;
}

bool LineairDBProxy::tx_write_secondary_index(LineairDBTransaction* tx,
                                               const std::string& index_name,
                                               const std::string& secondary_key,
                                               const std::string& primary_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_write_secondary_index called with tx_id=%ld, index=%s, key=%s",
              tx_id, index_name.c_str(), secondary_key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::TxWriteSecondaryIndex::Request request;
    LineairDB::Protocol::TxWriteSecondaryIndex::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_secondary_key(secondary_key);
    request.set_primary_key(primary_key);

    if (!send_protobuf_message(request, response, MessageType::TX_WRITE_SECONDARY_INDEX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_write_secondary_index completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

bool LineairDBProxy::tx_delete_secondary_index(LineairDBTransaction* tx,
                                                const std::string& index_name,
                                                const std::string& secondary_key,
                                                const std::string& primary_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_delete_secondary_index called with tx_id=%ld, index=%s, key=%s",
              tx_id, index_name.c_str(), secondary_key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::TxDeleteSecondaryIndex::Request request;
    LineairDB::Protocol::TxDeleteSecondaryIndex::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_secondary_key(secondary_key);
    request.set_primary_key(primary_key);

    if (!send_protobuf_message(request, response, MessageType::TX_DELETE_SECONDARY_INDEX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_delete_secondary_index completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

bool LineairDBProxy::tx_update_secondary_index(LineairDBTransaction* tx,
                                                const std::string& index_name,
                                                const std::string& old_secondary_key,
                                                const std::string& new_secondary_key,
                                                const std::string& primary_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_update_secondary_index called with tx_id=%ld, index=%s, old=%s, new=%s",
              tx_id, index_name.c_str(), old_secondary_key.c_str(), new_secondary_key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::TxUpdateSecondaryIndex::Request request;
    LineairDB::Protocol::TxUpdateSecondaryIndex::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_old_secondary_key(old_secondary_key);
    request.set_new_secondary_key(new_secondary_key);
    request.set_primary_key(primary_key);

    if (!send_protobuf_message(request, response, MessageType::TX_UPDATE_SECONDARY_INDEX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_update_secondary_index completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

// Primary key scan operations

std::vector<std::string> LineairDBProxy::tx_get_matching_keys_in_range(LineairDBTransaction* tx,
                                                                        const std::string& start_key,
                                                                        const std::string& end_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_get_matching_keys_in_range called with tx_id=%ld", tx_id);
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return {};
    }

    LineairDB::Protocol::TxGetMatchingKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingKeysInRange::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_start_key(start_key);
    request.set_end_key(end_key);

    if (!send_protobuf_message(request, response, MessageType::TX_GET_MATCHING_KEYS_IN_RANGE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return {};
    }

    tx->set_aborted(response.is_aborted());

    std::vector<std::string> keys;
    for (const auto& k : response.keys()) {
        keys.emplace_back(k);
    }

    LOG_DEBUG("CLIENT: tx_get_matching_keys_in_range completed, found %zu keys", keys.size());
    return keys;
}

std::vector<KeyValue> LineairDBProxy::tx_get_matching_keys_and_values_in_range(LineairDBTransaction* tx,
                                                                                const std::string& start_key,
                                                                                const std::string& end_key,
                                                                                uint64_t row_limit,
                                                                                bool reverse_scan) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_get_matching_keys_and_values_in_range called with tx_id=%ld", tx_id);
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        tx->mark_transport_error();
        return {};
    }

    LineairDB::Protocol::TxGetMatchingKeysAndValuesInRange::Request request;
    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_start_key(start_key);
    request.set_end_key(end_key);
    request.set_row_limit(row_limit);
    request.set_reverse_scan(reverse_scan);

    // Attach pushed predicate filter if available
    const auto& filter = tx->get_pushed_filter();
    if (!filter.empty()) {
        request.mutable_filter()->ParseFromString(filter);
    }

    std::string raw_response;
    if (!send_protobuf_recv_binary(request, raw_response, MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        tx->mark_transport_error();
        return {};
    }

    bool is_aborted = false;
    auto results = parse_binary_kv_response(raw_response, is_aborted);
    tx->set_aborted(is_aborted);

    LOG_DEBUG("CLIENT: tx_get_matching_keys_and_values_in_range completed, found %zu results", results.size());
    return results;
}

std::vector<KeyValue> LineairDBProxy::tx_get_matching_keys_and_values_from_prefix(LineairDBTransaction* tx,
                                                                                    const std::string& prefix) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_get_matching_keys_and_values_from_prefix called with tx_id=%ld, prefix=%s", tx_id, prefix.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        tx->mark_transport_error();  // fail closed: a dead connection is not "no rows"
        return {};
    }

    LineairDB::Protocol::TxGetMatchingKeysAndValuesFromPrefix::Request request;
    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_prefix(prefix);

    // Attach pushed predicate filter if available
    const auto& filter = tx->get_pushed_filter();
    if (!filter.empty()) {
        request.mutable_filter()->ParseFromString(filter);
    }

    std::string raw_response;
    if (!send_protobuf_recv_binary(request, raw_response, MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        tx->mark_transport_error();  // fail closed: a transport failure is not "no rows"
        return {};
    }

    bool is_aborted = false;
    auto results = parse_binary_kv_response(raw_response, is_aborted);
    tx->set_aborted(is_aborted);

    LOG_DEBUG("CLIENT: tx_get_matching_keys_and_values_from_prefix completed, found %zu results", results.size());
    return results;
}

// Zero-copy scan variant: parse binary response directly into caller-provided buffers.
// Same wire format as parse_binary_kv_response(), but avoids intermediate KeyValue copies.
// TODO: unify parse logic with parse_binary_kv_response() via callback-based parser
int LineairDBProxy::tx_scan_into_buffers(LineairDBTransaction* tx,
                                          const std::string& prefix,
                                          std::vector<std::string>& out_keys,
                                          std::vector<std::vector<std::byte>>& out_values,
                                          std::unordered_map<std::string, size_t>& out_cache) {
    int64_t tx_id = tx->get_tx_id();
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return -1;
    }

    LineairDB::Protocol::TxGetMatchingKeysAndValuesFromPrefix::Request request;
    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_prefix(prefix);

    // Attach pushed predicate filter if available
    const auto& filter = tx->get_pushed_filter();
    if (!filter.empty()) {
        request.mutable_filter()->ParseFromString(filter);
    }

    std::string raw_response;
    if (!send_protobuf_recv_binary(request, raw_response, MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return -1;
    }

    if (raw_response.size() < 5) {  // 1B is_aborted + 4B sentinel minimum
        tx->set_aborted(true);
        return -1;
    }

    // Walk the raw buffer with a pointer; same format as parse_binary_kv_response
    const char* p = raw_response.data();
    const char* end = p + raw_response.size();

    // First byte: is_aborted flag from server
    bool is_aborted = (static_cast<uint8_t>(*p) != 0);
    p++;
    tx->set_aborted(is_aborted);

    if (is_aborted) return 0;

    int count = 0;
    while (p + 4 <= end) {
        // Read key length
        uint32_t klen;
        std::memcpy(&klen, p, 4);
        p += 4;
        if (klen == 0) break;  // sentinel: no more entries

        if (p + klen + 4 > end) {
            LOG_WARNING("tx_scan_into_buffers: truncated at key (klen=%u, remaining=%ld)", klen, end - p);
            break;
        }
        std::string key(p, klen);
        p += klen;

        // Read value length
        uint32_t vlen;
        std::memcpy(&vlen, p, 4);
        p += 4;
        if (p + vlen > end) {
            LOG_WARNING("tx_scan_into_buffers: truncated at value (vlen=%u, remaining=%ld)", vlen, end - p);
            break;
        }

        // Skip tombstones (deleted rows still appear in scan)
        if (vlen == 0) { p += vlen; continue; }

        // Store directly into caller-provided buffers
        size_t idx = out_keys.size();
        out_keys.emplace_back(std::move(key));
        // Copy value bytes from raw_response into a new vector<std::byte>
        out_values.emplace_back(
            reinterpret_cast<const std::byte*>(p),
            reinterpret_cast<const std::byte*>(p) + vlen);
        out_cache[out_keys.back()] = idx;
        p += vlen;
        count++;
    }

    return count;
}

std::optional<std::string> LineairDBProxy::tx_fetch_last_key_in_range(LineairDBTransaction* tx,
                                                                       const std::string& start_key,
                                                                       const std::string& end_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_fetch_last_key_in_range called with tx_id=%ld", tx_id);
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return std::nullopt;
    }

    LineairDB::Protocol::TxFetchLastKeyInRange::Request request;
    LineairDB::Protocol::TxFetchLastKeyInRange::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_start_key(start_key);
    request.set_end_key(end_key);

    if (!send_protobuf_message(request, response, MessageType::TX_FETCH_LAST_KEY_IN_RANGE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return std::nullopt;
    }

    tx->set_aborted(response.is_aborted());

    if (response.found()) {
        return response.key();
    }
    return std::nullopt;
}

std::optional<std::string> LineairDBProxy::tx_fetch_first_key_with_prefix(LineairDBTransaction* tx,
                                                                           const std::string& prefix,
                                                                           const std::string& prefix_end) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_fetch_first_key_with_prefix called with tx_id=%ld, prefix=%s", tx_id, prefix.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return std::nullopt;
    }

    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_prefix(prefix);
    request.set_prefix_end(prefix_end);

    if (!send_protobuf_message(request, response, MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return std::nullopt;
    }

    tx->set_aborted(response.is_aborted());

    if (response.found()) {
        return response.key();
    }
    return std::nullopt;
}

std::optional<std::string> LineairDBProxy::tx_fetch_next_key_with_prefix(LineairDBTransaction* tx,
                                                                          const std::string& last_key,
                                                                          const std::string& prefix_end) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_fetch_next_key_with_prefix called with tx_id=%ld, last_key=%s", tx_id, last_key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return std::nullopt;
    }

    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_last_key(last_key);
    request.set_prefix_end(prefix_end);

    if (!send_protobuf_message(request, response, MessageType::TX_FETCH_NEXT_KEY_WITH_PREFIX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return std::nullopt;
    }

    tx->set_aborted(response.is_aborted());

    if (response.found()) {
        return response.key();
    }
    return std::nullopt;
}

// Secondary index scan operations

std::vector<std::string> LineairDBProxy::tx_get_matching_primary_keys_in_range(LineairDBTransaction* tx,
                                                                                const std::string& index_name,
                                                                                const std::string& start_key,
                                                                                const std::string& end_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_get_matching_primary_keys_in_range called with tx_id=%ld, index=%s", tx_id, index_name.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return {};
    }

    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_start_key(start_key);
    request.set_end_key(end_key);

    if (!send_protobuf_message(request, response, MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return {};
    }

    tx->set_aborted(response.is_aborted());

    std::vector<std::string> primary_keys;
    for (const auto& pk : response.primary_keys()) {
        primary_keys.emplace_back(pk);
    }

    LOG_DEBUG("CLIENT: tx_get_matching_primary_keys_in_range completed, found %zu keys", primary_keys.size());
    return primary_keys;
}

std::vector<std::string> LineairDBProxy::tx_get_matching_primary_keys_from_prefix(LineairDBTransaction* tx,
                                                                                    const std::string& index_name,
                                                                                    const std::string& prefix) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_get_matching_primary_keys_from_prefix called with tx_id=%ld, index=%s, prefix=%s",
              tx_id, index_name.c_str(), prefix.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return {};
    }

    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_prefix(prefix);

    if (!send_protobuf_message(request, response, MessageType::TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return {};
    }

    tx->set_aborted(response.is_aborted());

    std::vector<std::string> primary_keys;
    for (const auto& pk : response.primary_keys()) {
        primary_keys.emplace_back(pk);
    }

    LOG_DEBUG("CLIENT: tx_get_matching_primary_keys_from_prefix completed, found %zu keys", primary_keys.size());
    return primary_keys;
}

std::optional<std::string> LineairDBProxy::tx_fetch_last_primary_key_in_secondary_range(LineairDBTransaction* tx,
                                                                                          const std::string& index_name,
                                                                                          const std::string& start_key,
                                                                                          const std::string& end_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_fetch_last_primary_key_in_secondary_range called with tx_id=%ld, index=%s", tx_id, index_name.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return std::nullopt;
    }

    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Request request;
    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_start_key(start_key);
    request.set_end_key(end_key);

    if (!send_protobuf_message(request, response, MessageType::TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return std::nullopt;
    }

    tx->set_aborted(response.is_aborted());

    if (response.found()) {
        return response.primary_key();
    }
    return std::nullopt;
}

std::optional<SecondaryIndexEntry> LineairDBProxy::tx_fetch_last_secondary_entry_in_range(LineairDBTransaction* tx,
                                                                                            const std::string& index_name,
                                                                                            const std::string& start_key,
                                                                                            const std::string& end_key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_fetch_last_secondary_entry_in_range called with tx_id=%ld, index=%s", tx_id, index_name.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        tx->mark_transport_error();
        return std::nullopt;
    }

    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Request request;
    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(tx->get_selected_table_name());
    request.set_index_name(index_name);
    request.set_start_key(start_key);
    request.set_end_key(end_key);

    if (!send_protobuf_message(request, response, MessageType::TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        tx->mark_transport_error();
        return std::nullopt;
    }

    tx->set_aborted(response.is_aborted());

    if (response.found()) {
        SecondaryIndexEntry entry;
        entry.secondary_key = response.entry().secondary_key();
        for (const auto& pk : response.entry().primary_keys()) {
            entry.primary_keys.emplace_back(pk);
        }
        return entry;
    }
    return std::nullopt;
}

bool LineairDBProxy::db_create_table(
    const std::string& table_name,
    const std::vector<uint32_t>& pax_field_max_bytes,
    const std::vector<uint32_t>& pax_field_kind,
    const std::vector<int32_t>& pax_field_scale) {
    LOG_DEBUG("CLIENT: db_create_table called with table=%s", table_name.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::DbCreateTable::Request request;
    LineairDB::Protocol::DbCreateTable::Response response;

    request.set_table_name(table_name);
    for (const uint32_t width : pax_field_max_bytes) {
        request.add_pax_field_max_bytes(width);
    }
    for (const uint32_t kind : pax_field_kind) {
        request.add_pax_field_kind(kind);
    }
    for (const int32_t scale : pax_field_scale) {
        request.add_pax_field_scale(scale);
    }

    if (!send_protobuf_message(request, response, MessageType::DB_CREATE_TABLE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    LOG_DEBUG("CLIENT: db_create_table completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

bool LineairDBProxy::db_set_table(int64_t tx_id, const std::string& table_name) {
    LOG_DEBUG("CLIENT: db_set_table called with tx_id=%ld, table=%s", tx_id, table_name.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::DbSetTable::Request request;
    LineairDB::Protocol::DbSetTable::Response response;

    request.set_transaction_id(tx_id);
    request.set_table_name(table_name);

    if (!send_protobuf_message(request, response, MessageType::DB_SET_TABLE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    LOG_DEBUG("CLIENT: db_set_table completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

bool LineairDBProxy::db_create_secondary_index(const std::string& table_name,
                                                const std::string& index_name,
                                                uint32_t index_type) {
    LOG_DEBUG("CLIENT: db_create_secondary_index called with table=%s, index=%s, type=%u",
              table_name.c_str(), index_name.c_str(), index_type);
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::DbCreateSecondaryIndex::Request request;
    LineairDB::Protocol::DbCreateSecondaryIndex::Response response;

    request.set_table_name(table_name);
    request.set_index_name(index_name);
    request.set_index_type(index_type);

    if (!send_protobuf_message(request, response, MessageType::DB_CREATE_SECONDARY_INDEX)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    LOG_DEBUG("CLIENT: db_create_secondary_index completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

bool LineairDBProxy::db_end_transaction(int64_t tx_id, bool isFence,
                                        const std::vector<std::pair<std::string, int64_t>>& row_deltas,
                                        bool *transport_error) {
    if (transport_error != nullptr) *transport_error = false;
    LOG_DEBUG("CLIENT: db_end_transaction (with row_deltas) called with tx_id=%ld, fence=%s, deltas=%zu",
              tx_id, isFence ? "true" : "false", row_deltas.size());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        if (transport_error != nullptr) *transport_error = true;
        return false;
    }

    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;

    request.set_transaction_id(tx_id);
    request.set_fence(isFence);
    for (const auto& [table, delta] : row_deltas) {
        auto* rd = request.add_row_deltas();
        rd->set_table_name(table);
        rd->set_delta(delta);
    }

    if (!send_protobuf_message(request, response, MessageType::DB_END_TRANSACTION)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        if (transport_error != nullptr) *transport_error = true;
        return false;
    }

    // Cache updated table row counts for next transaction.
    table_stats_cache_.clear();
    for (const auto& ts : response.table_stats()) {
        table_stats_cache_[ts.table_name()] = ts.row_count();
    }

    LOG_DEBUG("CLIENT: db_end_transaction (with row_deltas) completed");
    return !response.is_aborted();
}

void LineairDBProxy::db_fence() {
    LOG_DEBUG("CLIENT: db_fence called");
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return;
    }

    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;
    LOG_DEBUG("CLIENT: Created fence request");

    if (!send_protobuf_message(request, response, MessageType::DB_FENCE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return;
    }

    LOG_DEBUG("CLIENT: db_fence completed");
}

bool LineairDBProxy::send_message(const std::string& serialized_request, std::string& serialized_response) {
    if (!connected_) {
        LOG_ERROR("SEND_MESSAGE: Not connected to server");
        return false;
    }

    LOG_DEBUG("SEND_MESSAGE: Sending message of size %zu bytes", serialized_request.size());

    // send message size first (4 bytes)
    uint32_t message_size = htonl(serialized_request.size());
    LOG_DEBUG("SEND_MESSAGE: Sending size header: %zu (network order: %u)", serialized_request.size(), message_size);

    ssize_t size_sent = send(socket_fd_, &message_size, sizeof(message_size), 0);
    if (size_sent != sizeof(message_size)) {
        LOG_ERROR("SEND_MESSAGE: Failed to send size header, sent %zd bytes instead of %zu", size_sent, sizeof(message_size));
        return false;
    }
    LOG_DEBUG("SEND_MESSAGE: Size header sent successfully");

    // send message body
    LOG_DEBUG("SEND_MESSAGE: Sending message body...");
    ssize_t body_sent = send(socket_fd_, serialized_request.data(), serialized_request.size(), 0);
    if (body_sent != static_cast<ssize_t>(serialized_request.size())) {
        LOG_ERROR("SEND_MESSAGE: Failed to send message body, sent %zd bytes instead of %zu", body_sent, serialized_request.size());
        return false;
    }
    LOG_DEBUG("SEND_MESSAGE: Message body sent successfully");

    // receive response size
    LOG_DEBUG("SEND_MESSAGE: Waiting for response size...");
    uint32_t response_size;
    ssize_t size_received = recv(socket_fd_, &response_size, sizeof(response_size), MSG_WAITALL);
    if (size_received != sizeof(response_size)) {
        LOG_ERROR("SEND_MESSAGE: Failed to receive response size, got %zd bytes", size_received);
        return false;
    }
    response_size = ntohl(response_size);
    LOG_DEBUG("SEND_MESSAGE: Received response size: %u bytes", response_size);

    // receive response body
    LOG_DEBUG("SEND_MESSAGE: Waiting for response body...");
    serialized_response.resize(response_size);
    ssize_t body_received = recv(socket_fd_, &serialized_response[0], response_size, MSG_WAITALL);
    if (body_received != static_cast<ssize_t>(response_size)) {
        LOG_ERROR("SEND_MESSAGE: Failed to receive response body, got %zd bytes instead of %u", body_received, response_size);
        return false;
    }
    LOG_DEBUG("SEND_MESSAGE: Response body received successfully");

    return true;
}

template<typename RequestType, typename ResponseType>
bool LineairDBProxy::send_protobuf_message(const RequestType& request,
                                           ResponseType& response,
                                           MessageType message_type,
                                           const std::string& meta) {
    // serialize request
    std::string serialized_request = request.SerializeAsString();

    // send message with header
    std::string serialized_response;
    if (!send_message_with_header(serialized_request, serialized_response,
                                  message_type, meta)) {
        LOG_ERROR("PROTOBUF_MESSAGE: Failed to send message with header");
        return false;
    }

    // deserialize response
    if (!response.ParseFromString(serialized_response)) {
        LOG_ERROR("PROTOBUF_MESSAGE: Failed to parse response");
        return false;
    }

    return true;
}

// Send protobuf-encoded request, receive raw binary response (no protobuf decode).
// Used for Scan RPCs where the server returns flat binary instead of protobuf.
template<typename RequestType>
bool LineairDBProxy::send_protobuf_recv_binary(const RequestType& request,
                                                std::string& raw_response,
                                                MessageType message_type,
                                                const std::string& meta) {
    std::string serialized_request = request.SerializeAsString();
    return send_message_with_header(serialized_request, raw_response,
                                    message_type, meta);
}

// Parse flat binary scan response into vector<KeyValue>.
// Wire format: [is_aborted:1B] [key_len:4B LE][key][val_len:4B LE][val]... [sentinel:key_len=0]
// TODO: unify parse logic with tx_scan_into_buffers() via callback-based parser
std::vector<KeyValue> LineairDBProxy::parse_binary_kv_response(const std::string& raw, bool& is_aborted) {
    std::vector<KeyValue> results;
    if (raw.size() < 5) {  // 1B is_aborted + 4B sentinel minimum
        is_aborted = true;
        return results;
    }

    // Walk the raw buffer with a pointer; each field is read via memcpy
    const char* p = raw.data();
    const char* end = p + raw.size();

    // First byte: is_aborted flag from server
    is_aborted = (static_cast<uint8_t>(*p) != 0);
    p++;

    while (p + 4 <= end) {
        // Read key length
        uint32_t klen;
        std::memcpy(&klen, p, 4);
        p += 4;
        if (klen == 0) break;  // sentinel: no more entries

        if (p + klen + 4 > end) {
            LOG_WARNING("parse_binary_kv_response: truncated at key (klen=%u, remaining=%ld)", klen, end - p);
            break;
        }
        std::string key(p, klen);
        p += klen;

        // Read value length
        uint32_t vlen;
        std::memcpy(&vlen, p, 4);
        p += 4;

        if (p + vlen > end) {
            LOG_WARNING("parse_binary_kv_response: truncated at value (vlen=%u, remaining=%ld)", vlen, end - p);
            break;
        }
        std::string val(p, vlen);
        p += vlen;

        results.emplace_back(KeyValue{std::move(key), std::move(val)});
    }

    return results;
}

bool LineairDBProxy::send_message_with_header(const std::string& serialized_request,
                                              std::string& serialized_response,
                                              MessageType message_type,
                                              const std::string& meta) {
    auto rpc_start_ts = std::chrono::steady_clock::now();
    const uint32_t req_bytes = static_cast<uint32_t>(serialized_request.size());

    if (!connected_) {
        LOG_ERROR("SEND_MESSAGE: Not connected!");
        return false;
    }
    if (serialized_request.size() > UINT32_MAX) {
        LOG_ERROR("SEND_MESSAGE: request %zu bytes exceeds the u32 frame limit",
                  serialized_request.size());
        return false;
    }

    LOG_DEBUG("SEND_MESSAGE: Sending message of size %zu bytes with message_type %u", 
              serialized_request.size(), static_cast<uint32_t>(message_type));

    // prepare message header
    MessageHeader header;
    header.sender_id = htobe64(1);  // TODO: replace with actual sender ID
    header.message_type = htonl(static_cast<uint32_t>(message_type));
    header.payload_size = htonl(static_cast<uint32_t>(serialized_request.size()));

    LOG_DEBUG("SEND_MESSAGE: Prepared header: sender_id=1, message_type=%u, payload_size=%zu", 
              static_cast<uint32_t>(message_type), serialized_request.size());

    // combine header and payload
    size_t total_size = sizeof(header) + serialized_request.size();
    std::vector<char> buffer(total_size);
    std::memcpy(buffer.data(), &header, sizeof(header));
    std::memcpy(buffer.data() + sizeof(header), serialized_request.c_str(), serialized_request.size());

    // send (handle partial writes for large messages)
    size_t total_sent = 0;
    while (total_sent < total_size) {
        ssize_t bytes_sent = send(socket_fd_, buffer.data() + total_sent,
                                  total_size - total_sent, 0);
        if (bytes_sent <= 0) {
            LOG_ERROR("SEND_MESSAGE: Failed to send message, sent %zu/%zu bytes", total_sent, total_size);
            return false;
        }
        total_sent += bytes_sent;
    }

    LOG_DEBUG("SEND_MESSAGE: Successfully sent %zd bytes", bytes_sent);

    // receive response header
    MessageHeader response_header;
    ssize_t header_received = recv(socket_fd_, &response_header, sizeof(response_header), MSG_WAITALL);
    if (header_received != sizeof(response_header)) {
        LOG_ERROR("SEND_MESSAGE: Failed to receive response header, received %zd bytes", header_received);
        return false;
    }

    // convert from network byte order to host byte order
    uint64_t response_sender_id = be64toh(response_header.sender_id);
    uint32_t response_message_type = ntohl(response_header.message_type);
    uint32_t response_payload_size = ntohl(response_header.payload_size);

    LOG_DEBUG("SEND_MESSAGE: Received response header: sender_id=%lu, message_type=%u, payload_size=%u", 
              response_sender_id, response_message_type, response_payload_size);

    // Receive the response payload. recv(MSG_WAITALL) still caps one call near
    // 2GB, so large read-plan responses must be drained in a loop.
    if (response_payload_size > 0) {
        serialized_response.resize(response_payload_size);
        size_t received_total = 0;
        while (received_total < response_payload_size) {
            const ssize_t chunk =
                recv(socket_fd_, &serialized_response[received_total],
                     response_payload_size - received_total, MSG_WAITALL);
            if (chunk <= 0) {
                LOG_ERROR("SEND_MESSAGE: Failed to receive response payload, received %zu/%u bytes",
                          received_total, response_payload_size);
                return false;
            }
            received_total += static_cast<size_t>(chunk);
        }
        LOG_DEBUG("SEND_MESSAGE: Successfully received response payload (%zu bytes)", received_total);
    } else {
        LOG_DEBUG("SEND_MESSAGE: No response payload (empty response)");
        serialized_response.clear();
    }

    LOG_DEBUG("SEND_MESSAGE: Message exchange completed successfully");
    if (current_trace_ != nullptr && current_trace_->active()) {
        auto rpc_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - rpc_start_ts)
                          .count();
        current_trace_->record(
            message_type, static_cast<uint64_t>(rpc_us), req_bytes,
            static_cast<uint32_t>(serialized_response.size()), meta);
    }
    return true;
}
