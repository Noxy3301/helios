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

#include "helios_proxy.hh"
#include "rpc_trace.hh"
#include "helios_log.hh"



HeliosProxy::HeliosProxy(const std::string& host, int port)
    : socket_fd_(-1), connected_(false), host_(host), port_(port) {
    LOG_INFO("HeliosProxy(%p): connecting to %s:%d",
             static_cast<const void*>(this), host_.c_str(), port_);
    if (!connect(host_, port_)) {
        std::cerr << "Failed to connect to Helios service at " << host_ << ":" << port_ << std::endl;
    }
}

HeliosProxy::~HeliosProxy() {
    LOG_INFO("HeliosProxy(%p): destructor, connected=%s",
             static_cast<const void*>(this), connected_ ? "true" : "false");
    disconnect();
}

bool HeliosProxy::connect(const std::string& host, int port) {
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

void HeliosProxy::disconnect() {
    if (socket_fd_ >= 0) {
        LOG_INFO("HeliosProxy(%p): disconnecting socket_fd=%d",
                 static_cast<const void*>(this), socket_fd_);
        close(socket_fd_);
        socket_fd_ = -1;
    }
    connected_ = false;
}

bool HeliosProxy::is_connected() const {
    return connected_;
}

// A transport failure closes the channel; the next RPC opens a new one.
bool HeliosProxy::ensure_connected() {
    return connected_ || connect(host_, port_);
}

bool HeliosProxy::fetch_table_stats(
    const std::string& ndv_table,
    const std::vector<std::pair<std::string, uint32_t>>& ndv_indexes,
    bool force_ndv) {
    if (!ensure_connected()) return false;

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_get_table_stats();
    if (!ndv_table.empty()) {
        request.set_ndv_table(ndv_table);
        request.set_ndv_force_recompute(force_ndv);
        for (const auto& [index_name, key_parts] : ndv_indexes) {
            auto* desc = request.add_ndv_indexes();
            desc->set_index_name(index_name);
            desc->set_num_key_parts(key_parts);
        }
    }

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        return false;
    }
    const auto& response = reply.get_table_stats();

    storage_boot_token_ = response.boot_token();
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

HeliosProxy::ReadResult HeliosProxy::tx_read(
    const std::string& table_name, const std::string& key) {
    ReadResult result;
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        return result;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_tx_read();
    request.set_table_name(table_name);
    request.set_key(key);

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send read message to server");
        return result;
    }
    const auto& response = reply.tx_read();

    result.ok = true;
    result.found = response.found();
    result.tid = response.tid();
    if (result.found) result.value = response.value();
    return result;
}

std::vector<HeliosProxy::ReadResult> HeliosProxy::tx_batch_read(
    const std::vector<ReadKey>& keys) {
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        return {};
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_tx_batch_read();
    for (const auto& key : keys) {
        auto* op = request.add_ops();
        op->set_table_name(key.table_name);
        op->set_key(key.key);
    }

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send batch_read message to server");
        return {};
    }
    const auto& response = reply.tx_batch_read();

    std::vector<ReadResult> results;
    results.reserve(response.results_size());
    for (const auto& r : response.results()) {
        ReadResult result;
        result.ok = true;
        result.found = r.found();
        result.value = r.found() ? r.value() : "";
        result.tid = r.tid();
        results.push_back(std::move(result));
    }
    return results;
}

HeliosProxy::ScanResult HeliosProxy::tx_scan(
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key, uint64_t row_limit, bool reverse_scan,
    bool keys_only) {
    ScanResult result;
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        result.transport_error = true;
        return result;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_tx_scan();
    request.set_table_name(table_name);
    request.set_start_key(start_key);
    request.set_end_key(end_key);
    request.set_row_limit(row_limit);
    request.set_reverse_scan(reverse_scan);
    request.set_keys_only(keys_only);

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send scan message to server");
        result.transport_error = true;
        return result;
    }
    auto& response = *reply.mutable_tx_scan();

    result.ok = response.ok();
    if (!result.ok) return result;
    result.rows.reserve(response.rows_size());
    for (auto& row : *response.mutable_rows()) {
        ScanRow out;
        out.key = std::move(*row.mutable_key());
        out.value = std::move(*row.mutable_value());
        out.tid = row.tid();
        result.rows.push_back(std::move(out));
    }
    return result;
}

HeliosProxy::ScanIndexResult HeliosProxy::tx_scan_index(
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key,
    uint64_t row_limit, bool reverse_scan, bool keys_only) {
    ScanIndexResult result;
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        result.transport_error = true;
        return result;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_tx_scan_index();
    request.set_table_name(table_name);
    request.set_index_name(index_name);
    request.set_start_key(start_key);
    request.set_end_key(end_key);
    request.set_row_limit(row_limit);
    request.set_reverse_scan(reverse_scan);
    request.set_keys_only(keys_only);

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send scan_index message to server");
        result.transport_error = true;
        return result;
    }
    auto& response = *reply.mutable_tx_scan_index();

    result.ok = response.ok();
    if (!result.ok) return result;
    result.rows.reserve(response.rows_size());
    for (auto& row : *response.mutable_rows()) {
        ScanIndexRow out;
        out.secondary_key = std::move(*row.mutable_secondary_key());
        out.primary_key = std::move(*row.mutable_primary_key());
        out.value = std::move(*row.mutable_value());
        out.tid = row.tid();
        result.rows.push_back(std::move(out));
    }
    return result;
}

bool HeliosProxy::tx_commit(
    const std::vector<ReadEntry>& reads,
    const std::vector<RangeReadEntry>& range_reads,
    const std::vector<WriteOp>& ops,
    const std::vector<std::pair<std::string, int64_t>>& row_deltas,
    std::string* abort_detail,
    bool* duplicate_key,
    bool* transport_error) {
    if (abort_detail != nullptr) abort_detail->clear();
    if (duplicate_key != nullptr) *duplicate_key = false;
    if (transport_error != nullptr) *transport_error = false;
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        if (transport_error != nullptr) *transport_error = true;
        return false;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_tx_commit();

    for (const auto& entry : reads) {
        auto* read = request.add_reads();
        read->set_table_name(entry.table_name);
        read->set_key(entry.key);
        read->set_tid(entry.tid);
    }

    for (const auto& entry : range_reads) {
        auto* range = request.add_range_reads();
        range->set_table_name(entry.table_name);
        range->set_index_name(entry.index_name);
        range->set_start_key(entry.start_key);
        range->set_end_key(entry.end_key);
        range->set_row_limit(entry.row_limit);
        range->set_reverse_scan(entry.reverse_scan);
        for (const auto& key : entry.result_keys) range->add_result_keys(key);
        for (const auto& key : entry.result_primary_keys) {
            range->add_result_primary_keys(key);
        }
    }

    for (const auto& op : ops) {
        switch (op.type) {
            case WriteOp::Type::Write: {
                auto* write = request.add_writes();
                write->set_table_name(op.table_name);
                write->set_key(op.key);
                write->set_value(op.value);
                write->set_op(op.is_insert
                                  ? Helios::Protocol::TxCommit::INSERT
                                  : Helios::Protocol::TxCommit::UPDATE);
                break;
            }
            case WriteOp::Type::Delete: {
                auto* write = request.add_writes();
                write->set_table_name(op.table_name);
                write->set_key(op.key);
                write->set_op(Helios::Protocol::TxCommit::DELETE);
                break;
            }
            case WriteOp::Type::SecondaryIndexWrite:
            case WriteOp::Type::SecondaryIndexDelete: {
                auto* si = request.add_secondary_index_ops();
                si->set_table_name(op.table_name);
                si->set_index_name(op.index_name);
                si->set_secondary_key(op.secondary_key);
                si->set_primary_key(op.primary_key);
                si->set_is_delete(op.type ==
                                  WriteOp::Type::SecondaryIndexDelete);
                break;
            }
        }
    }

    for (const auto& [table, delta] : row_deltas) {
        auto* rd = request.add_row_deltas();
        rd->set_table_name(table);
        rd->set_delta(delta);
    }

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send commit message to server");
        if (transport_error != nullptr) *transport_error = true;
        return false;
    }
    const auto& response = reply.tx_commit();

    table_stats_cache_.clear();
    for (const auto& ts : response.table_stats()) {
        table_stats_cache_[ts.table_name()] = ts.row_count();
    }
    if (!response.committed()) {
        if (abort_detail != nullptr) *abort_detail = response.abort_detail();
        if (duplicate_key != nullptr) {
            const auto reason = response.abort_reason();
            *duplicate_key =
                reason ==
                    Helios::Protocol::ABORT_REASON_DUPLICATE_PRIMARY_KEY ||
                reason ==
                    Helios::Protocol::ABORT_REASON_DUPLICATE_SECONDARY_KEY;
        }
    }
    return response.committed();
}

namespace {
void fill_bindings(
    const std::vector<HeliosProxy::ReadPlanKeyBinding>& bindings,
    google::protobuf::RepeatedPtrField<
        Helios::Protocol::TxExecuteReadPlan::KeyBinding>* out) {
    for (const auto& binding : bindings) {
        auto* b = out->Add();
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
}  // namespace

HeliosProxy::ReadPlanResult HeliosProxy::tx_execute_read_plan(
    const std::vector<ReadPlanStep>& steps) {
    ReadPlanResult result;
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        result.transport_error = true;
        return result;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_tx_execute_read_plan();
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
        fill_bindings(step.bindings, out->mutable_bindings());
        fill_bindings(step.end_bindings, out->mutable_end_bindings());
    }

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send execute read plan message to server");
        result.transport_error = true;
        return result;
    }
    // The reply is the flat payload, which a plan can fill past the size
    // protobuf encodes a message in.
    const std::string& raw = reply.tx_execute_read_plan();

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

    // Native-endian bytes spell "HELIOSRP" (Helios read plan response).
    static constexpr uint64_t kFlatMagic = 0x5052534F494C4548ull;
    Reader r(raw.data(), raw.size());
    if (r.u64() != kFlatMagic) {
        LOG_ERROR("RPC failed: bad flat read-plan response header");
        result.transport_error = true;
        return result;
    }
    const bool resp_ok = r.u8() != 0;
    const uint64_t count = r.u64();
    if (!r.ok) {
        result.transport_error = true;
        return result;
    }
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
        result.steps.push_back(std::move(out));
    }
    if (!r.ok) {
        LOG_ERROR("RPC failed: truncated flat read-plan response");
        result.ok = false;
        result.transport_error = true;
        result.steps.clear();
    }

    return result;
}

bool HeliosProxy::tx_execute_duckdb_query(
    const Helios::Protocol::TxExecuteDuckdbQuery::Request& request,
    Helios::Protocol::TxExecuteDuckdbQuery::Response* response) {
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }
    if (response == nullptr) {
        LOG_ERROR("RPC failed: resolved duckdb response is null");
        return false;
    }
    Helios::Protocol::Request envelope;
    *envelope.mutable_tx_execute_duckdb_query() = request;

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: resolved duckdb message");
        return false;
    }
    *response = std::move(*reply.mutable_tx_execute_duckdb_query());
    return true;
}

bool HeliosProxy::db_create_table(
    const std::string& table_name,
    const std::vector<uint32_t>& pax_field_max_bytes,
    const std::vector<uint32_t>& pax_field_kind,
    const std::vector<int32_t>& pax_field_scale) {
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_db_create_table();

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

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }
    const auto& response = reply.db_create_table();

    return response.success();
}

HeliosProxy::HiddenKeyReservation HeliosProxy::db_allocate_hidden_keys(
    const std::string& table_name, uint32_t count) {
    HiddenKeyReservation reservation;
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        reservation.transport_error = true;
        reservation.error = "not connected to the storage server";
        return reservation;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_db_allocate_hidden_keys();

    request.set_table_name(table_name);
    request.set_count(count);

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        reservation.transport_error = true;
        reservation.error = "the storage server did not answer";
        return reservation;
    }
    const auto& response = reply.db_allocate_hidden_keys();

    storage_boot_token_ = response.boot_token();
    if (!response.ok()) {
        reservation.permanent = response.permanent();
        reservation.error = response.error();
        LOG_ERROR("CLIENT: db_allocate_hidden_keys for %s rejected: %s",
                  table_name.c_str(), reservation.error.c_str());
        return reservation;
    }

    reservation.ok = true;
    reservation.first_id = response.first_id();
    reservation.boot_token = response.boot_token();
    return reservation;
}

bool HeliosProxy::db_create_secondary_index(const std::string& table_name,
                                                const std::string& index_name,
                                                uint32_t index_type) {
    if (!ensure_connected()) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_db_create_secondary_index();

    request.set_table_name(table_name);
    request.set_index_name(index_name);
    request.set_index_type(index_type);

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }
    const auto& response = reply.db_create_secondary_index();

    return response.success();
}

bool HeliosProxy::db_set_commit_durability(
    Helios::Protocol::DbSetCommitDurability::Mode mode, std::string* error) {
    if (!ensure_connected()) {
        *error = "cannot reach the storage server";
        return false;
    }

    Helios::Protocol::Request envelope;
    auto& request = *envelope.mutable_db_set_commit_durability();

    request.set_mode(mode);

    Helios::Protocol::Response reply;
    if (!send_request(envelope, reply)) {
        *error = "the storage server did not answer";
        return false;
    }
    const auto& response = reply.db_set_commit_durability();

    if (!response.ok()) {
        *error = response.error().empty() ? "the server refused the switch"
                                          : response.error();
        return false;
    }
    return true;
}

bool HeliosProxy::send_request(const Helios::Protocol::Request& request,
                                  Helios::Protocol::Response& response,
                                  const std::string& meta) {
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request)) {
        LOG_ERROR("SEND_REQUEST: Failed to serialize %s",
                  rpc_op_name(request.body_case()));
        return false;
    }

    std::string serialized_response;
    if (!exchange_message(serialized_request, serialized_response,
                          request.body_case(), meta)) {
        LOG_ERROR("SEND_REQUEST: Failed to send %s",
                  rpc_op_name(request.body_case()));
        // A transport error ends the transaction and the channel: the reset
        // invalidates the ranges this connection cached, and the close drops a
        // partially consumed response. The next transaction opens a new
        // channel.
        storage_boot_token_ = 0;
        disconnect();
        return false;
    }

    if (!response.ParseFromString(serialized_response)) {
        LOG_ERROR("SEND_REQUEST: Failed to parse the response envelope");
        return false;
    }

    // The arms are numbered alike, so the reply answers this request only
    // when the two cases agree on an arm that names an RPC.
    if (request.body_case() == Helios::Protocol::Request::BODY_NOT_SET ||
        static_cast<int>(response.body_case()) !=
            static_cast<int>(request.body_case())) {
        LOG_ERROR("SEND_REQUEST: %s answered with case %d: %s",
                  rpc_op_name(request.body_case()),
                  static_cast<int>(response.body_case()),
                  response.error().c_str());
        return false;
    }

    return true;
}

bool HeliosProxy::exchange_message(const std::string& serialized_request,
                                      std::string& serialized_response,
                                      RpcOp op, const std::string& meta) {
    auto rpc_start_ts = std::chrono::steady_clock::now();
    const uint32_t req_bytes = static_cast<uint32_t>(serialized_request.size());

    if (!ensure_connected()) {
        LOG_ERROR("SEND_MESSAGE: Not connected!");
        return false;
    }
    if (serialized_request.size() > UINT32_MAX) {
        LOG_ERROR("SEND_MESSAGE: request %zu bytes exceeds the u32 frame limit",
                  serialized_request.size());
        return false;
    }

    const uint32_t header = htonl(static_cast<uint32_t>(serialized_request.size()));

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


    // receive response header
    uint32_t response_header = 0;
    ssize_t header_received = recv(socket_fd_, &response_header, sizeof(response_header), MSG_WAITALL);
    if (header_received != sizeof(response_header)) {
        LOG_ERROR("SEND_MESSAGE: Failed to receive response header, received %zd bytes", header_received);
        return false;
    }

    const uint32_t response_payload_size = ntohl(response_header);


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
    } else {
        serialized_response.clear();
    }

    if (current_trace_ != nullptr && current_trace_->active()) {
        auto rpc_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - rpc_start_ts)
                          .count();
        current_trace_->record(
            op, static_cast<uint64_t>(rpc_us), req_bytes,
            static_cast<uint32_t>(serialized_response.size()), meta);
    }
    return true;
}
