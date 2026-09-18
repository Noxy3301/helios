#include "helios_rpc.hh"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "helios.pb.h"
#include "lineairdb/transaction.h"

// The reads, scans and the commit of a transaction; each request acts on the
// database directly and leaves nothing behind.

namespace {

// An empty column list gathers no data column, which is what keys_only asks
// for; a null list would gather the whole row.
const std::vector<uint32_t> kNoColumns;

}  // namespace

void HeliosRpc::handleTxRead(const std::string& message,
                                std::string& result) {
    Helios::Protocol::TxRead::Request request;
    Helios::Protocol::TxRead::Response response;

    request.ParseFromString(message);

    auto read_result =
        db_manager_->get_database()->Read(request.table_name(), request.key());
    response.set_found(read_result.found);
    response.set_tid(read_result.tid);
    if (read_result.found) {
        response.set_value(std::move(read_result.value));
    }

    result = response.SerializeAsString();
}

void HeliosRpc::handleTxBatchRead(const std::string& message,
                                     std::string& result) {
    Helios::Protocol::TxBatchRead::Request request;
    Helios::Protocol::TxBatchRead::Response response;

    request.ParseFromString(message);

    std::vector<std::pair<std::string, std::string>> keys;
    keys.reserve(request.ops_size());
    for (const auto& op : request.ops()) {
        keys.emplace_back(op.table_name(), op.key());
    }

    auto read_results = db_manager_->get_database()->BatchRead(keys);
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

void HeliosRpc::handleTxScan(const std::string& message,
                                std::string& result) {
    Helios::Protocol::TxScan::Request request;
    Helios::Protocol::TxScan::Response response;

    request.ParseFromString(message);

    auto scan = db_manager_->get_database()->Scan(
        request.table_name(), request.start_key(), request.end_key(),
        request.row_limit(), request.reverse_scan(),
        request.keys_only() ? &kNoColumns : nullptr);

    response.set_ok(scan.ok);
    for (auto& row : scan.rows) {
        auto* out = response.add_rows();
        out->set_key(std::move(row.key));
        out->set_tid(row.tid);
        if (!request.keys_only()) out->set_value(std::move(row.value));
    }

    result = response.SerializeAsString();
}

void HeliosRpc::handleTxScanIndex(const std::string& message,
                                     std::string& result) {
    Helios::Protocol::TxScanIndex::Request request;
    Helios::Protocol::TxScanIndex::Response response;

    request.ParseFromString(message);

    auto scan = db_manager_->get_database()->ScanIndex(
        request.table_name(), request.index_name(), request.start_key(),
        request.end_key(), request.row_limit(), request.reverse_scan(),
        request.keys_only() ? &kNoColumns : nullptr);

    response.set_ok(scan.ok);
    for (auto& row : scan.rows) {
        auto* out = response.add_rows();
        out->set_secondary_key(std::move(row.secondary_key));
        out->set_primary_key(std::move(row.primary_key));
        out->set_tid(row.tid);
        if (!request.keys_only()) out->set_value(std::move(row.value));
    }

    result = response.SerializeAsString();
}

void HeliosRpc::handleTxCommit(const std::string& message,
                                  std::string& result) {
    Helios::Protocol::TxCommit::Request request;
    Helios::Protocol::TxCommit::Response response;

    if (!request.ParseFromString(message)) {
        response.set_committed(false);
        response.set_abort_detail("malformed request");
        result = response.SerializeAsString();
        return;
    }

    auto& db = *db_manager_->get_database();
    helios::storage::silo::Transaction tx(db);
    for (const auto& read : request.reads()) {
        tx.Read(read.table_name(), read.key(), helios::storage::Tidword(read.tid()));
    }
    for (const auto& range : request.range_reads()) {
        std::vector<std::string_view> keys(range.result_keys().begin(),
                                           range.result_keys().end());
        std::vector<std::string_view> primary_keys(
            range.result_primary_keys().begin(),
            range.result_primary_keys().end());
        tx.RangeRead(range.table_name(), range.index_name(), range.start_key(),
                     range.end_key(), range.row_limit(), range.reverse_scan(),
                     std::move(keys), std::move(primary_keys));
    }

    std::string reason;
    bool fed = true;
    for (const auto& write : request.writes()) {
        helios::storage::RowOp op;
        switch (write.op()) {
            case Helios::Protocol::TxCommit::UPDATE: op = helios::storage::RowOp::kUpdate; break;
            case Helios::Protocol::TxCommit::INSERT: op = helios::storage::RowOp::kInsert; break;
            case Helios::Protocol::TxCommit::DELETE: op = helios::storage::RowOp::kDelete; break;
            default:
                reason = "unknown row op";
                fed = false;
                break;
        }
        if (!fed) break;
        if (!tx.Write(write.table_name(), write.key(), write.value(), op, reason)) {
            fed = false;
            break;
        }
    }
    if (fed) {
        for (const auto& op : request.secondary_index_ops()) {
            if (!tx.IndexWrite(op.table_name(), op.index_name(),
                               op.secondary_key(), op.primary_key(),
                               op.is_delete(), reason)) {
                fed = false;
                break;
            }
        }
    }
    const bool committed =
        fed && tx.Commit(db_manager_->commit_durability(), reason);

    response.set_committed(committed);
    if (!committed && !reason.empty()) {
        response.set_abort_detail(reason);
        if (reason == helios::storage::kDuplicatePrimaryKeyAbortReason) {
            response.set_abort_reason(
                Helios::Protocol::ABORT_REASON_DUPLICATE_PRIMARY_KEY);
        } else if (reason.rfind(helios::storage::kDuplicateSecondaryKeyAbortPrefix,
                                0) == 0) {
            response.set_abort_reason(
                Helios::Protocol::ABORT_REASON_DUPLICATE_SECONDARY_KEY);
        }
    }

    if (committed && request.row_deltas_size() > 0) {
        row_counts_->apply_deltas(request.row_deltas());
    }

    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    result = response.SerializeAsString();
}
