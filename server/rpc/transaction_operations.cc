#include "helios_rpc.hh"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/rpc_flat.h"
#include "helios.pb.h"
#include "helios/transaction.h"

// The reads, scans and the commit of a transaction; each request acts on the
// database directly and leaves nothing behind.

namespace {

// Borrows a commit request's range records for silo::Transaction::RangeRead.
std::vector<helios::storage::silo::Transaction::RangeRecord> range_records(
    const std::vector<helios::rpc::FlatRecord>& records) {
    std::vector<helios::storage::silo::Transaction::RangeRecord> out;
    out.reserve(records.size());
    for (const auto& record : records) {
        out.push_back({record.key, helios::storage::Tidword(record.tid)});
    }
    return out;
}

}  // namespace

bool HeliosRpc::handleTxRead(std::string_view body, std::string& reply,
                             std::string& value) {
    helios::rpc::FlatReader in(body);
    const std::string_view table = in.get_bytes();
    const std::string_view key = in.get_bytes();
    if (!in.done()) return false;

    auto read_result = db_manager_->get_database()->Read(table, key);
    helios::rpc::put<uint8_t>(reply, read_result.found);
    helios::rpc::put<uint64_t>(reply, read_result.tid);
    if (read_result.found) {
        value = std::move(read_result.value);
    }
    return true;
}

bool HeliosRpc::handleTxBatchRead(std::string_view body, std::string& reply) {
    auto& db = *db_manager_->get_database();
    helios::rpc::FlatReader in(body);
    const uint32_t n = in.get<uint32_t>();
    helios::rpc::put<uint32_t>(reply, n);
    for (uint32_t i = 0; i < n; i++) {
        const std::string_view table = in.get_bytes();
        const std::string_view key = in.get_bytes();
        if (!in.ok()) return false;

        auto read_result = db.Read(table, key);
        helios::rpc::put<uint8_t>(reply, read_result.found);
        helios::rpc::put<uint64_t>(reply, read_result.tid);
        helios::rpc::put_bytes(reply, read_result.found ? read_result.value
                                                        : std::string_view());
    }
    return in.done();
}

void HeliosRpc::handleTxScan(
    const Helios::Protocol::TxScan::Request& request,
    Helios::Protocol::TxScan::Response* response) {
    auto scan = db_manager_->get_database()->Scan(
        request.table_name(), request.start_key(), request.end_key(),
        request.row_limit(), request.reverse_scan(), request.keys_only());

    response->set_ok(scan.ok);
    for (auto& row : scan.rows) {
        auto* out = response->add_rows();
        out->set_key(std::move(row.key));
        out->set_tid(row.tid);
        out->set_value(std::move(row.value));
    }
    add_visited(response, scan.visited);
}

void HeliosRpc::handleTxScanIndex(
    const Helios::Protocol::TxScanIndex::Request& request,
    Helios::Protocol::TxScanIndex::Response* response) {
    auto scan = db_manager_->get_database()->ScanIndex(
        request.table_name(), request.index_name(), request.start_key(),
        request.end_key(), request.row_limit(), request.reverse_scan(),
        request.keys_only());

    response->set_ok(scan.ok);
    for (auto& row : scan.rows) {
        auto* out = response->add_rows();
        out->set_secondary_key(std::move(row.secondary_key));
        out->set_primary_key(std::move(row.primary_key));
        out->set_tid(row.tid);
        out->set_value(std::move(row.value));
    }
    add_visited(response, scan.visited);
}

bool HeliosRpc::handleTxCommit(std::string_view body, std::string& reply,
                               helios::storage::EpochNumber& await_epoch) {
    helios::rpc::FlatCommit request;
    if (!helios::rpc::unpack_commit(body, request)) return false;

    auto& db = *db_manager_->get_database();
    helios::storage::silo::Transaction tx(db);
    tx.reserve(request.reads.size(), request.ranges.size());
    for (const auto& read : request.reads) {
        tx.Read(read.table, read.key, helios::storage::Tidword(read.tid),
                read.column_mask);
    }
    for (const auto& range : request.ranges) {
        tx.RangeRead(range.table, range.index, range.start_key, range.end_key,
                     range.row_limit, range.reverse_scan,
                     range_records(range.rows), range_records(range.visited));
    }

    std::string reason;
    bool fed = true;
    for (const auto& write : request.writes) {
        helios::storage::RowOp op;
        switch (write.op) {
            case helios::rpc::FlatRowOp::kUpdate:
                op = helios::storage::RowOp::kUpdate;
                break;
            case helios::rpc::FlatRowOp::kInsert:
                op = helios::storage::RowOp::kInsert;
                break;
            case helios::rpc::FlatRowOp::kDelete:
                op = helios::storage::RowOp::kDelete;
                break;
            default:
                reason = "unknown row op";
                fed = false;
                break;
        }
        if (!fed) break;
        if (!tx.Write(write.table, write.key, write.value, op, reason,
                      write.column_mask)) {
            fed = false;
            break;
        }
    }
    if (fed) {
        for (const auto& op : request.index_ops) {
            if (!tx.IndexWrite(op.table, op.index, op.secondary_key,
                               op.primary_key, op.is_delete, reason)) {
                fed = false;
                break;
            }
        }
    }
    auto durability = db_manager_->commit_durability();
    if (request.async_durability) {
        durability = helios::storage::CommitDurability::kAsync;
    }
    // The caller holds a Sync reply until await_epoch is durable.
    const bool committed = fed && tx.Commit(durability, reason, &await_epoch);

    helios::rpc::FlatCommitReply response;
    response.committed = committed;
    if (!committed && !reason.empty()) {
        response.abort_detail = reason;
        response.duplicate_key =
            reason == helios::storage::kDuplicatePrimaryKeyAbortReason ||
            reason.rfind(helios::storage::kDuplicateSecondaryKeyAbortPrefix,
                         0) == 0;
    }

    if (committed && !request.row_deltas.empty()) {
        row_counts_->apply_deltas(request.row_deltas);
    }

    row_counts_->append_to(response.table_stats);
    helios::rpc::pack_commit_reply(response, reply);
    return true;
}
