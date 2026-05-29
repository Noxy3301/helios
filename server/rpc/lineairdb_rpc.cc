#include "lineairdb_rpc.hh"
#include "predicate_evaluator.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <set>
#include <unordered_set>
#include <utility>
#include "../../proxy/flat_read_plan_codec.hh"
#include "../../proxy/rpc_compress.hh"
#include "../network/message_handler.hh"
#include "tx_occ_store.hh"
#include <lineairdb/database.h>
#include <atomic>
#include <chrono>
#include <sys/socket.h>
#include <dlfcn.h>

namespace {

// helios 2-RPC OCC (Step 4 scaffolding): when the current TX_EXECUTE_READ_PLAN
// handler is running in physical-OCC mode it sets this thread_local to the
// TxOccState it is assembling. The filtered-row routing helper then routes
// (key, tid) tuples into TxOccState.filtered_rows instead of the proto's
// add_filtered_keys/tids, so the proxy never sees them. In Steps 1-4 no
// handler sets this pointer, so the route stays through proto = byte-
// equivalent to baseline. Step 5 introduces the setter on the physical-mode
// entry path.
thread_local helios::TxOccState* tls_current_tx_occ_state = nullptr;

// Route a filter-rejected row's (table, key, tid) either to the proto
// response (logical mode = baseline) or to the in-flight server-retained
// TxOccState (physical mode). `table` is required: a filtered row's table
// may differ from the last range_read's table (e.g. Q9 filters `part` but
// the last scan is `orders/nation`), so the commit handler cannot infer it.
inline void route_filtered_row(
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result,
    const std::string& table, const std::string& key, std::uint64_t tid) {
  if (tls_current_tx_occ_state != nullptr) {
    tls_current_tx_occ_state->filtered_rows.push_back({table, key, tid});
  } else {
    step_result->add_filtered_keys(key);
    step_result->add_filtered_tids(tid);
  }
}
// (Projection pushdown) Trim a full row VALUE to only the projected columns.
// Row format (ha_lineairdb set_write_buffer): [null_flags field][col_0]..[col_{N-1}],
// each field = [byteSize:1B][len:byteSize B][bytes], byteSize==0xFF => null (1 byte).
// `kept` = ascending unique 0-based column indices. Emits [null_flags][kept cols]
// (null_flags kept FULL). Returns false on any inconsistency; caller then ships
// the FULL value unchanged (safe fallback). num_columns = table->s->fields.
bool trim_row_value(const std::string& full,
                    const google::protobuf::RepeatedField<uint32_t>& kept,
                    uint32_t num_columns, std::string& out) {
    out.clear();
    const char* end = full.data() + full.size();
    auto read_field = [&](const char*& q, const char*& fstart,
                          size_t& flen) -> bool {
        fstart = q;
        if (q >= end) return false;
        uint8_t bs = static_cast<uint8_t>(*q);
        if (bs == 0xFF) { flen = 1; q += 1; return true; }
        if (q + 1 + bs > end) return false;
        size_t len = 0;
        for (uint8_t i = 0; i < bs; i++)
            len |= static_cast<size_t>(static_cast<uint8_t>(q[1 + i])) << (8 * i);
        if (q + 1 + bs + len > end) return false;
        flen = 1 + bs + len;
        q += flen;
        return true;
    };
    const char* q = full.data();
    const char* fs;
    size_t fl;
    if (!read_field(q, fs, fl)) return false;  // field 0 = null_flags
    out.append(fs, fl);
    int ki = 0;
    for (uint32_t c = 0; c < num_columns; c++) {  // column c is field index c+1
        const char* cs;
        size_t cl;
        if (!read_field(q, cs, cl)) return false;
        if (ki < kept.size() &&
            kept.Get(ki) == static_cast<uint32_t>(c)) {
            out.append(cs, cl);
            ki++;
        }
    }
    return ki == kept.size();  // every requested column was present
}
}  // namespace

namespace {
// HELIOS_MEMPROF: read jemalloc's live-heap accounting (allocator's view, not
// affected by RSS/swap noise). dlsym so we don't link-depend on jemalloc.
size_t je_stat(const char* name) {
    using mallctl_t = int (*)(const char*, void*, size_t*, void*, size_t);
    static mallctl_t mc = reinterpret_cast<mallctl_t>(dlsym(RTLD_DEFAULT, "mallctl"));
    if (!mc) return 0;
    // refresh the epoch so stats.* reflect the current state
    uint64_t epoch = 1; size_t esz = sizeof(epoch);
    mc("epoch", &epoch, &esz, &epoch, esz);
    size_t v = 0; size_t vsz = sizeof(v);
    if (mc(name, &v, &vsz, nullptr, 0) != 0) return 0;
    return v;
}
}  // namespace
#include <cstdlib>
#include <cstdio>

#include "lineairdb.pb.h"

namespace {

// Copy range-scan node-version snapshots into the proto repeated field.
void to_proto_range_versions(
    const std::vector<LineairDB::ExternalRangeValidationEntry>& in,
    google::protobuf::RepeatedPtrField<
        LineairDB::Protocol::RangeValidationEntry>* out_entries) {
    for (const auto& entry : in) {
        auto* out = out_entries->Add();
        out->set_table_name(entry.table_name);
        out->set_index_name(entry.index_name);
        out->set_owner_ptr(entry.owner_ptr);
        out->set_node_ptr(entry.node_ptr);
        out->set_version(entry.version);
        out->set_start_key(entry.start_key);
        out->set_end_key(entry.end_key);
        out->set_row_limit(entry.row_limit);
        out->set_reverse_scan(entry.reverse_scan);
        for (const auto& key : entry.result_keys) out->add_result_keys(key);
        for (const auto& key : entry.result_primary_keys) {
            out->add_result_primary_keys(key);
        }
    }
}

// Copy exact index-entry snapshots into the proto repeated field.
void to_proto_index_reads(
    const std::vector<LineairDB::ExternalIndexValidationEntry>& in,
    google::protobuf::RepeatedPtrField<
        LineairDB::Protocol::IndexValidationEntry>* out_entries) {
    for (const auto& entry : in) {
        auto* out = out_entries->Add();
        out->set_table_name(entry.table_name);
        out->set_index_name(entry.index_name);
        out->set_key(entry.key);
        out->set_tid(entry.tid);
        out->set_found(entry.found);
    }
}

// Bump the byte string to its lexicographic successor; returns empty on overflow.
std::string next_lexicographic_key(std::string key) {
    for (size_t i = key.size(); i-- > 0;) {
        auto byte = static_cast<unsigned char>(key[i]);
        if (byte != 0xFF) {
            key[i] = static_cast<char>(byte + 1);
            key.resize(i + 1);
            return key;
        }
    }
    return {};
}

// Return the bytes of column `column_index` from a serialized MySQL row payload.
std::string_view extract_value_column(const std::string& row,
                                      int column_index) {
    size_t offset = 0;
    int field_index = 0;
    const int target_field = column_index + 1; // field 0 is null flags.

    while (offset < row.size()) {
        const auto byte_size = static_cast<unsigned char>(row[offset]);
        ++offset;
        if (byte_size == 0xFF) {
            if (field_index == target_field) return {};
            ++field_index;
            continue;
        }
        if (offset + byte_size > row.size()) return {};

        size_t value_length = 0;
        for (unsigned int i = 0; i < byte_size; ++i) {
            value_length |= static_cast<size_t>(
                static_cast<unsigned char>(row[offset + i])) << (8 * i);
        }
        offset += byte_size;
        if (offset + value_length > row.size()) return {};

        if (field_index == target_field) {
            return std::string_view(row.data() + offset, value_length);
        }
        offset += value_length;
        ++field_index;
    }
    return {};
}

// Build the int-keyed primary key bytes. Mirrors the layout produced by
// ha_lineairdb::append_key_part_encoding, so server-side read-plan scan
// boundaries match what the proxy wrote into LineairDB:
//   [0x00 not-null marker | 0x10 INT type tag | 2-byte big-endian length=4
//    | 4-byte signed int with top bit flipped]
// Flipping the top bit makes byte-wise lexicographic sort agree with signed
// integer order, so Masstree can sort without knowing the column type.
// TODO: factor this and ha_lineairdb's encoder into a shared encoder so the
// two ends cannot drift.
std::string encode_int_key_part(int64_t value) {
    const auto encoded = static_cast<uint32_t>(static_cast<int32_t>(value)) ^
                         0x80000000U;
    std::string out;
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x10));
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x04));
    out.push_back(static_cast<char>((encoded >> 24) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 16) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 8) & 0xFF));
    out.push_back(static_cast<char>(encoded & 0xFF));
    return out;
}

// Parse a decimal-string column and encode it as int-keyed primary key bytes.
std::string encode_column_as_int_key(std::string_view column,
                                     int64_t int_delta) {
    std::string tmp(column);
    int64_t value = std::strtoll(tmp.c_str(), nullptr, 10);
    return encode_int_key_part(value + int_delta);
}

// Pick the source byte string for a binding (from a step's scan key, scan value, or value).
const std::string* select_source_bytes(
    const LineairDB::Protocol::TxExecuteReadPlan::StepResult& source,
    const LineairDB::Protocol::TxExecuteReadPlan::KeyBinding& binding,
    bool from_key, int row_override) {
    if (from_key) {
        if (source.scan_keys_size() == 0) return nullptr;
        int row = row_override >= 0 ? row_override : binding.source_row();
        if (binding.use_midpoint()) row = (source.scan_keys_size() - 1) / 2;
        row = std::min(row, source.scan_keys_size() - 1);
        return &source.scan_keys(row);
    }

    if (source.scan_values_size() > 0) {
        int row = row_override >= 0 ? row_override : binding.source_row();
        if (binding.use_midpoint()) row = (source.scan_values_size() - 1) / 2;
        row = std::min(row, source.scan_values_size() - 1);
        return &source.scan_values(row);
    }
    return &source.value();
}

// Compose a read-plan key prefix plus all bindings into the actual scan key.
std::string build_plan_key(
    const std::string& prefix,
    const google::protobuf::RepeatedPtrField<
        LineairDB::Protocol::TxExecuteReadPlan::KeyBinding>& bindings,
    const std::vector<LineairDB::Protocol::TxExecuteReadPlan::StepResult*>&
        previous_results,
    int row_override = -1,
    bool *complete = nullptr) {
    if (complete != nullptr) *complete = true;
    std::string key = prefix;
    for (const auto& binding : bindings) {
        const int source_step = static_cast<int>(binding.source_step());
        if (source_step < 0 ||
            source_step >= static_cast<int>(previous_results.size())) {
            if (complete != nullptr) *complete = false;
            continue;
        }

        const auto& source = *previous_results[source_step];
        std::string scratch;
        std::string_view extracted;
        if (binding.source_column() > 0) {
            const std::string* bytes =
                select_source_bytes(source, binding, false, row_override);
            if (bytes != nullptr) {
                extracted =
                    extract_value_column(*bytes, binding.source_column() - 1);
                if (binding.column_as_int_key()) {
                    scratch =
                        encode_column_as_int_key(extracted,
                                                 binding.int_delta());
                    extracted = scratch;
                }
            } else if (complete != nullptr) {
                *complete = false;
            }
        } else {
            const std::string* bytes =
                select_source_bytes(source, binding, binding.from_key(),
                                    row_override);
            if (bytes != nullptr) {
                uint32_t offset = binding.source_offset();
                uint32_t length = binding.source_length();
                if (offset < bytes->size()) {
                    if (length == 0) length = bytes->size() - offset;
                    length = std::min<uint32_t>(
                        length, static_cast<uint32_t>(bytes->size() - offset));
                    extracted = std::string_view(bytes->data() + offset,
                                                 length);
                } else if (complete != nullptr) {
                    *complete = false;
                }
            } else if (complete != nullptr) {
                *complete = false;
            }
        }
        if (bindings.size() > 0 && extracted.empty() && complete != nullptr) {
            *complete = false;
        }
        key.append(extracted.data(), extracted.size());
    }
    return key;
}

}  // namespace

LineairDBRpc::LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager,
                           std::shared_ptr<TableRowCounts> row_counts)
    : db_manager_(db_manager), tx_manager_(tx_manager), row_counts_(row_counts) {
}

void LineairDBRpc::handle_rpc(uint64_t sender_id, MessageType message_type,
                             const std::string& message, std::string& result) {
    LOG_DEBUG("Handling RPC: message_type=%u", static_cast<uint32_t>(message_type));

    // Close this thread's masstree RCU critical section after each
    // self-contained handler (stateless RPCs and DB_END_TRANSACTION) so
    // RCU can reclaim retired leaves; the next masstree op re-opens it.
    auto release_masstree_thread_epoch = [this]() {
        if (db_manager_) {
            auto db = db_manager_->get_database();
            if (db) db->ReleaseMasstreeThreadEpoch();
        }
    };
    switch(message_type) {
        // Transaction lifecycle
        case MessageType::TX_BEGIN_TRANSACTION:
            handleTxBeginTransaction(message, result);
            return;
        case MessageType::TX_ABORT:
            handleTxAbort(message, result);
            return;

        // Primary key operations
        case MessageType::TX_READ:
            handleTxRead(message, result);
            return;
        case MessageType::TX_BATCH_READ:
            handleTxBatchRead(message, result);
            return;
        case MessageType::TX_BATCH_WRITE:
            handleTxBatchWrite(message, result);
            return;
        case MessageType::TX_STATELESS_READ:
            handleTxStatelessRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_BATCH_READ:
            handleTxStatelessBatchRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_RANGE_SCAN:
            handleTxStatelessRangeScan(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_SECONDARY_RANGE_SCAN:
            handleTxStatelessSecondaryRangeScan(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_EXECUTE_READ_PLAN:
            handleTxExecuteReadPlan(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_GET_TABLE_STATS:
            handleTxGetTableStats(message, result);
            return;
        case MessageType::TX_VALIDATE_AND_COMMIT:
            handleTxValidateAndCommit(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_WRITE:
            handleTxWrite(message, result);
            return;
        case MessageType::TX_DELETE:
            handleTxDelete(message, result);
            return;

        // Secondary index operations
        case MessageType::TX_READ_SECONDARY_INDEX:
            handleTxReadSecondaryIndex(message, result);
            return;
        case MessageType::TX_WRITE_SECONDARY_INDEX:
            handleTxWriteSecondaryIndex(message, result);
            return;
        case MessageType::TX_DELETE_SECONDARY_INDEX:
            handleTxDeleteSecondaryIndex(message, result);
            return;
        case MessageType::TX_UPDATE_SECONDARY_INDEX:
            handleTxUpdateSecondaryIndex(message, result);
            return;

        // Primary key scan operations
        case MessageType::TX_GET_MATCHING_KEYS_IN_RANGE:
            handleTxGetMatchingKeysInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE:
            handleTxGetMatchingKeysAndValuesInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX:
            handleTxGetMatchingKeysAndValuesFromPrefix(message, result);
            return;
        case MessageType::TX_FETCH_LAST_KEY_IN_RANGE:
            handleTxFetchLastKeyInRange(message, result);
            return;
        case MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX:
            handleTxFetchFirstKeyWithPrefix(message, result);
            return;
        case MessageType::TX_FETCH_NEXT_KEY_WITH_PREFIX:
            handleTxFetchNextKeyWithPrefix(message, result);
            return;

        // Secondary index scan operations
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE:
            handleTxGetMatchingPrimaryKeysInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX:
            handleTxGetMatchingPrimaryKeysFromPrefix(message, result);
            return;
        case MessageType::TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE:
            handleTxFetchLastPrimaryKeyInSecondaryRange(message, result);
            return;
        case MessageType::TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE:
            handleTxFetchLastSecondaryEntryInRange(message, result);
            return;

        // Database operations
        case MessageType::DB_FENCE:
            handleDbFence(message, result);
            return;
        case MessageType::DB_END_TRANSACTION:
            handleDbEndTransaction(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::DB_CREATE_TABLE:
            handleDbCreateTable(message, result);
            return;
        case MessageType::DB_SET_TABLE:
            handleDbSetTable(message, result);
            return;
        case MessageType::DB_CREATE_SECONDARY_INDEX:
            handleDbCreateSecondaryIndex(message, result);
            return;

        default:
            LOG_ERROR("Unknown message type: %u", static_cast<uint32_t>(message_type));
            return;
    }
}

bool LineairDBRpc::key_prefix_is_matching(const std::string& key_prefix, const std::string& key) {
    if (key.substr(0, key_prefix.size()) != key_prefix) return false;
    return true;
}

void LineairDBRpc::handleTxBeginTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxBeginTransaction");

    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;

    request.ParseFromString(message);

    auto& tx = db_manager_->get_database()->BeginTransaction();
    int64_t tx_id = tx_manager_->generate_tx_id();
    tx_manager_->store_transaction(tx_id, &tx);

    response.set_transaction_id(tx_id);

    // Piggyback current table row counts so proxy has fresh stats.
    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    result = response.SerializeAsString();

    LOG_DEBUG("Created transaction: %ld", tx_id);
}

// Transaction-less row-count snapshot for the optimizer (no BeginTransaction,
// no tx_id, no side effects). Used by the proxy to seed accurate cardinalities
// at MySQL optimize time despite oneshot's deferred tx_begin.
void LineairDBRpc::handleTxGetTableStats(const std::string& message,
                                         std::string& result) {
    (void)message;
    LineairDB::Protocol::GetTableStats::Response response;
    if (row_counts_) {
        for (const auto& [name, count] : row_counts_->snapshot()) {
            auto* ts = response.add_table_stats();
            ts->set_table_name(name);
            ts->set_row_count(count);
        }
    }
    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxAbort(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxAbort");

    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        tx->Abort();
    } else {
        LOG_WARNING("Transaction not found for abort: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxRead(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxRead");

    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        auto read_result = tx->Read(request.key());
        response.set_is_aborted(tx->IsAborted());

        if (read_result.first != nullptr) {
            response.set_found(true);
            std::string value(reinterpret_cast<const char*>(read_result.first), read_result.second);
            response.set_value(value);
        } else {
            response.set_found(false);
        }

        LOG_DEBUG("Read key '%s' from transaction %ld: %s", request.key().c_str(), tx_id, (read_result.first != nullptr ? "found" : "not found"));
    } else {
        response.set_found(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for read: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxBatchRead(const std::string& message, std::string& result) {
    LineairDB::Protocol::TxBatchRead::Request request;
    LineairDB::Protocol::TxBatchRead::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        for (int i = 0; i < request.keys_size(); i++) {
            auto* read_result = response.add_results();
            auto pair = tx->Read(request.keys(i));
            if (pair.first != nullptr) {
                read_result->set_found(true);
                read_result->set_value(
                    reinterpret_cast<const char*>(pair.first), pair.second);
            } else {
                read_result->set_found(false);
            }
        }
        response.set_is_aborted(tx->IsAborted());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for batch_read: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxBatchWrite(const std::string& message, std::string& result) {
    LineairDB::Protocol::TxBatchWrite::Request request;
    LineairDB::Protocol::TxBatchWrite::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }

        if (!tx->IsAborted()) {
            for (int i = 0; i < request.ops_size(); i++) {
                const auto& op = request.ops(i);
                const std::string& op_table =
                    op.table_name().empty() ? request.table_name() : op.table_name();
                if (!op_table.empty()) {
                    tx->SetTable(op_table);
                }
                switch (op.type()) {
                    case LineairDB::Protocol::BATCH_OP_WRITE: {
                        const std::string& value_str = op.value();
                        tx->Write(op.key(),
                                  reinterpret_cast<const std::byte*>(value_str.c_str()),
                                  value_str.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_DELETE:
                        tx->Delete(op.key());
                        break;
                    case LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_WRITE: {
                        const std::string& pk = op.primary_key();
                        tx->WriteSecondaryIndex(
                            op.index_name(), op.secondary_key(),
                            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_DELETE: {
                        const std::string& pk = op.primary_key();
                        tx->DeleteSecondaryIndex(
                            op.index_name(), op.secondary_key(),
                            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_UNKNOWN:
                    default:
                        break;
                }
                if (tx->IsAborted()) break;
            }
        }

        response.set_success(!tx->IsAborted());
        response.set_is_aborted(tx->IsAborted());
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for batch_write: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

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

void LineairDBRpc::handleTxStatelessRangeScan(const std::string& message,
                                              std::string& result) {
    LineairDB::Protocol::TxStatelessRangeScan::Request request;
    LineairDB::Protocol::TxStatelessRangeScan::Response response;

    request.ParseFromString(message);

    auto scan_result = db_manager_->get_database()->StatelessRangeScan(
        request.table_name(), request.start_key(), request.end_key(),
        request.row_limit(), request.reverse_scan());
    response.set_ok(scan_result.ok);
    if (!scan_result.ok) {
        result = response.SerializeAsString();
        return;
    }

    for (const auto& row : scan_result.rows) {
        auto* out = response.add_rows();
        out->set_key(row.key);
        out->set_value(row.value);
        out->set_tid(row.tid);
        out->set_found(row.found);
    }
    to_proto_range_versions(scan_result.range_versions,
                          response.mutable_range_versions());
    for (const auto& entry : scan_result.index_reads) {
        auto* out = response.add_index_reads();
        out->set_table_name(entry.table_name);
        out->set_index_name(entry.index_name);
        out->set_key(entry.key);
        out->set_tid(entry.tid);
        out->set_found(entry.found);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessSecondaryRangeScan(
    const std::string& message, std::string& result) {
    LineairDB::Protocol::TxStatelessSecondaryRangeScan::Request request;
    LineairDB::Protocol::TxStatelessSecondaryRangeScan::Response response;

    request.ParseFromString(message);

    auto scan_result = db_manager_->get_database()->StatelessSecondaryRangeScan(
        request.table_name(), request.index_name(), request.start_key(),
        request.end_key(), request.row_limit(), request.reverse_scan());
    response.set_ok(scan_result.ok);
    if (!scan_result.ok) {
        result = response.SerializeAsString();
        return;
    }

    for (const auto& row : scan_result.rows) {
        auto* out = response.add_rows();
        out->set_secondary_key(row.secondary_key);
        out->set_primary_key(row.primary_key);
        out->set_value(row.value);
        out->set_tid(row.tid);
        out->set_found(row.found);
    }
    to_proto_range_versions(scan_result.range_versions,
                          response.mutable_range_versions());
    for (const auto& entry : scan_result.index_reads) {
        auto* out = response.add_index_reads();
        out->set_table_name(entry.table_name);
        out->set_index_name(entry.index_name);
        out->set_key(entry.key);
        out->set_tid(entry.tid);
        out->set_found(entry.found);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::buildExecuteReadPlanResponse(
    const std::string& message,
    LineairDB::Protocol::TxExecuteReadPlan::Response& response) {
    // Step F (Codex 2026-05-29): per-phase timers in server's read-plan
    // build. Diagnostic only. HELIOS_SERVER_TIMEPROF=1 to emit.
    const bool stp = (std::getenv("HELIOS_SERVER_TIMEPROF") != nullptr);
    auto tp_now = []() -> uint64_t {
      timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
      return uint64_t(t.tv_sec) * 1000000000ull + t.tv_nsec;
    };
    const uint64_t stp_total_start = stp ? tp_now() : 0;
    uint64_t stp_parse_ns = 0, stp_db_ns = 0, stp_proto_ns = 0;
    int stp_n_scan = 0, stp_n_pointread = 0, stp_n_secscan = 0;
    uint64_t stp_t_parse_in = stp ? tp_now() : 0;
    LineairDB::Protocol::TxExecuteReadPlan::Request request;
    const bool parsed_ok = request.ParseFromString(message);
    if (stp) stp_parse_ns = tp_now() - stp_t_parse_in;
    if (std::getenv("HELIOS_FE_DEBUG"))
        std::fprintf(stderr, "[PLAN] parse_ok=%d msg_sz=%zu steps=%d\n",
                     parsed_ok ? 1 : 0, message.size(), request.steps_size());
    response.set_ok(true);

    std::vector<LineairDB::Protocol::TxExecuteReadPlan::StepResult*>
        previous_results;
    previous_results.reserve(request.steps_size());

    // option-2 (Codex 2026-05-29): steps whose VALUE a later column-form binding
    // extracts from. If such a step is PROJECTED and trim_row_value fails (would
    // ship a full fallback row), the proxy's remapped source_column would read
    // the wrong column. Make trim failure FATAL (ok=false) for these steps
    // instead of silently shipping the full row.
    std::unordered_set<int> strict_proj_src;
    for (const auto& s : request.steps()) {
        for (const auto& b : s.bindings())
            if (b.source_column() > 0)
                strict_proj_src.insert(static_cast<int>(b.source_step()));
        for (const auto& b : s.end_bindings())
            if (b.source_column() > 0)
                strict_proj_src.insert(static_cast<int>(b.source_step()));
    }
    bool proj_trim_fatal = false;

    int step_idx = -1;
    for (const auto& step : request.steps()) {
        ++step_idx;  // before any `continue`, so the index tracks the step
        auto* step_result = response.add_results();
        previous_results.push_back(step_result);

        // Projection pushdown: trim each emitted base-row VALUE to the kept
        // columns (after any predicate filter ran on the full row). Absent
        // projection => identity (full row). On inconsistency, ship full (safe).
        // Step D-1 (Phase-4, Codex 2026-05-29): the previous lambda returned
        // std::string by value, forcing a COPY in the non-projection path
        // (Q1 SF=1: 6M × ~150B = ~900 MB redundant copy = ~500-700ms).
        // The new lambda mutates `v` (move-from) when no projection, so the
        // caller can do `step_result->add_scan_values(std::move(v))` without
        // an extra copy. The projection path still copies because the
        // trimmer reads `v` and emits into a separate buffer.
        const bool step_has_projection = step.has_projection();
        const bool strict_src_step = strict_proj_src.count(step_idx) > 0;
        // project_xfer: returns the std::string to be moved into the proto.
        // If projection: trim into `out` and return out. Else: return `v`
        // (the caller-supplied source) — caller must move-in.
        auto project_xfer = [&step, step_has_projection, strict_src_step,
                             &proj_trim_fatal](std::string& v) -> std::string {
            if (!step_has_projection) return std::move(v);
            std::string out;
            if (trim_row_value(v, step.projection().field_indexes(),
                               step.projection().num_columns(), out))
                return out;
            // Trim failed. For a column-form binding source, a full fallback row
            // would misalign the proxy's remapped source_column -> fail the plan.
            if (strict_src_step) proj_trim_fatal = true;
            return std::move(v);
        };

        bool start_complete = true;
        bool end_complete = true;
        const std::string start_key =
            build_plan_key(step.key_prefix(), step.bindings(),
                           previous_results, -1, &start_complete);
        std::string end_key =
            build_plan_key(step.end_key_prefix(), step.end_bindings(),
                           previous_results, -1, &end_complete);
        if (!start_complete || !end_complete) continue;
        if (step.is_scan() && end_key.empty()) {
            end_key = next_lexicographic_key(start_key);
        }

        if (step.for_each()) {
            int source_step = -1;
            if (step.bindings_size() > 0) {
                source_step = static_cast<int>(step.bindings(0).source_step());
            }
            if (source_step < 0 ||
                source_step >= static_cast<int>(previous_results.size()) - 1) {
                continue;
            }

            const auto* source = previous_results[source_step];
            const int row_count =
                std::max(source->scan_keys_size(), source->scan_values_size());
            if (std::getenv("HELIOS_FE_DEBUG"))
                std::fprintf(stderr,
                    "[FE] enter tbl=%s src_step=%d src_keys=%d src_vals=%d "
                    "row_count=%d is_scan=%d idx=%s\n",
                    step.table_name().c_str(), source_step,
                    source->scan_keys_size(), source->scan_values_size(),
                    row_count, step.is_scan() ? 1 : 0,
                    step.index_name().c_str());

            if (!step.is_scan()) {
                // FE: point read per source row (PK lookup).
                // Dedup by PK: many source rows map to the same inner row (e.g.
                // 16x for Q9 orders). The proxy cache is keyed by PK
                // (record_local_read), so emitting each distinct PK once is
                // sufficient and the proxy replays multiplicity itself. A single
                // whole key (no separator) is a collision-free dedup key.
                std::unordered_set<std::string> seen_keys;
                for (int row = 0; row < row_count; ++row) {
                    bool row_complete = true;
                    const std::string row_key =
                        build_plan_key(step.key_prefix(), step.bindings(),
                                       previous_results, row, &row_complete);
                    if (!row_complete) continue;
                    if (!seen_keys.insert(row_key).second) continue;
                    const uint64_t stp_db_t0 = stp ? tp_now() : 0;
                    auto read_result =
                        db_manager_->get_database()->StatelessRead(
                            step.table_name(), row_key);
                    if (stp) { stp_db_ns += tp_now() - stp_db_t0; ++stp_n_pointread; }
                    step_result->add_scan_keys(row_key);
                    step_result->add_scan_tids(read_result.tid);
                    if (read_result.found) {
                        step_result->add_scan_values(project_xfer(read_result.value));
                    } else {
                        step_result->add_scan_values("");
                    }
                }
                continue;
            }

            // FER / FES: per-source-row RANGE scan. PK-prefix range when
            // index_name is empty (e.g. lineitem by l_orderkey), secondary
            // range otherwise (e.g. orders by o_custkey). Each source row
            // yields one SubScan with its own [start,end) so the proxy can
            // build one local cache entry per join probe (O(1) lookup).
            const bool is_secondary = !step.index_name().empty();
            // Phase-3d: a per-step pushed predicate (the FER/FES table's own
            // single-table WHERE, e.g. lineitem.l_shipdate range) filters the
            // sub-scan rows server-side so deep join tables are not over-fetched.
            // range_versions (logical result_keys) stays the PRE-filter full
            // range so commit revalidation matches — same contract as S: steps.
            const bool fe_has_filter = step.has_filter() && step.filter().has_expr();
            const uint32_t fe_num_cols =
                fe_has_filter ? step.filter().num_columns() : 0;
            PredicateEvaluator fe_eval;
            auto fe_reject = [&](const std::string& value) -> bool {
                if (!fe_has_filter) return false;
                if (!fe_eval.parse_row(value.data(), value.size(), fe_num_cols))
                    return false;  // parse failure → keep (safe)
                return !fe_eval.evaluate(step.filter().expr());
            };
            // Dedup identical sub-ranges: many source rows yield the same
            // [start,end) probe (e.g. 4x for Q9 lineitem-by-partkey, since a
            // partkey recurs once per supplier). The proxy indexes subscans by
            // start key and replays multiplicity itself, so one subscan per
            // distinct range suffices. std::pair compares the two binary keys
            // independently — collision-free even when keys contain NUL (a
            // single concatenated string with a separator would NOT be, since
            // storage keys embed 0x00). (Codex design review.)
            std::set<std::pair<std::string, std::string>> seen_ranges;
            for (int row = 0; row < row_count; ++row) {
                bool row_complete = true;
                const std::string row_start =
                    build_plan_key(step.key_prefix(), step.bindings(),
                                   previous_results, row, &row_complete);
                if (!row_complete || row_start.empty()) continue;
                std::string row_end;
                if (step.end_bindings_size() > 0) {
                    bool end_complete = true;
                    row_end = build_plan_key(step.end_key_prefix(),
                                             step.end_bindings(),
                                             previous_results, row,
                                             &end_complete);
                    if (!end_complete || row_end.empty())
                        row_end = next_lexicographic_key(row_start);
                } else {
                    row_end = next_lexicographic_key(row_start);
                }
                if (row_end.empty()) {
                    if (std::getenv("HELIOS_FE_DEBUG"))
                        std::fprintf(stderr,
                            "[FE] empty row_end tbl=%s idx=%s start_sz=%zu\n",
                            step.table_name().c_str(),
                            step.index_name().c_str(), row_start.size());
                    continue;  // skip this probe; proxy falls back
                }
                if (!seen_ranges.insert({row_start, row_end}).second)
                    continue;  // identical sub-range already scanned + emitted
                auto* sub = step_result->add_subscans();
                sub->set_start_key(row_start);
                sub->set_end_key(row_end);
                if (is_secondary) {
                    auto sr = db_manager_->get_database()
                                  ->StatelessSecondaryRangeScan(
                                      step.table_name(), step.index_name(),
                                      row_start, row_end, 0, false);
                    if (!sr.ok) {
                        // Non-fatal: drop this sub-scan, the proxy falls back to
                        // a stateless probe. Do not abort the whole plan RPC.
                        if (std::getenv("HELIOS_FE_DEBUG"))
                            std::fprintf(stderr,
                                "[FES] scan !ok tbl=%s idx=%s start_sz=%zu "
                                "end_sz=%zu\n", step.table_name().c_str(),
                                step.index_name().c_str(), row_start.size(),
                                row_end.size());
                        step_result->mutable_subscans()->RemoveLast();
                        continue;
                    }
                    for (auto& r : sr.rows) {
                        if (fe_reject(r.value)) {  // (c) validate rejected row's TID
                            route_filtered_row(step_result, step.table_name(), r.primary_key, r.tid);
                            continue;
                        }
                        std::string pv = project_xfer(r.value);
                        sub->add_secondary_keys(r.secondary_key);
                        sub->add_scan_keys(r.primary_key);
                        sub->add_scan_values(pv);
                        sub->add_scan_tids(r.tid);
                        step_result->add_secondary_keys(
                            std::move(r.secondary_key));
                        step_result->add_scan_keys(std::move(r.primary_key));
                        step_result->add_scan_values(std::move(pv));
                        step_result->add_scan_tids(r.tid);
                    }
                    to_proto_range_versions(sr.range_versions,
                                            sub->mutable_range_versions());
                } else {
                    const uint64_t stp_db_t0 = stp ? tp_now() : 0;
                    auto sr = db_manager_->get_database()->StatelessRangeScan(
                        step.table_name(), row_start, row_end, 0, false);
                    if (stp) { stp_db_ns += tp_now() - stp_db_t0; ++stp_n_scan; }
                    if (!sr.ok) {
                        if (std::getenv("HELIOS_FE_DEBUG"))
                            std::fprintf(stderr,
                                "[FER] scan !ok tbl=%s start_sz=%zu end_sz=%zu\n",
                                step.table_name().c_str(), row_start.size(),
                                row_end.size());
                        step_result->mutable_subscans()->RemoveLast();
                        continue;
                    }
                    for (auto& r : sr.rows) {
                        if (fe_reject(r.value)) {  // (c) validate rejected row's TID
                            route_filtered_row(step_result, step.table_name(), r.key, r.tid);
                            continue;
                        }
                        std::string pv = project_xfer(r.value);
                        sub->add_scan_keys(r.key);
                        sub->add_scan_values(pv);
                        sub->add_scan_tids(r.tid);
                        step_result->add_scan_keys(std::move(r.key));
                        step_result->add_scan_values(std::move(pv));
                        step_result->add_scan_tids(r.tid);
                    }
                    to_proto_range_versions(sr.range_versions,
                                            sub->mutable_range_versions());
                }
            }
            if (std::getenv("HELIOS_FE_DEBUG"))
                std::fprintf(stderr,
                    "[FE] done tbl=%s subscans=%d flat_keys=%d\n",
                    step.table_name().c_str(), step_result->subscans_size(),
                    step_result->scan_keys_size());
            continue;
        }

        if (!step.is_scan()) {
            auto read_result =
                db_manager_->get_database()->StatelessRead(
                    step.table_name(), start_key);
            step_result->set_actual_key(start_key);
            step_result->set_actual_start_key(start_key);
            step_result->set_found(read_result.found);
            step_result->set_tid(read_result.tid);
            if (read_result.found) {
                step_result->set_value(project_xfer(read_result.value));
            }
            continue;
        }

        if (step.index_name().empty()) {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            // Phase-3b: when a predicate is attached we must disable the
            // pushed scan_limit at the LineairDB layer and re-apply it after
            // post-filter. Otherwise LineairDB returns the first N physical
            // rows and we filter from them — for `WHERE x=1 LIMIT 1` that
            // gives zero rows even when a matching row exists later. Per
            // Codex P1 review.
            const bool has_filter =
                step.has_filter() && step.filter().has_expr();
            const uint64_t scan_limit_for_lineairdb =
                has_filter ? 0 : step.scan_limit();
            const uint64_t stp_db_t0 = stp ? tp_now() : 0;
            auto scan_result =
                db_manager_->get_database()->StatelessRangeScan(
                    step.table_name(), start_key, end_key,
                    scan_limit_for_lineairdb, step.reverse_scan());
            if (stp) { stp_db_ns += tp_now() - stp_db_t0; ++stp_n_scan; }
            if (!scan_result.ok) {
                if (std::getenv("HELIOS_FE_DEBUG"))
                    std::fprintf(stderr,
                        "[PLAN] S !ok tbl=%s start_sz=%zu end_sz=%zu\n",
                        step.table_name().c_str(), start_key.size(),
                        end_key.size());
                response.set_ok(false);  // caller flat-encodes/streams this ok=false response
                return;
            }
            // Predicate pushdown: post-process scan_result.rows. parse_row
            // failures fall through to include the row (safe-fallback,
            // matching the existing TxGetMatching* handler semantics).
            //
            // INCOMPLETE — Codex review round 2 P1: rejected rows' TIDs are
            // currently dropped. Logical range validation only replays the
            // returned key list, so a concurrent UPDATE that flips a row's
            // predicate column into the matched set after our scan but
            // before commit will go undetected. Fix options: emit rejected
            // (key, tid) pairs in step_result for the proxy to add to its
            // validation set, or re-evaluate the predicate during commit
            // here. Filter pushdown is dormant in current callers (TPC-H
            // SELECT cond_push is no-op, TPC-C oneshot plans don't carry
            // unindexed S: scans with pushed_filter); enabling it for
            // Phase-3c must address this.
            if (has_filter && !scan_result.rows.empty()) {
                const auto& filter_expr = step.filter().expr();
                const uint32_t num_cols = step.filter().num_columns();
                PredicateEvaluator evaluator;
                std::vector<LineairDB::StatelessScanRow> filtered;
                filtered.reserve(scan_result.rows.size());
                for (auto& row : scan_result.rows) {
                    bool keep = true;
                    if (evaluator.parse_row(row.value.data(), row.value.size(),
                                            num_cols)) {
                        keep = evaluator.evaluate(filter_expr);
                    }
                    if (keep) {
                        filtered.push_back(std::move(row));
                    } else {
                        // (c) Don't ship the rejected row's value, but record its
                        // (key, tid) so the proxy validates it at commit: a
                        // concurrent UPDATE flipping it INTO the predicate changes
                        // its TID and aborts. Range key-list validation alone
                        // misses such value-only changes. (Physical-mode routes
                        // the row to TxOccState instead via route_filtered_row.)
                        route_filtered_row(step_result, step.table_name(), row.key, row.tid);
                    }
                }
                // Re-apply the pushed limit after filtering.
                if (step.scan_limit() > 0 &&
                    filtered.size() > step.scan_limit()) {
                    filtered.resize(step.scan_limit());
                }
                scan_result.rows = std::move(filtered);
            }
            for (auto& row : scan_result.rows) {
                step_result->add_scan_keys(std::move(row.key));
                step_result->add_scan_values(project_xfer(row.value));
                step_result->add_scan_tids(row.tid);
            }
            to_proto_range_versions(scan_result.range_versions,
                                  step_result->mutable_range_versions());
            to_proto_index_reads(scan_result.index_reads,
                               step_result->mutable_index_reads());

            // helios Phase-6 range-hash OCC: for a primary full-range scan in
            // physical mode (gated), capture a 32-byte footprint digest now so
            // the read-only commit path can revalidate via re-scan instead of
            // shipping/checking the O(rows) per-row read set. Gated by
            // HELIOS_RANGEHASH_OCC; the proxy decides (read-only only) whether
            // to actually USE it at commit. Computing it for a write txn is
            // harmless (never validated). One extra range scan at prefetch —
            // the cost of a footprint-identical digest vs the commit re-scan.
            // Phase-7: ON by default (opt out with HELIOS_RANGEHASH_OCC=0).
            static const char* rh_env = std::getenv("HELIOS_RANGEHASH_OCC");
            static const bool rangehash_on =
                (rh_env == nullptr) || (std::strcmp(rh_env, "0") != 0);
            // Only full-cover primary ranges (start_key == "") are hashed: the
            // proxy skips per-row reads exactly for rows served from a
            // full-cover cache entry, so the hashed set and the skipped set
            // align. Bounded/own-probe ranges keep per-row validation.
            // read-only no-validation: the digest is never validated (no commit
            // RPC), so skip the full-range SHA-256 footprint entirely — pure
            // dead CPU over millions of rows otherwise. (Codex Stage 1.)
            if (rangehash_on && !request.read_only_no_validate() &&
                tls_current_tx_occ_state != nullptr &&
                !step.for_each() && step.index_name().empty() &&
                start_key.empty()) {
                helios::TxOccState::RangeHash rh;
                rh.table_name = step.table_name();
                rh.start_key = start_key;
                rh.end_key = end_key;
                rh.row_limit = step.scan_limit();
                rh.reverse_scan = step.reverse_scan();
                if (db_manager_->get_database()->ComputePrimaryRangeFootprintHash(
                        rh.table_name, rh.start_key, rh.end_key, rh.row_limit,
                        rh.reverse_scan, rh.root)) {
                    tls_current_tx_occ_state->range_hashes.push_back(std::move(rh));
                }
            }
        } else {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            const uint64_t stp_db_t0 = stp ? tp_now() : 0;
            auto scan_result =
                db_manager_->get_database()->StatelessSecondaryRangeScan(
                    step.table_name(), step.index_name(), start_key, end_key,
                    step.scan_limit(), step.reverse_scan());
            if (stp) { stp_db_ns += tp_now() - stp_db_t0; ++stp_n_secscan; }
            if (!scan_result.ok) {
                response.set_ok(false);  // caller flat-encodes/streams this ok=false response
                return;
            }
            for (auto& row : scan_result.rows) {
                step_result->add_secondary_keys(std::move(row.secondary_key));
                step_result->add_scan_keys(std::move(row.primary_key));
                step_result->add_scan_values(project_xfer(row.value));
                step_result->add_scan_tids(row.tid);
            }
            to_proto_range_versions(scan_result.range_versions,
                                  step_result->mutable_range_versions());
            to_proto_index_reads(scan_result.index_reads,
                               step_result->mutable_index_reads());
        }
    }

    // option-2 guard: a projected binding-source step shipped a full fallback
    // row (trim failed), which would misalign the proxy's remapped
    // source_column. Fail the whole plan so the proxy aborts/falls back rather
    // than build a wrong key. (Rare: only on a malformed row.)
    if (proj_trim_fatal) {
        response.set_ok(false);
    }

    // [PLANSZ] per-step composition diagnostic (HELIOS_PLAN_SIZE). Logs, per
    // step, how many rows/keys/values/result_keys/index_reads/filtered the
    // server is shipping back, so over-fetch (e.g. the same big table
    // materialized in many steps) is visible. fflush forces the line out even
    // when stderr is block-buffered to a redirected file.
    if (std::getenv("HELIOS_PLAN_SIZE")) {
        size_t grand_val = 0, grand_rk = 0, grand_keys = 0;
        for (int si = 0; si < response.results_size(); ++si) {
            const auto& sr = response.results(si);
            const auto& st = request.steps(si);
            size_t valb = 0;
            for (const auto& v : sr.scan_values()) valb += v.size();
            valb += sr.value().size();
            size_t rk = 0;
            for (const auto& rv : sr.range_versions()) rk += rv.result_keys_size();
            size_t idxr = sr.index_reads_size();
            size_t fk = sr.filtered_keys_size();
            size_t sub = sr.subscans_size();
            grand_val += valb; grand_rk += rk; grand_keys += sr.scan_keys_size();
            std::fprintf(stderr,
                "[PLANSZ] step=%d tbl=%s idx=%s scan=%d foreach=%d "
                "scan_keys=%d sec_keys=%d val_bytes=%zu result_keys=%zu "
                "index_reads=%zu filtered=%zu subscans=%zu\n",
                si, st.table_name().c_str(), st.index_name().c_str(),
                st.is_scan() ? 1 : 0, st.for_each() ? 1 : 0,
                sr.scan_keys_size(), sr.secondary_keys_size(), valb, rk,
                idxr, fk, sub);
        }
        std::fprintf(stderr,
            "[PLANSZ] TOTAL steps=%d scan_keys=%zu val_bytes=%zu "
            "result_keys=%zu resp_bytes=%zu\n",
            response.results_size(), grand_keys, grand_val, grand_rk,
            response.ByteSizeLong());
        std::fflush(stderr);
    }

    if (stp) {
      const uint64_t stp_total = tp_now() - stp_total_start;
      const uint64_t stp_other = (stp_total > stp_parse_ns + stp_db_ns)
                                     ? stp_total - stp_parse_ns - stp_db_ns
                                     : 0;
      auto ms = [](uint64_t ns){ return ns / 1000000.0; };
      std::fprintf(stderr,
          "[STIMEPROF] build total=%.1fms parse=%.1fms db=%.1fms "
          "proto_copy_other=%.1fms steps=%d (scan=%d ptread=%d sec=%d)\n",
          ms(stp_total), ms(stp_parse_ns), ms(stp_db_ns), ms(stp_other),
          response.results_size(), stp_n_scan, stp_n_pointread, stp_n_secscan);
      std::fflush(stderr);
    }

    // (No serialization here: the caller flat-encodes `response`, either into a
    // std::string (buffered) or streamed to the socket (handleTxExecuteReadPlanStreamed).)
}

// Buffered entry point (kept for handle_rpc dispatch compatibility). The hot
// path (handle_client) uses handleTxExecuteReadPlanStreamed instead, which
// avoids building a full flat buffer on top of the proto response.
void LineairDBRpc::handleTxExecuteReadPlan(const std::string& message,
                                           std::string& result) {
    LineairDB::Protocol::TxExecuteReadPlan::Response response;
    buildExecuteReadPlanResponse(message, response);
    helios_flat::encode_response(response, &result);
}

namespace {
// (A) Streams flat bytes to a socket in ~1MB chunks so the server never holds
// a full second copy of the (multi-GB) response. Satisfies the codec Sink
// concept (append(const char*, size_t)).
struct SocketSink {
    int fd;
    bool failed = false;
    std::string buf;
    static constexpr size_t kChunk = 1u << 20;  // 1MB
    explicit SocketSink(int s) : fd(s) { buf.reserve(kChunk); }
    void send_all(const char* p, size_t n) {
        size_t sent = 0;
        while (sent < n) {
            ssize_t k = ::send(fd, p + sent, n - sent, 0);
            if (k <= 0) { failed = true; return; }
            sent += static_cast<size_t>(k);
        }
    }
    void append(const char* p, size_t n) {
        if (failed) return;
        buf.append(p, n);
        if (buf.size() >= kChunk) { send_all(buf.data(), buf.size()); buf.clear(); }
    }
    bool finish() {
        if (!failed && !buf.empty()) { send_all(buf.data(), buf.size()); buf.clear(); }
        return !failed;
    }
};
inline bool send_all_fd(int fd, const char* p, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t k = ::send(fd, p + sent, n - sent, 0);
        if (k <= 0) return false;
        sent += static_cast<size_t>(k);
    }
    return true;
}
}  // namespace

bool LineairDBRpc::handleTxExecuteReadPlanStreamed(int socket, uint64_t sender_id,
                                                   const std::string& message) {
    const bool memprof = std::getenv("HELIOS_MEMPROF") != nullptr;
    const size_t je_before = memprof ? je_stat("stats.allocated") : 0;

    // read-only no-validation (Stage 1): the proxy will skip the
    // commit-validation RPC, so we must NOT retain OCC state (no TxOccStore
    // Insert, no epoch pin) — else it leaks until connection-close/TTL. Parse
    // just this flag from the (small) plan request. (docs/phase7_readonly_novalidate.md)
    bool read_only_no_validate = false;
    {
        LineairDB::Protocol::TxExecuteReadPlan::Request ro_req;
        if (ro_req.ParseFromString(message))
            read_only_no_validate = ro_req.read_only_no_validate();
    }

    // ---- helios PHYSICAL OCC (Step 5) setup --------------------------------
    // Default = physical (Codex 2026-05-28: stateless.h:64 comment "always
    // emits the physical form" was stale; logical mode shipped
    // millions of result_keys per Q1/Q21 full scan and revalidated them on
    // commit. Set HELIOS_LOGICAL_OCC=1 to fall back to the legacy logical
    // path for A/B comparison.)
    static const bool physical_mode_enabled =
        (std::getenv("HELIOS_LOGICAL_OCC") == nullptr);
    static std::atomic<std::uint64_t> next_tx_occ_key{1};
    helios::TxOccState phys_state;
    std::uint64_t phys_key = 0;
    std::shared_ptr<LineairDB::Database> db_for_physical;
    if (physical_mode_enabled && db_manager_) {
        db_for_physical = db_manager_->get_database();
        if (db_for_physical) {
            phys_key = next_tx_occ_key.fetch_add(1, std::memory_order_relaxed);
            phys_state.tx_key = phys_key;
            phys_state.connection_fd = socket;
            // Set BEFORE the build: scans will see physical mode and emit
            // node_versions (instead of key lists) into per-step
            // range_versions; route_filtered_row will route filter-rejected
            // rows into phys_state.filtered_rows instead of proto.
            tls_current_tx_occ_state = &phys_state;
            db_for_physical->SetPhysicalValidationMode(true);
        }
    }

    LineairDB::Protocol::TxExecuteReadPlan::Response response;
    buildExecuteReadPlanResponse(message, response);

    // ---- helios PHYSICAL OCC (Step 5) post-build hook ---------------------
    // Simpler than the initial design: we DO NOT strip range_versions /
    // index_reads from the wire. In physical mode those are already small
    // (no result_keys; just owner_ptr/node_ptr/version of size ~24B per
    // range, plus tombstone IndexValidationEntries). The proxy echoes them
    // normally at commit, and the existing ValidateAndCommit path picks up
    // physical entries (end_key.empty() at database_impl.h:1122).
    //
    // The TxOccState role is reduced to: (1) pin the masstree epoch, (2)
    // retain filtered_rows server-side so the proxy never sees the 5.9M
    // filter-rejected (key,tid) pairs. The proxy's wire OCC data therefore
    // shrinks from O(rows) result_keys to O(leaves) node-versions, which is
    // exactly the Masstree-paper-correct cost.
    if (db_for_physical != nullptr) {
        // read-only no-validation: NO commit-validation RPC will come, so do
        // not register the epoch pin or retain TxOccState (else they leak until
        // connection-close/TTL). tx_occ_key stays 0. The range_versions still
        // shipped (Stage 1) carry node_ptrs the proxy never dereferences in this
        // mode, so skipping the pin is safe (no commit-time UAF possible). The
        // cleanup below (tls reset + physical-mode off) still runs.
        if (!read_only_no_validate) {
        const bool pinned = db_for_physical
                                ->RegisterTxEpochPinFromCurrentThread(phys_key);
        // Codex P1 #1 fix: the pin MUST be kept whenever ANY physical-mode
        // scan ran, regardless of filtered_rows. range_versions still carry
        // raw node_ptrs on the wire; without the pin, RCU can reclaim those
        // leaves between now and commit -> UAF when ValidateAndCommit
        // dereferences them at database_impl.h:1152.
        if (pinned) {
            phys_state.pinned_epoch = db_for_physical->GetTxPinFloor();
            const std::uint64_t ttl_ms = db_for_physical->GetTxPinTtlMs();
            if (ttl_ms > 0) {
                using clk = std::chrono::steady_clock;
                const auto now_ns = std::chrono::duration_cast<
                    std::chrono::nanoseconds>(clk::now().time_since_epoch())
                                        .count();
                phys_state.expires_at_ns = static_cast<std::uint64_t>(now_ns) +
                                           ttl_ms * 1000000ull;
            }
            helios::GlobalTxOccStore().Insert(std::move(phys_state));
            response.set_tx_occ_key(phys_key);
        }
        }  // !read_only_no_validate
        // Reset thread-locals/flag regardless of success.
        tls_current_tx_occ_state = nullptr;
        db_for_physical->SetPhysicalValidationMode(false);
    }
    // ---- end of Step 5 hook ----
    if (memprof) {
        const size_t je_after = je_stat("stats.allocated");
        const size_t je_res = je_stat("stats.resident");
        const uint64_t proto_bytes = response.SpaceUsedLong();
        const uint64_t flat_bytes = helios_flat::flat_size(response);
        std::fprintf(stderr,
            "[MEMPROF] read_plan: je_allocated baseline=%.2fGB after_build=%.2fGB "
            "delta(=proto+scratch)=%.2fGB | proto SpaceUsedLong=%.2fGB | "
            "flat_size=%.2fGB | je_resident=%.2fGB\n",
            je_before / 1e9, je_after / 1e9, (double)(je_after - je_before) / 1e9,
            proto_bytes / 1e9, flat_bytes / 1e9, je_res / 1e9);
        std::fflush(stderr);
    }
    // Mirror handle_rpc: close the masstree RCU epoch now that all KV reads are
    // done (streaming below touches no KV state).
    if (db_manager_) {
        auto db = db_manager_->get_database();
        if (db) db->ReleaseMasstreeThreadEpoch();
    }
    // (②) Optionally LZ4-compress the flat payload (env HELIOS_RPC_COMPRESS).
    // Payload is always CODEC-tagged: [0x00][flat] (raw) or [0x01][raw_total:8]
    // [chunks] (LZ4). The proxy strips the tag. Compression holds only the
    // compressed bytes (CompressSink), not the full flat buffer.
    const bool do_lz4 = std::getenv("HELIOS_RPC_COMPRESS") != nullptr &&
                        helios_zip::lz4_available();
    // Step F (Codex 2026-05-29) granular outer timer for the encode+send path.
    const bool sthp = (std::getenv("HELIOS_SERVER_TIMEPROF") != nullptr);
    auto sthp_now = []() -> uint64_t {
      timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
      return uint64_t(t.tv_sec) * 1000000000ull + t.tv_nsec;
    };
    if (do_lz4) {
        const uint64_t t_enc_start = sthp ? sthp_now() : 0;
        helios_zip::CompressSink cs;
        helios_flat::encode_response_into(response, cs);
        cs.finish();
        const uint64_t t_enc_end = sthp ? sthp_now() : 0;
        if (!cs.failed) {
            std::string hdr;
            hdr.push_back(static_cast<char>(helios_zip::kLZ4));
            helios_zip::put_u64(hdr, cs.raw_total_);
            const uint64_t total = hdr.size() + cs.out_.size();
            if (!MessageHandler::send_header(socket, sender_id,
                                             MessageType::TX_EXECUTE_READ_PLAN,
                                             total))
                return false;
            if (!send_all_fd(socket, hdr.data(), hdr.size())) return false;
            const bool sok = send_all_fd(socket, cs.out_.data(), cs.out_.size());
            if (sthp) {
              const uint64_t t_send_end = sthp_now();
              auto ms = [](uint64_t ns){ return ns / 1000000.0; };
              std::fprintf(stderr,
                  "[STIMEPROF] xmit lz4 enc=%.1fms send=%.1fms raw=%.2fMB "
                  "compressed=%.2fMB\n",
                  ms(t_enc_end - t_enc_start), ms(t_send_end - t_enc_end),
                  cs.raw_total_ / 1024.0 / 1024.0,
                  cs.out_.size() / 1024.0 / 1024.0);
              std::fflush(stderr);
            }
            return sok;
        }
        // compression failed -> fall through to raw
    }
    // RAW: [0x00] + streamed flat.
    const uint64_t t_size_start = sthp ? sthp_now() : 0;
    const uint64_t sz = 1 + helios_flat::flat_size(response);
    const uint64_t t_size_end = sthp ? sthp_now() : 0;
    if (!MessageHandler::send_header(socket, sender_id,
                                     MessageType::TX_EXECUTE_READ_PLAN, sz)) {
        return false;
    }
    const char codec_raw = static_cast<char>(helios_zip::kRaw);
    if (!send_all_fd(socket, &codec_raw, 1)) return false;
    SocketSink sink(socket);
    helios_flat::encode_response_into(response, sink);
    const bool fok = sink.finish();
    if (sthp) {
      const uint64_t t_send_end = sthp_now();
      auto ms = [](uint64_t ns){ return ns / 1000000.0; };
      std::fprintf(stderr,
          "[STIMEPROF] xmit raw size_walk=%.1fms send=%.1fms bytes=%.2fMB\n",
          ms(t_size_end - t_size_start), ms(t_send_end - t_size_end),
          (sz - 1) / 1024.0 / 1024.0);
      std::fflush(stderr);
    }
    return fok;
}

void LineairDBRpc::handleTxValidateAndCommit(const std::string& message,
                                             std::string& result) {
    // Codex Step 5/6 P2 fix: sweep stale TxOccState entries opportunistically.
    // The corresponding masstree pin sweeper runs every ~40ms via
    // MasstreeAdvanceEpoch; this complements it so server-side filtered_rows
    // don't leak if no commits arrive for a while.
    {
        using clk = std::chrono::steady_clock;
        const auto now_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch())
                .count();
        helios::GlobalTxOccStore().SweepExpired(static_cast<std::uint64_t>(now_ns));
    }
    LineairDB::Protocol::TxValidateAndCommit::Request request;
    LineairDB::Protocol::TxValidateAndCommit::Response response;

    request.ParseFromString(message);

    std::vector<LineairDB::ExternalReadEntry> reads;
    reads.reserve(request.reads_size());
    for (const auto& read : request.reads()) {
        reads.push_back({read.table_name(), read.key(), read.tid(),
                         read.found()});
    }

    std::vector<LineairDB::ExternalIndexValidationEntry> index_reads;
    index_reads.reserve(request.index_reads_size());
    for (const auto& read : request.index_reads()) {
        index_reads.push_back({read.table_name(), read.index_name(),
                               read.key(), read.tid(), read.found()});
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

    std::vector<LineairDB::ExternalRangeValidationEntry> range_reads;
    range_reads.reserve(request.range_reads_size());
    for (const auto& range : request.range_reads()) {
        LineairDB::ExternalRangeValidationEntry entry{
            range.table_name(), range.index_name(), range.owner_ptr(),
            range.node_ptr(), range.version()};
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

    // ---- helios PHYSICAL OCC (Step 6) commit merge ------------------------
    // When the proxy echoes a tx_occ_key, the server retained filtered_rows
    // server-side at prefetch time. Take them, validate the pin is still
    // alive (TTL / connection-close did not expire it), and append each as a
    // stateless point read so ValidateAndCommit re-checks its TID. The pin
    // is released regardless so masstree reclamation can resume. range/index
    // entries come on the wire as before (small in physical mode).
    std::uint64_t tx_occ_key = request.tx_occ_key();
    bool tx_occ_expired = false;
    bool tx_occ_lease_held = false;
    // helios Phase-6 range-hash OCC: retained footprint digests to re-validate
    // (read-only txns only; see use_range_hash). Moved out of the TxOccState
    // before it is consumed.
    std::vector<helios::TxOccState::RangeHash> range_hashes_to_check;
    const bool use_range_hash = request.use_range_hash();
    auto db = db_manager_->get_database();
    if (tx_occ_key != 0) {
        // Codex P1 #3 fix: atomically LEASE the pin so the TTL sweep cannot
        // reclaim leaves we are about to dereference. Failure = pin already
        // gone (TTL or close-hook) -> abort as expired.
        if (db && db->LeaseTxPinForValidation(tx_occ_key)) {
            tx_occ_lease_held = true;
        }
        auto state_opt = helios::GlobalTxOccStore().Take(tx_occ_key);
        if (!tx_occ_lease_held || !state_opt.has_value()) {
            tx_occ_expired = true;
        } else {
            auto& state = *state_opt;
            // table_name is carried per-row (FilteredRow.table_name).
            reads.reserve(reads.size() + state.filtered_rows.size());
            for (auto& fr : state.filtered_rows) {
                reads.push_back({fr.table_name, fr.key, fr.tid, true});
            }
            if (use_range_hash) {
                range_hashes_to_check = std::move(state.range_hashes);
            }
        }
    }

    // helios Phase-6: re-derive each retained primary full-range footprint
    // digest and compare to the prefetch capture. A mismatch means a value
    // update / insert / delete touched the range between prefetch and commit
    // → abort. This replaces the O(rows) per-row read set the proxy skipped
    // for read-only txns. Done before ValidateAndCommit; for a read-only txn
    // (no writes to install) there is no write-after-stale-read window, so the
    // re-scan need not be inside the locked phase (Codex 2026-05-29).
    bool range_hash_ok = true;
    std::string range_hash_abort;
    if (!tx_occ_expired && use_range_hash && db) {
        for (const auto& rh : range_hashes_to_check) {
            uint8_t cur[32];
            if (!db->ComputePrimaryRangeFootprintHash(
                    rh.table_name, rh.start_key, rh.end_key, rh.row_limit,
                    rh.reverse_scan, cur)) {
                range_hash_ok = false;
                range_hash_abort = "range_hash_recompute_failed";
                break;
            }
            if (std::memcmp(cur, rh.root, 32) != 0) {
                range_hash_ok = false;
                range_hash_abort = "range_hash_mismatch";
                break;
            }
        }
    }

    std::string abort_reason;
    const bool committed = !tx_occ_expired && range_hash_ok &&
        db_manager_->get_database()->ValidateAndCommit(reads, writes, si_ops,
                                                       range_reads,
                                                       index_reads,
                                                       &abort_reason);
    if (!range_hash_ok && abort_reason.empty()) abort_reason = range_hash_abort;
    // Codex P1 #2 fix: release the pin AFTER ValidateAndCommit (which is
    // when node_ptrs covered by the pin are dereferenced). Drop the lease
    // first so subsequent sweep can reclaim.
    if (tx_occ_lease_held && db) {
        db->DropTxPinValidationLease(tx_occ_key);
        db->ReleaseTxEpochPin(tx_occ_key);
    }
    if (tx_occ_expired) abort_reason = "tx_occ_key expired";
    response.set_committed(committed);
    if (!committed && !abort_reason.empty()) {
        response.set_abort_reason(abort_reason);
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

void LineairDBRpc::handleTxWrite(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWrite");

    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& value_str = request.value();
        tx->Write(request.key(), reinterpret_cast<const std::byte*>(value_str.c_str()), value_str.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("Wrote key '%s' to transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for write: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxDelete(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxDelete");

    LineairDB::Protocol::TxDelete::Request request;
    LineairDB::Protocol::TxDelete::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        tx->Delete(request.key());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("Deleted key '%s' from transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for delete: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxReadSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxReadSecondaryIndex");

    LineairDB::Protocol::TxReadSecondaryIndex::Request request;
    LineairDB::Protocol::TxReadSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        auto results = tx->ReadSecondaryIndex(request.index_name(), request.secondary_key());
        response.set_is_aborted(tx->IsAborted());

        for (const auto& [ptr, size] : results) {
            std::string value(reinterpret_cast<const char*>(ptr), size);
            response.add_values(value);
        }
        LOG_DEBUG("ReadSecondaryIndex index='%s' key='%s' tx=%ld: %d values",
                  request.index_name().c_str(), request.secondary_key().c_str(), tx_id, response.values_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for read_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxWriteSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWriteSecondaryIndex");

    LineairDB::Protocol::TxWriteSecondaryIndex::Request request;
    LineairDB::Protocol::TxWriteSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->WriteSecondaryIndex(request.index_name(), request.secondary_key(),
                                reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for write_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxDeleteSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxDeleteSecondaryIndex");

    LineairDB::Protocol::TxDeleteSecondaryIndex::Request request;
    LineairDB::Protocol::TxDeleteSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->DeleteSecondaryIndex(request.index_name(), request.secondary_key(),
                                 reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("DeleteSecondaryIndex index='%s' key='%s' tx=%ld",
                  request.index_name().c_str(), request.secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for delete_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxUpdateSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxUpdateSecondaryIndex");

    LineairDB::Protocol::TxUpdateSecondaryIndex::Request request;
    LineairDB::Protocol::TxUpdateSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->UpdateSecondaryIndex(request.index_name(),
                                 request.old_secondary_key(), request.new_secondary_key(),
                                 reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("UpdateSecondaryIndex index='%s' old='%s' new='%s' tx=%ld",
                  request.index_name().c_str(), request.old_secondary_key().c_str(),
                  request.new_secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for update_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingKeysInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysInRange");

    LineairDB::Protocol::TxGetMatchingKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingKeysInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        auto scan_result = tx->Scan(
            start_key, end_opt, [&response](auto key, auto) {
                response.add_keys(std::string(key));
                return false;
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
        }
        LOG_DEBUG("GetMatchingKeysInRange tx=%ld: %d keys", tx_id, response.keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_keys_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingKeysAndValuesInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysAndValuesInRange");

    LineairDB::Protocol::TxGetMatchingKeysAndValuesInRange::Request request;
    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);

    // Respond with flat binary instead of protobuf to avoid per-entry overhead.
    // Format: [is_aborted:1B] [key_len:4B][key][val_len:4B][val]... [sentinel:key_len=0]
    result.clear();
    result.reserve(4096);
    result.push_back(0);   // is_aborted placeholder (updated after Scan completes)

    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();
        const uint64_t row_limit = request.row_limit();
        const bool reverse_scan = request.reverse_scan();
        // Count rows actually returned after tombstone and predicate checks.
        uint64_t returned_rows = 0;

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        // Predicate pushdown: prepare filter if present
        const bool has_filter = request.has_filter() && request.filter().has_expr();
        const auto* filter_expr = has_filter ? &request.filter().expr() : nullptr;
        uint32_t filter_num_cols = has_filter ? request.filter().num_columns() : 0;
        PredicateEvaluator evaluator;

        // Scan callback: value is pair<const void*, size_t> from LineairDB
        auto append_matching_row =
            [&result, row_limit, &returned_rows, filter_expr, filter_num_cols,
             &evaluator](auto key, auto value) {
                // Skip tombstones (deleted rows)
                if (value.first == nullptr || value.second == 0) { return false; }
                // Predicate pushdown: evaluate filter if present
                if (filter_expr) {
                    if (evaluator.parse_row(static_cast<const char*>(value.first),
                                            value.second, filter_num_cols)) {
                        if (!evaluator.evaluate(*filter_expr)) {
                            return false;  // filter rejected → skip row, continue scanning
                        }
                    } else {
                        // Return parse-failed rows for MySQL to check, but do
                        // not count them toward a pushed LIMIT.
                        uint32_t klen = static_cast<uint32_t>(key.size());
                        uint32_t vlen = static_cast<uint32_t>(value.second);
                        result.append(reinterpret_cast<const char*>(&klen), 4);
                        result.append(key.data(), key.size());
                        result.append(reinterpret_cast<const char*>(&vlen), 4);
                        result.append(static_cast<const char*>(value.first), value.second);
                        return false;
                    }
                }
                // Append key-value entry in flat binary format
                uint32_t klen = static_cast<uint32_t>(key.size());
                uint32_t vlen = static_cast<uint32_t>(value.second);
                result.append(reinterpret_cast<const char*>(&klen), 4);
                result.append(key.data(), key.size());
                result.append(reinterpret_cast<const char*>(&vlen), 4);
                result.append(static_cast<const char*>(value.first), value.second);
                returned_rows++;
                // Stop the LineairDB scan once the pushed LIMIT is satisfied.
                return row_limit > 0 && returned_rows >= row_limit;
            };

        std::optional<size_t> scan_result;
        if (reverse_scan) {
            scan_result = tx->ScanReverse(start_key, end_opt, append_matching_row);
        } else {
            scan_result = tx->Scan(start_key, end_opt, append_matching_row);
        }

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            result[0] = 1;  // update is_aborted placeholder
        } else if (tx->IsAborted()) {
            result[0] = 1;
        }
    } else {
        result[0] = 1;
        LOG_WARNING("Transaction not found for get_matching_keys_and_values_in_range: %ld", tx_id);
    }

    uint32_t sentinel = 0;
    result.append(reinterpret_cast<const char*>(&sentinel), 4);  // sentinel: key_len=0 marks end of entries
}

void LineairDBRpc::handleTxGetMatchingKeysAndValuesFromPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysAndValuesFromPrefix");

    LineairDB::Protocol::TxGetMatchingKeysAndValuesFromPrefix::Request request;
    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);

    // Same flat binary format as handleTxGetMatchingKeysAndValuesInRange
    result.clear();
    result.reserve(4096);
    result.push_back(0);   // is_aborted placeholder

    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string prefix = request.prefix();
        bool first_key_checked = false;
        bool prefix_miss = false;

        // Predicate pushdown: prepare filter if present
        const bool has_filter = request.has_filter() && request.filter().has_expr();
        const auto* filter_expr = has_filter ? &request.filter().expr() : nullptr;
        uint32_t filter_num_cols = has_filter ? request.filter().num_columns() : 0;
        PredicateEvaluator evaluator;

        // Scan callback: value is pair<const void*, size_t> from LineairDB
        auto scan_result = tx->Scan(
            prefix, std::nullopt,
            [&result, &first_key_checked, &prefix_miss, &prefix,
             filter_expr, filter_num_cols, &evaluator, this](auto key, auto value) {
                // Check if first key matches the prefix; if not, abort scan early
                if (!first_key_checked) {
                    first_key_checked = true;
                    std::string key_str(key);
                    if (!key_prefix_is_matching(prefix, key_str)) { prefix_miss = true; return true; }
                }
                // Skip tombstones (deleted rows)
                if (value.first == nullptr || value.second == 0) { return false; }
                // Predicate pushdown: evaluate filter if present
                if (filter_expr) {
                    if (evaluator.parse_row(static_cast<const char*>(value.first),
                                            value.second, filter_num_cols)) {
                        if (!evaluator.evaluate(*filter_expr)) {
                            return false;  // filter rejected → skip row, continue scanning
                        }
                    }
                    // parse_row failure → include row (safe fallback)
                }
                // Append key-value entry in flat binary format
                uint32_t klen = static_cast<uint32_t>(key.size());
                uint32_t vlen = static_cast<uint32_t>(value.second);
                result.append(reinterpret_cast<const char*>(&klen), 4);
                result.append(key.data(), key.size());
                result.append(reinterpret_cast<const char*>(&vlen), 4);
                result.append(static_cast<const char*>(value.first), value.second);
                return false;  // continue scanning
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            result[0] = 1;
        } else if (tx->IsAborted()) {
            result[0] = 1;
        }
        if (prefix_miss) {
            // No matching keys found; discard entries, keep just header + sentinel
            result.resize(1);
        }
    } else {
        result[0] = 1;
        LOG_WARNING("Transaction not found for get_matching_keys_and_values_from_prefix: %ld", tx_id);
    }

    uint32_t sentinel = 0;
    result.append(reinterpret_cast<const char*>(&sentinel), 4);  // sentinel
}

void LineairDBRpc::handleTxFetchLastKeyInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastKeyInRange");

    LineairDB::Protocol::TxFetchLastKeyInRange::Request request;
    LineairDB::Protocol::TxFetchLastKeyInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        std::optional<std::string> result;
        auto scan_result = tx->ScanReverse(
            start_key, end_opt, [&result](auto key, auto) {
                result = std::string(key);
                return true;
            });

        // Phantom detection: if ScanReverse returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchLastKeyInRange tx=%ld: found=%s", tx_id, result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_key_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchFirstKeyWithPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchFirstKeyWithPrefix");

    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string prefix = request.prefix();
        std::string prefix_end = request.prefix_end();

        std::optional<std::string_view> end_opt;
        if (!prefix_end.empty()) { end_opt = prefix_end; }

        std::optional<std::string> result;
        auto scan_result = tx->Scan(
            prefix, end_opt, [&result, &prefix_end](auto key, auto value) {
                if (!prefix_end.empty() && key == prefix_end) {
                    return true; // exclusive end
                }
                // Skip tombstones
                if (value.first == nullptr || value.second == 0) {
                    return false; // Continue scanning
                }
                result = std::string(key);
                return true; // Stop after first valid key
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchFirstKeyWithPrefix tx=%ld prefix='%s': found=%s",
                  tx_id, prefix.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_first_key_with_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchNextKeyWithPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchNextKeyWithPrefix");

    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string last_key = request.last_key();
        std::string prefix_end = request.prefix_end();
        bool skip_first = true;

        std::optional<std::string_view> end_opt;
        if (!prefix_end.empty()) { end_opt = prefix_end; }

        std::optional<std::string> result;
        auto scan_result = tx->Scan(
            last_key, end_opt,
            [&result, &skip_first, &last_key, &prefix_end](auto key, auto value) {
                // Skip the last_key itself (we want the next one)
                if (skip_first && key == last_key) {
                    skip_first = false;
                    return false; // Continue scanning
                }
                if (!prefix_end.empty() && key == prefix_end) {
                    return true; // exclusive end
                }
                // Skip tombstones
                if (value.first == nullptr || value.second == 0) {
                    return false; // Continue scanning
                }
                result = std::string(key);
                return true; // Stop after first valid key
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchNextKeyWithPrefix tx=%ld last_key='%s': found=%s",
                  tx_id, last_key.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_next_key_with_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingPrimaryKeysInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingPrimaryKeysInRange");

    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        auto scan_result = tx->ScanSecondaryIndex(
            index_name, start_key, end_opt,
            [&response]([[maybe_unused]] std::string_view secondary_key,
                        const std::vector<std::string>& primary_keys) {
                for (const auto& pk : primary_keys) { response.add_primary_keys(pk); }
                return false;
            });

        // Phantom detection: ScanSecondaryIndex returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
        }
        LOG_DEBUG("GetMatchingPrimaryKeysInRange tx=%ld index='%s': %d keys",
                  tx_id, index_name.c_str(), response.primary_keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_primary_keys_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingPrimaryKeysFromPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingPrimaryKeysFromPrefix");

    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string prefix = request.prefix();
        bool first_key_checked = false;
        bool prefix_miss = false;

        auto scan_result = tx->ScanSecondaryIndex(
            index_name, prefix, std::nullopt,
            [&response, &first_key_checked, &prefix_miss, &prefix, this]
            (std::string_view secondary_key, const std::vector<std::string>& primary_keys) {
                if (!first_key_checked) {
                    first_key_checked = true;
                    std::string key_str(secondary_key);
                    if (!key_prefix_is_matching(prefix, key_str)) { prefix_miss = true; return true; }
                }
                for (const auto& pk : primary_keys) { response.add_primary_keys(pk); }
                return false;
            });

        // Phantom detection: ScanSecondaryIndex returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (prefix_miss) { response.clear_primary_keys(); }
        }
        LOG_DEBUG("GetMatchingPrimaryKeysFromPrefix tx=%ld index='%s' prefix='%s': %d keys",
                  tx_id, index_name.c_str(), prefix.c_str(), response.primary_keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_primary_keys_from_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchLastPrimaryKeyInSecondaryRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastPrimaryKeyInSecondaryRange");

    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Request request;
    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        std::optional<std::string> result;
        auto scan_result = tx->ScanSecondaryIndexReverse(
            index_name, start_key, end_opt,
            [&result]([[maybe_unused]] std::string_view secondary_key,
                      const std::vector<std::string>& primary_keys) {
                if (primary_keys.empty()) { return false; }
                result = primary_keys.back();
                return true;
            });

        // Phantom detection: ScanSecondaryIndexReverse returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_primary_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchLastPrimaryKeyInSecondaryRange tx=%ld index='%s': found=%s",
                  tx_id, index_name.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_primary_key_in_secondary_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchLastSecondaryEntryInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastSecondaryEntryInRange");

    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Request request;
    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        bool found = false;
        auto scan_result = tx->ScanSecondaryIndexReverse(
            index_name, start_key, end_opt,
            [&response, &found](std::string_view secondary_key,
                                const std::vector<std::string>& primary_keys) {
                if (primary_keys.empty()) { return false; }
                found = true;
                auto* entry = response.mutable_entry();
                entry->set_secondary_key(std::string(secondary_key));
                for (const auto& pk : primary_keys) { entry->add_primary_keys(pk); }
                return true;
            });

        // Phantom detection: ScanSecondaryIndexReverse returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            response.set_found(found);
        }
        LOG_DEBUG("FetchLastSecondaryEntryInRange tx=%ld index='%s': found=%s",
                  tx_id, index_name.c_str(), found ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_secondary_entry_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}
void LineairDBRpc::handleDbFence(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbFence");

    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;

    request.ParseFromString(message);

    db_manager_->get_database()->Fence();
    LOG_DEBUG("Database fence completed");

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbEndTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbEndTransaction");

    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool fence = request.fence();
        bool committed = db_manager_->get_database()->EndTransaction(
            *tx, [fence, tx_id](LineairDB::TxStatus status) {
                LOG_DEBUG("Transaction %ld ended with status: %d, fence=%s", tx_id, static_cast<int>(status), fence ? "true" : "false");
            });
        bool aborted = !committed;
        response.set_is_aborted(aborted);
        tx_manager_->remove_transaction(tx_id);

        // Apply row-count deltas on successful commit
        if (committed && request.row_deltas_size() > 0) {
            row_counts_->apply_deltas(request.row_deltas());
        }

        LOG_DEBUG("Ended transaction %ld with fence=%s (committed=%s)", tx_id, fence ? "true" : "false", committed ? "true" : "false");
        // Piggyback updated table row counts for the proxy's next transaction.
        for (const auto& [name, count] : row_counts_->snapshot()) {
            auto* ts = response.add_table_stats();
            ts->set_table_name(name);
            ts->set_row_count(count);
        }
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for end: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateTable(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbCreateTable");

    LineairDB::Protocol::DbCreateTable::Request request;
    LineairDB::Protocol::DbCreateTable::Response response;

    request.ParseFromString(message);

    bool success = db_manager_->get_database()->CreateTable(request.table_name());
    response.set_success(success);
    LOG_DEBUG("CreateTable '%s': %s", request.table_name().c_str(), success ? "success" : "already exists");

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbSetTable(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbSetTable");

    LineairDB::Protocol::DbSetTable::Request request;
    LineairDB::Protocol::DbSetTable::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool success = tx->SetTable(request.table_name());
        response.set_success(success);
        LOG_DEBUG("SetTable '%s' for tx=%ld: %s", request.table_name().c_str(), tx_id, success ? "success" : "failed");
    } else {
        response.set_success(false);
        LOG_WARNING("Transaction not found for set_table: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbCreateSecondaryIndex");

    LineairDB::Protocol::DbCreateSecondaryIndex::Request request;
    LineairDB::Protocol::DbCreateSecondaryIndex::Response response;

    request.ParseFromString(message);

    bool success = db_manager_->get_database()->CreateSecondaryIndex(
        request.table_name(), request.index_name(), request.index_type());
    response.set_success(success);

    result = response.SerializeAsString();
}
