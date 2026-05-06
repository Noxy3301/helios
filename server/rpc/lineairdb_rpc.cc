#include "lineairdb_rpc.hh"
#include "predicate_evaluator.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>
#include <cstring>

#include "lineairdb.pb.h"

LineairDBRpc::LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager,
                           std::shared_ptr<TableRowCounts> row_counts)
    : db_manager_(db_manager), tx_manager_(tx_manager), row_counts_(row_counts) {
}

void LineairDBRpc::handle_rpc(uint64_t sender_id, MessageType message_type,
                             const std::string& message, std::string& result) {
    LOG_DEBUG("Handling RPC: message_type=%u", static_cast<uint32_t>(message_type));

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
        auto scan_result = tx->Scan(
            start_key, end_opt, [&result, row_limit, &returned_rows,
                                  filter_expr, filter_num_cols, &evaluator](auto key, auto value) {
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
                returned_rows++;
                // Stop the LineairDB scan once the pushed LIMIT is satisfied.
                return row_limit > 0 && returned_rows >= row_limit;
            });

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
