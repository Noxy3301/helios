#include "lineairdb_rpc.hh"
#include "predicate_evaluator.hh"
#include "row_codec.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>
#include <cstdlib>
#include <utility>

#include "lineairdb.pb.h"
#include <map>
#include <set>

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
        // Transaction control
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

        // Stateless operations
        case MessageType::TX_STATELESS_READ:
            handleTxStatelessRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_BATCH_READ:
            handleTxStatelessBatchRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_VALIDATE_AND_COMMIT:
            handleTxValidateAndCommit(message, result);
            release_masstree_thread_epoch();
            return;

        // Read-plan execution
        case MessageType::TX_EXECUTE_READ_PLAN:
            handleTxExecuteReadPlan(message, result);
            release_masstree_thread_epoch();
            return;

        // Table statistics
        case MessageType::TX_GET_TABLE_STATS:
            handleTxGetTableStats(message, result);
            release_masstree_thread_epoch();
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
