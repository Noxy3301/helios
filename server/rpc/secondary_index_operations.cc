#include "lineairdb_rpc.hh"

#include <cstddef>
#include <cstdint>
#include <string>

#include "../../common/log.h"
#include "lineairdb.pb.h"

// Secondary-index entry read/write/delete/update RPC handlers. Range scan and
// fetch handlers stay with the ordered scan code.

void LineairDBRpc::handleTxReadSecondaryIndex(const std::string& message,
                                              std::string& result) {
    LOG_DEBUG("Handling TxReadSecondaryIndex");

    LineairDB::Protocol::TxReadSecondaryIndex::Request request;
    LineairDB::Protocol::TxReadSecondaryIndex::Response response;

    request.ParseFromString(message);

    const int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        auto results =
            tx->ReadSecondaryIndex(request.index_name(), request.secondary_key());
        response.set_is_aborted(tx->IsAborted());

        for (const auto& [ptr, size] : results) {
            std::string value(reinterpret_cast<const char*>(ptr), size);
            response.add_values(value);
        }
        LOG_DEBUG("ReadSecondaryIndex index='%s' key='%s' tx=%ld: %d values",
                  request.index_name().c_str(),
                  request.secondary_key().c_str(), tx_id,
                  response.values_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for read_secondary_index: %ld",
                    tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxWriteSecondaryIndex(const std::string& message,
                                               std::string& result) {
    LOG_DEBUG("Handling TxWriteSecondaryIndex");

    LineairDB::Protocol::TxWriteSecondaryIndex::Request request;
    LineairDB::Protocol::TxWriteSecondaryIndex::Response response;

    request.ParseFromString(message);

    const int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->WriteSecondaryIndex(
            request.index_name(), request.secondary_key(),
            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for write_secondary_index: %ld",
                    tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxDeleteSecondaryIndex(const std::string& message,
                                                std::string& result) {
    LOG_DEBUG("Handling TxDeleteSecondaryIndex");

    LineairDB::Protocol::TxDeleteSecondaryIndex::Request request;
    LineairDB::Protocol::TxDeleteSecondaryIndex::Response response;

    request.ParseFromString(message);

    const int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->DeleteSecondaryIndex(
            request.index_name(), request.secondary_key(),
            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("DeleteSecondaryIndex index='%s' key='%s' tx=%ld",
                  request.index_name().c_str(),
                  request.secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for delete_secondary_index: %ld",
                    tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxUpdateSecondaryIndex(const std::string& message,
                                                std::string& result) {
    LOG_DEBUG("Handling TxUpdateSecondaryIndex");

    LineairDB::Protocol::TxUpdateSecondaryIndex::Request request;
    LineairDB::Protocol::TxUpdateSecondaryIndex::Response response;

    request.ParseFromString(message);

    const int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->UpdateSecondaryIndex(
            request.index_name(), request.old_secondary_key(),
            request.new_secondary_key(),
            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("UpdateSecondaryIndex index='%s' old='%s' new='%s' tx=%ld",
                  request.index_name().c_str(),
                  request.old_secondary_key().c_str(),
                  request.new_secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for update_secondary_index: %ld",
                    tx_id);
    }

    result = response.SerializeAsString();
}
