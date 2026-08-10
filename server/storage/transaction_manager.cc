#include "transaction_manager.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>

TransactionManager::TransactionManager(LineairDB::Database& database)
    : database_(database), next_tx_id_(1) {}

int64_t TransactionManager::generate_tx_id() {
    return next_tx_id_.fetch_add(1);
}

void TransactionManager::store_transaction(int64_t tx_id, LineairDB::Transaction* tx) {
    transactions_[tx_id] = tx;
}

LineairDB::Transaction* TransactionManager::get_transaction(int64_t tx_id) {
    auto it = transactions_.find(tx_id);
    if (it == transactions_.end()) {
        LOG_WARNING("Transaction not found: %ld", tx_id);
        return nullptr;
    }
    return it->second;
}

void TransactionManager::remove_transaction(int64_t tx_id) {
    transactions_.erase(tx_id);
}

void TransactionManager::abort_all_and_end() {
    if (transactions_.empty()) return;

    // EndTransaction may delete the transaction object, so snapshot and clear first.
    std::vector<LineairDB::Transaction*> pending;
    pending.reserve(transactions_.size());
    for (const auto& [tx_id, tx] : transactions_) {
        (void)tx_id;
        pending.push_back(tx);
    }
    transactions_.clear();

    // EndTransaction attempts a commit unless the transaction is already aborted.
    for (auto* tx : pending) {
        tx->Abort();
    }

    LOG_WARNING("Connection dropped with %zu open transaction(s); aborting", pending.size());
    for (auto* tx : pending) {
        database_.EndTransaction(*tx, [](LineairDB::TxStatus) {});
    }
}
