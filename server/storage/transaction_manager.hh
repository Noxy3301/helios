#pragma once

#include <unordered_map>
#include <atomic>
#include <cstdint>

#include "lineairdb/lineairdb.h"

class TransactionManager {
public:
    explicit TransactionManager(LineairDB::Database& database);
    ~TransactionManager() = default;

    int64_t generate_tx_id();
    void store_transaction(int64_t tx_id, LineairDB::Transaction* tx);
    LineairDB::Transaction* get_transaction(int64_t tx_id);
    void remove_transaction(int64_t tx_id);

    /**
     * @brief Aborts and ends every transaction still registered, releasing the
     * calling thread's epoch participation. A connection that disconnects
     * mid-transaction never sends DB_END_TRANSACTION, and the thread running
     * it exits right after; without this the thread's epoch slot stays online
     * forever and the global epoch stops advancing. Must be called on the same
     * thread that began the transactions.
     */
    void abort_all_and_end();

private:
    LineairDB::Database& database_;  // non-owning; outlives this manager
    std::unordered_map<int64_t, LineairDB::Transaction*> transactions_;
    std::atomic<int64_t> next_tx_id_;
};
