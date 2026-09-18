#pragma once

#include <atomic>
#include <memory>

#include "lineairdb/database.h"

class DatabaseManager {
public:
    DatabaseManager();
    ~DatabaseManager() = default;

    std::shared_ptr<helios::storage::Database> get_database() const { return database_; }

    // The mode every commit runs under; DB_SET_COMMIT_DURABILITY moves it.
    helios::storage::CommitDurability commit_durability() const {
        return commit_durability_.load(std::memory_order_relaxed);
    }
    void set_commit_durability(helios::storage::CommitDurability mode) {
        commit_durability_.store(mode, std::memory_order_relaxed);
    }

private:
    std::shared_ptr<helios::storage::Database> database_;
    std::atomic<helios::storage::CommitDurability> commit_durability_;
};
