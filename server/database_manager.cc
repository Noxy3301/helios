#include "database_manager.hh"
#include "rpc/olap_executor.hh"
#include "server_config.hh"

#include <spdlog/spdlog.h>

#include <cstdlib>
#include <system_error>

DatabaseManager::DatabaseManager() {
    helios::storage::Config conf;
    conf.work_dir                   = config().datadir;
    conf.epoch_duration_ms          = config().epoch_duration_ms;
    conf.wal_budget_bytes           = config().wal_budget_bytes;
    set_commit_durability(config().commit_durability == "async"
                              ? helios::storage::CommitDurability::kAsync
                              : helios::storage::CommitDurability::kSync);
    olap::configure_limits();
    // The startup contract on its own line: bench and deploy scripts grep it.
    SPDLOG_INFO("Commit durability: {}", config().commit_durability);
    SPDLOG_INFO("Epoch {} ms", conf.epoch_duration_ms);
    try {
        database_ = std::make_shared<helios::storage::Database>(conf);
    } catch (const std::system_error& err) {
        SPDLOG_CRITICAL("Could not open the working directory '{}': {}",
                        conf.work_dir, err.what());
        std::exit(1);
    }
    SPDLOG_INFO("Database manager initialized");
}
