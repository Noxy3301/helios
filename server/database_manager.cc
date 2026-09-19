#include "database_manager.hh"
#include "rpc/duckdb_bridge_executor.hh"
#include "server_config.hh"

#include <spdlog/spdlog.h>

#include <cstdlib>
#include <system_error>

DatabaseManager::DatabaseManager() {
    helios::storage::Config conf;
    conf.work_dir                   = config().datadir;
    conf.epoch_duration_ms          = config().epoch_duration_ms;
    conf.wal_initial_capacity_bytes = config().wal_initial_capacity_bytes;
    conf.checkpoint_interval_ms     = config().checkpoint_interval_ms;
    conf.checkpoint_once_after_ms   = config().checkpoint_once_after_ms;
    conf.enable_recovery            = config().enable_recovery;
    set_commit_durability(config().commit_durability == "async"
                              ? helios::storage::CommitDurability::kAsync
                              : helios::storage::CommitDurability::kSync);
    duckdb_bridge::ConfigureLimits();
    // The startup contract on its own line: bench and deploy scripts grep it.
    SPDLOG_INFO("Commit durability: {}", config().commit_durability);
    SPDLOG_INFO("Epoch {} ms, WAL capacity {} bytes, recovery {}",
             conf.epoch_duration_ms,
             static_cast<unsigned long long>(conf.wal_initial_capacity_bytes),
             conf.enable_recovery ? "on" : "off");
    if (conf.checkpoint_interval_ms != 0 || conf.checkpoint_once_after_ms != 0) {
        SPDLOG_INFO("Checkpoint image: every {} ms, one after {} ms",
                    conf.checkpoint_interval_ms, conf.checkpoint_once_after_ms);
    }
    try {
        database_ = std::make_shared<helios::storage::Database>(conf);
    } catch (const std::system_error& err) {
        SPDLOG_CRITICAL("Could not open the working directory '{}': {}",
                        conf.work_dir, err.what());
        std::exit(1);
    }
    SPDLOG_INFO("Database manager initialized");
}
