#include "database_manager.hh"
#include "../../common/log.h"
#include "rpc/duckdb_bridge_executor.hh"
#include "server_config.hh"

#include <system_error>

DatabaseManager::DatabaseManager() {
    helios::storage::Config conf;
    conf.epoch_duration_ms          = config().epoch_duration_ms;
    conf.wal_initial_capacity_bytes = config().wal_initial_capacity_bytes;
    conf.checkpoint_interval_ms     = config().checkpoint_interval_ms;
    conf.checkpoint_once_after_ms   = config().checkpoint_once_after_ms;
    conf.enable_recovery            = config().enable_recovery;
    set_commit_durability(config().commit_durability == "async"
                              ? helios::storage::CommitDurability::kAsync
                              : helios::storage::CommitDurability::kSync);
    duckdb_bridge::ConfigureLimits();
    LOG_INFO("Epoch %zu ms, durability %s, WAL capacity %llu bytes, recovery %s",
             conf.epoch_duration_ms, config().commit_durability.c_str(),
             static_cast<unsigned long long>(conf.wal_initial_capacity_bytes),
             conf.enable_recovery ? "on" : "off");
    if (conf.checkpoint_interval_ms != 0 || conf.checkpoint_once_after_ms != 0) {
        LOG_INFO("Checkpoint image: every %zu ms, one after %zu ms",
                 conf.checkpoint_interval_ms, conf.checkpoint_once_after_ms);
    }
    try {
        database_ = std::make_shared<helios::storage::Database>(conf);
    } catch (const std::system_error& err) {
        LOG_FATAL("Could not open the working directory '%s': %s",
                  conf.work_dir.c_str(), err.what());
    }
    LOG_INFO("Database manager initialized");
}
