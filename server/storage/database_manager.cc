#include "database_manager.hh"
#include "../../common/log.h"

#include <cstdlib>
#include <iostream>

namespace {

bool env_enabled(const char* name) {
    const char* value = std::getenv(name);
    return value != nullptr && value[0] != '\0' && value[0] != '0';
}

}  // namespace

DatabaseManager::DatabaseManager() {
    // Initialize lineairdb
    // TODO: make configurable
    LineairDB::Config conf;
    conf.enable_checkpointing = false;
    conf.enable_recovery      = env_enabled("LINEAIRDB_ENABLE_RECOVERY");
    conf.enable_logging       = env_enabled("LINEAIRDB_ENABLE_LOGGING");
    conf.max_thread           = 1;
    conf.concurrency_control_protocol = LineairDB::Config::ConcurrencyControl::Silo;
    conf.index_structure = LineairDB::Config::IndexStructure::Masstree;
    conf.enable_pax_storage = true;
    database_ = std::make_shared<LineairDB::Database>(conf);
    LOG_INFO("Database manager initialized");
}
