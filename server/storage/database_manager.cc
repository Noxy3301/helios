#include "database_manager.hh"
#include "../../common/log.h"

#include <charconv>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace {

constexpr size_t kMinEpochDurationMs = 1;
constexpr size_t kMaxEpochDurationMs = 10000;

bool env_enabled(const char* name) {
    const char* value = std::getenv(name);
    return value != nullptr && value[0] != '\0' && value[0] != '0';
}

/**
 * @brief Applies LINEAIRDB_EPOCH_DURATION_MS, keeping the LineairDB default
 * when it is unset. The epoch window is the knob the durability sweep varies,
 * so a value that does not parse exactly is a startup error rather than a
 * silent fallback that would mislabel every measurement taken with it.
 */
void configure_epoch_duration(LineairDB::Config& config) {
    const char* raw = std::getenv("LINEAIRDB_EPOCH_DURATION_MS");
    if (raw == nullptr) return;

    const std::string_view input(raw);
    size_t parsed          = 0;
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), parsed, 10);
    const bool consumed_all = end == input.data() + input.size();
    if (input.empty() || error != std::errc{} || !consumed_all ||
        parsed < kMinEpochDurationMs || parsed > kMaxEpochDurationMs) {
        LOG_FATAL("Invalid LINEAIRDB_EPOCH_DURATION_MS='%s': expected an integer in [%zu,%zu]",
                  raw, kMinEpochDurationMs, kMaxEpochDurationMs);
    }
    config.epoch_duration_ms = parsed;
    LOG_INFO("Epoch duration set to %zu ms", parsed);
}

/**
 * @brief Applies LINEAIRDB_COMMIT_DURABILITY (volatile|async|sync, default
 * volatile rather than the library default). Anything else refuses startup: a
 * mapped alias or a defaulted typo would label a measurement with a contract
 * it did not run under.
 */
void configure_commit_durability(LineairDB::Config& config) {
    config.commit_durability = LineairDB::Config::CommitDurability::Volatile;

    const char* raw = std::getenv("LINEAIRDB_COMMIT_DURABILITY");
    if (raw == nullptr) {
        LOG_INFO("Commit durability: volatile (default)");
        return;
    }

    const std::string_view mode(raw);
    if (mode == "volatile") {
        config.commit_durability = LineairDB::Config::CommitDurability::Volatile;
    } else if (mode == "async") {
        config.commit_durability = LineairDB::Config::CommitDurability::Async;
    } else if (mode == "sync") {
        config.commit_durability = LineairDB::Config::CommitDurability::Sync;
    } else {
        LOG_FATAL("Invalid LINEAIRDB_COMMIT_DURABILITY='%s': expected one of volatile, async, sync",
                  raw);
    }
    LOG_INFO("Commit durability: %s", raw);
}

/**
 * @brief Warns when a retired durability variable is set and ignores it:
 * LINEAIRDB_COMMIT_DURABILITY alone decides the contract, and no combination
 * of the retired knobs expresses Async.
 */
void warn_about_retired_durability_env() {
    for (const char* name : {"LINEAIRDB_ENABLE_LOGGING", "LINEAIRDB_LOG_FSYNC"}) {
        if (std::getenv(name) == nullptr) continue;
        LOG_WARNING("%s is retired and ignored; use LINEAIRDB_COMMIT_DURABILITY", name);
    }
}

}  // namespace

DatabaseManager::DatabaseManager() {
    // Initialize lineairdb
    // TODO: make configurable
    LineairDB::Config conf;
    warn_about_retired_durability_env();
    configure_epoch_duration(conf);
    configure_commit_durability(conf);
    conf.enable_checkpointing = false;
    conf.enable_recovery      = env_enabled("LINEAIRDB_ENABLE_RECOVERY");
    conf.max_thread           = 1;
    conf.concurrency_control_protocol = LineairDB::Config::ConcurrencyControl::Silo;
    conf.index_structure = LineairDB::Config::IndexStructure::Masstree;
    conf.enable_pax_storage = true;
    database_ = std::make_shared<LineairDB::Database>(conf);
    LOG_INFO("Database manager initialized");
}
