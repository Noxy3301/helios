#include "database_manager.hh"
#include "../../common/log.h"
#include "rpc/duckdb_bridge_executor.hh"

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <system_error>

namespace {

constexpr size_t kMinEpochDurationMs = 1;
constexpr size_t kMaxEpochDurationMs = 10000;
// A log larger than this is a mistyped value rather than an intended reservation:
// the space is written out at startup and occupied for the life of the process.
constexpr uint64_t kMaxWalCapacityBytes = 64ull * 1024ull * 1024ull * 1024ull;  // 64 GiB
// A day between images is already far past any run this serves; beyond it the
// value is a mistyped one rather than a cadence.
constexpr size_t kMaxCheckpointIntervalMs = 24ull * 60ull * 60ull * 1000ull;  // one day

bool env_enabled(const char* name) {
    const char* value = std::getenv(name);
    return value != nullptr && value[0] != '\0' && value[0] != '0';
}

/**
 * @brief Applies LINEAIRDB_EPOCH_DURATION_MS, keeping the storage default
 * when it is unset.
 */
void configure_epoch_duration(helios::storage::Config& config) {
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
 * @brief Reads LINEAIRDB_COMMIT_DURABILITY (async|sync, default sync) into
 * the mode the server passes to every commit.
 */
helios::storage::CommitDurability configure_commit_durability() {
    const char* raw = std::getenv("LINEAIRDB_COMMIT_DURABILITY");
    if (raw == nullptr) {
        LOG_INFO("Commit durability: sync (default)");
        return helios::storage::CommitDurability::kSync;
    }

    const std::string_view mode(raw);
    auto durability = helios::storage::CommitDurability::kSync;
    if (mode == "async") {
        durability = helios::storage::CommitDurability::kAsync;
    } else if (mode != "sync") {
        LOG_FATAL("Invalid LINEAIRDB_COMMIT_DURABILITY='%s': expected async or sync", raw);
    }
    LOG_INFO("Commit durability: %s", raw);
    return durability;
}

/**
 * @brief Applies LINEAIRDB_WAL_INITIAL_CAPACITY_BYTES, the size the log is
 * written out to before records land in it in place.
 */
void configure_wal_capacity(helios::storage::Config& config) {
    const char* raw = std::getenv("LINEAIRDB_WAL_INITIAL_CAPACITY_BYTES");
    if (raw == nullptr) return;

    const std::string_view input(raw);
    uint64_t parsed          = 0;
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), parsed, 10);
    const bool consumed_all = end == input.data() + input.size();
    if (input.empty() || error != std::errc{} || !consumed_all ||
        parsed > kMaxWalCapacityBytes) {
        LOG_FATAL("Invalid LINEAIRDB_WAL_INITIAL_CAPACITY_BYTES='%s': expected an integer in [0,%llu]",
                  raw, static_cast<unsigned long long>(kMaxWalCapacityBytes));
    }
    config.wal_initial_capacity_bytes = parsed;
    LOG_INFO("WAL initial capacity set to %llu bytes",
             static_cast<unsigned long long>(parsed));
}

/**
 * @brief Reads one millisecond count, refusing anything that does not parse
 * exactly.
 */
size_t parse_milliseconds(const char* name, const char* raw) {
    const std::string_view input(raw);
    size_t parsed           = 0;
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), parsed, 10);
    const bool consumed_all = end == input.data() + input.size();
    if (input.empty() || error != std::errc{} || !consumed_all ||
        parsed > kMaxCheckpointIntervalMs) {
        LOG_FATAL("Invalid %s='%s': expected an integer in [0,%zu]", name, raw,
                  kMaxCheckpointIntervalMs);
    }
    return parsed;
}

/**
 * @brief Applies LINEAIRDB_CHECKPOINT_INTERVAL_MS and
 * LINEAIRDB_CHECKPOINT_ONCE_AFTER_MS; zero, the default, writes no image.
 */
void configure_checkpoint(helios::storage::Config& config) {
    const char* interval = std::getenv("LINEAIRDB_CHECKPOINT_INTERVAL_MS");
    if (interval != nullptr) {
        config.checkpoint_interval_ms =
            parse_milliseconds("LINEAIRDB_CHECKPOINT_INTERVAL_MS", interval);
    }
    const char* once = std::getenv("LINEAIRDB_CHECKPOINT_ONCE_AFTER_MS");
    if (once != nullptr) {
        config.checkpoint_once_after_ms =
            parse_milliseconds("LINEAIRDB_CHECKPOINT_ONCE_AFTER_MS", once);
    }
    if (config.checkpoint_interval_ms == 0 && config.checkpoint_once_after_ms == 0) {
        return;
    }
    LOG_INFO("Checkpoint image: every %zu ms, one after %zu ms",
             config.checkpoint_interval_ms, config.checkpoint_once_after_ms);
}

}  // namespace

DatabaseManager::DatabaseManager() {
    helios::storage::Config conf;
    configure_epoch_duration(conf);
    configure_wal_capacity(conf);
    configure_checkpoint(conf);
    conf.enable_recovery = env_enabled("LINEAIRDB_ENABLE_RECOVERY");
    set_commit_durability(configure_commit_durability());
    duckdb_bridge::ConfigureLimits();
    try {
        database_ = std::make_shared<helios::storage::Database>(conf);
    } catch (const std::system_error& err) {
        LOG_FATAL("Could not open the working directory '%s': %s",
                  conf.work_dir.c_str(), err.what());
    }
    LOG_INFO("Database manager initialized");
}
