#include "database_manager.hh"
#include "../../common/log.h"

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

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
 * @brief Applies LINEAIRDB_WAL_INITIAL_CAPACITY_BYTES. The log is written out
 * with zeroes to this size at startup and records land in it in place, which
 * keeps a commit's fdatasync from also persisting a new file size. A run wants
 * the whole log to fit: extending is synchronous, and the flush it stalls is
 * one a Sync commit is waiting on.
 */
void configure_wal_capacity(LineairDB::Config& config) {
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
 * exactly. A checkpoint knob that silently fell back to its default would
 * leave a run labelled with a cadence it never had.
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
 * LINEAIRDB_CHECKPOINT_ONCE_AFTER_MS. The image is scanned while transactions
 * keep running and shortens the log replay at startup; zero, the default,
 * writes none.
 */
void configure_checkpoint(LineairDB::Config& config) {
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
    configure_wal_capacity(conf);
    configure_checkpoint(conf);
    conf.enable_checkpointing = false;
    conf.enable_recovery      = env_enabled("LINEAIRDB_ENABLE_RECOVERY");
    conf.max_thread           = 1;
    conf.concurrency_control_protocol = LineairDB::Config::ConcurrencyControl::Silo;
    conf.index_structure = LineairDB::Config::IndexStructure::Masstree;
    conf.enable_pax_storage = true;
    database_ = std::make_shared<LineairDB::Database>(conf);
    LOG_INFO("Database manager initialized");
}
