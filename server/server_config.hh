#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

/**
 * @brief Every knob the server takes at startup, with its built-in default.
 *
 * @details The defaults are what the server runs under when no file is given;
 * helios.cnf documents the same set under [helios-storage].
 */
struct ServerConfig {
    /// TCP port the RPC listener binds.
    uint16_t server_port = 9999;
    /// How often the global epoch advances, in milliseconds.
    size_t epoch_duration_ms = 40;
    /// The contract every commit runs under: "sync" or "async".
    std::string commit_durability = "sync";
    /// Whether the log is replayed at startup.
    bool enable_recovery = false;
    /// How much of the log is written out and made writable in place at a time.
    uint64_t wal_initial_capacity_bytes = 64ull * 1024ull * 1024ull;
    /// Milliseconds between checkpoints; zero writes none.
    size_t checkpoint_interval_ms = 0;
    /// One checkpoint this many milliseconds after startup; zero writes none.
    size_t checkpoint_once_after_ms = 0;
    /// DuckDB thread pool size; zero takes a quarter of the hardware threads.
    uint64_t bridge_threads = 0;
    /// DuckDB memory bound, digits with an optional K, M or G suffix; empty
    /// takes DuckDB's own default.
    std::string bridge_mem_limit;
    /// Whether the analytical bridge writes its trace lines.
    bool bridge_debug = false;
    /// Upper bound on the read view's epoch-fence wait, in milliseconds.
    uint32_t read_view_fence_timeout_ms = 5000;
    /// The lowest level the log keeps: trace, debug, info, warning, error,
    /// critical or off.
    std::string log_level = "info";
};

/**
 * @brief The configuration this process runs under.
 */
const ServerConfig& config();

/**
 * @brief Reads the [helios-storage] section of `path` into that configuration; a later file overrides an earlier one.
 *
 * @details A file that cannot be opened, a key the server does not know, or a
 * value it cannot parse exits 1 with one line naming the key.
 */
void load_config(const char* path);

