#include "server_config.hh"
#include "../common/log.h"

#include <charconv>
#include <fstream>
#include <string_view>
#include <system_error>

namespace {

// A log larger than this is a mistyped value rather than an intended
// reservation: the space is written out at startup and occupied for the life
// of the process.
constexpr uint64_t kMaxWalCapacityBytes = 64ull * 1024ull * 1024ull * 1024ull;
// A day between images is already far past any run this serves.
constexpr uint64_t kMaxCheckpointMs = 24ull * 60ull * 60ull * 1000ull;

ServerConfig g_config;

[[noreturn]] void refuse(const std::string& key, const std::string& value,
                         const char* expected) {
    LOG_FATAL("Invalid configuration '%s = %s': expected %s", key.c_str(),
              value.c_str(), expected);
}

/// Digits only, within [lo, hi].
bool parse_number(const std::string& value, uint64_t lo, uint64_t hi,
                  uint64_t* out) {
    const std::string_view input(value);
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), *out, 10);
    if (input.empty() || error != std::errc{} ||
        end != input.data() + input.size()) {
        return false;
    }
    return *out >= lo && *out <= hi;
}

bool parse_bool(const std::string& value, bool* out) {
    if (value == "1" || value == "true" || value == "on") {
        *out = true;
    } else if (value == "0" || value == "false" || value == "off") {
        *out = false;
    } else {
        return false;
    }
    return true;
}

/// Applies one key, refusing an unknown name or an unparsable value.
void apply_key(const std::string& key, const std::string& value) {
    uint64_t number = 0;
    if (key == "server_port") {
        if (!parse_number(value, 1, 65535, &number)) {
            refuse(key, value, "an integer in [1,65535]");
        }
        g_config.server_port = static_cast<uint16_t>(number);
    } else if (key == "epoch_duration_ms") {
        if (!parse_number(value, 1, 10000, &number)) {
            refuse(key, value, "an integer in [1,10000]");
        }
        g_config.epoch_duration_ms = static_cast<size_t>(number);
    } else if (key == "commit_durability") {
        if (value != "sync" && value != "async") refuse(key, value, "sync or async");
        g_config.commit_durability = value;
    } else if (key == "enable_recovery") {
        if (!parse_bool(value, &g_config.enable_recovery)) {
            refuse(key, value, "0 or 1");
        }
    } else if (key == "wal_initial_capacity_bytes") {
        if (!parse_number(value, 0, kMaxWalCapacityBytes, &number)) {
            refuse(key, value, "an integer up to 64 GiB");
        }
        g_config.wal_initial_capacity_bytes = number;
    } else if (key == "checkpoint_interval_ms") {
        if (!parse_number(value, 0, kMaxCheckpointMs, &number)) {
            refuse(key, value, "an integer up to one day in milliseconds");
        }
        g_config.checkpoint_interval_ms = static_cast<size_t>(number);
    } else if (key == "checkpoint_once_after_ms") {
        if (!parse_number(value, 0, kMaxCheckpointMs, &number)) {
            refuse(key, value, "an integer up to one day in milliseconds");
        }
        g_config.checkpoint_once_after_ms = static_cast<size_t>(number);
    } else if (key == "bridge_threads") {
        if (!parse_number(value, 1, UINT32_MAX, &number)) {
            refuse(key, value, "a positive integer");
        }
        g_config.bridge_threads = number;
    } else if (key == "bridge_mem_limit") {
        // The byte count is parsed where DuckDB is configured.
        g_config.bridge_mem_limit = value;
    } else if (key == "bridge_debug") {
        if (!parse_bool(value, &g_config.bridge_debug)) {
            refuse(key, value, "0 or 1");
        }
    } else if (key == "read_view_fence_timeout_ms") {
        if (!parse_number(value, 1, UINT32_MAX, &number)) {
            refuse(key, value, "a positive integer");
        }
        g_config.read_view_fence_timeout_ms = static_cast<uint32_t>(number);
    } else {
        LOG_FATAL("Unknown configuration key '%s'", key.c_str());
    }
}

std::string trim(std::string_view text) {
    const auto first = text.find_first_not_of(" \t\r");
    if (first == std::string_view::npos) return {};
    const auto last = text.find_last_not_of(" \t\r");
    return std::string(text.substr(first, last - first + 1));
}

}  // namespace

const ServerConfig& config() { return g_config; }

void load_config(const char* path) {
    std::ifstream file(path);
    if (!file) LOG_FATAL("Could not open the configuration file '%s'", path);

    // Keys before any section header count as the server's own section.
    bool own_section = true;
    std::string line;
    while (std::getline(file, line)) {
        const std::string text = trim(line);
        if (text.empty() || text[0] == '#' || text[0] == ';') continue;
        if (text[0] == '[') {
            own_section = text == "[helios-storage]";
            continue;
        }
        if (!own_section) continue;
        const auto separator = text.find('=');
        if (separator == std::string::npos) {
            LOG_FATAL("Invalid configuration line '%s': expected key = value",
                      text.c_str());
        }
        apply_key(trim(std::string_view(text).substr(0, separator)),
                  trim(std::string_view(text).substr(separator + 1)));
    }
}
