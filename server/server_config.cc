#include "server_config.hh"

#include <spdlog/spdlog.h>

#include <charconv>
#include <cstdlib>
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
    SPDLOG_CRITICAL("Invalid configuration '{} = {}': expected {}", key, value,
                    expected);
    std::exit(1);
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

/// Digits with an optional K, M or G suffix, as a positive byte count.
bool parse_bytes(const std::string& value, uint64_t* out) {
    if (value.empty()) return false;
    uint64_t scale = 1;
    switch (value.back()) {
        case 'k':
        case 'K': scale = 1ull << 10; break;
        case 'm':
        case 'M': scale = 1ull << 20; break;
        case 'g':
        case 'G': scale = 1ull << 30; break;
        default: break;
    }
    const std::string digits =
        scale == 1 ? value : value.substr(0, value.size() - 1);
    if (!parse_number(digits, 1, UINT64_MAX / scale, out)) return false;
    *out *= scale;
    return true;
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
    if (key == "datadir") {
        if (value.empty()) refuse(key, value, "a directory path");
        g_config.datadir = value;
    } else if (key == "server_port") {
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
            refuse(key, value, "0 or 1, true or false, on or off");
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
        if (!parse_bytes(value, &g_config.bridge_mem_limit_bytes)) {
            refuse(key, value,
                   "a positive byte count with an optional K, M or G suffix");
        }
    } else if (key == "bridge_debug") {
        if (!parse_bool(value, &g_config.bridge_debug)) {
            refuse(key, value, "0 or 1, true or false, on or off");
        }
    } else if (key == "read_view_fence_timeout_ms") {
        if (!parse_number(value, 1, UINT32_MAX, &number)) {
            refuse(key, value, "a positive integer");
        }
        g_config.read_view_fence_timeout_ms = static_cast<uint32_t>(number);
    } else if (key == "log_level") {
        if (spdlog::level::from_str(value) == spdlog::level::off &&
            value != "off") {
            refuse(key, value, "a spdlog level name");
        }
        g_config.log_level = value;
    } else {
        SPDLOG_CRITICAL("Unknown configuration key '{}'", key);
        std::exit(1);
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
    if (!file) {
        SPDLOG_CRITICAL("Could not open the configuration file '{}'", path);
        std::exit(1);
    }

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
            SPDLOG_CRITICAL("Invalid configuration line '{}': expected key = value",
                            text);
            std::exit(1);
        }
        apply_key(trim(std::string_view(text).substr(0, separator)),
                  trim(std::string_view(text).substr(separator + 1)));
    }
    // A directory, or a read error mid-file, reaches here with eof unset.
    if (!file.eof()) {
        SPDLOG_CRITICAL("Could not read the configuration file '{}'", path);
        std::exit(1);
    }
}
