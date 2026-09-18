#include <spdlog/spdlog.h>

#include <csignal>
#include <cstdlib>
#include <string_view>

#include "helios_server.hh"
#include "server_config.hh"

int main(int argc, char** argv) {
    // A client that disconnects mid-response must not end the process.
    std::signal(SIGPIPE, SIG_IGN);

    for (int i = 1; i < argc; i++) {
        const std::string_view arg(argv[i]);
        if (arg == "--config" && i + 1 < argc) {
            load_config(argv[++i]);
            continue;
        }
        SPDLOG_CRITICAL("Usage: helios-storage [--config <path>]...");
        std::exit(1);
    }

    spdlog::set_level(spdlog::level::from_str(config().log_level));

    SPDLOG_INFO("Starting Helios server...");

    HeliosServer server;
    server.init();
    if (!server.run()) {  // Start listening
        SPDLOG_ERROR("The server could not listen; exiting");
        return 1;
    }

    return 0;
}
