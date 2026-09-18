#include <csignal>
#include <iostream>
#include <string_view>

#include "helios_server.hh"
#include "server_config.hh"
#include "../common/log.h"

int main(int argc, char** argv) {
    // A client that disconnects mid-response must not end the process.
    std::signal(SIGPIPE, SIG_IGN);

    for (int i = 1; i < argc; i++) {
        const std::string_view arg(argv[i]);
        if (arg == "--config" && i + 1 < argc) {
            load_config(argv[++i]);
            continue;
        }
        LOG_FATAL("Usage: helios-storage [--config <path>]...");
    }

    LOG_INFO("Starting Helios server...");

    HeliosServer server;
    server.init();
    if (!server.run()) {  // Start listening
        LOG_ERROR("The server could not listen; exiting");
        return 1;
    }

    return 0;
}
