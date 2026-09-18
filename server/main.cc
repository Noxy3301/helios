#include <csignal>
#include <iostream>

#include "helios_server.hh"
#include "../common/log.h"

int main(int argc, char** argv) {
    // A client that disconnects mid-response must not end the process.
    std::signal(SIGPIPE, SIG_IGN);

    LOG_INFO("Starting Helios server...");
    
    HeliosServer server;
    server.init();
    if (!server.run()) {  // Start listening
        LOG_ERROR("The server could not listen; exiting");
        return 1;
    }
    
    return 0;
}
