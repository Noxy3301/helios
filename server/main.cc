#include <iostream>

#include "lineairdb_server.hh"
#include "../common/log.h"

int main(int argc, char** argv) {
    LOG_INFO("Starting LineairDB server...");
    
    LineairDBServer server;
    server.init();
    if (!server.run()) {  // Start listening
        LOG_ERROR("The server could not listen; exiting");
        return 1;
    }
    
    return 0;
}
