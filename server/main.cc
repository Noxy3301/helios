#include <iostream>

#include "../common/log.h"
#include "lineairdb_server.hh"

int main(int argc, char **argv) {
  LOG_INFO("Starting LineairDB server...");

  LineairDBServer server;
  server.init();
  server.run();  // Start listening

  return 0;
}
