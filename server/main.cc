#include <iostream>

#include "../common/log.h"
#include "lineairdb_server.hh"
#include "rpc/rpc_timing.hh"

int main(int argc, char **argv) {
  LOG_INFO("Starting LineairDB server...");

  // No-op unless HELIOS_RPC_TIMING=1: installs a SIGTERM/SIGINT path that
  // writes the per-opcode RPC timing table to /tmp/helios_rpc_timing_<pid>.txt
  // and logs a summary before exiting.
  rpc_timing::install_shutdown_handler();

  LineairDBServer server;
  server.init();
  server.run();  // Start listening

  return 0;
}
