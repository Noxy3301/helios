#include "lineairdb_server.hh"

#include <iostream>

#include "../common/log.h"
#include "lineairdb.pb.h"
#include "rpc/lineairdb_rpc.hh"

LineairDBServer::LineairDBServer() : TcpServer(9999) {}

void LineairDBServer::init() {
  // Initialize components in dependency order
  if (!db_manager_) {
    db_manager_ = std::make_shared<DatabaseManager>();
  }

  LOG_INFO("LineairDB server initialized successfully");
}

void LineairDBServer::handle_client(int client_socket) {
  LOG_INFO("Handling client connection fd=%d", client_socket);
  // Per-connection managers
  auto tx_manager = std::make_shared<TransactionManager>(*db_manager_->get_database());
  auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager, row_counts_);

  while (true) {
    uint64_t sender_id;
    MessageType message_type;
    std::string payload;

    if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
      break;  // Client disconnected or error
    }

    std::string result;
    rpc_handler->handle_rpc(sender_id, message_type, payload, result);

    if (!MessageHandler::send_response_writev(client_socket, 0, message_type, result)) {
      break;  // Failed to send response
    }
  }

  // Must run on this thread: the epoch slot it releases is thread-local.
  tx_manager->abort_all_and_end();

  // An enrolled thread that exits pins min_active_epoch() forever.
  db_manager_->get_database()->ReleaseMasstreeThreadEpoch();
}
