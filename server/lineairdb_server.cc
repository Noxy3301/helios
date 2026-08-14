#include "lineairdb_server.hh"

#include <iostream>

#include "../common/log.h"
#include "lineairdb.pb.h"
#include "network/rpc_lane.hh"
#include "rpc/lineairdb_rpc.hh"

LineairDBServer::LineairDBServer() : TcpServer(9999) {}

void LineairDBServer::init() {
  // Initialize components in dependency order
  if (!db_manager_) {
    db_manager_ = std::make_shared<DatabaseManager>();
  }

  LOG_INFO("LineairDB server initialized successfully");
}

void LineairDBServer::handle_client(int client_socket, std::string primer) {
  LOG_INFO("Handling client connection fd=%d%s", client_socket,
           primer.empty() ? "" : " (migrated from reactor)");
  // Per-connection managers
  auto tx_manager = std::make_shared<TransactionManager>(*db_manager_->get_database());
  auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager, row_counts_);

  // A legacy fresh accept passes an empty primer, and a null primer pointer
  // reads exclusively from the socket. Only a reactor migration hand-off
  // supplies a non-empty primer.
  std::string *primer_ptr = primer.empty() ? nullptr : &primer;
  size_t primer_offset = 0;

  while (true) {
    uint64_t sender_id;
    MessageType message_type;
    std::string payload;

    if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload,
                                          primer_ptr, &primer_offset)) {
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

std::shared_ptr<Reactor::RpcDispatcher> LineairDBServer::create_dispatcher() {
  return std::make_shared<Dispatcher>(db_manager_, row_counts_);
}

RpcLane LineairDBServer::classify_rpc(MessageType type, std::string_view payload) const {
  return ::classify_rpc(type, payload);
}

LineairDBServer::Dispatcher::Dispatcher(std::shared_ptr<DatabaseManager> db_manager,
                                         std::shared_ptr<TableRowCounts> row_counts)
    : tx_manager_(std::make_shared<TransactionManager>(*db_manager->get_database())),
      rpc_(std::make_shared<LineairDBRpc>(db_manager, tx_manager_, row_counts)) {}

void LineairDBServer::Dispatcher::handle_rpc(uint64_t sender_id, MessageType message_type,
                                              const std::string &payload, std::string &result) {
  rpc_->handle_rpc(sender_id, message_type, payload, result);
}
