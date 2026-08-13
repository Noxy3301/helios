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

std::unique_ptr<Reactor::RpcDispatcher> LineairDBServer::create_dispatcher() {
  return std::make_unique<Dispatcher>(db_manager_, row_counts_);
}

bool LineairDBServer::is_reactor_fast_path(MessageType type) const {
  // Only the two stateless opcodes: a storage transaction opens and closes
  // within one handler call, so neither depends on state a prior RPC left
  // behind. TX_WRITE_SECONDARY_INDEX and TX_GET_MATCHING_KEYS_AND_VALUES_
  // FROM_PREFIX look self-contained but key off a stored transaction_id, so
  // they are conversational and must migrate, like every other opcode here.
  switch (type) {
    case MessageType::TX_VALIDATE_AND_COMMIT:
    case MessageType::TX_EXECUTE_READ_PLAN:
      return true;
    default:
      return false;
  }
}

LineairDBServer::Dispatcher::Dispatcher(std::shared_ptr<DatabaseManager> db_manager,
                                         std::shared_ptr<TableRowCounts> row_counts)
    : tx_manager_(std::make_shared<TransactionManager>(*db_manager->get_database())),
      rpc_(std::make_shared<LineairDBRpc>(db_manager, tx_manager_, row_counts)) {}

void LineairDBServer::Dispatcher::handle_rpc(uint64_t sender_id, MessageType message_type,
                                              const std::string &payload, std::string &result) {
  rpc_->handle_rpc(sender_id, message_type, payload, result);
}
