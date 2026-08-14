#include "lineairdb_server.hh"

#include <iostream>

#include "../common/log.h"
#include "lineairdb.pb.h"
#include "network/rpc_lane.hh"
#include "rpc/lineairdb_rpc.hh"
#include "rpc/rpc_budget.hh"

namespace {
// Installs `budget` as the thread's current budget for the guard's scope
// and always clears it back to null on the way out, including every early
// return inside handle_rpc.
struct ScopedRpcBudget {
  explicit ScopedRpcBudget(RpcExecutionBudget *budget) { set_current_rpc_budget(budget); }
  ~ScopedRpcBudget() { set_current_rpc_budget(nullptr); }
  ScopedRpcBudget(const ScopedRpcBudget &) = delete;
  ScopedRpcBudget &operator=(const ScopedRpcBudget &) = delete;
};
}  // namespace

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

RpcClassification LineairDBServer::classify_rpc(MessageType type, std::string_view payload) const {
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

bool LineairDBServer::Dispatcher::handle_fast_rpc(uint64_t sender_id, MessageType message_type,
                                                   std::string_view payload,
                                                   const google::protobuf::Message *parsed,
                                                   std::string &result) {
  if (message_type == MessageType::TX_EXECUTE_READ_PLAN) {
    RpcExecutionBudget budget{kFastReadPlanRowBudget, kFastReadPlanByteBudget};
    ScopedRpcBudget guard(&budget);
    if (parsed != nullptr) {
      // classify_rpc parses exactly the request type keyed by the opcode,
      // and the reactor passes the same opcode alongside, so this downcast
      // is safe by construction.
      rpc_->handle_read_plan(
          static_cast<const LineairDB::Protocol::TxExecuteReadPlan::Request &>(*parsed),
          payload.size(), result);
    } else {
      // Defensive fallback: never taken for kFast per classify_rpc's
      // contract (parsed is always non-null there).
      handle_rpc(sender_id, message_type, std::string(payload), result);
    }
    if (budget.exceeded) {
      result.clear();  // never sent; the caller re-dispatches this request
      return false;
    }
    return true;
  }

  if (message_type == MessageType::TX_VALIDATE_AND_COMMIT && parsed != nullptr) {
    rpc_->handle_validate_and_commit(
        static_cast<const LineairDB::Protocol::TxValidateAndCommit::Request &>(*parsed),
        payload.size(), result);
    return true;
  }

  handle_rpc(sender_id, message_type, std::string(payload), result);
  return true;
}
