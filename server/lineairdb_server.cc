#include "lineairdb_server.hh"
#include "../common/log.h"
#include "lineairdb.pb.h"
#include "rpc/lineairdb_rpc.hh"
#include "rpc/tx_occ_store.hh"
#include "storage/database_manager.hh"
#include <lineairdb/database.h>

#include <iostream>

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
    auto tx_manager = std::make_shared<TransactionManager>();
    auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager, row_counts_);

    while (true) {
        uint64_t sender_id;
        MessageType message_type;
        std::string payload;

        if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
            break;  // Client disconnected or error
        }

        // (A) The read-plan response can be multi-GB; stream it flat directly to
        // the socket instead of building a full flat buffer on top of the proto.
        if (message_type == MessageType::TX_EXECUTE_READ_PLAN) {
            if (!rpc_handler->handleTxExecuteReadPlanStreamed(client_socket, 0, payload)) {
                break;
            }
            continue;
        }

        std::string result;
        rpc_handler->handle_rpc(sender_id, message_type, payload, result);

        if (!MessageHandler::send_response_writev(client_socket, 0, message_type, result)) {
            break;  // Failed to send response
        }
    }
    // helios 2-RPC OCC: connection-close cleanup. Release any per-tx OCC
    // state still owned by this client (proxy crash / network drop after
    // prefetch but before commit). Each released TxKey also frees its
    // masstree epoch pin so reclamation can resume. In Step 2 the store
    // is empty (no Insert callers yet), so this is a no-op.
    auto released =
        helios::GlobalTxOccStore().ReleaseConnection(client_socket);
    if (!released.empty() && db_manager_) {
        if (auto db = db_manager_->get_database()) {
            for (auto k : released) db->ReleaseTxEpochPin(k);
            LOG_INFO("[OCC] released %zu tx pin(s) on close fd=%d",
                     released.size(), client_socket);
        }
    }
}
