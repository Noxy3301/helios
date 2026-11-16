#include "storage_server.hh"
#include "server_config.hh"
#include <spdlog/spdlog.h>
#include "helios.pb.h"
#include "rpc/helios_rpc.hh"

#include <cstdint>
#include <iostream>

StorageServer::StorageServer() : TcpServer(config().server_port) {}

void StorageServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }
    // Commits only move the counts, so a restart starts them from the rows
    // recovery restored; otherwise the optimizer sees near-empty tables.
    const auto& recovered = db_manager_->get_database()->recovered_row_counts();
    row_counts_->seed(recovered);
    SPDLOG_INFO("Row counts seeded for {} tables", recovered.size());

    SPDLOG_INFO("Storage server initialized on port {}, boot token {}",
             static_cast<unsigned>(config().server_port), hidden_keys_->boot_token);
}

void StorageServer::handle_client(int client_socket) {
    SPDLOG_INFO("Handling client connection fd={}", client_socket);
    auto rpc_handler =
        std::make_shared<HeliosRpc>(db_manager_, row_counts_, hidden_keys_);

    while (true) {
        std::string request, request_payload;

        if (!MessageHandler::receive_message(client_socket, request,
                                             request_payload)) {
            break;  // Client disconnected or error
        }

        std::string reply, reply_payload;
        if (!rpc_handler->handle_rpc(request, request_payload, reply,
                                     reply_payload)) {
            break;  // Nothing to answer with
        }

        if (!MessageHandler::send_response(client_socket, reply, reply_payload)) {
            break;  // Failed to send response
        }
    }
}
