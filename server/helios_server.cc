#include "helios_server.hh"
#include "server_config.hh"
#include <spdlog/spdlog.h>
#include "helios.pb.h"
#include "rpc/helios_rpc.hh"

#include <cstdint>
#include <iostream>

HeliosServer::HeliosServer() : TcpServer(config().server_port) {}

void HeliosServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }

    SPDLOG_INFO("Helios server initialized successfully on port {}, boot token {}",
             static_cast<unsigned>(config().server_port), hidden_keys_->boot_token);
}

void HeliosServer::handle_client(int client_socket) {
    SPDLOG_INFO("Handling client connection fd={}", client_socket);
    auto rpc_handler =
        std::make_shared<HeliosRpc>(db_manager_, row_counts_, hidden_keys_);

    while (true) {
        std::string request;

        if (!MessageHandler::receive_message(client_socket, request)) {
            break;  // Client disconnected or error
        }

        std::string reply, reply_payload;
        if (!rpc_handler->handle_rpc(request, reply, reply_payload)) {
            break;  // Nothing to answer with
        }

        if (!MessageHandler::send_response(client_socket, reply, reply_payload)) {
            break;  // Failed to send response
        }
    }
}
