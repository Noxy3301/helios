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
        MessageType message_type;
        std::string payload;

        if (!MessageHandler::receive_message(client_socket, message_type, payload)) {
            break;  // Client disconnected or error
        }

        std::string result;
        rpc_handler->handle_rpc(message_type, payload, result);

        if (!MessageHandler::send_response_writev(client_socket, message_type, result)) {
            break;  // Failed to send response
        }
    }
}
