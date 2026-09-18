#include "helios_server.hh"
#include "../common/log.h"
#include "helios.pb.h"
#include "rpc/helios_rpc.hh"

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace {

constexpr uint16_t kDefaultPort = 9999;

// HELIOS_SERVER_PORT, or the default when unset. A value that is not a
// port refuses startup: falling back would serve a caller that asked for
// another port.
uint16_t listen_port() {
    static const uint16_t port = []() -> uint16_t {
        const char* raw = std::getenv("HELIOS_SERVER_PORT");
        if (raw == nullptr) return kDefaultPort;

        const std::string_view input(raw);
        unsigned parsed         = 0;
        const auto [end, error] =
            std::from_chars(input.data(), input.data() + input.size(), parsed, 10);
        const bool consumed_all = end == input.data() + input.size();
        if (input.empty() || error != std::errc{} || !consumed_all ||
            parsed < 1 || parsed > 65535) {
            LOG_FATAL("Invalid HELIOS_SERVER_PORT='%s': expected an integer in [1,65535]",
                      raw);
        }
        return static_cast<uint16_t>(parsed);
    }();
    return port;
}

}  // namespace

HeliosServer::HeliosServer() : TcpServer(listen_port()) {}

void HeliosServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }

    LOG_INFO("Helios server initialized successfully on port %u, boot token %llu",
             static_cast<unsigned>(listen_port()),
             static_cast<unsigned long long>(storage_boot_token()));
}

void HeliosServer::handle_client(int client_socket) {
    LOG_INFO("Handling client connection fd=%d", client_socket);
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
