#include "lineairdb_server.hh"
#include "../common/log.h"
#include "lineairdb.pb.h"
#include "rpc/lineairdb_rpc.hh"

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace {

constexpr uint16_t kDefaultPort = 9999;

/**
 * @brief Applies LINEAIRDB_SERVER_PORT, keeping the default when it is unset.
 * A value that is not a decimal port refuses startup: falling back would serve
 * a caller that asked for another port.
 */
uint16_t listen_port() {
    static const uint16_t port = []() -> uint16_t {
        const char* raw = std::getenv("LINEAIRDB_SERVER_PORT");
        if (raw == nullptr) return kDefaultPort;

        const std::string_view input(raw);
        unsigned parsed         = 0;
        const auto [end, error] =
            std::from_chars(input.data(), input.data() + input.size(), parsed, 10);
        const bool consumed_all = end == input.data() + input.size();
        if (input.empty() || error != std::errc{} || !consumed_all ||
            parsed < 1 || parsed > 65535) {
            LOG_FATAL("Invalid LINEAIRDB_SERVER_PORT='%s': expected an integer in [1,65535]",
                      raw);
        }
        return static_cast<uint16_t>(parsed);
    }();
    return port;
}

}  // namespace

LineairDBServer::LineairDBServer() : TcpServer(listen_port()) {}

void LineairDBServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }

    LOG_INFO("LineairDB server initialized successfully on port %u, boot token %llu",
             static_cast<unsigned>(listen_port()),
             static_cast<unsigned long long>(storage_boot_token()));
}

void LineairDBServer::handle_client(int client_socket) {
    LOG_INFO("Handling client connection fd=%d", client_socket);
    // Per-connection managers
    auto tx_manager = std::make_shared<TransactionManager>(*db_manager_->get_database());
    auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager,
                                                      row_counts_, hidden_keys_);

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
