#pragma once

#include <memory>

#include "network/tcp_server.hh"
#include "network/message_handler.hh"
#include "rpc/helios_rpc.hh"
#include "database_manager.hh"

class HeliosServer : public TcpServer {
public:
    HeliosServer();
    ~HeliosServer() = default;

    void init();

protected:
    void handle_client(int client_socket) override;

private:
    // Core components
    std::shared_ptr<DatabaseManager> db_manager_;
    std::shared_ptr<TableRowCounts> row_counts_ = std::make_shared<TableRowCounts>();
    std::shared_ptr<HiddenKeyAllocator> hidden_keys_ =
        std::make_shared<HiddenKeyAllocator>();
};
