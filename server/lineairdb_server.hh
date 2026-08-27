#pragma once

#include <memory>

#include "network/tcp_server.hh"
#include "network/message_handler.hh"
#include "rpc/lineairdb_rpc.hh"
#include "storage/database_manager.hh"
#include "storage/transaction_manager.hh"

class LineairDBServer : public TcpServer {
public:
    LineairDBServer();
    ~LineairDBServer() = default;

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
