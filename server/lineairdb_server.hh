#ifndef HELIOS_LINEAIRDB_SERVER_HH
#define HELIOS_LINEAIRDB_SERVER_HH

#include <memory>
#include <string>
#include <string_view>

#include "network/message_handler.hh"
#include "network/reactor.hh"
#include "network/tcp_server.hh"
#include "rpc/lineairdb_rpc.hh"
#include "storage/database_manager.hh"
#include "storage/transaction_manager.hh"

class LineairDBServer : public TcpServer {
 public:
  LineairDBServer();
  ~LineairDBServer() = default;

  void init();

 protected:
  void handle_client(int client_socket, std::string primer = std::string()) override;
  std::shared_ptr<Reactor::RpcDispatcher> create_dispatcher() override;
  RpcLane classify_rpc(MessageType type, std::string_view payload) const override;

 private:
  // Reactor-mode dispatch surface: wraps its own LineairDBRpc +
  // TransactionManager (mirrors the per-connection managers
  // handle_client() sets up). Serves kFast (this thread) and kSlow (a
  // helper-pool thread; helper_pool.hh) -- both single-shot and stateless,
  // so nothing here opens a stored, cross-RPC transaction. kConv opcodes
  // migrate before reaching a Dispatcher, so handle_client()'s disconnect
  // cleanup (abort_all_and_end + ReleaseMasstreeThreadEpoch) has no
  // counterpart here.
  class Dispatcher : public Reactor::RpcDispatcher {
   public:
    Dispatcher(std::shared_ptr<DatabaseManager> db_manager,
               std::shared_ptr<TableRowCounts> row_counts);
    void handle_rpc(uint64_t sender_id, MessageType message_type, const std::string &payload,
                     std::string &result) override;
    // TX_EXECUTE_READ_PLAN only: installs a runtime row/byte budget for the
    // call (see rpc_budget.hh) and returns false, discarding `result`, when
    // it was exceeded. Every other opcode just forwards to handle_rpc.
    bool handle_fast_rpc(uint64_t sender_id, MessageType message_type,
                          const std::string &payload, std::string &result) override;

   private:
    std::shared_ptr<TransactionManager> tx_manager_;
    std::shared_ptr<LineairDBRpc> rpc_;
  };

  // Core components
  std::shared_ptr<DatabaseManager> db_manager_;
  std::shared_ptr<TableRowCounts> row_counts_ = std::make_shared<TableRowCounts>();
};

#endif  // HELIOS_LINEAIRDB_SERVER_HH
