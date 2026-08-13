#ifndef HELIOS_LINEAIRDB_SERVER_HH
#define HELIOS_LINEAIRDB_SERVER_HH

#include <memory>
#include <string>

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
  std::unique_ptr<Reactor::RpcDispatcher> create_dispatcher() override;
  bool is_reactor_fast_path(MessageType type) const override;

 private:
  // Reactor-mode fast path: wraps its own LineairDBRpc + TransactionManager,
  // mirroring the per-connection managers handle_client() sets up per
  // thread. is_reactor_fast_path() admits only the two stateless opcodes, so
  // every conversational opcode migrates before reaching a Dispatcher --
  // nothing here ever opens a stored transaction, so the disconnect cleanup
  // handle_client() runs (abort_all_and_end + ReleaseMasstreeThreadEpoch)
  // has no counterpart on this path.
  class Dispatcher : public Reactor::RpcDispatcher {
   public:
    Dispatcher(std::shared_ptr<DatabaseManager> db_manager,
               std::shared_ptr<TableRowCounts> row_counts);
    void handle_rpc(uint64_t sender_id, MessageType message_type, const std::string &payload,
                     std::string &result) override;

   private:
    std::shared_ptr<TransactionManager> tx_manager_;
    std::shared_ptr<LineairDBRpc> rpc_;
  };

  // Core components
  std::shared_ptr<DatabaseManager> db_manager_;
  std::shared_ptr<TableRowCounts> row_counts_ = std::make_shared<TableRowCounts>();
};

#endif  // HELIOS_LINEAIRDB_SERVER_HH
