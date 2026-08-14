#ifndef HELIOS_NETWORK_TCP_SERVER_HH
#define HELIOS_NETWORK_TCP_SERVER_HH

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "reactor.hh"
#include "rpc_lane.hh"

class TcpServer {
 public:
  TcpServer(uint16_t port = 9999);
  virtual ~TcpServer() = default;

  void run();

 protected:
  // Legacy thread-per-connection handler. Also reused, primed, as the
  // migration target for reactor-mode connections classified kSlow or kConv
  // (see rpc_lane.hh); `primer` then holds wire bytes a reactor thread
  // already read off the socket. A legacy fresh accept passes an empty
  // primer; reactor fresh accepts never reach this handler.
  virtual void handle_client(int client_socket, std::string primer = std::string()) = 0;

  // Reactor mode (HELIOS_TRANSPORT=reactor) hooks; unused otherwise.
  // Per-connection RPC dispatch state, used both on the fast path (this
  // reactor thread) and the kSlow path (a helper-pool thread; see
  // reactor.hh) -- a shared_ptr so a helper Job can co-own it. Wraps the
  // same handler + per-connection transaction manager handle_client uses.
  virtual std::shared_ptr<Reactor::RpcDispatcher> create_dispatcher() = 0;
  // Classifies (opcode, payload) into the lane a reactor thread dispatches
  // it to; see rpc_lane.hh.
  virtual RpcClassification classify_rpc(MessageType type, std::string_view payload) const = 0;

 private:
  uint16_t port_;

  bool setup_and_listen(int &server_socket);
  void accept_clients(int server_socket);
  void accept_clients_reactor(int server_socket);
  // Spawns handle_client() on a dedicated thread, primed with `primer`.
  // Shared by reactor migration; the legacy accept path calls handle_client()
  // through accept_clients() directly (empty primer).
  void spawn_legacy_thread(int client_socket, std::string primer);
};

#endif  // HELIOS_NETWORK_TCP_SERVER_HH
