#ifndef HELIOS_NETWORK_TCP_SERVER_HH
#define HELIOS_NETWORK_TCP_SERVER_HH

#include <cstdint>
#include <memory>
#include <string>

#include "reactor.hh"

class TcpServer {
 public:
  TcpServer(uint16_t port = 9999);
  virtual ~TcpServer() = default;

  void run();

 protected:
  // Legacy thread-per-connection handler. Also reused, primed, as the
  // migration target for reactor-mode connections whose opcode isn't in the
  // fast-path allowlist (see reactor.hh); `primer` then holds wire bytes a
  // reactor thread already read off the socket. A legacy fresh accept passes
  // an empty primer; reactor fresh accepts never reach this handler.
  virtual void handle_client(int client_socket, std::string primer = std::string()) = 0;

  // Reactor mode (HELIOS_TRANSPORT=reactor) hooks; unused otherwise.
  // Per-connection RPC dispatch state for the fast path (e.g. wraps the same
  // handler + per-connection transaction manager handle_client uses).
  virtual std::unique_ptr<Reactor::RpcDispatcher> create_dispatcher() = 0;
  // True if `type` is safe to execute inline on a reactor thread.
  virtual bool is_reactor_fast_path(MessageType type) const = 0;

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
