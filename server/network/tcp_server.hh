#ifndef HELIOS_NETWORK_TCP_SERVER_HH
#define HELIOS_NETWORK_TCP_SERVER_HH

#include <cstdint>

class TcpServer {
 public:
  TcpServer(uint16_t port = 9999);
  virtual ~TcpServer() = default;

  void run();

 protected:
  virtual void handle_client(int client_socket) = 0;

 private:
  uint16_t port_;

  bool setup_and_listen(int &server_socket);
  void accept_clients(int server_socket);
};

#endif  // HELIOS_NETWORK_TCP_SERVER_HH
