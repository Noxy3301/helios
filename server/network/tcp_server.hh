#pragma once

#include <cstdint>

class TcpServer {
public:
    TcpServer(uint16_t port = 9999);
    virtual ~TcpServer() = default;
    
    // False when the listening socket could not be established: a second
    // instance must fail rather than exit as if it had served.
    bool run();
    
protected:
    virtual void handle_client(int client_socket) = 0;
    
private:
    uint16_t port_;
    
    bool setup_and_listen(int& server_socket);
    void accept_clients(int server_socket);
};
