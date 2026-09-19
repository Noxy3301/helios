#include "tcp_server.hh"
#include <spdlog/spdlog.h>

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <atomic>

namespace {

// Connections being served, for the accept and close log lines.
std::atomic<int> active_connections{0};

}  // namespace

TcpServer::TcpServer(uint16_t port) : port_(port) {}

bool TcpServer::run() {
    SPDLOG_INFO("Starting server on port {}", port_);
    
    int server_socket;
    if (!setup_and_listen(server_socket)) {
        return false;
    }
    
    SPDLOG_INFO("Server listening on port {}", port_);
    accept_clients(server_socket);
    close(server_socket);
    return true;
}

bool TcpServer::setup_and_listen(int& server_socket) {
    // Create socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        int err = errno;
        SPDLOG_ERROR("Failed to create socket: {} (errno={})", std::strerror(err), err);
        return false;
    }
    
    // Set SO_REUSEADDR option
    int reuse = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "Failed to set SO_REUSEADDR" << std::endl;
        close(server_socket);
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    // Bind socket
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Failed to bind socket" << std::endl;
        close(server_socket);
        return false;
    }

    // Listen for connections
    if (listen(server_socket, 128) < 0) {
        std::cerr << "Failed to listen on socket" << std::endl;
        close(server_socket);
        return false;
    }
    
    return true;
}

void TcpServer::serve_client(int client_socket, std::string client_ip) {
    handle_client(client_socket);
    // Ensure socket is closed when done
    int fd = client_socket;
    close(client_socket);
    int left = --active_connections;
    SPDLOG_INFO("Closed connection fd={} ({}) (active={})", fd,
                client_ip.c_str(), left);
}

void TcpServer::accept_clients(int server_socket) {
    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);

        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_addr_len);
        if (client_socket < 0) {
            int err = errno;
            SPDLOG_ERROR("Failed to accept client connection: {} (errno={})",
                      std::strerror(err), err);
            if (err == EINTR) {
                continue;  // retry on interrupt
            }
            // Sleep briefly to avoid busy loop on persistent failure conditions
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // Disable Nagle's algorithm for low-latency RPC
        int flag = 1;
        setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        // Hand off each client to a dedicated thread
        auto client_ip = std::string(inet_ntoa(client_addr.sin_addr));
        int now_active = ++active_connections;
        SPDLOG_INFO("Accepted connection fd={} from {} (active={})", client_socket, client_ip.c_str(), now_active);

        std::thread(&TcpServer::serve_client, this, client_socket, client_ip)
            .detach();
    }
}
