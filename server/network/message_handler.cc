#include "message_handler.hh"
#include <spdlog/spdlog.h>

#include <cstdint>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>

bool MessageHandler::receive_message(int socket, std::string& payload) {
    uint32_t net_size = 0;
    ssize_t header_read = recv(socket, &net_size, sizeof(net_size), MSG_WAITALL);
    if (header_read != static_cast<ssize_t>(sizeof(net_size))) {
        if (header_read < 0) {
            SPDLOG_ERROR("Failed to receive message header");
        } else {
            SPDLOG_DEBUG("Client disconnected while receiving header");
        }
        return false;
    }

    const uint32_t payload_size = ntohl(net_size);
    SPDLOG_DEBUG("Received header: payload_size={}", payload_size);

    payload.clear();
    if (payload_size > 0) {
        payload.resize(payload_size);
        ssize_t body_read = recv(socket, &payload[0], payload_size, MSG_WAITALL);
        if (body_read != static_cast<ssize_t>(payload_size)) {
            if (body_read < 0) {
                SPDLOG_ERROR("Failed to receive message payload");
            } else {
                SPDLOG_DEBUG("Client disconnected while receiving payload");
            }
            return false;
        }
    }

    return true;
}

bool MessageHandler::send_response(int socket, const std::string& payload) {
    SPDLOG_DEBUG("Sending response ({} bytes)", payload.size());

    if (payload.size() > UINT32_MAX) {
        SPDLOG_ERROR("Response payload {} bytes exceeds the u32 frame limit; dropping",
                  payload.size());
        return false;
    }

    uint32_t net_size = htonl(static_cast<uint32_t>(payload.size()));

    struct iovec iov[2];
    iov[0].iov_base = &net_size;
    iov[0].iov_len = sizeof(net_size);
    iov[1].iov_base = const_cast<char*>(payload.data());
    iov[1].iov_len = payload.size();

    size_t total_size = sizeof(net_size) + payload.size();
    size_t total_sent = 0;

    while (total_sent < total_size) {
        ssize_t bytes_sent = writev(socket, iov, 2);
        if (bytes_sent <= 0) {
            SPDLOG_ERROR("writev failed (sent {}/{} bytes)", total_sent, total_size);
            return false;
        }
        total_sent += bytes_sent;
        if (total_sent < total_size) {
            // Adjust iov for partial write
            size_t consumed = bytes_sent;
            for (int i = 0; i < 2; i++) {
                if (consumed >= iov[i].iov_len) {
                    consumed -= iov[i].iov_len;
                    iov[i].iov_len = 0;
                } else {
                    iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + consumed;
                    iov[i].iov_len -= consumed;
                    break;
                }
            }
        }
    }

    SPDLOG_DEBUG("Response sent successfully");
    return true;
}
