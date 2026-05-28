#include "message_handler.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <arpa/inet.h>

bool MessageHandler::receive_message(int socket, uint64_t& sender_id, 
                                    MessageType& message_type, std::string& payload) {
    // Read fixed-size header
    MessageHeader net_header{};
    ssize_t header_read = recv(socket, &net_header, sizeof(net_header), MSG_WAITALL);
    if (header_read != static_cast<ssize_t>(sizeof(net_header))) {
        if (header_read < 0) {
            LOG_ERROR("Failed to receive message header");
        } else {
            LOG_DEBUG("Client disconnected while receiving header");
        }
        return false;
    }

    // Convert message header (network order -> host order)
    sender_id = be64toh(net_header.sender_id);
    message_type = static_cast<MessageType>(ntohl(net_header.message_type));
    uint64_t payload_size = be64toh(net_header.payload_size);

    LOG_DEBUG("Received header: sender_id=%lu, message_type=%u, payload_size=%u", 
              sender_id, static_cast<uint32_t>(message_type), payload_size);

    // Read payload (if exists). Loop in bounded chunks: a single recv() with
    // MSG_WAITALL is not reliable for multi-GB counts (it can return short, and
    // some kernels cap a single transfer), so drive it ourselves.
    payload.clear();
    if (payload_size > 0) {
        payload.resize(payload_size);
        uint64_t got = 0;
        while (got < payload_size) {
            size_t chunk = static_cast<size_t>(payload_size - got);
            if (chunk > (1u << 30)) chunk = (1u << 30);  // 1GB per recv
            ssize_t n = recv(socket, &payload[got], chunk, MSG_WAITALL);
            if (n <= 0) {
                if (n < 0) {
                    LOG_ERROR("Failed to receive message payload (got %lu/%lu)",
                              (unsigned long)got, (unsigned long)payload_size);
                } else {
                    LOG_DEBUG("Client disconnected while receiving payload");
                }
                return false;
            }
            got += static_cast<uint64_t>(n);
        }
    }

    return true;
}

bool MessageHandler::send_response(int socket, uint64_t sender_id, 
                                  MessageType message_type, const std::string& payload) {
    LOG_DEBUG("Sending response (%zu bytes)", payload.size());
    
    // Prepare response header
    MessageHeader response_header{};
    response_header.sender_id = htobe64(sender_id);
    response_header.message_type = htonl(static_cast<uint32_t>(message_type));
    response_header._pad = 0;
    response_header.payload_size = htobe64(static_cast<uint64_t>(payload.size()));

    // Combine header and response
    size_t response_total_size = sizeof(response_header) + payload.size();
    std::vector<char> response_buffer(response_total_size);
    std::memcpy(response_buffer.data(), &response_header, sizeof(response_header));
    std::memcpy(response_buffer.data() + sizeof(response_header), payload.c_str(), payload.size());

    // Send response (handle partial writes for large messages)
    size_t total_sent = 0;
    while (total_sent < response_total_size) {
        ssize_t bytes_sent = send(socket, response_buffer.data() + total_sent,
                                  response_total_size - total_sent, 0);
        if (bytes_sent <= 0) {
            LOG_ERROR("Failed to send response (sent %zu/%zu bytes)", total_sent, response_total_size);
            return false;
        }
        total_sent += bytes_sent;
    }
    
    LOG_DEBUG("Response sent successfully");
    return true;
}

bool MessageHandler::send_response_writev(int socket, uint64_t sender_id,
                                          MessageType message_type, const std::string& payload) {
    LOG_DEBUG("Sending response via writev (%zu bytes)", payload.size());

    MessageHeader response_header{};
    response_header.sender_id = htobe64(sender_id);
    response_header.message_type = htonl(static_cast<uint32_t>(message_type));
    response_header._pad = 0;
    response_header.payload_size = htobe64(static_cast<uint64_t>(payload.size()));

    struct iovec iov[2];
    iov[0].iov_base = &response_header;
    iov[0].iov_len = sizeof(response_header);
    iov[1].iov_base = const_cast<char*>(payload.data());
    iov[1].iov_len = payload.size();

    size_t total_size = sizeof(response_header) + payload.size();
    size_t total_sent = 0;

    while (total_sent < total_size) {
        ssize_t bytes_sent = writev(socket, iov, 2);
        if (bytes_sent <= 0) {
            LOG_ERROR("writev failed (sent %zu/%zu bytes)", total_sent, total_size);
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

    LOG_DEBUG("writev response sent successfully");
    return true;
}

bool MessageHandler::send_header(int socket, uint64_t sender_id,
                                 MessageType message_type, uint64_t payload_size) {
    MessageHeader header{};
    header.sender_id = htobe64(sender_id);
    header.message_type = htonl(static_cast<uint32_t>(message_type));
    header._pad = 0;
    header.payload_size = htobe64(payload_size);
    const char* p = reinterpret_cast<const char*>(&header);
    size_t total = sizeof(header), sent = 0;
    while (sent < total) {
        ssize_t n = send(socket, p + sent, total - sent, 0);
        if (n <= 0) { LOG_ERROR("send_header failed"); return false; }
        sent += static_cast<size_t>(n);
    }
    return true;
}
