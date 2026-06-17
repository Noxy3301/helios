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
    uint32_t payload_size = ntohl(net_header.payload_size);

    LOG_DEBUG("Received header: sender_id=%lu, message_type=%u, payload_size=%u", 
              sender_id, static_cast<uint32_t>(message_type), payload_size);

    // Read payload (if exists)
    payload.clear();
    if (payload_size > 0) {
        payload.resize(payload_size);
        ssize_t body_read = recv(socket, &payload[0], payload_size, MSG_WAITALL);
        if (body_read != static_cast<ssize_t>(payload_size)) {
            if (body_read < 0) {
                LOG_ERROR("Failed to receive message payload");
            } else {
                LOG_DEBUG("Client disconnected while receiving payload");
            }
            return false;
        }
    }

    return true;
}

bool MessageHandler::send_response(int socket, uint64_t sender_id,
                                  MessageType message_type, const std::string& payload) {
    LOG_DEBUG("Sending response (%zu bytes)", payload.size());

    if (payload.size() > UINT32_MAX) {
        LOG_ERROR("Response payload %zu bytes exceeds the u32 frame limit; dropping",
                  payload.size());
        return false;
    }

    // Prepare response header
    MessageHeader response_header;
    response_header.sender_id = htobe64(sender_id);
    response_header.message_type = htonl(static_cast<uint32_t>(message_type));
    response_header.payload_size = htonl(static_cast<uint32_t>(payload.size()));

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

    if (payload.size() > UINT32_MAX) {
        LOG_ERROR("Response payload %zu bytes exceeds the u32 frame limit; dropping",
                  payload.size());
        return false;
    }

    MessageHeader response_header;
    response_header.sender_id = htobe64(sender_id);
    response_header.message_type = htonl(static_cast<uint32_t>(message_type));
    response_header.payload_size = htonl(static_cast<uint32_t>(payload.size()));

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
