#pragma once

#include <string>

#include "../protocol/message.hh"

class MessageHandler {
public:
    static bool receive_message(int socket, uint64_t& sender_id,
                               MessageType& message_type, std::string& payload);
    static bool send_response(int socket, uint64_t sender_id,
                             MessageType message_type, const std::string& payload);
    // writev-based send: avoids copying header+payload into one buffer
    static bool send_response_writev(int socket, uint64_t sender_id,
                                     MessageType message_type, const std::string& payload);
    // Sends ONLY the framed header (with a precomputed payload_size); the caller
    // then streams the body itself. Used to stream a large flat response without
    // materializing it in one buffer. Returns false on socket error.
    static bool send_header(int socket, uint64_t sender_id,
                            MessageType message_type, uint64_t payload_size);
};
