#pragma once

#include <string>

#include "../protocol/message.hh"

class MessageHandler {
public:
    static bool receive_message(int socket, MessageType& message_type,
                                std::string& payload);
    static bool send_response(int socket, MessageType message_type,
                              const std::string& payload);
    // writev-based send: avoids copying header+payload into one buffer
    static bool send_response_writev(int socket, MessageType message_type,
                                     const std::string& payload);
};
