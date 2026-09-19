#pragma once

#include <string>

class MessageHandler {
public:
    static bool receive_message(int socket, std::string& payload);
    // writev-based send: avoids copying header+payload into one buffer
    static bool send_response(int socket, const std::string& payload);
};
