#pragma once

#include <string>

// One frame: [u32 env_len][envelope][u32 pay_len][payload], both lengths in
// network order. See common/rpc_frame.h.
class MessageHandler {
public:
    // Reads one request, whose payload part a client leaves empty.
    static bool receive_message(int socket, std::string& envelope);
    // writev-based send: avoids copying the parts into one buffer
    static bool send_response(int socket, const std::string& envelope,
                              const std::string& payload);
};
