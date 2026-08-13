#ifndef HELIOS_NETWORK_MESSAGE_HANDLER_HH
#define HELIOS_NETWORK_MESSAGE_HANDLER_HH

#include <string>

#include "../protocol/message.hh"

class MessageHandler {
 public:
  static bool receive_message(int socket, uint64_t &sender_id,
                               MessageType &message_type, std::string &payload);
  static bool send_response(int socket, uint64_t sender_id,
                             MessageType message_type, const std::string &payload);
  // writev-based send: avoids copying header+payload into one buffer
  static bool send_response_writev(int socket, uint64_t sender_id,
                                    MessageType message_type, const std::string &payload);
};

#endif  // HELIOS_NETWORK_MESSAGE_HANDLER_HH
