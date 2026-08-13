#ifndef HELIOS_NETWORK_MESSAGE_HANDLER_HH
#define HELIOS_NETWORK_MESSAGE_HANDLER_HH

#include <cstddef>
#include <string>

#include "../protocol/message.hh"

class MessageHandler {
 public:
  // `primer`/`primer_offset`, when both non-null, are drained first: bytes a
  // reactor thread already pulled off the wire before migrating this
  // connection to the legacy per-connection loop (see reactor.hh). Null
  // primer arguments read exclusively from the socket.
  static bool receive_message(int socket, uint64_t &sender_id,
                               MessageType &message_type, std::string &payload,
                               std::string *primer = nullptr, size_t *primer_offset = nullptr);
  static bool send_response(int socket, uint64_t sender_id,
                             MessageType message_type, const std::string &payload);
  // writev-based send: avoids copying header+payload into one buffer
  static bool send_response_writev(int socket, uint64_t sender_id,
                                    MessageType message_type, const std::string &payload);
};

#endif  // HELIOS_NETWORK_MESSAGE_HANDLER_HH
