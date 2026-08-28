// lineairdb_ctl.cc
// Command-line client for the storage server's control RPCs

#include <arpa/inet.h>
#include <endian.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "lineairdb.pb.h"
#include "protocol/message.hh"

namespace {

using DurabilityRpc = LineairDB::Protocol::DbSetCommitDurability;

constexpr unsigned long kMaxPort = 65535;
// Outlasts the server's fixed one-hour barrier
constexpr long kReceiveTimeoutSeconds = 60 * 60 + 30;

int fail(const std::string &text) {
  std::fprintf(stderr, "error: %s\n", text.c_str());
  return 1;
}

int usage(const char *problem = nullptr) {
  if (problem != nullptr) std::fprintf(stderr, "error: %s\n", problem);
  std::fprintf(stderr,
               "usage: lineairdb-ctl [--host <dotted IPv4>] [--port 1-65535] "
               "set-durability <async|sync>\n");
  return 1;
}

// A whole decimal token within [low, high], or -1 for anything else
long parse_bounded(const char *token, unsigned long low, unsigned long high) {
  if (token == nullptr || *token == '\0') return -1;
  // strtoul skips blanks and wraps a negation; neither is a value here
  if (std::isdigit(static_cast<unsigned char>(*token)) == 0) return -1;
  errno = 0;
  char *end = nullptr;
  const unsigned long value = std::strtoul(token, &end, 10);
  if (errno != 0 || end == token || *end != '\0') return -1;
  if (value < low || value > high) return -1;
  return static_cast<long>(value);
}

bool send_all(int fd, const char *data, size_t size) {
  size_t sent = 0;
  while (sent < size) {
    const ssize_t chunk =
        ::send(fd, data + sent, size - sent, MSG_NOSIGNAL);
    if (chunk <= 0) return false;
    sent += static_cast<size_t>(chunk);
  }
  return true;
}

bool recv_all(int fd, char *data, size_t size) {
  size_t received = 0;
  while (received < size) {
    const ssize_t chunk = ::recv(fd, data + received, size - received, 0);
    if (chunk <= 0) return false;
    received += static_cast<size_t>(chunk);
  }
  return true;
}

int connect_to(const sockaddr_in &address) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  if (::connect(fd, reinterpret_cast<const sockaddr *>(&address),
                sizeof(address)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

// Sends one framed request and reads the framed response payload back
bool exchange(int fd, MessageType type, const std::string &request,
              std::string &response) {
  MessageHeader header{};
  header.sender_id = htobe64(1);
  header.message_type = htonl(static_cast<uint32_t>(type));
  header.payload_size = htonl(static_cast<uint32_t>(request.size()));

  std::string frame(reinterpret_cast<const char *>(&header), sizeof(header));
  frame.append(request);
  if (!send_all(fd, frame.data(), frame.size())) return false;

  MessageHeader reply{};
  if (!recv_all(fd, reinterpret_cast<char *>(&reply), sizeof(reply))) {
    return false;
  }
  const uint32_t payload_size = ntohl(reply.payload_size);
  response.resize(payload_size);
  if (payload_size == 0) return true;
  return recv_all(fd, &response[0], payload_size);
}

int set_durability(const std::string &host, uint16_t port,
                   DurabilityRpc::Mode mode) {
  DurabilityRpc::Request request;
  request.set_mode(mode);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
    return usage("--host takes a dotted IPv4 address");
  }

  const int fd = connect_to(address);
  if (fd < 0) return fail("cannot connect to " + host + ":" +
                          std::to_string(port));

  // Without it a reply that never arrives would hang the command forever
  timeval receive_timeout{kReceiveTimeoutSeconds, 0};
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout,
                   sizeof(receive_timeout)) != 0) {
    ::close(fd);
    return fail(std::string("cannot set the receive timeout: ") +
                std::strerror(errno));
  }

  std::string payload;
  const bool exchanged = exchange(fd, MessageType::DB_SET_COMMIT_DURABILITY,
                                  request.SerializeAsString(), payload);
  ::close(fd);
  if (!exchanged) return fail("no response from the server");

  DurabilityRpc::Response response;
  if (!response.ParseFromString(payload)) return fail("malformed response");
  if (!response.ok()) {
    const std::string text = response.error().empty()
                                 ? "the server refused the switch"
                                 : response.error();
    // The mode says whether the policy was published before the failure
    return fail(text + " (mode=" +
                DurabilityRpc::Mode_Name(response.mode()) + ")");
  }
  std::printf("ok mode=%s\n",
              DurabilityRpc::Mode_Name(response.mode()).c_str());
  return 0;
}

}  // namespace

int main(int argc, char **argv) {
  std::string host = "127.0.0.1";
  uint16_t port = 9999;
  std::string command;
  std::string argument;

  for (int index = 1; index < argc; index++) {
    const std::string token = argv[index];
    const bool has_value = index + 1 < argc;
    if (token == "--host" && has_value) {
      host = argv[++index];
    } else if (token == "--port" && has_value) {
      const long parsed = parse_bounded(argv[++index], 1, kMaxPort);
      if (parsed < 0) return usage("--port takes 1-65535");
      port = static_cast<uint16_t>(parsed);
    } else if (command.empty()) {
      command = token;
    } else if (argument.empty()) {
      argument = token;
    } else {
      return usage();
    }
  }

  if (command != "set-durability") return usage();
  if (argument == "async") {
    return set_durability(host, port, DurabilityRpc::ASYNC);
  }
  if (argument == "sync") {
    return set_durability(host, port, DurabilityRpc::SYNC);
  }
  return usage();
}
