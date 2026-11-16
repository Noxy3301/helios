#include "message_handler.hh"
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>

#include "common/rpc_frame.h"

namespace {

// Bytes one recv asks for. A client sends one request and waits for its reply,
// so a request that fits arrives in one recv instead of one per frame field.
constexpr size_t kRecvBytes = 16 * 1024;

// Reads exactly size bytes, looping because recv(MSG_WAITALL) returns short on
// a signal.
bool recv_all(int socket, char* out, size_t size) {
    size_t read_total = 0;
    while (read_total < size) {
        const ssize_t chunk =
            recv(socket, out + read_total, size - read_total, MSG_WAITALL);
        if (chunk <= 0) {
            if (chunk < 0) {
                SPDLOG_ERROR("Failed to receive {} frame bytes", size);
            } else {
                SPDLOG_DEBUG("Client disconnected while receiving a frame");
            }
            return false;
        }
        read_total += static_cast<size_t>(chunk);
    }
    return true;
}

}  // namespace

bool MessageHandler::receive_message(int socket, std::string& envelope) {
    char head[kRecvBytes];
    uint32_t net_len = 0;
    size_t got = 0;
    while (got < sizeof(net_len)) {
        const ssize_t chunk = recv(socket, head + got, sizeof(head) - got, 0);
        if (chunk <= 0) {
            if (chunk < 0) {
                SPDLOG_ERROR("Failed to receive a frame length");
            } else {
                SPDLOG_DEBUG(
                    "Client disconnected while receiving a frame length");
            }
            return false;
        }
        got += static_cast<size_t>(chunk);
    }
    std::memcpy(&net_len, head, sizeof(net_len));
    const uint32_t size = ntohl(net_len);
    if (size > helios::rpc::kMaxPartBytes) {
        SPDLOG_ERROR("Frame part of {} bytes exceeds the {} byte limit", size,
                     helios::rpc::kMaxPartBytes);
        return false;
    }

    const char* rest = head + sizeof(net_len);
    got -= sizeof(net_len);
    const size_t in_envelope = std::min<size_t>(got, size);
    envelope.resize(size);
    std::memcpy(envelope.data(), rest, in_envelope);
    if (!recv_all(socket, envelope.data() + in_envelope, size - in_envelope)) {
        return false;
    }

    uint32_t pay_len = 0;
    const size_t in_pay_len = got - in_envelope;
    // A client waits for each reply, so bytes past this frame break protocol.
    if (in_pay_len > sizeof(pay_len)) {
        SPDLOG_ERROR("Request is followed by {} unexpected bytes",
                     in_pay_len - sizeof(pay_len));
        return false;
    }
    std::memcpy(&pay_len, rest + in_envelope, in_pay_len);
    if (!recv_all(socket, reinterpret_cast<char*>(&pay_len) + in_pay_len,
                  sizeof(pay_len) - in_pay_len)) {
        return false;
    }
    if (pay_len != 0) {
        SPDLOG_ERROR("Request carries a {} byte payload part", ntohl(pay_len));
        return false;
    }
    return true;
}

bool MessageHandler::send_response(int socket, const std::string& envelope,
                                   const std::string& payload) {
    SPDLOG_DEBUG("Sending response ({} + {} bytes)", envelope.size(),
                 payload.size());

    if (envelope.size() > UINT32_MAX || payload.size() > UINT32_MAX) {
        SPDLOG_ERROR("Response {} + {} bytes exceeds the u32 frame limit; dropping",
                  envelope.size(), payload.size());
        return false;
    }

    const uint32_t env_len = htonl(static_cast<uint32_t>(envelope.size()));
    const uint32_t pay_len = htonl(static_cast<uint32_t>(payload.size()));

    struct iovec iov[4];
    iov[0].iov_base = const_cast<uint32_t*>(&env_len);
    iov[0].iov_len = sizeof(env_len);
    iov[1].iov_base = const_cast<char*>(envelope.data());
    iov[1].iov_len = envelope.size();
    iov[2].iov_base = const_cast<uint32_t*>(&pay_len);
    iov[2].iov_len = sizeof(pay_len);
    iov[3].iov_base = const_cast<char*>(payload.data());
    iov[3].iov_len = payload.size();

    size_t total_size = sizeof(env_len) + envelope.size() + sizeof(pay_len) +
                        payload.size();
    size_t total_sent = 0;

    while (total_sent < total_size) {
        ssize_t bytes_sent = writev(socket, iov, 4);
        if (bytes_sent <= 0) {
            SPDLOG_ERROR("writev failed (sent {}/{} bytes)", total_sent, total_size);
            return false;
        }
        total_sent += bytes_sent;
        if (total_sent < total_size) {
            // Adjust iov for partial write
            size_t consumed = bytes_sent;
            for (int i = 0; i < 4; i++) {
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
