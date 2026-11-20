#include "message_handler.hh"
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <endian.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/rpc_frame.h"

using helios::rpc::append_len;
using helios::rpc::kInitialBufferBytes;
using helios::rpc::kKeptBufferBytes;
using helios::rpc::load_len;

FrameReader::FrameReader(Connection& conn)
    : conn_(conn), socket_(conn.socket()) {}

bool FrameReader::fill(size_t need) {
    if (end_ - begin_ >= need) return true;
    if (begin_ + need > buffer_.size()) {
        // Move the unread tail to the front, growing only for a larger frame.
        const size_t unread = end_ - begin_;
        if (need > buffer_.size()) {
            std::string grown(std::max(need, kInitialBufferBytes), '\0');
            std::memcpy(grown.data(), buffer_.data() + begin_, unread);
            buffer_.swap(grown);
        } else {
            std::memmove(buffer_.data(), buffer_.data() + begin_, unread);
        }
        begin_ = 0;
        end_ = unread;
    }
    while (end_ - begin_ < need) {
        // Wait for request bytes, sending the replies others queue meanwhile.
        pollfd fds[2] = {{socket_, POLLIN, 0}, {conn_.wake_fd(), POLLIN, 0}};
        if (poll(fds, 2, -1) < 0) {
            if (errno == EINTR) continue;
            SPDLOG_ERROR("Failed to poll fd={}", socket_);
            return false;
        }
        if (fds[1].revents != 0) {
            conn_.take_wake();
            conn_.flush();
        }
        if (fds[0].revents == 0) continue;
        const ssize_t got =
            recv(socket_, buffer_.data() + end_, buffer_.size() - end_, 0);
        if (got <= 0) {
            if (got < 0) {
                SPDLOG_ERROR("Failed to receive a request frame");
            } else {
                SPDLOG_DEBUG("Client disconnected while receiving a frame");
            }
            return false;
        }
        end_ += static_cast<size_t>(got);
    }
    return true;
}

bool FrameReader::next(uint64_t& id, std::string_view& envelope,
                       std::string_view& payload) {
    begin_ += consumed_;
    consumed_ = 0;
    if (begin_ == end_) {
        begin_ = end_ = 0;
        if (buffer_.size() > kKeptBufferBytes) std::string().swap(buffer_);
    }
    if (!fill(helios::rpc::kFrameHeadBytes)) return false;
    uint64_t net_id;
    std::memcpy(&net_id, buffer_.data() + begin_, sizeof(net_id));
    id = be64toh(net_id);
    const uint32_t env_len = load_len(buffer_.data() + begin_ + sizeof(net_id));
    if (env_len > helios::rpc::kMaxPartBytes) {
        SPDLOG_ERROR("Frame part of {} bytes exceeds the {} byte limit",
                     env_len, helios::rpc::kMaxPartBytes);
        return false;
    }
    const size_t pay_at = helios::rpc::kFrameHeadBytes + size_t{env_len};
    if (!fill(pay_at + sizeof(uint32_t))) return false;
    const uint32_t pay_len = load_len(buffer_.data() + begin_ + pay_at);
    if (pay_len != 0 && env_len != 1) {
        SPDLOG_ERROR("Request carries a {} byte payload part", pay_len);
        return false;
    }
    if (pay_len > helios::rpc::kMaxPartBytes) {
        SPDLOG_ERROR("Frame part of {} bytes exceeds the {} byte limit",
                     pay_len, helios::rpc::kMaxPartBytes);
        return false;
    }
    const size_t frame = pay_at + sizeof(uint32_t) + pay_len;
    if (!fill(frame)) return false;
    const char* head = buffer_.data() + begin_;
    envelope = std::string_view(head + helios::rpc::kFrameHeadBytes, env_len);
    payload = std::string_view(head + pay_at + sizeof(uint32_t), pay_len);
    consumed_ = frame;
    return true;
}

bool FrameReader::buffered_frame() const {
    const size_t begin = begin_ + consumed_;
    const size_t have = end_ - begin;
    if (have < helios::rpc::kFrameHeadBytes) return false;
    const size_t pay_at =
        helios::rpc::kFrameHeadBytes +
        size_t{load_len(buffer_.data() + begin + sizeof(uint64_t))};
    if (have < pay_at + sizeof(uint32_t)) return false;
    return have >= pay_at + sizeof(uint32_t) +
                       load_len(buffer_.data() + begin + pay_at);
}

Connection::Connection(int socket)
    : socket_(socket), wake_fd_(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
    if (wake_fd_ < 0) {
        // Without a wake, replies from other threads would never go out.
        SPDLOG_ERROR("Failed to create the wake eventfd for fd={}", socket_);
        shutdown();
    }
}

Connection::~Connection() {
    close(socket_);
    if (wake_fd_ >= 0) close(wake_fd_);
}

bool Connection::append(uint64_t id, const std::string& envelope,
                        const std::string& payload) {
    if (envelope.size() > helios::rpc::kMaxPartBytes ||
        payload.size() > helios::rpc::kMaxPartBytes) {
        SPDLOG_ERROR("Reply of {} + {} bytes exceeds the {} byte part limit; "
                     "closing fd={}", envelope.size(), payload.size(),
                     helios::rpc::kMaxPartBytes, socket_);
        shutdown();
        return false;
    }
    const uint64_t net_id = htobe64(id);

    std::lock_guard<std::mutex> lock(mutex_);
    if (failed_) return false;
    const bool first = out_.empty();
    out_.append(reinterpret_cast<const char*>(&net_id), sizeof(net_id));
    append_len(out_, envelope.size());
    out_.append(envelope);
    append_len(out_, payload.size());
    out_.append(payload);
    return first;
}

void Connection::wake() { eventfd_write(wake_fd_, 1); }

void Connection::take_wake() {
    eventfd_t count;
    eventfd_read(wake_fd_, &count);
}

void Connection::flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!out_.empty() && !failed_) {
        sending_.swap(out_);
        lock.unlock();
        size_t sent = 0;
        while (sent < sending_.size()) {
            const ssize_t n = send(socket_, sending_.data() + sent,
                                   sending_.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) break;
            sent += static_cast<size_t>(n);
        }
        const bool ok = sent == sending_.size();
        if (!ok) {
            SPDLOG_ERROR("Failed to send replies on fd={} (sent {}/{} bytes)",
                         socket_, sent, sending_.size());
        }
        if (sending_.capacity() > kKeptBufferBytes) {
            std::string().swap(sending_);
        } else {
            sending_.clear();
        }
        lock.lock();
        if (!ok) {
            failed_ = true;
            out_.clear();
            ::shutdown(socket_, SHUT_RDWR);
        }
    }
}

void Connection::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    failed_ = true;
    out_.clear();
    ::shutdown(socket_, SHUT_RDWR);
}
