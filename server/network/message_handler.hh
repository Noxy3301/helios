#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

// One frame: [u64 id][u32 env_len][envelope][u32 pay_len][payload], all in
// network order. See common/rpc_frame.h.

class Connection;

// Reads request frames from one connection through a buffer, so one recv can
// take many frames. While it waits for bytes, it sends the replies that other
// threads queue on `conn`.
class FrameReader {
public:
    explicit FrameReader(Connection& conn);

    // Reads one request. Only a flat request has a payload part. Both views
    // stay valid until the next call.
    bool next(uint64_t& id, std::string_view& envelope,
              std::string_view& payload);
    // Whether the next request is already buffered whole, so next() will not
    // wait on the socket.
    bool buffered_frame() const;

private:
    Connection& conn_;
    int socket_;
    std::string buffer_;
    size_t begin_ = 0;     // first unread byte
    size_t end_ = 0;       // one past the last received byte
    size_t consumed_ = 0;  // bytes of the frame handed out last

    // Ensures `need` unread bytes are buffered.
    bool fill(size_t need);
};

/**
 * @brief The reply side of one accepted connection, shared by every thread
 * that answers its requests.
 *
 * @details Owns the socket and closes it on destruction, so no reply can go
 * out on a descriptor the kernel has handed to a later connection. Any thread
 * appends replies, but only the thread reading the connection sends them, so
 * a peer that reads slowly holds up no other connection. Another thread wakes
 * the reading thread after it appends. A failed send shuts the socket down,
 * which ends the reading thread's loop, and later replies are dropped.
 * Thread-safe, except that only the reading thread calls flush() and
 * take_wake().
 */
class Connection {
public:
    explicit Connection(int socket);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // Queues one reply under its request's id. A part over the frame limit
    // ends the connection instead. Returns true when the queue was empty, so
    // a thread other than the reading one must call wake().
    bool append(uint64_t id, const std::string& envelope,
                const std::string& payload);
    // Sends every queued reply.
    void flush();
    // Makes the reading thread's wait for request bytes return to flush().
    void wake();
    // Clears a pending wake.
    void take_wake();
    // Ends the connection for both directions; later replies are dropped.
    void shutdown();

    int socket() const { return socket_; }
    int wake_fd() const { return wake_fd_; }

private:
    std::mutex mutex_;
    const int socket_;
    const int wake_fd_;    // eventfd that wake() signals
    std::string out_;      // replies not yet taken by a send
    std::string sending_;  // the flusher's bytes, touched without the lock
    bool failed_ = false;
};
