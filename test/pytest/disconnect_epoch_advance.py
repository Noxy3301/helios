"""
Regression: a client that disconnects mid-transaction must not stop the epoch.

The server hands each connection to its own detached thread, and LineairDB's
thread-local epoch slots live in a list that outlives the thread. A connection
that begins a transaction and then vanishes without DB_END_TRANSACTION used to
leave its slot online forever, which pins EpochFramework::GetSmallestEpoch and
freezes the global epoch after a single tick.

The oracle is epoch advance itself, not a later commit: a commit succeeds even
with a pinned slot when the durability contract is Volatile. DB_FENCE returns
only after EpochFramework::Sync observes two epoch changes, so a bounded
DB_FENCE response is a direct observation that the epoch still moves.

Speaks the RPC wire protocol directly because the failure is below SQL: both
requests used here carry an empty protobuf payload, so no generated Python
protobuf module is needed.
"""
import socket
import struct
import sys
import time

HOST = "127.0.0.1"
PORT = 9999

TX_BEGIN_TRANSACTION = 1
DB_FENCE = 20

# sender_id:u64, message_type:u32, payload_size:u32, all network byte order
HEADER = struct.Struct("!QII")

# Two epoch changes at the 40ms default is ~80ms; the bound is loose enough to
# absorb thread-pool drain and index linearization, tight enough that a frozen
# epoch cannot pass.
FENCE_TIMEOUT_SECONDS = 5.0
CONNECT_TIMEOUT_SECONDS = 5.0


def recv_exact(sock, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("unexpected EOF from server")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def rpc(sock, message_type):
    """Send an empty-payload request and drain the response."""
    sock.sendall(HEADER.pack(0, message_type, 0))
    sender_id, response_type, payload_size = HEADER.unpack(
        recv_exact(sock, HEADER.size))
    if response_type != message_type:
        raise RuntimeError(
            f"response type {response_type} does not match request {message_type}")
    return recv_exact(sock, payload_size)


def leak_transaction():
    """Begin a transaction, then drop the connection without ending it."""
    sock = socket.create_connection((HOST, PORT), CONNECT_TIMEOUT_SECONDS)
    sock.settimeout(CONNECT_TIMEOUT_SECONDS)
    rpc(sock, TX_BEGIN_TRANSACTION)  # response proves the transaction began
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()


def measure_fence():
    sock = socket.create_connection((HOST, PORT), CONNECT_TIMEOUT_SECONDS)
    sock.settimeout(FENCE_TIMEOUT_SECONDS)
    try:
        started = time.monotonic()
        rpc(sock, DB_FENCE)
        return time.monotonic() - started
    finally:
        sock.close()


def main():
    print("TEST: epoch keeps advancing after a mid-transaction disconnect")

    baseline = measure_fence()
    print(f"  DB_FENCE before the disconnect: {baseline:.3f}s")

    leak_transaction()
    print("  connection dropped while a transaction was open")

    try:
        elapsed = measure_fence()
    except socket.timeout:
        print(f"FAIL: DB_FENCE did not return within {FENCE_TIMEOUT_SECONDS}s; "
              "the global epoch is frozen")
        return 1
    print(f"  DB_FENCE after the disconnect: {elapsed:.3f}s")

    # A second disconnect exercises the teardown path more than once on
    # different connection threads.
    leak_transaction()
    try:
        elapsed = measure_fence()
    except socket.timeout:
        print(f"FAIL: DB_FENCE did not return within {FENCE_TIMEOUT_SECONDS}s "
              "after a second disconnect")
        return 1
    print(f"  DB_FENCE after a second disconnect: {elapsed:.3f}s")

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
