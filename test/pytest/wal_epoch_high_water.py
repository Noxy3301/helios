"""
Regression: a log whose frontier sits at the epoch high-water mark must stop
startup, on both startup paths.

Recovery resumes strictly above the frontier it read, so that a transaction
joining the frontier's own epoch cannot see that epoch already reported durable
and return a Sync acknowledgement before its record was written. Adding one to
an epoch at the wrap point would break the ordering that comparison rests on,
so the server refuses to start instead.

Both paths compute the resume epoch: with recovery the records are replayed
first, without recovery they are discarded and only the frontier is used. A
regression in either one is silent -- the server starts and serves -- so this
test drives the real binary and checks that it exits non-zero with the
high-water message and without reporting a successful initialization. The
control case, a frontier nowhere near the mark, checks the opposite pair: the
server initializes and the high-water message never appears.

Not a LineairDB gtest death test: constructing a Database in-process hangs in
this fork for reasons unrelated to durability, so the child would hang rather
than die.
"""
import os
import struct
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER = os.path.join(REPO, "build", "server", "lineairdb-server")

# EpochFramework::kEpochHighWater
HIGH_WATER = 0xFFFFFFFF - (1 << 20)

# Wal frame constants, little-endian.
MAGIC = 0x4C57414C
VERSION = 1
FLAGS = 0
HEADER = struct.Struct("<IHHIII")

STARTUP_TIMEOUT_SECONDS = 20


def crc32c(data):
    """CRC-32C (Castagnoli), reflected polynomial 0x82F63B78."""
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def make_frame(epoch):
    """One frame carrying a single record with no key-value pairs.

    An empty pair list is enough: the scanner checks that the frame decodes, that
    it holds at least one record, and that each record's epoch matches the
    header. It does not require a payload, and recovery simply replays nothing.
    msgpack encoding of [[epoch, []]]: a one-element array holding a two-element
    array of a uint32 and an empty array.
    """
    payload = b"\x91\x92" + b"\xce" + struct.pack(">I", epoch) + b"\x90"
    header_without_crc = struct.pack("<IHHII", MAGIC, VERSION, FLAGS,
                                     len(payload), epoch)
    checksum = crc32c(header_without_crc + payload)
    return header_without_crc + struct.pack("<I", checksum) + payload


def run_server(work_dir, extra_env):
    """Starts the server with work_dir as its cwd and returns (rc, output)."""
    env = {**os.environ, "LINEAIRDB_EPOCH_DURATION_MS": "10", **extra_env}
    env.pop("LINEAIRDB_ENABLE_RECOVERY", None)
    env.update(extra_env)
    try:
        done = subprocess.run([SERVER], cwd=work_dir, env=env,
                              stdin=subprocess.DEVNULL,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, timeout=STARTUP_TIMEOUT_SECONDS)
        return done.returncode, done.stdout
    except subprocess.TimeoutExpired as expired:
        # Still running at the deadline, which is what a server that was not
        # refused does when the port is free. TimeoutExpired carries its output
        # undecoded even when the call asked for text.
        captured = expired.stdout or b""
        if isinstance(captured, bytes):
            captured = captured.decode("utf-8", errors="replace")
        return None, captured


def with_crafted_log(frontier, extra_env):
    with tempfile.TemporaryDirectory(prefix="helios_high_water_") as work_dir:
        log_dir = os.path.join(work_dir, "lineairdb_logs")
        os.makedirs(log_dir)
        with open(os.path.join(log_dir, "wal.log"), "wb") as wal:
            wal.write(make_frame(frontier))
        return run_server(work_dir, extra_env)


def expect_refusal(label, frontier, extra_env):
    rc, output = with_crafted_log(frontier, extra_env)
    if rc is None:
        print(f"FAIL [{label}]: the server kept running instead of refusing to "
              f"start")
        return False
    if rc == 0:
        print(f"FAIL [{label}]: the server exited cleanly (rc=0) instead of "
              f"failing startup")
        return False
    # Distinguishes a startup refusal from the epoch writer's later abort, which
    # is what happens if the bound is put on the frontier instead of on the
    # epoch that startup resumes at.
    if "high-water mark" not in output:
        print(f"FAIL [{label}]: exited rc={rc} but not for the high-water "
              f"reason; output was:\n{output[-800:]}")
        return False
    if "server initialized successfully" in output:
        print(f"FAIL [{label}]: the server finished initializing before failing; "
              f"the refusal must happen during startup")
        return False
    print(f"  [{label}] refused to start, rc={rc}")
    return True


def expect_startup(label, frontier, extra_env):
    """The control: startup must not be refused for a frontier far from the mark.

    The oracle is the startup decision, not the exit code. Whether the process
    then keeps running depends on whether port 9999 is free, and the test suite
    runs with a server already listening on it -- so it reaches initialization
    and exits on the bind instead of serving.
    """
    rc, output = with_crafted_log(frontier, extra_env)
    if "high-water mark" in output:
        print(f"FAIL [{label}]: startup was refused for a frontier that is "
              f"nowhere near the mark; output was:\n{output[-800:]}")
        return False
    if "server initialized successfully" not in output:
        print(f"FAIL [{label}]: the server never finished initializing "
              f"(rc={rc}); output was:\n{output[-800:]}")
        return False
    print(f"  [{label}] initialized without refusing")
    return True


def main():
    print("TEST: a resumed epoch reaching the high-water mark refuses startup")
    if not os.path.exists(SERVER):
        print(f"FAIL: server binary not found at {SERVER}")
        return 1

    # The scan runs under every contract, so the mode here only has to be one
    # that keeps a log; recovery is what the two cases vary.
    off = {"LINEAIRDB_COMMIT_DURABILITY": "async"}
    on = {"LINEAIRDB_COMMIT_DURABILITY": "async",
          "LINEAIRDB_ENABLE_RECOVERY": "1"}

    ok = True
    # Both startup paths compute the resume epoch: without recovery the records
    # are discarded and only the frontier is used, with recovery the frontier is
    # combined with the epochs seen while replaying.
    ok &= expect_refusal("at the mark, recovery off", HIGH_WATER, off)
    ok &= expect_refusal("at the mark, recovery on", HIGH_WATER, on)
    # One below the mark still has to be refused: resuming above it lands exactly
    # on the mark, which the epoch writer cannot advance past.
    ok &= expect_refusal("one below the mark, recovery off", HIGH_WATER - 1, off)
    ok &= expect_refusal("one below the mark, recovery on", HIGH_WATER - 1, on)
    # The scanner accepts UINT32_MAX by design, so the refusal has to be written
    # so that adding one cannot wrap past it and resume the epoch at zero -- the
    # value that means "no participant".
    ok &= expect_refusal("maximum epoch, recovery off", 0xFFFFFFFF, off)
    ok &= expect_refusal("maximum epoch, recovery on", 0xFFFFFFFF, on)
    # A frontier far from the mark must still start, so the refusal above is the
    # boundary and not "any crafted log is rejected".
    ok &= expect_startup("far from the mark, recovery on", 1000, on)
    if not ok:
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
