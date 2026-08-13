"""
The reservation size the write-ahead log starts with is taken from
LINEAIRDB_WAL_INITIAL_CAPACITY_BYTES, and a value that does not parse exactly has
to stop startup.

The value decides whether a commit's fdatasync also persists a new file size. A run
that silently fell back to a default would be labelled with a reservation it did not
have, and the measurement taken from it would be wrong in a way nothing else would
reveal. Refusing to start is what keeps that from happening.

The oracle is the startup decision rather than the exit code, because whether the
server then keeps running depends on whether port 9999 is free, and the suite runs
with one already listening on it.
"""
import os
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER = os.path.join(REPO, "build", "server", "lineairdb-server")

VARIABLE = "LINEAIRDB_WAL_INITIAL_CAPACITY_BYTES"
STARTUP_TIMEOUT_SECONDS = 20
# server/storage/database_manager.cc: kMaxWalCapacityBytes
MAX_BYTES = 64 * 1024**3


def run_server(value):
    """Starts the server with the variable set and returns its output."""
    env = {**os.environ, "LINEAIRDB_EPOCH_DURATION_MS": "10", VARIABLE: value}
    env.pop("LINEAIRDB_ENABLE_RECOVERY", None)
    with tempfile.TemporaryDirectory(prefix="helios_wal_capacity_") as work_dir:
        try:
            done = subprocess.run([SERVER], cwd=work_dir, env=env,
                                  stdin=subprocess.DEVNULL,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT,
                                  text=True,
                                  timeout=STARTUP_TIMEOUT_SECONDS)
            return done.returncode, done.stdout
        except subprocess.TimeoutExpired as expired:
            # Still running at the deadline, which is what a server that was not
            # refused does when the port is free. TimeoutExpired carries its output
            # undecoded even when the call asked for text.
            captured = expired.stdout or b""
            if isinstance(captured, bytes):
                captured = captured.decode("utf-8", errors="replace")
            return None, captured


def expect_refusal(label, value):
    rc, output = run_server(value)
    if rc is None:
        print(f"FAIL [{label}]: the server kept running instead of refusing "
              f"{VARIABLE}={value!r}")
        return False
    if f"Invalid {VARIABLE}" not in output:
        print(f"FAIL [{label}]: exited rc={rc} but not for the parsing reason; "
              f"output was:\n{output[-800:]}")
        return False
    if "server initialized successfully" in output:
        print(f"FAIL [{label}]: the server finished initializing before failing; "
              f"the refusal must happen during startup")
        return False
    print(f"  [{label}] refused to start, rc={rc}")
    return True


def expect_startup(label, value, reported):
    rc, output = run_server(value)
    if f"Invalid {VARIABLE}" in output:
        print(f"FAIL [{label}]: startup was refused for a value that parses; "
              f"output was:\n{output[-800:]}")
        return False
    if "server initialized successfully" not in output:
        print(f"FAIL [{label}]: the server never finished initializing (rc={rc}); "
              f"output was:\n{output[-800:]}")
        return False
    if reported not in output:
        print(f"FAIL [{label}]: the accepted value was not reported as {reported!r}; "
              f"output was:\n{output[-800:]}")
        return False
    print(f"  [{label}] started and reported the value")
    return True


def main():
    if not os.path.exists(SERVER):
        print(f"FAIL: {SERVER} is missing; build the server first")
        return 1

    ok = True
    ok &= expect_refusal("not a number", "abc")
    ok &= expect_refusal("trailing text", "1048576x")
    ok &= expect_refusal("empty", "")
    ok &= expect_refusal("negative", "-1")
    ok &= expect_refusal("above the maximum", str(MAX_BYTES + 1))
    # Accepted: a small reservation, and zero for a log that grows as it is written.
    ok &= expect_startup("one mebibyte", "1048576", "1048576 bytes")
    ok &= expect_startup("no reservation", "0", "0 bytes")

    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
