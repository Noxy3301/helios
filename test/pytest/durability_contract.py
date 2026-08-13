"""
The Sync durability contract, observed from outside the server.

Two properties are checked, both of which a unit test cannot reach because both
are about what a client is told and when:

  1. Ordering. A Sync commit is acknowledged after its log reaches the device,
     never before. The proof stops the flusher immediately before it calls
     fdatasync: while it is parked there, the INSERT that is waiting for that
     flush must not have returned, and it must return once the flusher is
     released. A sleep would only widen a window; parking the flusher
     establishes the order.

  2. Survival. Every row whose INSERT returned under Sync is present after the
     server is killed and restarted with recovery. This is a process-crash
     test, not a power-loss test: SIGKILL leaves the page cache intact, so it
     shows that the acknowledgement matched a completed write, not that the
     device retained it.

The server is started by this test rather than by the runner: the debug sync
handshake passes pipe descriptors to it, and its working directory is a
temporary one so the log under test is the only log involved.
"""
import os
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER = os.path.join(ROOT, "build", "server", "lineairdb-server")
MYSQL_SOCKET = "/tmp/mysql.sock"
MYSQLD_PORT = "3307"

SERVER_PORT_WAIT_SECONDS = 20
MYSQLD_PORT_WAIT_SECONDS = 90
ARRIVAL_WAIT_SECONDS = 20
# Well above one epoch window plus an fdatasync, so a commit that is not
# waiting has certainly returned by the time this elapses.
NOT_ANSWERED_SECONDS = 1.5
ROWS = 5


def log(message):
    print(f"\t{message}", flush=True)


def port_is_open(port):
    result = subprocess.run(["ss", "-ltn"], capture_output=True, text=True)
    return f":{port}" in result.stdout


def wait_for_port(port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if port_is_open(port):
            return True
        time.sleep(0.25)
    return False


def stop_stack():
    subprocess.run([os.path.join(ROOT, "scripts", "stop_mysql.sh")],
                   cwd=ROOT, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    subprocess.run([os.path.join(ROOT, "scripts", "stop_server.sh")],
                   cwd=ROOT, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    time.sleep(1)


def start_server(work_dir, mode, extra_env=None, pass_fds=()):
    """Starts the server directly, so its log directory and inherited
    descriptors are the test's to choose."""
    env = dict(os.environ)
    env["LINEAIRDB_COMMIT_DURABILITY"] = mode
    env["LINEAIRDB_EPOCH_DURATION_MS"] = "40"
    env.pop("LINEAIRDB_ENABLE_RECOVERY", None)
    if extra_env:
        env.update(extra_env)
    out = open(os.path.join(work_dir, "server.out"), "wb")
    process = subprocess.Popen([SERVER], cwd=work_dir, env=env,
                               stdin=subprocess.DEVNULL, stdout=out,
                               stderr=subprocess.STDOUT, pass_fds=pass_fds)
    if not wait_for_port(9999, SERVER_PORT_WAIT_SECONDS):
        process.kill()
        raise RuntimeError("the storage server never listened on 9999")
    return process


def start_mysqld():
    done = subprocess.run(
        ["setsid", "-w", "./scripts/start_mysql.sh",
         "--mysqld-port", MYSQLD_PORT,
         "--server-host", "127.0.0.1", "--server-port", "9999"],
        cwd=ROOT, capture_output=True, text=True)
    if not wait_for_port(MYSQLD_PORT, MYSQLD_PORT_WAIT_SECONDS):
        raise RuntimeError(f"mysqld never listened on {MYSQLD_PORT}: "
                           f"{done.stdout[-500:]}")


def sql(statements, expect_rows=False):
    """Runs statements through the client and returns stdout."""
    client = os.path.join(ROOT, "build", "runtime_output_directory", "mysql")
    done = subprocess.run([client, "-u", "root", f"--socket={MYSQL_SOCKET}",
                           "--batch", "--skip-column-names", "-e", statements],
                          cwd=ROOT, capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"SQL failed: {statements}\n{done.stderr[-500:]}")
    return done.stdout


def drain_arrivals(arrived_r, release_w):
    """Releases every sync point already waiting.

    Startup and DDL commit their own records, and each of those flushes stops
    at the point too. Releasing them first leaves the flusher idle, so the next
    arrival is the one this test caused.
    """
    released = 0
    while True:
        readable, _, _ = select.select([arrived_r], [], [], 0.5)
        if not readable:
            return released
        os.read(arrived_r, 1)
        os.write(release_w, b"r")
        released += 1


def wait_for_arrival(arrived_r, timeout):
    readable, _, _ = select.select([arrived_r], [], [], timeout)
    if not readable:
        return False
    os.read(arrived_r, 1)
    return True


class Committer(threading.Thread):
    """One autocommit INSERT, so returning means the commit was acknowledged."""

    def __init__(self, statement):
        super().__init__(daemon=True)
        self.statement = statement
        self.returned_at = None
        self.error = None

    def run(self):
        try:
            sql(self.statement)
            self.returned_at = time.monotonic()
        except Exception as failure:  # noqa: BLE001 - reported by the test
            self.error = failure


def test_acknowledgement_follows_the_fdatasync(work_dir):
    log("Sync: the acknowledgement must follow the fdatasync")
    arrived_r, arrived_w = os.pipe()
    release_r, release_w = os.pipe()
    server = None
    try:
        point = f"arrive_and_wait:{arrived_w}:{release_r}"
        server = start_server(
            work_dir, "sync",
            extra_env={"LINEAIRDB_DEBUG_SYNC_WAL_BEFORE_FDATASYNC": point},
            pass_fds=(arrived_w, release_r))
        start_mysqld()

        sql("CREATE DATABASE IF NOT EXISTS dur;")
        sql("USE dur; DROP TABLE IF EXISTS ordering;"
            " CREATE TABLE ordering (id INT PRIMARY KEY, v INT)"
            " ENGINE=lineairdb;")
        # Let every flush the DDL caused reach the point, then clear them.
        time.sleep(1.0)
        released = drain_arrivals(arrived_r, release_w)
        log(f"released {released} flush(es) from startup and DDL")

        committer = Committer("USE dur; INSERT INTO ordering VALUES (1, 1);")
        committer.start()

        if not wait_for_arrival(arrived_r, ARRIVAL_WAIT_SECONDS):
            log("FAIL: the flush never reached the point before fdatasync")
            return 1
        held_at = time.monotonic()

        committer.join(NOT_ANSWERED_SECONDS)
        if committer.returned_at is not None:
            log("FAIL: the INSERT was acknowledged while the fdatasync that "
                "would make it durable was still stopped")
            return 1
        if committer.error is not None:
            log(f"FAIL: the INSERT failed rather than waiting: "
                f"{committer.error}")
            return 1
        log(f"held for {time.monotonic() - held_at:.2f}s with no "
            f"acknowledgement")

        released_at = time.monotonic()
        os.write(release_w, b"r")
        # The row's record may sit in a later group than the one that was held,
        # and each group stops at the point. Keep releasing until the commit is
        # acknowledged: what the assertion above established is that it was not
        # acknowledged while a flush was stopped.
        deadline = released_at + ARRIVAL_WAIT_SECONDS
        while (committer.returned_at is None and committer.error is None
               and time.monotonic() < deadline):
            committer.join(0.2)
            if wait_for_arrival(arrived_r, 0.05):
                os.write(release_w, b"r")
        if committer.returned_at is None:
            log("FAIL: the INSERT never returned after the fdatasync was "
                "released")
            return 1
        if committer.error is not None:
            log(f"FAIL: the INSERT failed after release: {committer.error}")
            return 1
        log(f"acknowledged {committer.returned_at - released_at:.3f}s after "
            f"release")

        # Release anything the row's own flush queued behind it, so shutdown
        # does not block on the point.
        drain_arrivals(arrived_r, release_w)
        rows = sql("SELECT v FROM dur.ordering WHERE id = 1;").split()
        if rows != ["1"]:
            log(f"FAIL: the acknowledged row is not readable: {rows}")
            return 1
        log("PASS")
        return 0
    finally:
        # Order matters: the flusher may be parked at the point, and shutdown
        # drains it.
        try:
            drain_arrivals(arrived_r, release_w)
        except OSError:
            pass
        stop_stack()
        if server is not None:
            server.poll()
        for fd in (arrived_r, arrived_w, release_r, release_w):
            try:
                os.close(fd)
            except OSError:
                pass


def test_acknowledged_rows_survive_a_process_crash(work_dir):
    log("Sync: acknowledged rows must survive SIGKILL and recovery")
    server = None
    try:
        server = start_server(work_dir, "sync")
        start_mysqld()

        sql("CREATE DATABASE IF NOT EXISTS dur;")
        sql("USE dur; DROP TABLE IF EXISTS crash;"
            " CREATE TABLE crash (id INT PRIMARY KEY, v INT)"
            " ENGINE=lineairdb;")
        acknowledged = []
        for row in range(1, ROWS + 1):
            sql(f"USE dur; INSERT INTO crash VALUES ({row}, {row * 10});")
            acknowledged.append(row)
        log(f"acknowledged rows: {acknowledged}")

        wal = os.path.join(work_dir, "lineairdb_logs", "wal.log")
        size_before = os.path.getsize(wal)

        # SIGKILL, not shutdown: a clean stop would flush and prove nothing.
        subprocess.run([os.path.join(ROOT, "scripts", "stop_mysql.sh")],
                       cwd=ROOT, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        server.send_signal(signal.SIGKILL)
        server.wait(timeout=10)
        server = None
        log(f"killed the server; wal.log is {size_before} bytes")

        server = start_server(work_dir, "sync",
                              extra_env={"LINEAIRDB_ENABLE_RECOVERY": "1"})
        start_mysqld()
        rows = sql("SELECT id, v FROM dur.crash ORDER BY id;")
        recovered = {}
        for line in rows.strip().splitlines():
            key, value = line.split()
            recovered[int(key)] = int(value)
        expected = {row: row * 10 for row in acknowledged}
        if recovered != expected:
            log(f"FAIL: recovered {recovered}, expected {expected}")
            return 1
        log(f"recovered every acknowledged row: {recovered}")
        log("PASS")
        return 0
    finally:
        stop_stack()
        if server is not None:
            server.poll()


def test_a_failed_fdatasync_is_never_acknowledged(work_dir):
    log("Sync: a commit whose fdatasync fails must not be acknowledged")
    server = None
    try:
        # The first group flush fails. Table creation writes no records, so the
        # first group is the one the INSERT below produces.
        server = start_server(
            work_dir, "sync",
            extra_env={"LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER": "0"})
        start_mysqld()
        sql("CREATE DATABASE IF NOT EXISTS dur;")
        sql("USE dur; DROP TABLE IF EXISTS failed_sync;"
            " CREATE TABLE failed_sync (id INT PRIMARY KEY, v INT)"
            " ENGINE=lineairdb;")

        committer = Committer("USE dur; INSERT INTO failed_sync VALUES (1, 1);")
        committer.start()
        committer.join(ARRIVAL_WAIT_SECONDS)

        if committer.returned_at is not None:
            log("FAIL: the INSERT was acknowledged although the fdatasync that "
                "would make it durable returned EIO")
            return 1
        log(f"the INSERT did not succeed: "
            f"{'error' if committer.error else 'still waiting'}")

        # Fail-stop is the other half of the contract: a process that cannot
        # write the log must not go on acknowledging commits.
        for _ in range(40):
            if server.poll() is not None:
                break
            time.sleep(0.25)
        status = server.poll()
        if status is None:
            log("FAIL: the server kept running with a log it cannot write")
            return 1
        if status != -signal.SIGABRT:
            log(f"FAIL: the server exited with {status}, not SIGABRT")
            return 1
        output = open(os.path.join(work_dir, "server.out"),
                      encoding="utf-8", errors="replace").read()
        if "the log cannot be written" not in output:
            log(f"FAIL: no durability error in the server output:\n"
                f"{output[-500:]}")
            return 1
        log("the server stopped with the durability error")
        server = None
        log("PASS")
        return 0
    finally:
        stop_stack()
        if server is not None:
            server.poll()


def main():
    print("TEST: the Sync commit durability contract")
    if not os.path.exists(SERVER):
        print(f"FAIL: server binary not found at {SERVER}")
        return 1
    if shutil.which("ss") is None:
        print("FAIL: ss is required to detect listening ports")
        return 1

    # The runner starts its own stack before each test; this one needs a server
    # it configured itself.
    stop_stack()

    failures = 0
    for scenario in (test_acknowledgement_follows_the_fdatasync,
                     test_acknowledged_rows_survive_a_process_crash,
                     test_a_failed_fdatasync_is_never_acknowledged):
        work_dir = tempfile.mkdtemp(prefix="helios_durability_")
        try:
            failures += scenario(work_dir)
        except Exception as failure:  # noqa: BLE001 - one scenario failing
            log(f"FAIL: {scenario.__name__} raised {failure!r}")
            failures += 1
            stop_stack()
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    if failures:
        print("FAILED")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
