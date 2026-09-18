"""
Switching a running server from Async to Sync, observed from outside.

A load runs faster under Async, and the load is not part of a measurement, so
the run wants Async while it populates and Sync from the first measured
transaction onwards. One scenario: after
`SET GLOBAL helios_commit_durability = 'sync'`, a commit behaves as it does on
a server that started Sync, which is a bounded wait plus an order. The wait is
that nothing may be acknowledged during NOT_ANSWERED_SECONDS while that
commit's fdatasync is held at a sync point; the order is that releasing the
point lets it return, and a replay of the log finds every row that returned.

The server is started by this test rather than by the runner: the debug sync
handshake passes pipe descriptors to it, and its working directory is a
temporary one so the log under test is the only log involved. It still uses
9999 and 3307 and stops whatever stack holds them, so nothing else may run
beside it; the runner restarts the stack around each file.
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.server_conf import write_conf

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER = os.path.join(ROOT, "build", "server", "helios-storage")
MYSQL_SOCKET = "/tmp/mysql.sock"
MYSQLD_PORT = "3307"

SERVER_PORT_WAIT_SECONDS = 20
MYSQLD_PORT_WAIT_SECONDS = 90
# start_mysql.sh polls for readiness without a bound of its own
MYSQLD_START_TIMEOUT_SECONDS = 180
ARRIVAL_WAIT_SECONDS = 20
# Well above one epoch window plus an fdatasync, so a command or a commit that
# is not waiting has certainly returned by the time this elapses.
NOT_ANSWERED_SECONDS = 1.5

WAL_POINT = "HELIOS_DEBUG_SYNC_WAL_BEFORE_FDATASYNC"


def log(message):
    print(f"\t{message}", flush=True)


class SyncPoint:
    """One debug sync point, armed through the server's environment."""

    def __init__(self, variable):
        self.variable = variable
        self.arrived_r, self.arrived_w = os.pipe()
        self.release_r, self.release_w = os.pipe()

    @property
    def action(self):
        return f"arrive_and_wait:{self.arrived_w}:{self.release_r}"

    @property
    def pass_fds(self):
        return (self.arrived_w, self.release_r)

    def wait(self, timeout):
        readable, _, _ = select.select([self.arrived_r], [], [], timeout)
        if not readable:
            return False
        os.read(self.arrived_r, 1)
        return True

    def release(self):
        os.write(self.release_w, b"r")

    def drain(self, idle=0.5):
        """Releases everything already parked, leaving the point idle."""
        released = 0
        while self.wait(idle):
            self.release()
            released += 1
        return released

    def close(self):
        for fd in (self.arrived_r, self.arrived_w, self.release_r,
                   self.release_w):
            try:
                os.close(fd)
            except OSError:
                pass


def env_for(points):
    return {point.variable: point.action for point in points}


def fds_for(points):
    return tuple(fd for point in points for fd in point.pass_fds)


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


def wait_for_port_free(port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not port_is_open(port):
            return True
        time.sleep(0.25)
    return False


def stop_stack():
    """Leaves both ports free, or says so.

    A scenario that starts while the previous server still holds 9999 binds
    nothing and talks to that server instead, which reads as a wrong answer
    rather than as a failure to start.
    """
    for _ in range(3):
        subprocess.run([os.path.join(ROOT, "scripts", "stop_mysql.sh")],
                       cwd=ROOT, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        subprocess.run([os.path.join(ROOT, "scripts", "stop_server.sh")],
                       cwd=ROOT, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        if wait_for_port_free(9999, 10) and wait_for_port_free(MYSQLD_PORT, 10):
            time.sleep(0.5)
            return
    raise RuntimeError("ports 9999/3307 are still held after stop_stack")


def start_server(work_dir, mode, conf=None, extra_env=None, pass_fds=()):
    """Starts the server directly, so its log directory and inherited
    descriptors are the test's to choose."""
    # The debug sync points are the one knob that stays in the environment.
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    path = write_conf(work_dir, commit_durability=mode, epoch_duration_ms=40,
                      **(conf or {}))
    if port_is_open(9999):
        raise RuntimeError("port 9999 is already held; this server would not "
                           "be the one under test")
    out = open(os.path.join(work_dir, "server.out"), "ab")
    process = subprocess.Popen([SERVER, "--config", path], cwd=work_dir, env=env,
                               stdin=subprocess.DEVNULL, stdout=out,
                               stderr=subprocess.STDOUT, pass_fds=pass_fds)
    if not wait_for_port(9999, SERVER_PORT_WAIT_SECONDS):
        process.kill()
        raise RuntimeError("the storage server never listened on 9999")
    return process


def start_mysqld():
    # Its own session, so a script stuck in a readiness loop can be killed
    # together with everything it forked.
    script = subprocess.Popen(
        ["./scripts/start_mysql.sh",
         "--mysqld-port", MYSQLD_PORT,
         "--server-host", "127.0.0.1", "--server-port", "9999"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True)
    try:
        output = script.communicate(timeout=MYSQLD_START_TIMEOUT_SECONDS)[0]
    except subprocess.TimeoutExpired:
        os.killpg(script.pid, signal.SIGKILL)
        script.wait()
        subprocess.run([os.path.join(ROOT, "scripts", "stop_mysql.sh")],
                       cwd=ROOT, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        raise RuntimeError("the mysqld start script did not finish within "
                           f"{MYSQLD_START_TIMEOUT_SECONDS}s")
    if not wait_for_port(MYSQLD_PORT, MYSQLD_PORT_WAIT_SECONDS):
        raise RuntimeError(f"mysqld never listened on {MYSQLD_PORT}: "
                           f"{output[-500:]}")
    # The port opening is not the readiness these tests need: every statement
    # goes through the socket, and a dying instance can still hold the port.
    deadline = time.monotonic() + MYSQLD_PORT_WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            sql("SELECT 1;")
            return
        except RuntimeError:
            time.sleep(0.5)
    raise RuntimeError(f"mysqld never answered on {MYSQL_SOCKET}: "
                       f"{output[-500:]}")


def sql(statements):
    """Runs statements through the client and returns stdout."""
    client = os.path.join(ROOT, "build", "runtime_output_directory", "mysql")
    done = subprocess.run([client, "-u", "root", f"--socket={MYSQL_SOCKET}",
                           "--batch", "--skip-column-names", "-e", statements],
                          cwd=ROOT, capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"SQL failed: {statements}\n{done.stderr[-500:]}")
    return done.stdout


def create_table(name):
    sql("CREATE DATABASE IF NOT EXISTS dur;")
    sql(f"USE dur; DROP TABLE IF EXISTS {name};"
        f" CREATE TABLE {name} (id INT PRIMARY KEY, v INT) ENGINE=helios;")


def start_switch(mode):
    """Runs the switch statement without waiting for it."""
    client = os.path.join(ROOT, "build", "runtime_output_directory", "mysql")
    return subprocess.Popen(
        [client, "-u", "root", "--protocol=TCP", "-h", "127.0.0.1",
         "-P", MYSQLD_PORT, "-e",
         f"SET GLOBAL helios_commit_durability = '{mode}'"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


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


def release_until(point, done, timeout, parked):
    """Keeps releasing arrivals at `point` until `done()` holds.

    A record can sit in a later group than the one that was held, and every
    group stops at the point; what the caller established before calling this
    is that nothing returned while a group was stopped.

    `parked` says whether the caller already consumed an arrival and owes it a
    release. Every release must answer exactly one arrival: a spare token stays
    in the pipe and would wave the next arrival straight through, which reads
    as "stopped" to the test and is not.
    """
    deadline = time.monotonic() + timeout
    if parked:
        point.release()
    while not done() and time.monotonic() < deadline:
        if point.wait(0.05):
            point.release()
        else:
            time.sleep(0.05)
    return done()


def kill_now(server):
    """SIGKILL both processes: neither shutdown path may write anything."""
    server.send_signal(signal.SIGKILL)
    server.wait(timeout=10)
    subprocess.run([os.path.join(ROOT, "scripts", "stop_mysql.sh")],
                   cwd=ROOT, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)


def recovered_rows(work_dir, table):
    """Restarts the server with recovery and reads the table back."""
    server = start_server(work_dir, "async", conf={"enable_recovery": 1})
    start_mysqld()
    rows = {}
    for line in sql(f"SELECT id, v FROM dur.{table} ORDER BY id;") \
            .strip().splitlines():
        key, value = line.split()
        rows[int(key)] = int(value)
    return server, rows


def test_the_sync_contract_applies_after_the_switch(work_dir):
    log("after the switch a commit must wait for its own fdatasync")
    wal = SyncPoint(WAL_POINT)
    points = (wal,)
    server = None
    try:
        server = start_server(work_dir, "async", extra_env=env_for(points),
                              pass_fds=fds_for(points))
        start_mysqld()
        create_table("after")
        time.sleep(1.0)
        wal.drain()

        expected = {1: 10}
        sql("USE dur; INSERT INTO after VALUES (1, 10);")
        switch = start_switch("sync")
        # Nothing consumed yet: the loop pairs each arrival itself.
        if not release_until(wal, lambda: switch.poll() is not None,
                             ARRIVAL_WAIT_SECONDS, parked=False):
            switch.kill()
            log("FAIL: the switch never returned")
            return 1
        stdout, stderr = switch.communicate()
        if switch.returncode != 0:
            log(f"FAIL: the switch reported rc={switch.returncode} "
                f"stdout={stdout.strip()!r} stderr={stderr.strip()!r}")
            return 1
        wal.drain()

        for row in (2, 3):
            committer = Committer(
                f"USE dur; INSERT INTO after VALUES ({row}, {row * 10});")
            committer.start()
            if not wal.wait(ARRIVAL_WAIT_SECONDS):
                log(f"FAIL: row {row} caused no flush to reach the point")
                return 1
            committer.join(NOT_ANSWERED_SECONDS)
            if committer.error is not None:
                log(f"FAIL: the INSERT failed rather than waiting: "
                    f"{committer.error}")
                return 1
            if committer.returned_at is not None:
                log(f"FAIL: row {row} was acknowledged while the fdatasync "
                    f"that would make it durable was still stopped")
                return 1
            if not release_until(wal,
                                 lambda: committer.returned_at is not None
                                 or committer.error is not None,
                                 ARRIVAL_WAIT_SECONDS, parked=True):
                log(f"FAIL: row {row} never returned after the release")
                return 1
            if committer.error is not None:
                log(f"FAIL: the INSERT failed after release: "
                    f"{committer.error}")
                return 1
            expected[row] = row * 10
        log(f"every row after the switch waited for its own fdatasync: "
            f"{sorted(set(expected) - {1})}")

        wal.drain()
        kill_now(server)
        server, rows = recovered_rows(work_dir, "after")
        if rows != expected:
            log(f"FAIL: recovered {rows}, expected {expected}")
            return 1
        log(f"recovered every acknowledged row: {rows}")
        log("PASS")
        return 0
    finally:
        for point in points:
            try:
                point.drain(0.2)
            except OSError:
                pass
        stop_stack()
        if server is not None:
            server.poll()
        for point in points:
            point.close()


def main():
    print("TEST: the runtime switch from async to sync durability")
    if not os.path.exists(SERVER):
        print(f"FAIL: binary not found at {SERVER}")
        return 1
    if shutil.which("ss") is None:
        print("FAIL: ss is required to detect listening ports")
        return 1

    # The runner starts a stack before every test file; this scenario needs a
    # server it configured itself, so that one goes first.
    stop_stack()

    failures = 0
    for scenario in (test_the_sync_contract_applies_after_the_switch,):
        work_dir = tempfile.mkdtemp(prefix="helios_durability_switch_")
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
