"""An insert whose absent read outlived the reaper's purge must not win.

The reaper-ABA schedule: the insert's absent read resolves the tombstone's
index entry, the reaper purges it, and the insert then claims a fresh entry.
A row committed into that fresh entry must survive and the insert must be
refused at COMMIT. The server is started here rather than by the runner
because the reaper's sync point is armed through its environment and its
descriptors.
"""
import os
import select
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import mysql.connector

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.connection import get_connection  # noqa: E402

SERVER = os.path.join(ROOT, "build", "server", "helios-storage")
MYSQLD_PORT = "3307"
DBNAME = "ha_helios_purge_race"
SYNC_ENV = "HELIOS_DEBUG_SYNC_REAPER_PURGE_LOCKED_WINDOW"

SERVER_PORT_WAIT_SECONDS = 20
MYSQLD_PORT_WAIT_SECONDS = 90
ARRIVAL_WAIT_SECONDS = 30
STATEMENT_DEADLINE_SECONDS = 60.0
# The REPLACE commits on its own; the parked INSERT reaches the storage only
# at its COMMIT, which is issued after the REPLACE has installed its row.
REPLACE_INSTALL_SECONDS = 5.0
PURGE_RELEASE_DELAY_SECONDS = 1.5


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
        time.sleep(0.2)
    return False


def stop_stack():
    for script in ("stop_mysql.sh", "stop_server.sh"):
        subprocess.run([os.path.join(ROOT, "scripts", script)], cwd=ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)


def start_server(work_dir, arrived_w, release_r):
    env = dict(os.environ)
    env[SYNC_ENV] = f"arrive_and_wait:{arrived_w}:{release_r}"
    out = open(os.path.join(work_dir, "server.out"), "wb")
    process = subprocess.Popen([SERVER], cwd=work_dir, env=env,
                               stdin=subprocess.DEVNULL, stdout=out,
                               stderr=subprocess.STDOUT,
                               pass_fds=(arrived_w, release_r))
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


class Statement(threading.Thread):
    """Runs one statement on its own connection and keeps its outcome."""

    def __init__(self, sql, user, password):
        super().__init__(daemon=True)
        self.sql = sql
        self.user = user
        self.password = password
        self.errno = None
        self.error_message = None
        self.failed_to_run = None

    def run(self):
        try:
            connection = get_connection(user=self.user, password=self.password)
            connection.autocommit = True
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute(self.sql)
                except mysql.connector.Error as err:
                    self.errno = err.errno
                    self.error_message = str(err)
                cursor.close()
            finally:
                connection.close()
        except Exception as failure:  # noqa: BLE001 - reported by the test
            self.failed_to_run = failure


def wait_for_arrival(arrived_r, timeout):
    readable, _, _ = select.select([arrived_r], [], [], timeout)
    if not readable:
        return False
    os.read(arrived_r, 1)
    return True


def commit_refusal_names_the_race(errno, message):
    # The refusal is made at the commit, which has no handler in scope, so it
    # arrives wrapped. The purge recycled the entry the absent read observed,
    # so the read fails validation (149) before the duplicate check runs; a
    # duplicate refusal (121) would mean the entry was never purged.
    return errno == 1180 and "Got error 149" in message


def test_insert_after_a_purged_absent_read(user, password,
                                           arrived_r, release_w):
    log("a row installed after the read's entry was purged must survive")
    connection = get_connection(user=user, password=password)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"""CREATE TABLE {DBNAME}.t (
                           id INT NOT NULL,
                           v VARCHAR(32) NOT NULL,
                           PRIMARY KEY (id)
                       ) ENGINE = Helios""")
    cursor.execute(f"INSERT INTO {DBNAME}.t VALUES (1, 'old')")
    cursor.execute(f"DELETE FROM {DBNAME}.t WHERE id = 1")

    # The delete's tombstone becomes purge-eligible one full epoch later, and
    # the reaper then parks inside the purge's locked window.
    if not wait_for_arrival(arrived_r, ARRIVAL_WAIT_SECONDS):
        log(f"FAIL: the purge never reached its locked window within "
            f"{ARRIVAL_WAIT_SECONDS}s")
        return 1

    # The absent read resolves the parked entry and spins on its lock bit, so
    # it can only return after the release publishes the retired TID. A timer
    # thread releases the reaper; the read blocking for at least that long is
    # the proof that it resolved the entry before the purge.
    t_conn = get_connection(user=user, password=password)
    t_conn.autocommit = True
    t_cursor = t_conn.cursor()
    t_cursor.execute("BEGIN")
    releaser = threading.Timer(PURGE_RELEASE_DELAY_SECONDS,
                               os.write, args=(release_w, b"r"))
    releaser.start()
    read_started = time.monotonic()
    t_cursor.execute(f"SELECT v FROM {DBNAME}.t WHERE id = 1")
    absent = t_cursor.fetchall()
    read_elapsed = time.monotonic() - read_started
    if absent != []:
        log(f"FAIL: the read returned {absent}, expected no row")
        return 1
    if read_elapsed < PURGE_RELEASE_DELAY_SECONDS - 0.2:
        log(f"FAIL: the read returned in {read_elapsed:.2f}s, so it never "
            f"resolved the entry the reaper held")
        return 1

    # The INSERT stays in this transaction's write buffer; the replacer installs
    # and commits its row under the purged key first, and the COMMIT's
    # validation of that key refuses the INSERT.
    t_cursor.execute(f"INSERT INTO {DBNAME}.t VALUES (1, 'mine')")
    replacer = Statement(f"REPLACE INTO {DBNAME}.t VALUES (1, 'replacer')",
                         user, password)
    replacer.start()
    replacer.join(REPLACE_INSTALL_SECONDS)
    if replacer.failed_to_run is not None:
        log(f"FAIL: the REPLACE could not run: {replacer.failed_to_run!r}")
        return 1

    commit_errno = None
    commit_message = ""
    try:
        t_cursor.execute("COMMIT")
    except mysql.connector.Error as err:
        commit_errno = err.errno
        commit_message = str(err)

    replacer.join(STATEMENT_DEADLINE_SECONDS)
    if replacer.is_alive():
        log("FAIL: the REPLACE never returned")
        return 1
    if replacer.errno is not None:
        log(f"FAIL: the REPLACE was rejected with {replacer.errno}")
        return 1
    try:
        t_cursor.execute("ROLLBACK")
    except mysql.connector.Error:
        pass
    t_cursor.close()
    t_conn.close()

    if commit_errno is None:
        log("FAIL: the INSERT committed over the row installed in the "
            "entry it claimed after the purge")
        return 1
    if not commit_refusal_names_the_race(commit_errno, commit_message):
        log(f"FAIL: the INSERT was rejected with {commit_errno} "
            f"({commit_message}), not as the fresh-entry schedule refuses it")
        return 1

    cursor.execute(f"SELECT id, v FROM {DBNAME}.t ORDER BY id")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    if rows != [(1, "replacer")]:
        log(f"FAIL: table holds {rows}, expected the replaced row")
        return 1

    log(f"PASS (the INSERT was rejected: {commit_message})")
    return 0


def main():
    print("TEST: an insert must not overwrite a row installed in the entry it "
          "claimed after a purge")
    if not os.path.exists(SERVER):
        print(f"FAIL: server binary not found at {SERVER}")
        return 1
    if shutil.which("ss") is None:
        print("FAIL: ss is required to detect listening ports")
        return 1

    # The runner starts its own stack before each test; this one needs a server
    # whose debug sync point is armed.
    stop_stack()

    work_dir = tempfile.mkdtemp(prefix="helios_purge_race_")
    server = None
    arrived_r, arrived_w = os.pipe()
    release_r, release_w = os.pipe()
    failures = 0
    try:
        server = start_server(work_dir, arrived_w, release_r)
        start_mysqld()
        failures = test_insert_after_a_purged_absent_read(
            args.user, args.password, arrived_r, release_w)
    except Exception as failure:  # noqa: BLE001 - reported by the test
        print(f"FAIL: {failure!r}")
        failures = 1
    finally:
        for fd in (arrived_r, arrived_w, release_r, release_w):
            os.close(fd)
        stop_stack()
        # This server was started here, outside the scripts' bookkeeping, so it
        # is this test's to stop.
        if server is not None and server.poll() is None:
            server.kill()
            server.wait()
        shutil.rmtree(work_dir, ignore_errors=True)

    if failures:
        print("FAILED")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str,
                        help='name of user', default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='password for the user', default="")
    args = parser.parse_args()
    sys.exit(main())
