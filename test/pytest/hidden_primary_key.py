"""Hidden primary keys are unique across query layers.

Most cases use the storage server on 9999 and a second mysqld on 3308 beside
the caller's 3307. The cases that restart a server need their own, so each
brings up a private stack on HELIOS_TEST_SERVER_PORT and stops it by pid, never
through stop_server.sh or stop_mysql.sh, which kill by command-line substring
and would take down another checkout's stack.
"""
import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

import mysql.connector
from mysql.connector import errorcode

from utils.connection import get_connection

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SCRIPTS_DIR = os.path.join(ROOT_DIR, "scripts")
SERVER_BIN = os.path.join(ROOT_DIR, "build", "server", "lineairdb-server")

SECOND_PORT = 3308
# The first start of an instance initializes its data directory
SECOND_START_TIMEOUT = 300.0
SECOND_STOP_TIMEOUT = 60.0

# Private stack for the storage-restart case. 19999 is taken on some machines,
# hence the override.
RESTART_SERVER_PORT = int(os.environ.get("HELIOS_TEST_SERVER_PORT", "19998"))
RESTART_MYSQLD_PORTS = (13307, 13308)
SERVER_START_TIMEOUT = 60.0

DATABASE = "ha_lineairdb_test"

# The plugin reserves hidden keys in blocks of this size, so a session that
# writes more than that has to refill mid-statement-stream.
HIDDEN_KEY_RANGE = 1000
ROWS_PAST_ONE_RANGE = 1500
# Rows in the single-statement case, wider than one default range
ROWS_ONE_STATEMENT = 2500
ROWS_PER_STATEMENT = 100

_table_seq = 0


def unique_table(prefix):
    """A fresh name per run: DROP TABLE does not remove the stored rows."""
    global _table_seq
    _table_seq += 1
    return f"{prefix}_{int(time.time() * 1000000)}_{_table_seq}"


def run_script(argv, timeout):
    """Run a launcher script with stdin closed and a hard timeout."""
    try:
        return subprocess.run(
            argv,
            cwd=ROOT_DIR,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as err:
        print(f"\tFailed: {' '.join(argv)} timed out after {timeout:.0f}s")
        if err.stdout:
            print(err.stdout[-2000:])
        return None


def port_is_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(1.0)
        return probe.connect_ex(("127.0.0.1", port)) == 0


def own_pid(pid_file):
    """Pid in `pid_file`, but only when this checkout's tree owns the process."""
    try:
        with open(pid_file) as handle:
            pid = int(handle.read().strip())
    except (OSError, ValueError):
        return None
    try:
        cwd = os.readlink(f"/proc/{pid}/cwd")
    except OSError:
        return None
    # Other checkouts run their own instances; only ours may be signalled
    return pid if cwd.startswith(ROOT_DIR + os.sep) else None


def await_exit(pid, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            return True
        time.sleep(0.2)
    return False


def stop_mysqld(port):
    pid = own_pid(f"/tmp/mysql_{port}.pid")
    if pid is None:
        return True
    os.kill(pid, signal.SIGTERM)
    return await_exit(pid, SECOND_STOP_TIMEOUT)


def start_mysqld(port, server_port=9999):
    if port_is_open(port):
        if own_pid(f"/tmp/mysql_{port}.pid") is None:
            print(f"\tFailed: port {port} is held by a foreign process")
            return False
        if not stop_mysqld(port):
            print(f"\tFailed: the instance on port {port} did not stop")
            return False
    result = run_script(
        [os.path.join(SCRIPTS_DIR, "start_mysql.sh"),
         "--mysqld-port", str(port),
         "--server-host", "127.0.0.1",
         "--server-port", str(server_port)],
        timeout=SECOND_START_TIMEOUT,
    )
    if result is None:
        return False
    if result.returncode != 0:
        print(f"\tFailed: start_mysql.sh for port {port} exited {result.returncode}")
        print(result.stdout[-2000:])
        return False
    return True


def connection_on(port, user, password):
    # start_mysql.sh provisions and proves the loopback account over TCP
    return mysql.connector.connect(user=user, password=password,
                                   host="127.0.0.1", port=port)


def start_storage_server(work_dir, recovery=True):
    """Start a private storage server in `work_dir` (which owns its WAL).

    With recovery on the commit contract is Sync, so a row is on the device
    before the restart below; with it off the restart starts from nothing,
    which is what the reserved-but-not-durable window needs.
    """
    env = dict(os.environ)
    env["LINEAIRDB_SERVER_PORT"] = str(RESTART_SERVER_PORT)
    if recovery:
        env["LINEAIRDB_ENABLE_RECOVERY"] = "1"
        env["LINEAIRDB_COMMIT_DURABILITY"] = "sync"
    else:
        env.pop("LINEAIRDB_ENABLE_RECOVERY", None)
        env.pop("LINEAIRDB_COMMIT_DURABILITY", None)
    jemalloc = "/lib/x86_64-linux-gnu/libjemalloc.so.2"
    if os.path.exists(jemalloc):
        env["LD_PRELOAD"] = jemalloc

    log = open(os.path.join(work_dir, "server.log"), "a")
    process = subprocess.Popen([SERVER_BIN], cwd=work_dir, env=env,
                               stdin=subprocess.DEVNULL, stdout=log,
                               stderr=subprocess.STDOUT)
    deadline = time.time() + SERVER_START_TIMEOUT
    while time.time() < deadline:
        if process.poll() is not None:
            print(f"\tFailed: the storage server exited with "
                  f"{process.returncode}; see {work_dir}/server.log")
            return None
        if port_is_open(RESTART_SERVER_PORT):
            return process
        time.sleep(0.2)
    stop_storage_server(process)
    print(f"\tFailed: the storage server did not listen on "
          f"{RESTART_SERVER_PORT} within {SERVER_START_TIMEOUT:.0f}s")
    return None


def resume_lines(work_dir, table, offset):
    """Resume lines the server logged for `table` past `offset`.

    LOG_INFO writes to stderr, which start_storage_server sends to this file;
    the offset skips the run that wrote before a restart.
    """
    with open(os.path.join(work_dir, "server.log"), errors="replace") as handle:
        handle.seek(offset)
        return [line.rstrip() for line in handle
                if "resume at" in line and table in line]


def stop_storage_server(process):
    if process is None or process.poll() is not None:
        return
    process.kill()
    process.wait(timeout=SECOND_STOP_TIMEOUT)


def reset_schema(cursor):
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
    cursor.execute(f"CREATE DATABASE {DATABASE}")


def create_hidden_pk_table(cursor, table):
    cursor.execute(
        f"""CREATE TABLE {DATABASE}.{table} (
            payload VARCHAR(64) NOT NULL
        ) ENGINE = LineairDB"""
    )


def create_declared_pk_table(cursor, table):
    cursor.execute(
        f"""CREATE TABLE {DATABASE}.{table} (
            id INT NOT NULL,
            payload VARCHAR(64) NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE = LineairDB"""
    )


def insert_payloads(cursor, table, values):
    for start in range(0, len(values), ROWS_PER_STATEMENT):
        chunk = values[start:start + ROWS_PER_STATEMENT]
        rows = ", ".join(f"('{value}')" for value in chunk)
        cursor.execute(f"INSERT INTO {DATABASE}.{table} (payload) VALUES {rows}")


def read_payloads(cursor, table):
    cursor.execute(f"SELECT payload FROM {DATABASE}.{table}")
    return [row[0] for row in cursor.fetchall()]


def report_payloads(expected, observed):
    """One line describing how `observed` differs from `expected`."""
    missing = sorted(set(expected) - set(observed))
    unexpected = sorted(set(observed) - set(expected))
    return (f"{len(observed)} rows, expected {len(expected)}; "
            f"missing {missing[:5]}, unexpected {unexpected[:5]}")


def test_single_node_distinct_rows(cursor):
    print("HIDDEN PRIMARY KEY: ONE QUERY LAYER TEST")
    table = unique_table("hidden_single")
    create_hidden_pk_table(cursor, table)

    expected = [f"single-{i:04d}" for i in range(64)]
    insert_payloads(cursor, table, expected)

    observed = read_payloads(cursor, table)
    if sorted(observed) != sorted(expected):
        print(f"\tFailed: {report_payloads(expected, observed)}")
        return 1

    print("\tPassed!")
    return 0


def test_two_nodes_share_key_space(cursor, second_cursor):
    print("HIDDEN PRIMARY KEY: TWO QUERY LAYERS TEST")
    table = unique_table("hidden_two_nodes")
    create_hidden_pk_table(cursor, table)
    create_hidden_pk_table(second_cursor, table)

    first = [f"node1-{i:04d}" for i in range(64)]
    second = [f"node2-{i:04d}" for i in range(64)]
    insert_payloads(cursor, table, first)
    insert_payloads(second_cursor, table, second)

    expected = first + second
    for name, cur in (("the first", cursor), ("the second", second_cursor)):
        observed = read_payloads(cur, table)
        if sorted(observed) != sorted(expected):
            print(f"\tFailed: {name} query layer sees "
                  f"{report_payloads(expected, observed)}")
            return 1

    print("\tPassed!")
    return 0


def test_restart_does_not_reuse_ids(second_cursor, user, password):
    print("HIDDEN PRIMARY KEY: QUERY LAYER RESTART TEST")
    table = unique_table("hidden_restart")
    create_hidden_pk_table(second_cursor, table)

    before = [f"before-{i:04d}" for i in range(64)]
    insert_payloads(second_cursor, table, before)

    if not stop_mysqld(SECOND_PORT):
        print(f"\tFailed: the instance on port {SECOND_PORT} did not stop")
        return 1
    if not start_mysqld(SECOND_PORT):
        return 1

    restarted = connection_on(SECOND_PORT, user, password)
    restarted.autocommit = True
    restarted_cursor = restarted.cursor()
    try:
        after = [f"after-{i:04d}" for i in range(64)]
        insert_payloads(restarted_cursor, table, after)

        expected = before + after
        observed = read_payloads(restarted_cursor, table)
        if sorted(observed) != sorted(expected):
            print(f"\tFailed: {report_payloads(expected, observed)}")
            return 1
    finally:
        restarted_cursor.close()
        restarted.close()

    print("\tPassed!")
    return 0


def test_declared_primary_key_unchanged(cursor):
    print("HIDDEN PRIMARY KEY: DECLARED PRIMARY KEY UNAFFECTED TEST")
    table = unique_table("hidden_declared")
    create_declared_pk_table(cursor, table)

    rows = [(i, f"declared-{i:04d}") for i in range(64)]
    values = ", ".join(f"({i}, '{payload}')" for i, payload in rows)
    cursor.execute(f"INSERT INTO {DATABASE}.{table} (id, payload) VALUES {values}")

    cursor.execute(f"SELECT id, payload FROM {DATABASE}.{table} ORDER BY id")
    observed = cursor.fetchall()
    if [tuple(row) for row in observed] != rows:
        print(f"\tFailed: read back {len(observed)} rows, expected {len(rows)}")
        return 1

    cursor.execute(f"SELECT payload FROM {DATABASE}.{table} WHERE id = 7")
    point = cursor.fetchall()
    if [row[0] for row in point] != ["declared-0007"]:
        print(f"\tFailed: point lookup returned {point}")
        return 1

    print("\tPassed!")
    return 0


def test_multi_row_insert_one_statement(user, password):
    """One statement wider than a default range takes one reservation.

    MySQL hands its row estimate to start_bulk_insert, so 2500 rows come from
    a single reservation rather than three default blocks. Only the server says
    how wide it was, so this case needs a private stack whose log it can read.
    """
    print("HIDDEN PRIMARY KEY: MULTI-ROW INSERT SIZING TEST")
    port_a, port_b = RESTART_MYSQLD_PORTS

    if port_is_open(RESTART_SERVER_PORT):
        print(f"\tFailed: port {RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="hpk_sizing_")
    server = None
    started = []
    connections = []
    try:
        server = start_storage_server(work_dir, recovery=False)
        if server is None:
            return 1
        for port in (port_a, port_b):
            started.append(port)
            if not start_mysqld(port, RESTART_SERVER_PORT):
                return 1

        table = unique_table("hidden_multirow")
        first = connection_on(port_a, user, password)
        connections.append(first)
        first.autocommit = True
        cursor_a = first.cursor()
        reset_schema(cursor_a)
        create_hidden_pk_table(cursor_a, table)

        expected = [f"m-{i:05d}" for i in range(ROWS_ONE_STATEMENT)]
        rows = ", ".join(f"('{value}')" for value in expected)
        cursor_a.execute(f"INSERT INTO {DATABASE}.{table} (payload) VALUES {rows}")

        observed = read_payloads(cursor_a, table)
        if sorted(observed) != sorted(expected):
            print(f"\tFailed: {report_payloads(expected, observed)}")
            return 1

        # Re-creating the table clears the server's announced set while the
        # watermark row survives, so B's first reservation logs where the
        # counter actually stands.
        second = connection_on(port_b, user, password)
        connections.append(second)
        second.autocommit = True
        cursor_b = second.cursor()
        reset_schema(cursor_b)
        create_hidden_pk_table(cursor_b, table)

        log_offset = os.path.getsize(os.path.join(work_dir, "server.log"))
        insert_payloads(cursor_b, table, ["b-after"])

        # One sized reservation leaves the counter at 2500; three default
        # blocks would have left it at 3000.
        expected_resume = f"resume at {ROWS_ONE_STATEMENT}"
        resumed = resume_lines(work_dir, table, log_offset)
        if len(resumed) != 1 or not resumed[0].endswith(expected_resume):
            print(f"\tFailed: expected one '{expected_resume}' line, got {resumed}")
            return 1
    finally:
        for connection in connections:
            try:
                connection.close()
            except mysql.connector.Error:
                pass
        for port in started:
            if not stop_mysqld(port):
                print(f"\tWarning: the instance on port {port} is still running")
        stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)

    print("\tPassed!")
    return 0


def test_range_refill_two_connections(cursor, user, password):
    print("HIDDEN PRIMARY KEY: RANGE REFILL FROM TWO CONNECTIONS TEST")
    table = unique_table("hidden_refill")
    create_hidden_pk_table(cursor, table)

    batches = {
        "a": [f"a-{i:05d}" for i in range(ROWS_PAST_ONE_RANGE)],
        "b": [f"b-{i:05d}" for i in range(ROWS_PAST_ONE_RANGE)],
    }
    failures = {}

    def writer(name):
        connection = get_connection(user=user, password=password)
        connection.autocommit = True
        writer_cursor = connection.cursor()
        try:
            insert_payloads(writer_cursor, table, batches[name])
        except mysql.connector.Error as err:
            failures[name] = err
        finally:
            writer_cursor.close()
            connection.close()

    threads = [threading.Thread(target=writer, args=(name,))
               for name in batches]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if failures:
        print(f"\tFailed: {failures}")
        return 1

    expected = batches["a"] + batches["b"]
    observed = read_payloads(cursor, table)
    if sorted(observed) != sorted(expected):
        print(f"\tFailed: {report_payloads(expected, observed)}")
        return 1

    print("\tPassed!")
    return 0


def test_reservation_survives_storage_restart(user, password):
    """A restart must not reissue ids a query layer is still holding.

    A watermark that did not survive restarts at 0, inside the range A reserved
    and still mostly holds. Only the persisted one keeps B off those ids.
    """
    print("HIDDEN PRIMARY KEY: STORAGE SERVER RESTART TEST")
    port_a, port_b = RESTART_MYSQLD_PORTS

    if port_is_open(RESTART_SERVER_PORT):
        print(f"\tFailed: port {RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="hpk_restart_")
    server = None
    started = []
    connections = []
    try:
        server = start_storage_server(work_dir)
        if server is None:
            return 1
        for port in (port_a, port_b):
            # Recorded first: start_mysql.sh can leave a daemon behind when it
            # fails after launching it, and a timeout kills only the launcher
            started.append(port)
            if not start_mysqld(port, RESTART_SERVER_PORT):
                return 1

        table = unique_table("hidden_storage_restart")
        first = connection_on(port_a, user, password)
        connections.append(first)
        first.autocommit = True
        cursor_a = first.cursor()
        reset_schema(cursor_a)
        create_hidden_pk_table(cursor_a, table)

        second = connection_on(port_b, user, password)
        connections.append(second)
        second.autocommit = True
        cursor_b = second.cursor()
        reset_schema(cursor_b)
        # B only learns the table here; it must not reserve anything until the
        # server has restarted, or it would answer from its own cached range.
        create_hidden_pk_table(cursor_b, table)

        # One row reserves a whole range on A and writes the first id of it
        insert_payloads(cursor_a, table, ["seed"])

        # Both sessions die with the server; the mysqld processes stay up, and
        # with them the per-mysqld range A already holds.
        for connection in (first, second):
            connection.close()
            connections.remove(connection)

        # Taken before the restart so the resume check sees only the new run
        log_offset = os.path.getsize(os.path.join(work_dir, "server.log"))

        stop_storage_server(server)
        server = start_storage_server(work_dir)
        if server is None:
            return 1

        # B reserves from the restarted server first: a watermark that did not
        # survive restarts at 0, inside the range A still holds. Letting A
        # reserve first would move the counter past it and hide the overlap.
        resumed_b = connection_on(port_b, user, password)
        connections.append(resumed_b)
        resumed_b.autocommit = True
        cursor_resumed_b = resumed_b.cursor()
        from_b = [f"b-{i:04d}" for i in range(8)]
        insert_payloads(cursor_resumed_b, table, from_b)

        resumed_a = connection_on(port_a, user, password)
        connections.append(resumed_a)
        resumed_a.autocommit = True
        cursor_resumed_a = resumed_a.cursor()
        from_a = [f"a-{i:04d}" for i in range(8)]
        insert_payloads(cursor_resumed_a, table, from_a)

        # The payloads alone would not pin the value down: B's row at key 0
        # collides with the durable seed either way. The resume line asserts
        # the watermark itself, which a lost one would report as 0.
        expected_resume = f"resume at {HIDDEN_KEY_RANGE}"
        resumed = resume_lines(work_dir, table, log_offset)
        if len(resumed) != 1 or not resumed[0].endswith(expected_resume):
            print(f"\tFailed: expected one '{expected_resume}' line, got {resumed}")
            return 1

        expected = ["seed"] + from_a + from_b
        observed = read_payloads(cursor_resumed_b, table)
        if sorted(observed) != sorted(expected):
            print(f"\tFailed: {report_payloads(expected, observed)}")
            return 1
    finally:
        for connection in connections:
            try:
                connection.close()
            except mysql.connector.Error:
                pass
        for port in started:
            if not stop_mysqld(port):
                print(f"\tWarning: the instance on port {port} is still running")
        stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)

    print("\tPassed!")
    return 0


def test_reserved_range_dies_with_the_server(user, password):
    """A range reserved but never written must not survive its server.

    Nothing durable records the reservation at the moment it is handed out, so
    a restarted server can grant the same ids to someone else. The boot token
    is what stops the first layer from spending a range the new server has
    already given away.
    """
    print("HIDDEN PRIMARY KEY: RESERVED RANGE VS SERVER RESTART TEST")
    port_a, port_b = RESTART_MYSQLD_PORTS

    if port_is_open(RESTART_SERVER_PORT):
        print(f"\tFailed: port {RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="hpk_token_")
    server = None
    started = []
    connections = []
    try:
        # No recovery: the restart below comes up empty, which is exactly the
        # state a lost reservation leaves behind.
        server = start_storage_server(work_dir, recovery=False)
        if server is None:
            return 1
        for port in (port_a, port_b):
            started.append(port)
            if not start_mysqld(port, RESTART_SERVER_PORT):
                return 1

        table = unique_table("hidden_token")
        first = connection_on(port_a, user, password)
        connections.append(first)
        cursor_a = first.cursor()
        reset_schema(cursor_a)
        create_hidden_pk_table(cursor_a, table)

        # Reserve a range and write nothing under it: the INSERT reserves on
        # its way through, the ROLLBACK drops the row.
        cursor_a.execute("BEGIN")
        insert_payloads(cursor_a, table, ["discarded"])
        cursor_a.execute("ROLLBACK")
        first.commit()
        first.close()
        connections.remove(first)

        stop_storage_server(server)
        server = start_storage_server(work_dir, recovery=False)
        if server is None:
            return 1

        # B meets a server that knows nothing, so it is handed the same ids A
        # is still holding. B creates the table here, which also re-creates it
        # in the restarted server; A must not, or it would lose its range.
        second = connection_on(port_b, user, password)
        connections.append(second)
        second.autocommit = True
        cursor_b = second.cursor()
        reset_schema(cursor_b)
        create_hidden_pk_table(cursor_b, table)
        from_b = [f"b-{i:04d}" for i in range(8)]
        insert_payloads(cursor_b, table, from_b)

        resumed_a = connection_on(port_a, user, password)
        connections.append(resumed_a)
        resumed_a.autocommit = True
        cursor_resumed_a = resumed_a.cursor()
        from_a = [f"a-{i:04d}" for i in range(8)]
        insert_payloads(cursor_resumed_a, table, from_a)

        expected = from_a + from_b
        observed = read_payloads(cursor_resumed_a, table)
        if sorted(observed) != sorted(expected):
            print(f"\tFailed: {report_payloads(expected, observed)}")
            return 1
    finally:
        for connection in connections:
            try:
                connection.close()
            except mysql.connector.Error:
                pass
        for port in started:
            if not stop_mysqld(port):
                print(f"\tWarning: the instance on port {port} is still running")
        stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)

    print("\tPassed!")
    return 0


def test_prefetch_range_dies_with_the_server(user, password):
    """A connection must not spend a range its server no longer owns.

    The reservation rides the normal path even here: INSERT is not one of the
    statements thd_can_use_prefetch admits, so the sysvar below only covers the
    reads. What invalidates the cached range is the reset every transport
    failure performs; the proxy does not reconnect, so the connection that
    takes that failure is spent with it.
    """
    print("HIDDEN PRIMARY KEY: PREFETCH RANGE VS SERVER RESTART TEST")
    port_a, port_b = RESTART_MYSQLD_PORTS

    if port_is_open(RESTART_SERVER_PORT):
        print(f"\tFailed: port {RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="hpk_prefetch_")
    server = None
    started = []
    connections = []
    try:
        # No recovery: the restart hands the same ids out a second time
        server = start_storage_server(work_dir, recovery=False)
        if server is None:
            return 1
        for port in (port_a, port_b):
            started.append(port)
            if not start_mysqld(port, RESTART_SERVER_PORT):
                return 1

        table = unique_table("hidden_prefetch")
        first = connection_on(port_a, user, password)
        connections.append(first)
        first.autocommit = True
        cursor_a = first.cursor()
        reset_schema(cursor_a)
        # GLOBAL is the only scope the sysvar has; this mysqld is private to
        # the case, so nothing else sees it

        cursor_a.execute("SET GLOBAL lineairdb_prefetch_execution=ON")
        create_hidden_pk_table(cursor_a, table)
        # Reserves a range and spends its first id. The row itself dies with
        # the server below; the point is the rest of the range A still holds.
        insert_payloads(cursor_a, table, ["a-doomed"])

        stop_storage_server(server)
        server = start_storage_server(work_dir, recovery=False)
        if server is None:
            return 1

        # B meets a server that knows nothing and is handed the ids A holds.
        # Several rows, so B occupies the ids just above the one A spent: a
        # stale range put back to use would land on top of them.
        second = connection_on(port_b, user, password)
        connections.append(second)
        second.autocommit = True
        cursor_b = second.cursor()
        reset_schema(cursor_b)
        create_hidden_pk_table(cursor_b, table)
        from_b = [f"b-{i:04d}" for i in range(4)]
        insert_payloads(cursor_b, table, from_b)

        # A's socket died with the server. This write is the failed RPC that
        # resets the token, and it must not be reported as a duplicate key.
        try:
            insert_payloads(cursor_a, table, ["a-lost"])
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                print(f"\tFailed: stale range reported a duplicate: {err}")
                return 1

        # A fresh connection on the same mysqld must reserve rather than spend
        # what the old run granted.
        resumed_a = connection_on(port_a, user, password)
        connections.append(resumed_a)
        resumed_a.autocommit = True
        cursor_resumed_a = resumed_a.cursor()
        insert_payloads(cursor_resumed_a, table, ["a-after"])

        expected = from_b + ["a-after"]
        observed = read_payloads(cursor_resumed_a, table)
        if sorted(observed) != sorted(expected):
            print(f"\tFailed: {report_payloads(expected, observed)}")
            return 1
    finally:
        for connection in connections:
            try:
                connection.close()
            except mysql.connector.Error:
                pass
        for port in started:
            if not stop_mysqld(port):
                print(f"\tWarning: the instance on port {port} is still running")
        stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)

    print("\tPassed!")
    return 0


def prefetch_insert(cursor, table, payload):
    """One INSERT inside a transaction prefetch mode will take.

    Prefetch is fixed at the transaction's first statement and INSERT is not
    one thd_can_use_prefetch admits, so the SELECT has to come first for the
    INSERT to run on a prefetch connection at all.
    """
    cursor.execute("START TRANSACTION")
    cursor.execute(f"SELECT payload FROM {DATABASE}.{table}")
    cursor.fetchall()
    cursor.execute(f"INSERT INTO {DATABASE}.{table} (payload) VALUES ('{payload}')")
    cursor.execute("COMMIT")


def test_prefetch_reservation_dies_with_the_server(user, password):
    """A prefetch transaction must not spend a range from a dead run.

    A prefetch connection never sends TX_BEGIN, so it cannot learn the current
    run there, and closing cleanly leaves no failed RPC to reset anything. What
    keeps the next connection off A's old range is that it starts at token 0
    and re-reserves.
    """
    print("HIDDEN PRIMARY KEY: PREFETCH RESERVATION VS SERVER RESTART TEST")
    port_a, port_b = RESTART_MYSQLD_PORTS

    if port_is_open(RESTART_SERVER_PORT):
        print(f"\tFailed: port {RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="hpk_prefetch_tx_")
    server = None
    started = []
    connections = []
    prefetch_was = None
    try:
        # No recovery: the restart hands the same ids out a second time
        server = start_storage_server(work_dir, recovery=False)
        if server is None:
            return 1
        for port in (port_a, port_b):
            started.append(port)
            if not start_mysqld(port, RESTART_SERVER_PORT):
                return 1

        table = unique_table("hidden_prefetch_tx")
        first = connection_on(port_a, user, password)
        connections.append(first)
        first.autocommit = True
        cursor_a = first.cursor()
        reset_schema(cursor_a)
        cursor_a.execute("SELECT @@GLOBAL.lineairdb_prefetch_execution")
        prefetch_was = cursor_a.fetchone()[0]
        cursor_a.execute("SET GLOBAL lineairdb_prefetch_execution=ON")
        create_hidden_pk_table(cursor_a, table)

        # Reserves a range under this run and spends its first id
        prefetch_insert(cursor_a, table, "a-doomed")

        # Closing cleanly is the point: no RPC fails, so nothing resets a
        # token, and only a fresh connection's zero keeps the range off B's.
        first.close()
        connections.remove(first)

        stop_storage_server(server)
        server = start_storage_server(work_dir, recovery=False)
        if server is None:
            return 1

        # B meets a server that knows nothing and takes the ids just above the
        # one A spent, which is where a revived stale range would land.
        second = connection_on(port_b, user, password)
        connections.append(second)
        second.autocommit = True
        cursor_b = second.cursor()
        reset_schema(cursor_b)
        create_hidden_pk_table(cursor_b, table)
        from_b = [f"b-{i:04d}" for i in range(4)]
        insert_payloads(cursor_b, table, from_b)

        resumed_a = connection_on(port_a, user, password)
        connections.append(resumed_a)
        resumed_a.autocommit = True
        cursor_resumed_a = resumed_a.cursor()
        try:
            prefetch_insert(cursor_resumed_a, table, "a-after")
        except mysql.connector.Error as err:
            print(f"\tFailed: the stale range was spent: {err}")
            return 1

        expected = from_b + ["a-after"]
        observed = read_payloads(cursor_b, table)
        if sorted(observed) != sorted(expected):
            print(f"\tFailed: {report_payloads(expected, observed)}")
            return 1
    finally:
        if prefetch_was is not None and connections:
            try:
                restore = connections[-1].cursor()
                restore.execute("SET GLOBAL lineairdb_prefetch_execution="
                                f"{'ON' if int(prefetch_was) else 'OFF'}")
                restore.close()
            except mysql.connector.Error:
                pass
        for connection in connections:
            try:
                connection.close()
            except mysql.connector.Error:
                pass
        for port in started:
            if not stop_mysqld(port):
                print(f"\tWarning: the instance on port {port} is still running")
        stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)

    print("\tPassed!")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    db.autocommit = True
    cursor = db.cursor()
    reset_schema(cursor)

    second_db = None
    result = 0
    try:
        # Inside the try: a failed start can still have left the daemon up
        if not start_mysqld(SECOND_PORT):
            print("\nSOME TESTS FAILED!")
            sys.exit(1)

        second_db = connection_on(SECOND_PORT, args.user, args.password)
        second_db.autocommit = True
        second_cursor = second_db.cursor()
        reset_schema(second_cursor)

        result |= test_single_node_distinct_rows(cursor)
        result |= test_two_nodes_share_key_space(cursor, second_cursor)
        result |= test_restart_does_not_reuse_ids(second_cursor, args.user,
                                                  args.password)
        result |= test_declared_primary_key_unchanged(cursor)
        result |= test_multi_row_insert_one_statement(args.user,
                                                      args.password)
        result |= test_range_refill_two_connections(cursor, args.user,
                                                    args.password)
        result |= test_reservation_survives_storage_restart(args.user,
                                                            args.password)
        result |= test_reserved_range_dies_with_the_server(args.user,
                                                           args.password)
        result |= test_prefetch_range_dies_with_the_server(args.user,
                                                           args.password)
        result |= test_prefetch_reservation_dies_with_the_server(args.user,
                                                                 args.password)
    finally:
        if second_db is not None:
            try:
                second_db.close()
            except mysql.connector.Error:
                pass
        cursor.close()
        db.close()
        if not stop_mysqld(SECOND_PORT):
            print(f"\tWarning: the instance on port {SECOND_PORT} is still running")

    if result == 0:
        print("\nALL TESTS PASSED!")
    else:
        print("\nSOME TESTS FAILED!")

    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str,
                        help='name of user',
                        default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='password for the user',
                        default="")
    args = parser.parse_args()
    main()
