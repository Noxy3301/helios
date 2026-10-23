"""DROP INDEX removes a secondary index from reads, checks and recovery.

On a private stack, drops a non-unique and a UNIQUE index, re-creates both
names with other shapes, restarts the storage server and reads back on both
read paths. Writes to a dropped index must not come back after recovery.
"""
import argparse
import shutil
import sys
import tempfile

import mysql.connector

import hidden_primary_key as hpk

T = f"{hpk.DATABASE}.t"
PATHS = ("row", "plan")


def ids(*keys):
    return [(k,) for k in keys]


# (statement, rows or errno). A SELECT runs on each read path, a write once.
BEFORE_RESTART = [
    (f"CREATE TABLE {T} (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, "
     "KEY ka (a), UNIQUE KEY ub (b)) ENGINE = Helios", []),
    (f"INSERT INTO {T} VALUES (1, 10, 100, 1000), (2, 20, 200, 2000), "
     "(3, 10, 300, 3000)", []),
    (f"INSERT INTO {T} VALUES (90, 90, 100, 9000)", 1062),
    (f"ALTER TABLE {T} DROP INDEX ka", []),
    (f"SELECT id FROM {T} FORCE INDEX (ka) WHERE a = 10", 1176),
    (f"INSERT INTO {T} VALUES (4, 10, 400, 4000)", []),
    (f"SELECT id FROM {T} WHERE a = 10 ORDER BY id", ids(1, 3, 4)),
    (f"SELECT id FROM {T} FORCE INDEX (ub) WHERE b = 200", ids(2)),
    (f"DROP INDEX ub ON {T}", []),
    (f"SELECT id FROM {T} FORCE INDEX (ub) WHERE b = 100", 1176),
    (f"INSERT INTO {T} VALUES (5, 50, 100, 5000)", []),
    (f"SELECT id FROM {T} WHERE b = 100 ORDER BY id", ids(1, 5)),
    # Same names, other shapes: ka non-unique on b, ub UNIQUE on c
    (f"CREATE INDEX ka ON {T} (b)", []),
    (f"CREATE UNIQUE INDEX ub ON {T} (c)", []),
    (f"INSERT INTO {T} VALUES (6, 10, 100, 6000)", []),
    (f"INSERT INTO {T} VALUES (91, 70, 700, 6000)", 1062),
    (f"SELECT id FROM {T} FORCE INDEX (ka) WHERE b = 100 ORDER BY id",
     ids(1, 5, 6)),
    (f"SELECT id FROM {T} FORCE INDEX (ub) WHERE c = 6000", ids(6)),
    (f"SELECT COUNT(*) FROM {T}", [(6,)]),
]

AFTER_RESTART = [
    (f"SELECT COUNT(*) FROM {T}", [(6,)]),
    # A replayed entry of the old ka (a) or ub (b) shows up as an extra row
    (f"SELECT id FROM {T} FORCE INDEX (ka) WHERE b >= 0 ORDER BY b, id",
     ids(1, 5, 6, 2, 3, 4)),
    (f"SELECT id FROM {T} FORCE INDEX (ub) WHERE c >= 0 ORDER BY c",
     ids(1, 2, 3, 4, 5, 6)),
    # 200 was a key only of the dropped ub (b); the new ub (c) must accept it
    (f"INSERT INTO {T} VALUES (8, 80, 800, 200)", []),
    (f"INSERT INTO {T} VALUES (92, 90, 900, 6000)", 1062),
    (f"SELECT id FROM {T} FORCE INDEX (ub) WHERE c = 200", ids(8)),
    (f"SELECT id FROM {T} WHERE a = 10 ORDER BY id", ids(1, 3, 4, 6)),
    (f"SELECT COUNT(*) FROM {T}", [(7,)]),
]


def run(cursor, stmt):
    """Rows of stmt, or its errno."""
    try:
        cursor.execute(stmt)
        return cursor.fetchall() if cursor.with_rows else []
    except mysql.connector.Error as err:
        return err.errno


def check(cursor, steps, label):
    failed = 0
    for stmt, want in steps:
        paths = PATHS if stmt.startswith("SELECT") else (None,)
        for path in paths:
            if path is not None:
                cursor.execute(f"SET GLOBAL helios_read_path = '{path}'")
            got = run(cursor, stmt)
            if got != want:
                failed += 1
                print(f"\tFailed [{label} {path}]: {stmt}\n"
                      f"\t\tgot {got}, expected {want}")
    return failed


def connect(port, user, password):
    conn = hpk.connection_on(port, user, password)
    conn.autocommit = True
    return conn


def test_drop_index(user, password):
    print("DROP INDEX: reads, unique checks and recovery")
    port = hpk.RESTART_MYSQLD_PORTS[0]
    if hpk.port_is_open(hpk.RESTART_SERVER_PORT):
        print(f"\tFailed: port {hpk.RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="drop_index_")
    server = None
    started = False
    conn = None
    try:
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1
        started = True
        if not hpk.start_mysqld(port, hpk.RESTART_SERVER_PORT):
            return 1

        conn = connect(port, user, password)
        cursor = conn.cursor()
        hpk.reset_schema(cursor)
        failed = check(cursor, BEFORE_RESTART, "before restart")

        conn.close()
        hpk.stop_storage_server(server)
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1

        conn = connect(port, user, password)
        failed += check(conn.cursor(), AFTER_RESTART, "after restart")
        if failed:
            return 1
        print("\tPassed!")
        return 0
    except mysql.connector.Error as err:
        print(f"\tFailed: {err}")
        return 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except mysql.connector.Error:
                pass
        if started and not hpk.stop_mysqld(port):
            print(f"\tWarning: the instance on port {port} is still running")
        hpk.stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()
    sys.exit(test_drop_index(args.user, args.password))
