"""An UPDATE that changes the primary key moves the row, as InnoDB does.

Each case runs on a Helios table and an InnoDB twin, on both read paths, and
compares every statement's result. The restart case commits moves on a private
stack, restarts its storage server and reads the moved rows back.
"""
import argparse
import shutil
import sys
import tempfile
import time

import mysql.connector

import hidden_primary_key as hpk
from utils.connection import get_connection

DBNAME = f"ha_helios_pk_update_{int(time.time())}"

INT_PK = "id INT PRIMARY KEY, v INT NOT NULL, KEY iv (v)"
ROWS = "VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"

# Each case: name, twin DDL body, rows, then statements. A result is compared with
# InnoDB's: rows as a multiset, or in order when the statement starts with "=";
# DML compares the affected rows and the warning codes, or the errno.
CASES = [
    ("single", INT_PK, ROWS, [
        "UPDATE {t} SET id = id + 100 WHERE id = 2",
        "SELECT id, v FROM {t}",
        "SELECT v FROM {t} WHERE id = 2",
        "SELECT v FROM {t} WHERE id = 102",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v = 20",
        "UPDATE {t} SET id = 200, v = 99 WHERE id = 102",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v = 20",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v = 99",
        "=SELECT id, v FROM {t} FORCE INDEX (iv) WHERE v >= 0 ORDER BY v",
    ]),
    ("all_rows", INT_PK, ROWS, [
        "UPDATE {t} SET id = id + 100",
        "=SELECT id, v FROM {t} ORDER BY id",
        "SELECT id FROM {t} WHERE id BETWEEN 2 AND 103",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v BETWEEN 20 AND 40",
        "SELECT COUNT(*) FROM {t}",
    ]),
    # A secondary index scan feeds the moves while they happen.
    ("by_index", INT_PK, ROWS, [
        "UPDATE {t} FORCE INDEX (iv) SET id = id + 100 WHERE v >= 20",
        "=SELECT id, v FROM {t} ORDER BY id",
        "UPDATE {t} FORCE INDEX (iv) SET id = id + 100 WHERE v = 30",
        "=SELECT id, v FROM {t} ORDER BY id",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v >= 20",
    ]),
    # MySQL moves the rows one at a time in scan order, so a shift onto keys
    # still held fails unless ORDER BY frees each key first.
    ("shift", INT_PK, ROWS, [
        "UPDATE {t} SET id = id + 1 WHERE id BETWEEN 2 AND 4",
        "UPDATE {t} SET id = id + 1",
        "=SELECT id, v FROM {t} ORDER BY id",
        "UPDATE {t} SET id = id + 1 WHERE id >= 2 ORDER BY id DESC",
        "=SELECT id, v FROM {t} ORDER BY id",
        "UPDATE {t} SET id = id - 1 WHERE id >= 3",
        "=SELECT id, v FROM {t} ORDER BY id",
        "UPDATE {t} SET id = id + 10 ORDER BY v DESC",
        "=SELECT id, v FROM {t} ORDER BY id",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v >= 30",
    ]),
    ("composite", "a INT, b INT, v INT NOT NULL, PRIMARY KEY (a, b), "
     "KEY iv (v)", "VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)", [
         "UPDATE {t} SET b = b + 10 WHERE a = 1",
         "UPDATE {t} SET a = 3 WHERE a = 2 AND b = 1",
         "UPDATE {t} SET a = 1 WHERE a = 3",
         "UPDATE {t} SET b = 12 WHERE a = 1 AND b = 11",
         "=SELECT a, b, v FROM {t} ORDER BY a, b",
         "SELECT b, v FROM {t} WHERE a = 1",
         "SELECT a, b FROM {t} FORCE INDEX (iv) WHERE v = 30",
     ]),
    ("duplicate", INT_PK, ROWS, [
        "UPDATE {t} SET id = 2 WHERE id = 1",
        "UPDATE {t} SET id = 9 WHERE id >= 4",
        "=SELECT id, v FROM {t} ORDER BY id",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v IN (10, 40, 50)",
    ]),
    ("ignore", INT_PK, "VALUES (1, 10), (2, 20), (3, 30), (11, 110)", [
        "UPDATE IGNORE {t} SET id = id + 10",
        "=SELECT id, v FROM {t} ORDER BY id",
        "UPDATE IGNORE {t} SET id = id + 1 WHERE id < 20",
        "=SELECT id, v FROM {t} ORDER BY id",
        "SELECT id, v FROM {t} FORCE INDEX (iv) WHERE v >= 0",
    ]),
    ("unique", "id INT PRIMARY KEY, u INT NOT NULL, v INT NOT NULL, "
     "UNIQUE KEY uu (u)", "VALUES (1, 100, 1), (2, 200, 2), (3, 300, 3)", [
         "UPDATE {t} SET id = id + 10 WHERE id = 1",
         "SELECT id FROM {t} WHERE u = 100",
         "UPDATE {t} SET id = 20, u = 200 WHERE id = 11",
         "UPDATE {t} SET id = 21, u = 400 WHERE id = 11",
         "SELECT id FROM {t} WHERE u = 100",
         "SELECT id FROM {t} WHERE u = 400",
         "UPDATE IGNORE {t} SET id = id + 100, u = 300 WHERE id = 2",
         "UPDATE {t} SET id = id + 100",
         "=SELECT id, u, v FROM {t} ORDER BY id",
         "SELECT id FROM {t} WHERE u = 200",
     ]),
    ("collation", "v VARCHAR(20) PRIMARY KEY, n INT NOT NULL, KEY ink (n)",
     "VALUES ('abc', 1), ('xyz', 2)", [
         "UPDATE {t} SET v = 'ABC' WHERE v = 'abc'",
         "SELECT v, n FROM {t}",
         "SELECT v FROM {t} FORCE INDEX (ink) WHERE n = 1",
         "UPDATE {t} SET v = 'ábc' WHERE v = 'abc'",
         "SELECT HEX(v), n FROM {t}",
         "UPDATE {t} SET v = 'XYZ' WHERE n = 1",
         "UPDATE {t} SET v = 'abd' WHERE n = 1",
         "SELECT HEX(v), n FROM {t}",
         "SELECT n FROM {t} WHERE v = 'ABD'",
         "SELECT v FROM {t} FORCE INDEX (ink) WHERE n = 1",
     ]),
    ("hidden_pk", "id INT NOT NULL, v INT NOT NULL, KEY iid (id)", ROWS, [
        "UPDATE {t} SET id = id + 100",
        "SELECT id, v FROM {t}",
        "SELECT v FROM {t} FORCE INDEX (iid) WHERE id = 103",
    ]),
    ("rollback", INT_PK, ROWS, [
        "BEGIN",
        "UPDATE {t} SET id = id + 100 WHERE id <= 3",
        "SELECT id, v FROM {t}",
        "SELECT id, v FROM {t} WHERE id BETWEEN 2 AND 102",
        "SELECT v FROM {t} WHERE id = 1",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v = 20",
        "UPDATE {t} SET id = id - 100 WHERE id = 101",
        "SELECT id, v FROM {t}",
        "ROLLBACK",
        "=SELECT id, v FROM {t} ORDER BY id",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v <= 30",
    ]),
    ("commit", INT_PK, ROWS, [
        "BEGIN",
        "UPDATE {t} SET id = id + 100 WHERE id <= 3",
        "UPDATE {t} SET id = id - 100 WHERE id = 101",
        "COMMIT",
        "=SELECT id, v FROM {t} ORDER BY id",
        "SELECT id FROM {t} FORCE INDEX (iv) WHERE v <= 30",
        "SELECT v FROM {t} WHERE id = 2",
    ]),
]

RESTART_DDL = ("id INT PRIMARY KEY, u INT NOT NULL, v INT NOT NULL, "
               "UNIQUE KEY uu (u), KEY iv (v)")
RESTART_ROWS = "VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30), (4, 400, 40)"
RESTART_MOVES = [
    "BEGIN",
    "UPDATE {t} SET id = id + 100 WHERE id <= 2",
    "UPDATE {t} SET id = 50, v = 55 WHERE id = 3",
    "COMMIT",
]
RESTART_READS = [
    "SELECT id, u, v FROM {t}",
    "SELECT id FROM {t} WHERE id IN (1, 2, 3, 101, 102, 50)",
    "SELECT id FROM {t} FORCE INDEX (uu) WHERE u IN (100, 200, 300)",
    "SELECT id FROM {t} FORCE INDEX (iv) WHERE v IN (10, 20, 30, 55)",
]


def run(cursor, sql, ordered):
    """('rows', rows sorted unless ordered), ('rowcount', n, warning codes) or
    ('error', errno)."""
    try:
        cursor.execute(sql)
        if cursor.with_rows:
            rows = cursor.fetchall()
            return "rows", rows if ordered else sorted(rows, key=repr)
        count = cursor.rowcount
        cursor.execute("SHOW WARNINGS")
        return "rowcount", count, [w[1] for w in cursor.fetchall()]
    except mysql.connector.Error as e:
        return "error", e.errno


def twins(cursor, name, ddl, rows):
    for engine in ("Helios", "InnoDB"):
        t = f"{name}_{engine.lower()}"
        cursor.execute(f"CREATE TABLE {t} ({ddl}) ENGINE={engine}")
        cursor.execute(f"INSERT INTO {t} {rows}")


def compare(helios, innodb, path, name, stmts):
    """Run stmts on the Helios table through one cursor and on its InnoDB twin
    through the other; returns (statements, failures)."""
    failed = 0
    for stmt in stmts:
        ordered = stmt.startswith("=")
        stmt = stmt.lstrip("=")
        got = run(helios, stmt.format(t=f"{name}_helios"), ordered)
        want = run(innodb, stmt.format(t=f"{name}_innodb"), ordered)
        if got != want:
            failed += 1
            print(f"\tFAILED [{path}] {name}: {stmt}\n"
                  f"\t\thelios {got}\n\t\tinnodb {want}")
    return len(stmts), failed


def test_cases(helios, innodb):
    print("PRIMARY KEY UPDATE: Helios against InnoDB twins")
    failed = total = 0
    for path in ("plan", "row"):
        helios.execute(f"SET GLOBAL helios_read_path='{path}'")
        for name, ddl, rows, stmts in CASES:
            name = f"{name}_{path}"
            twins(helios, name, ddl, rows)
            n, bad = compare(helios, innodb, path, name, stmts)
            total += n
            failed += bad
    print(f"\t{total - failed}/{total} statements match")
    return 1 if failed else 0


def connect(port, user, password):
    conn = hpk.connection_on(port, user, password)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"USE {hpk.DATABASE}")
    return conn, cursor


def test_restart(user, password):
    print("PRIMARY KEY UPDATE: moved rows across a storage server restart")
    port = hpk.RESTART_MYSQLD_PORTS[0]
    if hpk.port_is_open(hpk.RESTART_SERVER_PORT):
        print(f"\tFailed: port {hpk.RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="primary_key_update_")
    server = None
    started = False
    conns = []
    try:
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1
        started = True
        if not hpk.start_mysqld(port, hpk.RESTART_SERVER_PORT):
            return 1

        setup = hpk.connection_on(port, user, password)
        setup.autocommit = True
        hpk.reset_schema(setup.cursor())
        setup.close()

        conns = [connect(port, user, password) for _ in range(2)]
        helios, innodb = conns[0][1], conns[1][1]
        failed = 0
        for path in ("plan", "row"):
            helios.execute(f"SET GLOBAL helios_read_path='{path}'")
            twins(helios, f"t_{path}", RESTART_DDL, RESTART_ROWS)
            failed += compare(helios, innodb, path, f"t_{path}",
                              RESTART_MOVES)[1]

        for conn, _ in conns:
            conn.close()
        hpk.stop_storage_server(server)
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1

        conns = [connect(port, user, password) for _ in range(2)]
        helios, innodb = conns[0][1], conns[1][1]
        for path in ("plan", "row"):
            helios.execute(f"SET GLOBAL helios_read_path='{path}'")
            for table in ("t_plan", "t_row"):
                failed += compare(helios, innodb, f"{path} after restart",
                                  table, RESTART_READS)[1]
        if failed:
            return 1
        print("\tPassed!")
        return 0
    except mysql.connector.Error as err:
        print(f"\tFailed: {err}")
        return 1
    finally:
        for conn, _ in conns:
            try:
                conn.close()
            except mysql.connector.Error:
                pass
        if started and not hpk.stop_mysqld(port):
            print(f"\tWarning: the instance on port {port} is still running")
        hpk.stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)


def main():
    conns = []
    rc = 0
    try:
        for _ in range(2):
            conn = get_connection(user=args.user, password=args.password)
            conn.autocommit = True
            conns.append(conn)
        helios, innodb = conns[0].cursor(), conns[1].cursor()
        helios.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
        helios.execute(f"CREATE DATABASE {DBNAME}")
        for cursor in (helios, innodb):
            cursor.execute(f"USE {DBNAME}")
        rc |= test_cases(helios, innodb)
    except mysql.connector.Error as e:
        print(f"\tFAILED (unexpected mysql error): {e}")
        rc = 1
    finally:
        try:
            conns[0].cursor().execute("SET GLOBAL helios_read_path='plan'")
            conns[0].cursor().execute(f"DROP DATABASE IF EXISTS {DBNAME}")
        except (mysql.connector.Error, IndexError):
            pass
        for conn in conns:
            conn.close()
    rc |= test_restart(args.user, args.password)
    sys.exit(rc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()
    main()
