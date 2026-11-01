"""A UNIQUE index whose key parts are the primary key's.

The primary index serves such an index: its duplicates are the primary key's,
reported under PRIMARY, and a read that names it sees exactly the rows a
primary-key read sees. Run on the row and the plan read path.
"""
import argparse
import sys

import mysql.connector

from utils.connection import get_connection


DBNAME = "ha_helios_unique_pk_alias"


def run(cursor, sql):
    """Run one statement; return ("err", errno, message), ("rc", rowcount) or
    ("rows", rows)."""
    try:
        cursor.execute(sql)
        if cursor.with_rows:
            return ("rows", cursor.fetchall())
        return ("rc", cursor.rowcount)
    except mysql.connector.Error as err:
        return ("err", err.errno, err.msg)


def single_cases(t):
    return [
        ("seed", f"INSERT INTO {t} VALUES (1,10),(2,20),(3,30),(5,50)",
         ("rc", 4)),
        ("duplicate insert", f"INSERT INTO {t} VALUES (2,99)",
         ("err", 1062, f"Duplicate entry '2' for key '{t}.PRIMARY'")),
        ("upsert of a taken key",
         f"INSERT INTO {t} VALUES (3,31) ON DUPLICATE KEY UPDATE v = VALUES(v)",
         ("rc", 2)),
        ("upsert of a free key",
         f"INSERT INTO {t} VALUES (4,40) ON DUPLICATE KEY UPDATE v = VALUES(v)",
         ("rc", 1)),
        ("replace of a taken key", f"REPLACE INTO {t} VALUES (5,55)", None),
        ("replace of a free key", f"REPLACE INTO {t} VALUES (6,60)", ("rc", 1)),
        ("rows after upserts", f"SELECT id, v FROM {t} ORDER BY id",
         ("rows", [(1, 10), (2, 20), (3, 31), (4, 40), (5, 55), (6, 60)])),
        ("primary key update", f"UPDATE {t} SET id = 7 WHERE id = 1", ("rc", 1)),
        ("alias sees the new key",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) WHERE id = 7",
         ("rows", [(7, 10)])),
        ("alias drops the old key",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) WHERE id = 1",
         ("rows", [])),
        ("alias point lookup",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) WHERE id = 3",
         ("rows", [(3, 31)])),
        ("alias range",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) "
         "WHERE id BETWEEN 2 AND 5 ORDER BY id",
         ("rows", [(2, 20), (3, 31), (4, 40), (5, 55)])),
        ("alias last row",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) ORDER BY id DESC LIMIT 1",
         ("rows", [(7, 10)])),
        ("alias IN list",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) "
         "WHERE id IN (2, 4, 6, 9) ORDER BY id",
         ("rows", [(2, 20), (4, 40), (6, 60)])),
        ("delete through the alias",
         f"DELETE {t} FROM {t} FORCE INDEX (id_uidx) WHERE id = 3", ("rc", 1)),
        ("alias drops the deleted key",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) WHERE id = 3",
         ("rows", [])),
        ("alias full scan",
         f"SELECT id, v FROM {t} FORCE INDEX (id_uidx) ORDER BY id",
         ("rows", [(2, 20), (4, 40), (5, 55), (6, 60), (7, 10)])),
    ]


def composite_cases(t):
    return [
        ("seed", f"INSERT INTO {t} VALUES (1,1,11),(1,2,12),(1,3,13),(2,1,21)",
         ("rc", 4)),
        ("duplicate insert", f"INSERT INTO {t} VALUES (1,2,99)",
         ("err", 1062, f"Duplicate entry '1-2' for key '{t}.PRIMARY'")),
        ("upsert of a taken key",
         f"INSERT INTO {t} VALUES (1,3,14) ON DUPLICATE KEY UPDATE v = VALUES(v)",
         ("rc", 2)),
        ("upsert of a free key",
         f"INSERT INTO {t} VALUES (2,2,22) ON DUPLICATE KEY UPDATE v = VALUES(v)",
         ("rc", 1)),
        ("replace of a taken key", f"REPLACE INTO {t} VALUES (2,1,23)", None),
        ("replace of a free key", f"REPLACE INTO {t} VALUES (3,1,31)",
         ("rc", 1)),
        ("rows after upserts", f"SELECT a, b, v FROM {t} ORDER BY a, b",
         ("rows", [(1, 1, 11), (1, 2, 12), (1, 3, 14), (2, 1, 23), (2, 2, 22),
                   (3, 1, 31)])),
        ("primary key update",
         f"UPDATE {t} SET b = 5 WHERE a = 1 AND b = 1", ("rc", 1)),
        ("alias sees the new key",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) WHERE a = 1 AND b = 5",
         ("rows", [(1, 5, 11)])),
        ("alias drops the old key",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) WHERE a = 1 AND b = 1",
         ("rows", [])),
        ("alias point lookup",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) WHERE a = 1 AND b = 2",
         ("rows", [(1, 2, 12)])),
        ("alias prefix",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) "
         "WHERE a = 1 ORDER BY a, b",
         ("rows", [(1, 2, 12), (1, 3, 14), (1, 5, 11)])),
        ("alias range",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) "
         "WHERE a = 1 AND b BETWEEN 2 AND 4 ORDER BY a, b",
         ("rows", [(1, 2, 12), (1, 3, 14)])),
        ("alias last row",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) "
         "ORDER BY a DESC, b DESC LIMIT 1",
         ("rows", [(3, 1, 31)])),
        ("alias IN list",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) "
         "WHERE (a, b) IN ((1,2),(2,2),(3,1),(9,9)) ORDER BY a, b",
         ("rows", [(1, 2, 12), (2, 2, 22), (3, 1, 31)])),
        ("delete through the alias",
         f"DELETE {t} FROM {t} FORCE INDEX (ab_uidx) WHERE a = 1 AND b = 2",
         ("rc", 1)),
        ("alias drops the deleted key",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) WHERE a = 1 AND b = 2",
         ("rows", [])),
        ("alias full scan",
         f"SELECT a, b, v FROM {t} FORCE INDEX (ab_uidx) ORDER BY a, b",
         ("rows", [(1, 3, 14), (1, 5, 11), (2, 1, 23), (2, 2, 22),
                   (3, 1, 31)])),
    ]


def run_cases(cursor, title, cases):
    print(title)
    failed = 0
    for label, sql, expected in cases:
        got = run(cursor, sql)
        # REPLACE overwrites a taken key in place, so only its rows are checked
        if expected is None and got[0] == "rc":
            continue
        if got != expected:
            print(f"\tFailed: {label}: {sql}\n\t\texpected {expected}, got {got}")
            failed = 1
    if failed == 0:
        print("\tPassed!")
    return failed


def connect(path):
    db = get_connection(user=args.user, password=args.password)
    db.autocommit = True
    cursor = db.cursor()
    cursor.execute(f"SET GLOBAL helios_read_path='{path}'")
    db.close()
    # A fresh session, so nothing from the previous path is carried over.
    db = get_connection(user=args.user, password=args.password)
    db.autocommit = True
    cursor = db.cursor()
    cursor.execute(f"USE {DBNAME}")
    return db, cursor


def test_path(path):
    db, cursor = connect(path)
    single, composite = f"single_{path}", f"composite_{path}"
    cursor.execute(
        f"CREATE TABLE {single} (id INT NOT NULL, v INT NOT NULL, "
        "PRIMARY KEY (id), UNIQUE INDEX id_uidx (id)) ENGINE=Helios")
    cursor.execute(
        f"CREATE TABLE {composite} (a INT NOT NULL, b INT NOT NULL, "
        "v INT NOT NULL, PRIMARY KEY (a, b), UNIQUE INDEX ab_uidx (a, b)) "
        "ENGINE=Helios")
    result = run_cases(cursor, f"UNIQUE ALIAS OF PRIMARY (id), {path} PATH TEST",
                       single_cases(single))
    result |= run_cases(cursor,
                        f"UNIQUE ALIAS OF PRIMARY (a, b), {path} PATH TEST",
                        composite_cases(composite))
    db.close()
    return result


def test_tx_plan_step():
    # An SI step that names the alias scans the primary index, and the scan
    # is validated at commit like any other.
    print("UNIQUE ALIAS IN A TX-PLAN SI STEP TEST")
    a, ca = connect("plan")
    b, cb = connect("plan")
    try:
        ca.execute("SET @_tx_plan='SI:single_plan:id_uidx:4'")
        ca.execute("START TRANSACTION")
        got = run(ca, "SELECT id, v FROM single_plan FORCE INDEX (id_uidx) "
                      "WHERE id = 4")
        commit = run(ca, "COMMIT")
        if got != ("rows", [(4, 40)]) or commit[0] == "err":
            print(f"\tFailed: point step read {got}, commit {commit}")
            return 1

        ca.execute("SET @_tx_plan='SI:composite_plan:ab_uidx:1'")
        ca.execute("START TRANSACTION")
        got = run(ca, "SELECT a, b, v FROM composite_plan FORCE INDEX (ab_uidx) "
                      "WHERE a = 1 ORDER BY a, b")
        if got != ("rows", [(1, 3, 14), (1, 5, 11)]):
            run(ca, "ROLLBACK")
            print(f"\tFailed: prefix step read {got}")
            return 1
        cb.execute("INSERT INTO composite_plan VALUES (1,9,19)")
        commit = run(ca, "COMMIT")
        if commit[0] != "err":
            print("\tFailed: commit succeeded past a phantom in the scanned prefix")
            return 1
    finally:
        ca.execute("SET @_tx_plan=NULL")
        a.close()
        b.close()
    print(f"\tPassed! (phantom rejected with {commit[1]})")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    db.close()

    result = 0
    result |= test_path("row")
    result |= test_path("plan")
    result |= test_tx_plan_step()
    # Leave the stack on the default path for whoever runs next.
    connect("plan")[0].close()

    if result == 0:
        print("\nALL TESTS PASSED!")
    else:
        print("\nSOME TESTS FAILED!")
    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Connect to MySQL")
    parser.add_argument("--user", type=str, default="root")
    parser.add_argument("--password", type=str, default="")
    args = parser.parse_args()
    main()
