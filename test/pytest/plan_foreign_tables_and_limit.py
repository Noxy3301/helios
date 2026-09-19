import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_helios_plan_foreign_{int(time.time())}"


def reset(cursor, db):
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    db.commit()


def run(cursor, sql):
    """Run one statement; return (rows, errno, message)."""
    try:
        cursor.execute(sql)
        rows = cursor.fetchall() if cursor.with_rows else []
        return rows, None, ""
    except mysql.connector.Error as err:
        return [], err.errno, str(err)


def check(rows, errno, message, expected, label):
    if errno is not None:
        print(f"\t{label}: error {errno}: {message}")
        return 1
    if rows != expected:
        print(f"\t{label}: got {rows}, want {expected}")
        return 1
    return 0


def test_foreign_engine_leaves(cursor, db):
    """A leaf of another engine must not be compiled into the read plan."""
    print("PLAN FOREIGN ENGINE LEAF TEST")
    cursor.execute(
        "CREATE TABLE fe_helios (id INT NOT NULL PRIMARY KEY, v INT NOT NULL) "
        "ENGINE=Helios"
    )
    cursor.execute(
        "CREATE TABLE fe_inno (id INT NOT NULL PRIMARY KEY, v INT NOT NULL) "
        "ENGINE=InnoDB"
    )
    cursor.execute("INSERT INTO fe_helios VALUES (1,10),(2,20),(3,30)")
    cursor.execute("INSERT INTO fe_inno VALUES (1,100),(2,200)")
    db.commit()

    cursor.execute("SET GLOBAL helios_read_path='plan'")

    result = 0
    rows, errno, msg = run(
        cursor,
        "SELECT l.id, l.v, i.v FROM fe_helios l JOIN fe_inno i ON i.id = l.id "
        "ORDER BY l.id",
    )
    result |= check(rows, errno, msg, [(1, 10, 100), (2, 20, 200)], "join")

    _, errno, msg = run(
        cursor,
        "UPDATE fe_helios l JOIN fe_inno i ON i.id = l.id SET l.v = l.v + i.v",
    )
    if errno is not None:
        print(f"\tmulti-table UPDATE: error {errno}: {msg}")
        result |= 1
    else:
        db.commit()
        rows, errno, msg = run(cursor, "SELECT id, v FROM fe_helios ORDER BY id")
        result |= check(
            rows, errno, msg, [(1, 110), (2, 220), (3, 30)], "UPDATE readback"
        )

    rows, errno, msg = run(
        cursor,
        "SELECT id FROM fe_helios WHERE id IN (SELECT id FROM fe_inno) ORDER BY id",
    )
    result |= check(rows, errno, msg, [(1,), (2,)], "IN subquery")

    db.commit()
    if result == 0:
        print("\tPassed!")
    return result


def test_subquery_limit_not_pushed(cursor, db):
    """An outer LIMIT must not truncate a scan owned by an inner block."""
    print("SUBQUERY SCAN LIMIT TEST")
    cursor.execute(
        "CREATE TABLE sl_t (pk INT NOT NULL PRIMARY KEY, x INT NOT NULL) "
        "ENGINE=Helios"
    )
    # seq is field 0 and k is field 1, so the key suffix part (seq) shares a
    # field index with the outer ORDER BY column (sl_t.pk).
    cursor.execute(
        "CREATE TABLE sl_t2 (seq INT NOT NULL, k INT NOT NULL, v INT NOT NULL, "
        "PRIMARY KEY (k, seq)) ENGINE=Helios"
    )
    cursor.execute("INSERT INTO sl_t VALUES (1,1),(2,2)")
    cursor.execute(
        "INSERT INTO sl_t2 VALUES "
        "(1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(1,2,6),(2,2,7)"
    )
    db.commit()

    cursor.execute("SET GLOBAL helios_read_path='row'")
    rows, errno, msg = run(
        cursor,
        "SELECT t.pk, (SELECT COUNT(*) FROM sl_t2 WHERE sl_t2.k = t.pk) "
        "FROM sl_t t ORDER BY t.pk LIMIT 1",
    )
    result = check(rows, errno, msg, [(1, 5)], "correlated COUNT")

    # The same subquery without an outer LIMIT is the unaffected reference.
    rows, errno, msg = run(
        cursor,
        "SELECT t.pk, (SELECT COUNT(*) FROM sl_t2 WHERE sl_t2.k = t.pk) "
        "FROM sl_t t ORDER BY t.pk",
    )
    result |= check(rows, errno, msg, [(1, 5), (2, 2)], "correlated COUNT no LIMIT")

    cursor.execute("SET GLOBAL helios_read_path='plan'")
    db.commit()
    if result == 0:
        print("\tPassed!")
    return result


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    reset(cursor, db)

    result = 0
    result |= test_foreign_engine_leaves(cursor, db)
    result |= test_subquery_limit_not_pushed(cursor, db)

    cursor.execute("SET GLOBAL helios_read_path='plan'")
    cursor.close()
    db.close()

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
