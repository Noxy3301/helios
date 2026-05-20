import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_lineairdb_oneshot_plan_{int(time.time())}"


def reset(cursor, db):
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    db.commit()


def expect_commit_error(cursor):
    try:
        cursor.execute("COMMIT")
        return False
    except mysql.connector.Error:
        return True


def test_prefetch_scope(cursor, db):
    print("ONESHOT PLAN PREFETCH SCOPE TEST")
    cursor.execute(
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL) "
        "ENGINE=LineairDB"
    )
    cursor.execute("INSERT INTO t VALUES (1,100),(2,200),(3,300)")
    db.commit()

    cursor.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
    cursor.execute("SET @_ldb_plan='R:t:1;R:t:2;R:t:3'")
    cursor.execute("START TRANSACTION")
    cursor.execute("SELECT v FROM t WHERE id=1")
    if cursor.fetchone()[0] != 100:
        return 1
    cursor.execute("UPDATE t SET v=v+10 WHERE id=3")
    cursor.execute("COMMIT")

    cursor.execute("UPDATE t SET v=777 WHERE id=1")
    db.commit()

    # A new transaction gets a fresh plan and must read the latest value.
    cursor.execute("SET @_ldb_plan='R:t:1'")
    cursor.execute("START TRANSACTION")
    cursor.execute("SELECT v FROM t WHERE id=1")
    if cursor.fetchone()[0] != 777:
        return 1
    cursor.execute("COMMIT")
    print("\tPassed!")
    return 0


def test_conflict_abort():
    print("ONESHOT PLAN CONFLICT TEST")
    a = get_connection(user=args.user, password=args.password)
    b = get_connection(user=args.user, password=args.password)
    ca = a.cursor()
    cb = b.cursor()
    try:
        ca.execute(f"USE {DBNAME}")
        cb.execute(f"USE {DBNAME}")
        ca.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
        ca.execute("SET @_ldb_plan='R:t:1'")
        ca.execute("START TRANSACTION")
        ca.execute("SELECT v FROM t WHERE id=1")
        ca.fetchone()

        cb.execute("UPDATE t SET v=v+1 WHERE id=1")
        b.commit()

        ca.execute("UPDATE t SET v=v+100 WHERE id=1")
        if not expect_commit_error(ca):
            return 1

        cb.execute("SELECT v FROM t WHERE id=1")
        if cb.fetchone()[0] != 778:
            return 1
    finally:
        try:
            ca.close()
            cb.close()
            a.close()
            b.close()
        except Exception:
            pass
    print("\tPassed!")
    return 0


def test_unique_commit_check(cursor, db):
    print("ONESHOT PLAN UNIQUE SECONDARY TEST")
    cursor.execute(
        "CREATE TABLE unique_c (id INT NOT NULL, c INT NOT NULL, "
        "PRIMARY KEY(id), UNIQUE KEY u_c(c)) ENGINE=LineairDB"
    )
    cursor.execute("INSERT INTO unique_c VALUES (1,10)")
    db.commit()

    cursor.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
    cursor.execute("SET @_ldb_plan='R:t:2'")
    cursor.execute("START TRANSACTION")
    cursor.execute("SELECT v FROM t WHERE id=2")
    cursor.fetchone()
    cursor.execute("INSERT INTO unique_c VALUES (2,10)")
    if not expect_commit_error(cursor):
        return 1

    cursor.execute("SELECT COUNT(*) FROM unique_c")
    if cursor.fetchone()[0] != 1:
        return 1
    print("\tPassed!")
    return 0


def test_range_clean_commit(cursor, db):
    print("ONESHOT PLAN RANGE CLEAN COMMIT TEST")
    cursor.execute(
        "CREATE TABLE range_clean_t (id INT NOT NULL PRIMARY KEY, "
        "v INT NOT NULL) ENGINE=LineairDB"
    )
    cursor.execute("INSERT INTO range_clean_t VALUES (1,1),(10,10),(20,20)")
    db.commit()

    cursor.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
    cursor.execute("SET @_ldb_plan='S:range_clean_t:10:E:30'")
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT id FROM range_clean_t FORCE INDEX(PRIMARY) "
        "WHERE id >= 10 AND id < 30 ORDER BY id"
    )
    if [row[0] for row in cursor.fetchall()] != [10, 20]:
        return 1
    cursor.execute("COMMIT")
    print("\tPassed!")
    return 0


def test_range_phantom_abort():
    print("ONESHOT PLAN RANGE PHANTOM TEST")
    a = get_connection(user=args.user, password=args.password)
    b = get_connection(user=args.user, password=args.password)
    ca = a.cursor()
    cb = b.cursor()
    try:
        ca.execute(f"USE {DBNAME}")
        cb.execute(f"USE {DBNAME}")
        ca.execute(
            "CREATE TABLE range_t (id INT NOT NULL PRIMARY KEY, "
            "v INT NOT NULL) ENGINE=LineairDB"
        )
        ca.execute("INSERT INTO range_t VALUES (1,1),(10,10),(20,20),(30,30)")
        a.commit()

        ca.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
        ca.execute("SET @_ldb_plan='S:range_t:10:E:30'")
        ca.execute("START TRANSACTION")
        ca.execute(
            "SELECT id FROM range_t FORCE INDEX(PRIMARY) "
            "WHERE id >= 10 AND id < 30 ORDER BY id"
        )
        if [row[0] for row in ca.fetchall()] != [10, 20]:
            return 1

        cb.execute("INSERT INTO range_t VALUES (15,15)")
        b.commit()

        if not expect_commit_error(ca):
            return 1
    finally:
        try:
            ca.close()
            cb.close()
            a.close()
            b.close()
        except Exception:
            pass
    print("\tPassed!")
    return 0


def test_range_tombstone_reinsert_abort():
    print("ONESHOT PLAN RANGE TOMBSTONE REINSERT TEST")
    a = get_connection(user=args.user, password=args.password)
    b = get_connection(user=args.user, password=args.password)
    ca = a.cursor()
    cb = b.cursor()
    try:
        ca.execute(f"USE {DBNAME}")
        cb.execute(f"USE {DBNAME}")
        ca.execute(
            "CREATE TABLE tombstone_t (id INT NOT NULL PRIMARY KEY, "
            "v INT NOT NULL) ENGINE=LineairDB"
        )
        ca.execute(
            "INSERT INTO tombstone_t VALUES (10,10),(15,15),(20,20)"
        )
        ca.execute("DELETE FROM tombstone_t WHERE id=15")
        a.commit()

        ca.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
        ca.execute("SET @_ldb_plan='S:tombstone_t:10:E:30'")
        ca.execute("START TRANSACTION")
        ca.execute(
            "SELECT id FROM tombstone_t FORCE INDEX(PRIMARY) "
            "WHERE id >= 10 AND id < 30 ORDER BY id"
        )
        if [row[0] for row in ca.fetchall()] != [10, 20]:
            return 1

        cb.execute("INSERT INTO tombstone_t VALUES (15,150)")
        b.commit()

        if not expect_commit_error(ca):
            return 1
    finally:
        try:
            ca.close()
            cb.close()
            a.close()
            b.close()
        except Exception:
            pass
    print("\tPassed!")
    return 0


def test_secondary_range_clean_commit(cursor, db):
    print("ONESHOT PLAN SECONDARY RANGE CLEAN COMMIT TEST")
    cursor.execute(
        "CREATE TABLE si_range_clean_t ("
        "id INT NOT NULL PRIMARY KEY, "
        "c INT NOT NULL, "
        "v INT NOT NULL, "
        "KEY c_idx(c)) ENGINE=LineairDB"
    )
    cursor.execute(
        "INSERT INTO si_range_clean_t VALUES "
        "(1,10,1),(2,10,2),(3,20,3)"
    )
    db.commit()

    cursor.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
    cursor.execute("SET @_ldb_plan='SI:si_range_clean_t:c_idx:10'")
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT id FROM si_range_clean_t FORCE INDEX(c_idx) "
        "WHERE c = 10 ORDER BY id"
    )
    if [row[0] for row in cursor.fetchall()] != [1, 2]:
        return 1
    cursor.execute("COMMIT")
    print("\tPassed!")
    return 0


def test_secondary_range_phantom_abort():
    print("ONESHOT PLAN SECONDARY RANGE PHANTOM TEST")
    a = get_connection(user=args.user, password=args.password)
    b = get_connection(user=args.user, password=args.password)
    ca = a.cursor()
    cb = b.cursor()
    try:
        ca.execute(f"USE {DBNAME}")
        cb.execute(f"USE {DBNAME}")
        ca.execute(
            "CREATE TABLE si_range_t ("
            "id INT NOT NULL PRIMARY KEY, "
            "c INT NOT NULL, "
            "v INT NOT NULL, "
            "KEY c_idx(c)) ENGINE=LineairDB"
        )
        ca.execute(
            "INSERT INTO si_range_t VALUES "
            "(1,10,1),(2,10,2),(3,20,3)"
        )
        a.commit()

        ca.execute("SET GLOBAL lineairdb_oneshot_execution=ON")
        ca.execute("SET @_ldb_plan='SI:si_range_t:c_idx:10'")
        ca.execute("START TRANSACTION")
        ca.execute(
            "SELECT id FROM si_range_t FORCE INDEX(c_idx) "
            "WHERE c = 10 ORDER BY id"
        )
        if [row[0] for row in ca.fetchall()] != [1, 2]:
            return 1

        cb.execute("INSERT INTO si_range_t VALUES (4,10,4)")
        b.commit()

        if not expect_commit_error(ca):
            return 1
    finally:
        try:
            ca.close()
            cb.close()
            a.close()
            b.close()
        except Exception:
            pass
    print("\tPassed!")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    reset(cursor, db)

    result = 0
    result |= test_prefetch_scope(cursor, db)
    result |= test_conflict_abort()
    result |= test_unique_commit_check(cursor, db)
    result |= test_range_clean_commit(cursor, db)
    result |= test_range_phantom_abort()
    result |= test_range_tombstone_reinsert_abort()
    result |= test_secondary_range_clean_commit(cursor, db)
    result |= test_secondary_range_phantom_abort()

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
