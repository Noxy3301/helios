import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_helios_after_key_prefix_{int(time.time())}"

DDL = ("a INT NOT NULL, b INT NOT NULL, k1 INT NOT NULL, k2 INT NOT NULL, "
       "PRIMARY KEY (a, b), KEY ik (k1, k2)")
ROWS = ",".join(f"({a},{b},{a},{b})" for a in range(3, 8) for b in (1, 2, 3))

# MIN() over an open lower bound and MAX() over an open upper bound read one
# key by HA_READ_AFTER_KEY or HA_READ_BEFORE_KEY, and MySQL keeps it without
# rechecking the WHERE.
STMTS = [
    "SELECT MIN(a) FROM {t} WHERE a > 5",
    "SELECT MIN(k1) FROM {t} FORCE INDEX(ik) WHERE k1 > 5",
    "SELECT MAX(a) FROM {t} WHERE a < 5",
    "SELECT MAX(k1) FROM {t} FORCE INDEX(ik) WHERE k1 < 5",
    # A full secondary key, which a stored entry may extend by its primary key.
    "SELECT MIN(k2) FROM {t} FORCE INDEX(ik) WHERE k1 = 5 AND k2 > 2",
]


def run(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def test_after_key(cursor):
    print("AFTER KEY: open bounds on a composite key against InnoDB twins")
    for engine in ("Helios", "InnoDB"):
        cursor.execute(f"CREATE TABLE t_{engine.lower()} ({DDL}) "
                       f"ENGINE={engine}")
        cursor.execute(f"INSERT INTO t_{engine.lower()} VALUES {ROWS}")
    failed = total = 0
    for path in ("plan", "row"):
        cursor.execute(f"SET GLOBAL helios_read_path='{path}'")
        for stmt in STMTS:
            got = run(cursor, stmt.format(t="t_helios"))
            want = run(cursor, stmt.format(t="t_innodb"))
            total += 1
            if got != want:
                failed += 1
                print(f"\tFAILED [{path}] {stmt.format(t='t')}\n"
                      f"\t\thelios {got}\n\t\tinnodb {want}")
    print(f"\t{total - failed}/{total} statements match")
    return 1 if failed else 0


def main():
    db = get_connection(user=args.user, password=args.password)
    db.autocommit = True
    cursor = db.cursor()
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    rc = 0
    try:
        rc = test_after_key(cursor)
    except mysql.connector.Error as e:
        print(f"\tFAILED (unexpected mysql error): {e}")
        rc = 1
    finally:
        try:
            cursor.execute("SET GLOBAL helios_read_path='plan'")
            cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
            db.close()
        except mysql.connector.Error:
            pass
    sys.exit(rc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()
    main()
