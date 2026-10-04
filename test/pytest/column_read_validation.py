"""A plain primary-key SELECT is validated by the columns it reads.

T1 reads a row by its primary key and T2 changes it and commits. T1's COMMIT
succeeds when T2 assigns no column T1 reads, and fails with 1180 wrapping 149
when T2 assigns one, when T1 reads every column, or when T1 writes the row.
"""
import argparse
import itertools
import sys
import time
from decimal import Decimal

import mysql.connector

from utils.connection import get_connection

DBNAME = "ha_helios_column_read"

# A table's columns and its first row.
STRINGS = ("a VARCHAR(32) NOT NULL, b VARCHAR(32) NOT NULL",
           "(1, 'a0', 'b0')")
NUMBERS = ("x DECIMAL(12,2) NOT NULL, y INT NOT NULL",
           "(1, 10.00, 20)")


def main():
    db = get_connection(user=args.user, password=args.password)
    db.autocommit = True
    cursor = db.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")

    read_a = "SELECT a FROM {t} WHERE id = 1"
    read_x = "SELECT x FROM {t} WHERE id = 1"
    cases = [
        ("AN UNREAD COLUMN CHANGE COMMITS", STRINGS,
         [read_a], "UPDATE {t} SET b = 'b2' WHERE id = 1",
         True, (1, "a0", "b2")),
        ("A READ COLUMN CHANGE ABORTS", STRINGS,
         [read_a], "UPDATE {t} SET a = 'a1' WHERE id = 1",
         False, (1, "a1", "b0")),
        ("SELECT * READS EVERY COLUMN", STRINGS,
         ["SELECT * FROM {t} WHERE id = 1"],
         "UPDATE {t} SET b = 'b2' WHERE id = 1",
         False, (1, "a0", "b2")),
        ("A ROW T1 ALSO WRITES ABORTS", STRINGS,
         [read_a, "UPDATE {t} SET a = 'a9' WHERE id = 1"],
         "UPDATE {t} SET b = 'b2' WHERE id = 1",
         False, (1, "a0", "b2")),
        # Assigning a read column its current value writes it. MySQL skips an
        # UPDATE that leaves the whole row unchanged before the engine sees
        # it, so this one also changes b.
        ("A VALUE-EQUAL WRITE ABORTS", STRINGS,
         [read_a], "UPDATE {t} SET a = 'a0', b = 'b2' WHERE id = 1",
         False, (1, "a0", "b2")),
        ("AN UNREAD NUMERIC COLUMN CHANGE COMMITS", NUMBERS,
         [read_x], "UPDATE {t} SET y = 22 WHERE id = 1",
         True, (1, Decimal("10.00"), 22)),
        # A REPLACE assigns every column, even an unchanged one.
        ("A VALUE-EQUAL NUMERIC WRITE ABORTS", NUMBERS,
         [read_x], "REPLACE INTO {t} VALUES (1, 10.00, 20)",
         False, (1, Decimal("10.00"), 20)),
        ("A READ NUMERIC COLUMN CHANGE ABORTS", NUMBERS,
         [read_x], "UPDATE {t} SET x = 10.01 WHERE id = 1",
         False, (1, Decimal("10.01"), 20)),
    ]

    result = 0
    try:
        for seq, (read_path, case) in enumerate(
                itertools.product(("plan", "row"), cases)):
            (title, (columns, first_row), t1_statements,
             t2_statement, expect_commit, expected_row) = case
            print(f"{title} (helios_read_path={read_path})")
            cursor.execute(f"SET GLOBAL helios_read_path = '{read_path}'")

            # A dropped table keeps its rows in the storage, so never reuse a
            # name.
            table = f"{DBNAME}.t{int(time.time() * 1000000)}_{seq}"
            cursor.execute(f"CREATE TABLE {table} (id INT NOT NULL, "
                           f"{columns}, PRIMARY KEY (id)) ENGINE = Helios")
            cursor.execute(f"INSERT INTO {table} VALUES {first_row}")

            # T1 runs its statements, T2 commits its change, and T1 commits.
            t1_conn = get_connection(user=args.user, password=args.password)
            t1_conn.autocommit = True
            t1 = t1_conn.cursor()
            t1.execute("BEGIN")
            for sql in t1_statements:
                t1.execute(sql.format(t=table))
                if t1.with_rows:
                    t1.fetchall()

            cursor.execute(t2_statement.format(t=table))

            commit_errno = None
            commit_message = ""
            try:
                t1.execute("COMMIT")
            except mysql.connector.Error as err:
                commit_errno = err.errno
                commit_message = str(err)
            t1.close()
            t1_conn.close()

            failed = 0
            if expect_commit and commit_errno is not None:
                print(f"\tFailed: T1's COMMIT was rejected: {commit_message}")
                failed = 1
            # MySQL wraps every nonzero handlerton commit result as 1180, so a
            # lost race at the commit arrives as the wrapped 149.
            if not expect_commit and (commit_errno != 1180 or
                                      "Got error 149" not in commit_message):
                print(f"\tFailed: T1's COMMIT returned {commit_errno} "
                      f"({commit_message}), expected the retryable conflict")
                failed = 1

            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            if rows != [expected_row]:
                print(f"\tFailed: table holds {rows}, "
                      f"expected [{expected_row}]")
                failed = 1

            if failed == 0:
                print("\tPassed!")
            result |= failed
    finally:
        cursor.execute("SET GLOBAL helios_read_path = 'plan'")

    if result == 0:
        print("\nALL TESTS PASSED!")
    else:
        print("\nSOME TESTS FAILED!")
    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str,
                        help='name of user', default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='password for the user', default="")
    args = parser.parse_args()
    main()
