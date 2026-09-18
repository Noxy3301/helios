"""CREATE TABLE must refuse a column too wide for a PAX cell.

The storage keeps no row outside PAX, so a table whose PAX schema cannot be
built has to be refused at CREATE TABLE. Otherwise the CREATE succeeds and
every later write fails at the commit, which the client sees as 1180 rather
than as a DDL error.
"""
import argparse
import sys

import mysql.connector

from utils.connection import get_connection

DBNAME = "ha_lineairdb_pax_limits"

# Widest payload a PAX cell holds (proxy/ddl.cc kMaxCellBytes).
MAX_CELL_BYTES = 2048
# The limit counts the charset octets: utf8mb4 holds four bytes per character.
MAX_UTF8MB4_CHARS = MAX_CELL_BYTES // 4

UNSUPPORTED_COLUMN = 1235

_last_error_message = ""


def reset(db, cursor):
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    db.commit()


def run(cursor, sql):
    """Run one statement; return its error number, or None when it succeeded."""
    global _last_error_message
    try:
        cursor.execute(sql)
        if cursor.with_rows:
            cursor.fetchall()
    except mysql.connector.Error as err:
        _last_error_message = str(err)
        return err.errno
    return None


def create_table(cursor, table, column):
    return run(cursor, f"""CREATE TABLE {DBNAME}.{table} (
            id INT NOT NULL,
            c {column},
            PRIMARY KEY (id)
        ) ENGINE = LineairDB""")


def table_exists(cursor, table):
    cursor.execute(f"SHOW TABLES FROM {DBNAME} LIKE '{table}'")
    return cursor.fetchall() != []


def test_refused_column(cursor, table, column):
    print(f"CREATE TABLE WITH {column} TEST")
    errno = create_table(cursor, table, column)
    if errno != UNSUPPORTED_COLUMN:
        print(f"\tFailed: expected {UNSUPPORTED_COLUMN}, got {errno} "
              f"({_last_error_message})")
        return 1
    if "PAX cell limit" not in _last_error_message or \
            "column c" not in _last_error_message:
        print(f"\tFailed: the error does not name the column and the limit: "
              f"{_last_error_message}")
        return 1

    if table_exists(cursor, table):
        print("\tFailed: the refused table exists")
        return 1

    # The name must be free: a supported definition takes it, which also
    # proves nothing was left on the server under that name.
    errno = create_table(cursor, table, "VARCHAR(32) NOT NULL")
    if errno is not None:
        print(f"\tFailed: the name is not free, CREATE gave {errno} "
              f"({_last_error_message})")
        return 1
    if run(cursor, f"INSERT INTO {DBNAME}.{table} VALUES (1, 'v')") is not None:
        print(f"\tFailed: insert rejected with {_last_error_message}")
        return 1

    print(f"\tPassed! ({_last_error_message})")
    return 0


def test_column_at_the_limit(cursor):
    print(f"CREATE TABLE WITH VARCHAR({MAX_UTF8MB4_CHARS}) utf8mb4 TEST")
    table = "at_limit"
    errno = create_table(cursor, table,
                         f"VARCHAR({MAX_UTF8MB4_CHARS}) CHARACTER SET utf8mb4 NOT NULL")
    if errno is not None:
        print(f"\tFailed: CREATE rejected with {errno} ({_last_error_message})")
        return 1

    value = "\U0001F600" * MAX_UTF8MB4_CHARS  # four bytes each
    errno = run(cursor, f"INSERT INTO {DBNAME}.{table} VALUES (1, '{value}')")
    if errno is not None:
        print(f"\tFailed: insert rejected with {errno} ({_last_error_message})")
        return 1

    cursor.execute(f"SELECT id, c FROM {DBNAME}.{table}")
    rows = cursor.fetchall()
    if rows != [(1, value)]:
        widths = [(i, len(c)) for i, c in rows]
        print(f"\tFailed: table holds {widths}, expected one row of "
              f"{MAX_UTF8MB4_CHARS} chars")
        return 1

    print("\tPassed!")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()

    reset(db, cursor)
    db.autocommit = True

    result = 0
    result |= test_refused_column(cursor, "with_text", "TEXT")
    result |= test_refused_column(cursor, "with_blob", "BLOB")
    result |= test_refused_column(cursor, "with_varchar",
                                  f"VARCHAR({MAX_UTF8MB4_CHARS + 1}) CHARACTER SET utf8mb4 NOT NULL")
    result |= test_refused_column(cursor, "with_latin1_varchar",
                                  f"VARCHAR({MAX_CELL_BYTES + 1}) CHARACTER SET latin1 NOT NULL")
    result |= test_column_at_the_limit(cursor)

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
