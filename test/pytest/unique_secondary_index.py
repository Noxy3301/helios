import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection

_last_error_message = ""


def reset(db, cursor):
    cursor.execute('DROP DATABASE IF EXISTS ha_helios_test')
    cursor.execute('CREATE DATABASE ha_helios_test')
    db.commit()


def expect_duplicate_insert_error(cursor, sql):
    try:
        cursor.execute(sql)
        return False, "duplicate insert unexpectedly succeeded"
    except mysql.connector.Error as err:
        return True, str(err)


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


def create_unique_table(cursor):
    table = f"unique_update_{int(time.time() * 1000000)}"
    cursor.execute(
        f'''CREATE TABLE ha_helios_test.{table} (
            id INT NOT NULL,
            email VARCHAR(63) NOT NULL,
            name VARCHAR(64),
            PRIMARY KEY (id),
            UNIQUE INDEX email_uidx (email)
        ) ENGINE = Helios'''
    )
    return table


def seed_two_rows(cursor, table, first_email, second_email):
    return run(cursor, f"INSERT INTO ha_helios_test.{table} "
                       "(id, email, name) VALUES "
                       f"(1, '{first_email}', 'a'), (2, '{second_email}', 'b')")


def rows_of(cursor, table):
    cursor.execute(f"SELECT id, email FROM ha_helios_test.{table} "
                   "ORDER BY id")
    return cursor.fetchall()


def warnings_of(cursor):
    cursor.execute("SHOW WARNINGS")
    return cursor.fetchall()


def test_update_onto_taken_unique_value(db, cursor):
    # Moving a row onto a UNIQUE value another row holds is the statement's to
    # report, as it is for an INSERT.
    print("UNIQUE SECONDARY INDEX (UPDATE ONTO A TAKEN VALUE) TEST")
    table = create_unique_table(cursor)
    db.commit()

    errno = run(cursor, f"INSERT INTO ha_helios_test.{table} "
                        "(id, email, name) VALUES (1, 'a@example.com', 'a'), "
                        "(2, 'b@example.com', 'b')")
    if errno is not None:
        print(f"\tFailed: the seed insert was rejected with {errno}")
        return 1
    db.commit()

    errno = run(cursor, f"UPDATE ha_helios_test.{table} "
                        "SET email = 'a@example.com' WHERE id = 2")
    if errno != 1062:
        print(f"\tFailed: expected 1062 at the statement, got {errno} "
              f"({_last_error_message})")
        db.rollback()
        return 1
    db.rollback()

    cursor.execute(f"SELECT id, email FROM ha_helios_test.{table} "
                   "ORDER BY id")
    rows = cursor.fetchall()
    if rows != [(1, 'a@example.com'), (2, 'b@example.com')]:
        print(f"\tFailed: table holds {rows}, expected both rows unchanged")
        return 1

    print("\tPassed!")
    return 0


def test_update_onto_taken_unique_value_no_checks(db, cursor):
    # With unique_checks off nothing probes, so the commit is what refuses it;
    # a commit has no handler in scope, so MySQL wraps handler error 121.
    print("UNIQUE SECONDARY INDEX (UPDATE, unique_checks=0) TEST")
    table = create_unique_table(cursor)
    db.commit()

    errno = run(cursor, f"INSERT INTO ha_helios_test.{table} "
                        "(id, email, name) VALUES (1, 'c@example.com', 'c'), "
                        "(2, 'd@example.com', 'd')")
    if errno is not None:
        print(f"\tFailed: the seed insert was rejected with {errno}")
        return 1
    db.commit()

    cursor.execute("SET SESSION unique_checks = 0")
    try:
        errno = run(cursor, "BEGIN")
        if errno is None:
            errno = run(cursor, f"UPDATE ha_helios_test.{table} "
                                "SET email = 'c@example.com' WHERE id = 2")
        if errno is not None:
            print(f"\tFailed: the UPDATE was rejected with {errno} "
                  f"({_last_error_message}), expected it to reach the COMMIT")
            run(cursor, "ROLLBACK")
            return 1
        errno = run(cursor, "COMMIT")
        run(cursor, "ROLLBACK")
    finally:
        cursor.execute("SET SESSION unique_checks = 1")

    if errno != 1180 or "Got error 121" not in _last_error_message:
        print(f"\tFailed: expected the commit duplicate (1180 wrapping 121), "
              f"got {errno} ({_last_error_message})")
        return 1

    cursor.execute(f"SELECT id, email FROM ha_helios_test.{table} "
                   "ORDER BY id")
    rows = cursor.fetchall()
    if rows != [(1, 'c@example.com'), (2, 'd@example.com')]:
        print(f"\tFailed: table holds {rows}, expected both rows unchanged")
        return 1

    print(f"\tPassed! (rejected with {errno} on the COMMIT)")
    return 0


def test_unique_index_defined_in_create_table(db, cursor):
    print("UNIQUE SECONDARY INDEX (CREATE TABLE) TEST")
    table_name = f"unique_create_{int(time.time() * 1000000)}"

    cursor.execute(
        f'''CREATE TABLE ha_helios_test.{table_name} (
            id INT NOT NULL,
            email VARCHAR(63) NOT NULL,
            name VARCHAR(64),
            PRIMARY KEY (id),
            UNIQUE INDEX email_uidx (email)
        ) ENGINE = Helios'''
    )
    db.commit()

    cursor.execute(
        f"INSERT INTO ha_helios_test.{table_name} (id, email, name) "
        "VALUES (1, 'alice@example.com', 'alice')"
    )
    db.commit()

    ok, detail = expect_duplicate_insert_error(
        cursor,
        f"INSERT INTO ha_helios_test.{table_name} (id, email, name) "
        "VALUES (2, 'alice@example.com', 'bob')",
    )
    if not ok:
        db.commit()
        print(f"\tFailed: {detail}")
        return 1

    db.rollback()
    cursor.execute(
        f"SELECT COUNT(*) FROM ha_helios_test.{table_name} "
        "WHERE email = 'alice@example.com'"
    )
    count = cursor.fetchone()[0]
    if count != 1:
        print(f"\tFailed: expected 1 row for duplicate key, got {count}")
        return 1

    print(f"\tPassed! (Expected duplicate error: {detail})")
    return 0


def test_rejected_update_reaches_no_commit(db, cursor):
    # The engine registers at session scope, so MySQL does not roll the
    # statement back: a refused UPDATE must have staged nothing for the COMMIT.
    print("UNIQUE SECONDARY INDEX (REFUSED UPDATE, THEN COMMIT) TEST")
    table = create_unique_table(cursor)
    db.commit()

    if seed_two_rows(cursor, table, 'e@example.com', 'f@example.com'):
        print(f"\tFailed: the seed insert was rejected ({_last_error_message})")
        return 1
    db.commit()

    run(cursor, "BEGIN")
    errno = run(cursor, f"UPDATE ha_helios_test.{table} "
                        "SET email = 'e@example.com' WHERE id = 2")
    if errno != 1062:
        print(f"\tFailed: expected 1062 at the statement, got {errno} "
              f"({_last_error_message})")
        run(cursor, "ROLLBACK")
        return 1

    errno = run(cursor, "COMMIT")
    if errno is not None:
        print(f"\tFailed: the COMMIT was rejected with {errno} "
              f"({_last_error_message})")
        return 1

    rows = rows_of(cursor, table)
    if rows != [(1, 'e@example.com'), (2, 'f@example.com')]:
        print(f"\tFailed: table holds {rows}, expected both rows unchanged")
        return 1

    print("\tPassed!")
    return 0


def test_update_ignore_onto_taken_unique_value(db, cursor):
    # IGNORE turns the duplicate into a warning and skips the row, so the
    # COMMIT installs nothing of it.
    print("UNIQUE SECONDARY INDEX (UPDATE IGNORE) TEST")
    table = create_unique_table(cursor)
    db.commit()

    if seed_two_rows(cursor, table, 'g@example.com', 'h@example.com'):
        print(f"\tFailed: the seed insert was rejected ({_last_error_message})")
        return 1
    db.commit()

    run(cursor, "BEGIN")
    errno = run(cursor, f"UPDATE IGNORE ha_helios_test.{table} "
                        "SET email = 'g@example.com' WHERE id = 2")
    if errno is not None:
        print(f"\tFailed: UPDATE IGNORE was rejected with {errno} "
              f"({_last_error_message})")
        run(cursor, "ROLLBACK")
        return 1

    warnings = warnings_of(cursor)
    if not warnings:
        print("\tFailed: UPDATE IGNORE reported no duplicate-key warning")
        run(cursor, "ROLLBACK")
        return 1

    errno = run(cursor, "COMMIT")
    if errno is not None:
        print(f"\tFailed: the COMMIT was rejected with {errno} "
              f"({_last_error_message})")
        return 1

    rows = rows_of(cursor, table)
    if rows != [(1, 'g@example.com'), (2, 'h@example.com')]:
        print(f"\tFailed: table holds {rows}, expected both rows unchanged")
        return 1

    print(f"\tPassed! (warning: {warnings[0]})")
    return 0


def test_rejected_insert_reaches_no_commit(db, cursor):
    print("UNIQUE SECONDARY INDEX (REFUSED INSERT, THEN COMMIT) TEST")
    table = create_unique_table(cursor)
    db.commit()

    if seed_two_rows(cursor, table, 'i@example.com', 'j@example.com'):
        print(f"\tFailed: the seed insert was rejected ({_last_error_message})")
        return 1
    db.commit()

    run(cursor, "BEGIN")
    errno = run(cursor, f"INSERT INTO ha_helios_test.{table} "
                        "(id, email, name) VALUES (3, 'i@example.com', 'c')")
    if errno != 1062:
        print(f"\tFailed: expected 1062 at the statement, got {errno} "
              f"({_last_error_message})")
        run(cursor, "ROLLBACK")
        return 1

    errno = run(cursor, "COMMIT")
    if errno is not None:
        print(f"\tFailed: the COMMIT was rejected with {errno} "
              f"({_last_error_message})")
        return 1

    rows = rows_of(cursor, table)
    if rows != [(1, 'i@example.com'), (2, 'j@example.com')]:
        print(f"\tFailed: table holds {rows}, expected only the seeded rows")
        return 1

    print("\tPassed!")
    return 0


def test_insert_ignore_onto_taken_unique_value(db, cursor):
    print("UNIQUE SECONDARY INDEX (INSERT IGNORE) TEST")
    table = create_unique_table(cursor)
    db.commit()

    if seed_two_rows(cursor, table, 'k@example.com', 'l@example.com'):
        print(f"\tFailed: the seed insert was rejected ({_last_error_message})")
        return 1
    db.commit()

    run(cursor, "BEGIN")
    errno = run(cursor, f"INSERT IGNORE INTO ha_helios_test.{table} "
                        "(id, email, name) VALUES (3, 'k@example.com', 'c')")
    if errno is not None:
        print(f"\tFailed: INSERT IGNORE was rejected with {errno} "
              f"({_last_error_message})")
        run(cursor, "ROLLBACK")
        return 1

    warnings = warnings_of(cursor)
    if not warnings:
        print("\tFailed: INSERT IGNORE reported no duplicate-key warning")
        run(cursor, "ROLLBACK")
        return 1

    errno = run(cursor, "COMMIT")
    if errno is not None:
        print(f"\tFailed: the COMMIT was rejected with {errno} "
              f"({_last_error_message})")
        return 1

    rows = rows_of(cursor, table)
    if rows != [(1, 'k@example.com'), (2, 'l@example.com')]:
        print(f"\tFailed: table holds {rows}, expected the new row to be absent")
        return 1

    print(f"\tPassed! (warning: {warnings[0]})")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()

    reset(db, cursor)

    result = 0
    result |= test_unique_index_defined_in_create_table(db, cursor)
    result |= test_update_onto_taken_unique_value(db, cursor)
    result |= test_update_onto_taken_unique_value_no_checks(db, cursor)
    result |= test_rejected_update_reaches_no_commit(db, cursor)
    result |= test_update_ignore_onto_taken_unique_value(db, cursor)
    result |= test_rejected_insert_reaches_no_commit(db, cursor)
    result |= test_insert_ignore_onto_taken_unique_value(db, cursor)

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
