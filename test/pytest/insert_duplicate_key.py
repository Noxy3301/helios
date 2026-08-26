"""INSERT must refuse a primary key that already holds a row.

The check is deferred: a plain INSERT buffers its rows and the storage server
refuses the key when the buffer is sent, which is the end of the statement for
a normal transaction and the commit for a prefetch one. REPLACE, INSERT IGNORE
and ON DUPLICATE KEY UPDATE need the answer at the row, so those read the key
first. Run with lineairdb_prefetch_execution off and on; the prefetch case
turns it on for itself either way.
"""
import argparse
import sys
import threading
import time

import mysql.connector

from utils.connection import get_connection

DBNAME = "ha_lineairdb_insert_dup"

COMMIT_DEADLINE_SECONDS = 20.0
COMMIT_HEAD_START_SECONDS = 0.5

_table_seq = 0
_last_error_message = ""


def is_duplicate(errno):
    """1062 names the key at the row; a rejection raised on the commit has no
    handler in scope and MySQL wraps it as 1180 'Got error 121'."""
    return errno == 1062 or (errno == 1180 and
                             "Got error 121" in _last_error_message)


def is_conflict(errno):
    """A lost race is 1213 at the statement or 1180 'Got error 149' at the
    commit; both are what retrying clients such as BenchBase retry."""
    return errno == 1213 or (errno == 1180 and
                             "Got error 149" in _last_error_message)


def is_commit_duplicate(errno):
    """A commit has no handler in scope, so its duplicate can only arrive
    wrapped; a bare 1062 there would mean detection moved phases."""
    return errno == 1180 and "Got error 121" in _last_error_message


def is_commit_conflict(errno):
    """MySQL wraps every nonzero handlerton commit result as 1180, so a lost
    race at the commit can only arrive as the wrapped 149."""
    return errno == 1180 and "Got error 149" in _last_error_message


def reset(db, cursor):
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    db.commit()


def create_table(cursor, primary_key=True):
    global _table_seq
    _table_seq += 1
    table = f"t{int(time.time() * 1000000)}_{_table_seq}"
    key = "PRIMARY KEY (id)" if primary_key else "INDEX id_idx (id)"
    cursor.execute(
        f"""CREATE TABLE {DBNAME}.{table} (
            id INT NOT NULL,
            v VARCHAR(32) NOT NULL,
            {key}
        ) ENGINE = LineairDB"""
    )
    return table


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


def run_transaction(cursor, statements):
    """Run BEGIN + statements + COMMIT; return (errno, failing statement)."""
    for sql in ["BEGIN"] + statements + ["COMMIT"]:
        errno = run(cursor, sql)
        if errno is not None:
            run(cursor, "ROLLBACK")
            return errno, sql
    return None, None


def rows(cursor, table):
    cursor.execute(f"SELECT id, v FROM {DBNAME}.{table} ORDER BY id")
    return cursor.fetchall()


def seed(cursor, table, values):
    errno = run(cursor, insert_sql(table, values))
    return errno


def insert_sql(table, values, prefix="INSERT"):
    body = ", ".join(f"({i}, '{v}')" for i, v in values)
    return f"{prefix} INTO {DBNAME}.{table} VALUES {body}"


def test_duplicate_in_autocommit(cursor):
    print("INSERT DUPLICATE PRIMARY KEY, AUTOCOMMIT TEST")
    table = create_table(cursor)
    if seed(cursor, table, [(1, "first")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    errno = run(cursor, insert_sql(table, [(1, "second")]))
    if errno != 1062:
        print(f"\tFailed: expected 1062, got {errno}")
        return 1

    surviving = rows(cursor, table)
    if surviving != [(1, "first")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'first')]")
        return 1

    print("\tPassed!")
    return 0


def test_duplicate_in_transaction(cursor):
    print("INSERT DUPLICATE PRIMARY KEY, INSIDE A TRANSACTION TEST")
    table = create_table(cursor)
    if seed(cursor, table, [(1, "first")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    errno, failed_at = run_transaction(cursor, [
        insert_sql(table, [(10, "fresh")]),
        insert_sql(table, [(1, "second")]),
    ])
    if not is_duplicate(errno):
        print(f"\tFailed: expected a duplicate shape, got {errno} "
              f"({_last_error_message})")
        return 1

    surviving = rows(cursor, table)
    if surviving != [(1, "first")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'first')]")
        return 1

    print(f"\tPassed! (rejected with {errno} on {failed_at})")
    return 0


def test_duplicate_within_one_statement(cursor):
    print("INSERT DUPLICATE PRIMARY KEY, TWICE IN ONE STATEMENT TEST")
    table = create_table(cursor)

    errno = run(cursor, insert_sql(table, [(1, "a"), (1, "b")]))
    if errno != 1062:
        print(f"\tFailed: expected 1062, got {errno}")
        return 1

    surviving = rows(cursor, table)
    if surviving != []:
        print(f"\tFailed: table holds {surviving}, expected no rows")
        return 1

    print("\tPassed!")
    return 0


def test_replace_overwrites(cursor):
    print("REPLACE OF AN EXISTING PRIMARY KEY TEST")
    table = create_table(cursor)
    if seed(cursor, table, [(1, "first")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    errno = run(cursor, insert_sql(table, [(1, "replaced")], prefix="REPLACE"))
    if errno is not None:
        print(f"\tFailed: REPLACE rejected with {errno}")
        return 1

    surviving = rows(cursor, table)
    if surviving != [(1, "replaced")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'replaced')]")
        return 1

    print("\tPassed!")
    return 0


def test_on_duplicate_key_update(cursor):
    print("INSERT ON DUPLICATE KEY UPDATE TEST")
    table = create_table(cursor)
    if seed(cursor, table, [(1, "first")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    errno = run(cursor, insert_sql(table, [(1, "ignored")]) +
                " ON DUPLICATE KEY UPDATE v = 'updated'")
    if errno is not None:
        print(f"\tFailed: rejected with {errno}")
        return 1

    surviving = rows(cursor, table)
    if surviving != [(1, "updated")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'updated')]")
        return 1

    print("\tPassed!")
    return 0


def test_insert_ignore(cursor):
    print("INSERT IGNORE OF A DUPLICATE TEST")
    table = create_table(cursor)
    if seed(cursor, table, [(1, "first")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    errno = run(cursor, insert_sql(table, [(1, "ignored")],
                                   prefix="INSERT IGNORE"))
    if errno is not None:
        print(f"\tFailed: INSERT IGNORE rejected with {errno}")
        return 1

    cursor.execute("SHOW WARNINGS")
    warnings = cursor.fetchall()
    if len(warnings) != 1 or warnings[0][1] != 1062:
        print(f"\tFailed: expected one 1062 warning, got {warnings}")
        return 1

    surviving = rows(cursor, table)
    if surviving != [(1, "first")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'first')]")
        return 1

    print("\tPassed!")
    return 0


def test_delete_then_insert(cursor):
    print("DELETE THEN INSERT THE SAME KEY IN ONE TRANSACTION TEST")
    table = create_table(cursor)
    if seed(cursor, table, [(1, "first")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    errno, failed_at = run_transaction(cursor, [
        f"DELETE FROM {DBNAME}.{table} WHERE id = 1",
        insert_sql(table, [(1, "reinserted")]),
    ])
    if errno is not None:
        print(f"\tFailed: rejected with {errno} on {failed_at}")
        return 1

    surviving = rows(cursor, table)
    if surviving != [(1, "reinserted")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'reinserted')]")
        return 1

    print("\tPassed!")
    return 0


def test_insert_delete_then_insert(cursor):
    # The staged row is a tombstone by the time the second insert runs, and a
    # tombstone still carries the buffer the first insert wrote.
    print("INSERT, DELETE, THEN INSERT THE SAME KEY IN ONE TRANSACTION TEST")
    table = create_table(cursor)

    errno, failed_at = run_transaction(cursor, [
        insert_sql(table, [(1, "first")]),
        f"DELETE FROM {DBNAME}.{table} WHERE id = 1",
        insert_sql(table, [(1, "second")]),
    ])
    if errno is not None:
        print(f"\tFailed: rejected with {errno} on {failed_at}")
        return 1

    surviving = rows(cursor, table)
    if surviving != [(1, "second")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'second')]")
        return 1

    print("\tPassed!")
    return 0


def start_statement(cursor, sql):
    """Run one statement on a worker thread. Returns (thread, outcome dict)."""
    outcome = {}

    def worker():
        outcome["errno"] = run(cursor, sql)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread, outcome


def test_concurrent_sessions(cursor, user, password):
    print("TWO SESSIONS INSERTING THE SAME NEW KEY TEST")
    table = create_table(cursor)

    session_a = get_connection(user=user, password=password)
    session_b = get_connection(user=user, password=password)
    cursor_a = session_a.cursor()
    cursor_b = session_b.cursor()
    stalled = False
    try:
        for cur in (cursor_a, cursor_b):
            cur.execute("BEGIN")

        errno = run(cursor_a, insert_sql(table, [(1, "a")]))
        if errno is not None:
            print(f"\tFailed: session A insert rejected with {errno}")
            return 1
        errno = run(cursor_b, insert_sql(table, [(1, "b")]))
        if errno is not None:
            print(f"\tFailed: session B insert rejected with {errno}")
            return 1

        # A commits on its own thread: a plugin built with FENCE=true holds that
        # commit until B's transaction ends, so B has to run meanwhile. The head
        # start keeps A the first to reach the server in either build.
        thread_a, outcome_a = start_statement(cursor_a, "COMMIT")
        time.sleep(COMMIT_HEAD_START_SECONDS)

        errno = run(cursor_b, "COMMIT")
        run(cursor_b, "ROLLBACK")
        if errno is None:
            print("\tFailed: both sessions committed the same key")
            return 1
        # A blind race for a fresh key has no stale read, so the loser may
        # truthfully hear either a duplicate or a lost race, in commit shape.
        if not (is_commit_duplicate(errno) or is_commit_conflict(errno)):
            print(f"\tFailed: expected a commit-shaped duplicate or conflict, "
                  f"got {errno} ({_last_error_message})")
            return 1

        thread_a.join(COMMIT_DEADLINE_SECONDS)
        if thread_a.is_alive():
            stalled = True
            print(f"\tFailed: the first COMMIT did not return within "
                  f"{COMMIT_DEADLINE_SECONDS:.0f}s of the second one failing")
            return 1
        if outcome_a.get("errno") is not None:
            print(f"\tFailed: first committer rejected with {outcome_a['errno']}")
            return 1
    finally:
        if not stalled:
            cursor_a.close()
            session_a.close()
        cursor_b.close()
        session_b.close()

    surviving = rows(cursor, table)
    if surviving != [(1, "a")]:
        print(f"\tFailed: table holds {surviving}, expected [(1, 'a')]")
        return 1

    print(f"\tPassed! (second committer rejected with {errno})")
    return 0


def test_conflicting_read_outranks_the_duplicate(cursor, user, password):
    # The TPC-C shape: two sessions take the same next order id, the first
    # commits its increment and its order, and the second's insert then lands
    # on a key that exists. Its district read is already stale, so this is a
    # conflict to retry, not a duplicate to report -- BenchBase retries 1213
    # and commit-time 1180, never 1062.
    print("A STALE READ OUTRANKS THE DUPLICATE IT CAUSED TEST")
    district = create_table(cursor)
    orders = create_table(cursor)
    if seed(cursor, district, [(1, "100")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    session_a = get_connection(user=user, password=password)
    session_b = get_connection(user=user, password=password)
    cursor_a = session_a.cursor()
    cursor_b = session_b.cursor()
    stalled = False
    try:
        for cur in (cursor_a, cursor_b):
            cur.execute("BEGIN")
        for cur in (cursor_a, cursor_b):
            cur.execute(f"SELECT v FROM {DBNAME}.{district} WHERE id = 1")
            if cur.fetchall()[0][0] != "100":
                print("\tFailed: the next order id was not read back")
                return 1

        errno = run(cursor_a,
                    f"UPDATE {DBNAME}.{district} SET v = '101' WHERE id = 1")
        if errno is None:
            errno = run(cursor_a, insert_sql(orders, [(100, "a")]))
        if errno is not None:
            print(f"\tFailed: session A rejected with {errno}")
            return 1

        # A commits on its own thread: a plugin built with FENCE=true holds
        # that commit until B's transaction ends.
        thread_a, outcome_a = start_statement(cursor_a, "COMMIT")
        time.sleep(COMMIT_HEAD_START_SECONDS)

        errno = run(cursor_b, insert_sql(orders, [(100, "b")]))
        if errno is None:
            errno = run(cursor_b, "COMMIT")
        run(cursor_b, "ROLLBACK")
        # Only the conflict shape retries; a duplicate shape here is the
        # regression this test exists for. Under FENCE=true, B reaches 1213
        # through ordinary validation; only FENCE=false hits the row guard.
        if not is_conflict(errno):
            print(f"\tFailed: expected a conflict shape, got {errno} "
                  f"({_last_error_message})")
            return 1

        thread_a.join(COMMIT_DEADLINE_SECONDS)
        if thread_a.is_alive():
            stalled = True
            print("\tFailed: the first COMMIT did not return")
            return 1
        if outcome_a.get("errno") is not None:
            print(f"\tFailed: first committer rejected with {outcome_a['errno']}")
            return 1
    finally:
        if not stalled:
            cursor_a.close()
            session_a.close()
        cursor_b.close()
        session_b.close()

    surviving = rows(cursor, orders)
    if surviving != [(100, "a")]:
        print(f"\tFailed: table holds {surviving}, expected [(100, 'a')]")
        return 1

    print(f"\tPassed! (session B rejected with {errno})")
    return 0


def test_table_without_primary_key(cursor):
    print("TABLE WITH NO DECLARED PRIMARY KEY TEST")
    table = create_table(cursor, primary_key=False)

    for _ in range(3):
        errno = run(cursor, insert_sql(table, [(1, "same")]))
        if errno is not None:
            print(f"\tFailed: insert rejected with {errno}")
            return 1

    cursor.execute(f"SELECT COUNT(*) FROM {DBNAME}.{table}")
    total = cursor.fetchone()[0]
    if total != 3:
        print(f"\tFailed: table holds {total} rows, expected 3")
        return 1

    print("\tPassed!")
    return 0


def test_prefetch_commit_path(cursor, db):
    # The prefetch commit installs rows itself, so it carries its own duplicate
    # check. A transaction reaches it only when its first statement is
    # prefetch-eligible, which is why this one opens with a staged read.
    print("INSERT DUPLICATE PRIMARY KEY ON THE PREFETCH COMMIT PATH TEST")
    table = create_table(cursor)
    if seed(cursor, table, [(1, "first"), (2, "second")]) is not None:
        print("\tFailed: seed insert rejected")
        return 1

    cursor.execute("SET GLOBAL lineairdb_prefetch_execution=ON")
    try:
        cursor.execute(f"SET @_tx_plan='R:{table}:2'")
        cursor.execute(f"USE {DBNAME}")
        errno = run(cursor, "START TRANSACTION")
        if errno is None:
            errno = run(cursor, f"SELECT v FROM {DBNAME}.{table} WHERE id = 2")
        if errno is not None:
            print(f"\tFailed: staged read rejected with {errno}")
            run(cursor, "ROLLBACK")
            return 1

        errno = run(cursor, insert_sql(table, [(1, "dup")]))
        if errno is not None:
            print(f"\tFailed: the staged INSERT must not fail at the "
                  f"statement, got {errno} ({_last_error_message})")
            run(cursor, "ROLLBACK")
            return 1
        commit_errno = run(cursor, "COMMIT")
        run(cursor, "ROLLBACK")
        if commit_errno is None:
            print("\tFailed: the duplicate committed")
            return 1
        if not is_commit_duplicate(commit_errno):
            print(f"\tFailed: expected the wrapped commit duplicate, got "
                  f"{commit_errno} ({_last_error_message})")
            return 1
    finally:
        cursor.execute(f"SET GLOBAL lineairdb_prefetch_execution="
                       f"{prefetch_setting()}")
        cursor.execute("SET @_tx_plan=NULL")

    surviving = rows(cursor, table)
    if surviving != [(1, "first"), (2, "second")]:
        print(f"\tFailed: table holds {surviving}, expected the seeded rows")
        return 1

    print(f"\tPassed! (rejected with {commit_errno} on the COMMIT)")
    return 0


def prefetch_setting():
    return "ON" if args.prefetch else "OFF"


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()

    reset(db, cursor)
    cursor.execute(f"SET GLOBAL lineairdb_prefetch_execution="
                   f"{prefetch_setting()}")
    print(f"lineairdb_prefetch_execution={prefetch_setting()}")
    # Transactions are opened with an explicit BEGIN so that a lone statement
    # is its own transaction and every error arrives through the cursor.
    db.autocommit = True

    result = 0
    result |= test_duplicate_in_autocommit(cursor)
    result |= test_duplicate_in_transaction(cursor)
    result |= test_duplicate_within_one_statement(cursor)
    result |= test_replace_overwrites(cursor)
    result |= test_on_duplicate_key_update(cursor)
    result |= test_insert_ignore(cursor)
    result |= test_delete_then_insert(cursor)
    result |= test_insert_delete_then_insert(cursor)
    result |= test_concurrent_sessions(cursor, args.user, args.password)
    result |= test_conflicting_read_outranks_the_duplicate(
        cursor, args.user, args.password)
    result |= test_table_without_primary_key(cursor)
    result |= test_prefetch_commit_path(cursor, db)

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
    parser.add_argument('--prefetch', action='store_true',
                        help='run with lineairdb_prefetch_execution ON')
    args = parser.parse_args()
    main()
