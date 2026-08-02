import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_lineairdb_prefetch_index_tail_{int(time.time())}"

# The staged tail window is INDEX_CURSOR_READ_AHEAD_SIZE (1024) rows; use a
# table larger than one window so window-boundary behavior is exercised.
ROWS = 2500

_table_seq = 0


def reset(cursor, db):
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    db.commit()


def load_rows(cursor, db, n):
    # DROP TABLE / DROP DATABASE do not purge LineairDB storage rows (known
    # upstream bug): a same-name recreate still sees the old rows. Use a fresh
    # table name per load so shrinking loads are not masked.
    global _table_seq
    _table_seq += 1
    name = f"t{_table_seq}"
    cursor.execute(
        f"CREATE TABLE {name} (id INT NOT NULL PRIMARY KEY, v INT NOT NULL) "
        "ENGINE=LineairDB"
    )
    batch = []
    for i in range(1, n + 1):
        batch.append(f"({i},{i * 10})")
        if len(batch) == 500:
            cursor.execute(f"INSERT INTO {name} VALUES " + ",".join(batch))
            batch = []
    if batch:
        cursor.execute(f"INSERT INTO {name} VALUES " + ",".join(batch))
    db.commit()
    return name


def stmt_prefetch_on(cursor):
    # Statement-scoped autogen prefetch: prefetch ON, no @_tx_plan.
    cursor.execute("SET GLOBAL lineairdb_prefetch_execution=ON")
    cursor.execute("SET @_tx_plan=NULL")


def prefetch_off(cursor):
    cursor.execute("SET GLOBAL lineairdb_prefetch_execution=OFF")


def recover(cursor):
    try:
        cursor.execute("ROLLBACK")
    except mysql.connector.Error:
        pass


def test_max_under_stmt_prefetch(cursor, db):
    print("PREFETCH INDEX TAIL: MAX under stmt-scoped prefetch")
    t = load_rows(cursor, db, ROWS)
    stmt_prefetch_on(cursor)
    cursor.execute(f"SELECT MAX(id) FROM {t}")
    got = cursor.fetchone()[0]
    if got != ROWS:
        print(f"\tFAILED: MAX(id) = {got}, want {ROWS}")
        return 1
    # MAX must also be correct when the statement runs inside an explicit tx.
    cursor.execute("START TRANSACTION")
    cursor.execute(f"SELECT MAX(id) FROM {t}")
    got = cursor.fetchone()[0]
    cursor.execute("COMMIT")
    if got != ROWS:
        print(f"\tFAILED: in-tx MAX(id) = {got}, want {ROWS}")
        return 1
    print("\tPassed!")
    return 0


def test_max_small_and_empty(cursor, db):
    print("PREFETCH INDEX TAIL: MAX on small and empty tables")
    stmt_prefetch_on(cursor)
    t = load_rows(cursor, db, 0)
    cursor.execute(f"SELECT MAX(id) FROM {t}")
    if cursor.fetchone()[0] is not None:
        print("\tFAILED: MAX on empty table is not NULL")
        return 1
    t = load_rows(cursor, db, 3)
    cursor.execute(f"SELECT MAX(id) FROM {t}")
    got = cursor.fetchone()[0]
    if got != 3:
        print(f"\tFAILED: MAX(id) = {got}, want 3")
        return 1
    print("\tPassed!")
    return 0


def test_desc_limit_within_window(cursor, db):
    # This shape works under the staged tail window and must succeed.
    print("PREFETCH INDEX TAIL: ORDER BY pk DESC LIMIT within window")
    t = load_rows(cursor, db, ROWS)
    stmt_prefetch_on(cursor)
    cursor.execute(f"SELECT id FROM {t} ORDER BY id DESC LIMIT 5")
    got = [r[0] for r in cursor.fetchall()]
    want = list(range(ROWS, ROWS - 5, -1))
    if got != want:
        print(f"\tFAILED: got {got}, want {want}")
        return 1
    print("\tPassed!")
    return 0


ER_NOT_SUPPORTED_YET = 1235


def test_desc_walk_past_window_never_silent(cursor, db):
    # A derived-table COUNT would let the optimizer drop the ORDER BY and
    # bypass index_last; force the backward index walk so the read really
    # runs off the staged tail window.
    print("PREFETCH INDEX TAIL: full DESC walk past window is loud or correct")
    t = load_rows(cursor, db, ROWS)
    stmt_prefetch_on(cursor)
    try:
        cursor.execute(f"SELECT id FROM {t} FORCE INDEX(PRIMARY) "
                       "ORDER BY id DESC")
        got = [r[0] for r in cursor.fetchall()]
    except mysql.connector.Error as e:
        recover(cursor)
        if e.errno != ER_NOT_SUPPORTED_YET:
            print(f"\tFAILED: unexpected error {e.errno}: {e}")
            return 1
        print(f"\tPassed (loud reject: {e.errno})")
        return 0
    if got != list(range(ROWS, 0, -1)):
        print(f"\tFAILED: silent truncation, got {len(got)} rows, want {ROWS}")
        return 1
    print("\tPassed!")
    return 0


def test_own_write_then_max_never_stale(cursor, db):
    print("PREFETCH INDEX TAIL: in-tx INSERT then MAX is loud or correct")
    t = load_rows(cursor, db, ROWS)
    stmt_prefetch_on(cursor)
    cursor.execute("START TRANSACTION")
    cursor.execute(f"INSERT INTO {t} VALUES ({ROWS + 1},0)")
    failed = None
    try:
        cursor.execute(f"SELECT MAX(id) FROM {t}")
        got = cursor.fetchone()[0]
        if got != ROWS + 1:
            failed = f"stale MAX(id) = {got}, want {ROWS + 1} or a loud reject"
    except mysql.connector.Error as e:
        if e.errno != ER_NOT_SUPPORTED_YET:
            failed = f"unexpected error {e.errno}: {e}"
        else:
            print(f"\tloud reject mid-tx: {e.errno}")
    recover(cursor)
    if failed is not None:
        print(f"\tFAILED: {failed}")
        return 1
    print("\tPassed!")
    return 0


def test_window_boundaries(cursor, db):
    # INDEX_CURSOR_READ_AHEAD_SIZE is 1024; MAX must be right at 1, K-1, K, K+1.
    print("PREFETCH INDEX TAIL: MAX at window-size boundaries")
    stmt_prefetch_on(cursor)
    for n in (1, 1023, 1024, 1025):
        t = load_rows(cursor, db, n)
        cursor.execute(f"SELECT MAX(id) FROM {t}")
        got = cursor.fetchone()[0]
        if got != n:
            print(f"\tFAILED: rows={n}, MAX(id) = {got}")
            return 1
    print("\tPassed!")
    return 0


def test_all_tombstone(cursor, db):
    # The staged tail scan skips tombstones server-side; MAX must be NULL.
    print("PREFETCH INDEX TAIL: MAX after deleting every row")
    t = load_rows(cursor, db, 200)
    # A key-less DELETE is a full table scan, which stmt-prefetch rejects by
    # design; run the cleanup in baseline mode, then query under prefetch.
    prefetch_off(cursor)
    cursor.execute(f"DELETE FROM {t}")
    db.commit()
    stmt_prefetch_on(cursor)
    cursor.execute(f"SELECT MAX(id) FROM {t}")
    got = cursor.fetchone()[0]
    if got is not None:
        print(f"\tFAILED: MAX over tombstones = {got}, want NULL")
        return 1
    print("\tPassed!")
    return 0


def test_exact_window_full_desc_walk(cursor, db):
    # Exactly K live rows: a full backward walk cannot distinguish true EOF
    # from window overrun (documented limitation), so a loud reject is
    # acceptable; silent truncation is not.
    print("PREFETCH INDEX TAIL: exactly-K full DESC walk is loud or correct")
    t = load_rows(cursor, db, 1024)
    stmt_prefetch_on(cursor)
    try:
        cursor.execute(f"SELECT id FROM {t} FORCE INDEX(PRIMARY) "
                       "ORDER BY id DESC")
        got = [r[0] for r in cursor.fetchall()]
    except mysql.connector.Error as e:
        recover(cursor)
        if e.errno != ER_NOT_SUPPORTED_YET:
            print(f"\tFAILED: unexpected error {e.errno}: {e}")
            return 1
        print(f"\tPassed (loud reject: {e.errno})")
        return 0
    if got != list(range(1024, 0, -1)):
        print(f"\tFAILED: silent truncation, got {len(got)} rows, want 1024")
        return 1
    print("\tPassed!")
    return 0


def test_baseline_regression(cursor, db):
    print("PREFETCH INDEX TAIL: baseline (prefetch OFF) MAX still works")
    t = load_rows(cursor, db, ROWS)
    prefetch_off(cursor)
    cursor.execute(f"SELECT MAX(id) FROM {t}")
    got = cursor.fetchone()[0]
    if got != ROWS:
        print(f"\tFAILED: baseline MAX(id) = {got}, want {ROWS}")
        return 1
    print("\tPassed!")
    return 0


TESTS = [
    test_max_under_stmt_prefetch,
    test_max_small_and_empty,
    test_desc_limit_within_window,
    test_desc_walk_past_window_never_silent,
    test_own_write_then_max_never_stale,
    test_window_boundaries,
    test_all_tombstone,
    test_exact_window_full_desc_walk,
    test_baseline_regression,
]


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    reset(cursor, db)

    rc = 0
    for test in TESTS:
        try:
            rc |= test(cursor, db)
        except mysql.connector.Error as e:
            # Never leave an aborted session behind; fail this test and go on.
            print(f"\tFAILED (unexpected mysql error): {e}")
            recover(cursor)
            rc = 1

    # Close the test connection first: with autocommit off its last SELECT
    # holds an implicit transaction whose metadata lock would block the
    # DROP DATABASE below forever.
    try:
        db.rollback()
        cursor.close()
        db.close()
    except Exception:
        pass
    # Clean up on a fresh connection: the shared one may hold aborted state.
    try:
        db2 = get_connection(user=args.user, password=args.password)
        c2 = db2.cursor()
        c2.execute("SET GLOBAL lineairdb_prefetch_execution=OFF")
        c2.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
        db2.commit()
        db2.close()
    except Exception:
        pass
    sys.exit(rc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()
    main()
