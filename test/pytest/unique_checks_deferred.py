"""UNIQUE secondary index behaviour under unique_checks=0.

The index write is buffered with the row instead of taking a synchronous RPC,
so the server enforces uniqueness when the buffer is written. These cases check
that a duplicate is still refused: within a statement, across the auto-flush
batch boundary, across sessions, and on the losing side of a commit race.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time

import mysql.connector

from utils.connection import get_connection

# Upper bound for a commit that has to wait for another session's transaction
COMMIT_DEADLINE_SECONDS = 20.0

# Lead given to the first committer so it reaches the server first
COMMIT_HEAD_START_SECONDS = 0.5

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# The RPC-shape case runs its own traced mysqld so the shared one is untouched
RPC_TRACE_PORT = 3308
RPC_TRACE_ROWS = 5
# write_row's synchronous branch issues one of these per row; the deferred
# branch issues none and leaves the index write to the batched write.
SYNC_INDEX_RPC = "TX_WRITE_SECONDARY_INDEX"
BATCHED_WRITE_RPC = "TX_BATCH_WRITE"

_table_seq = 0
_last_error_message = ""


def is_conflict(errno):
    """A duplicate unique key is an OCC abort: 1213 when a statement's own
    flush reports it, or 1180 wrapping handler error 149 when the commit does,
    which has no handler in scope."""
    return errno == 1213 or (errno == 1180 and
                             "Got error 149" in _last_error_message)


def reset(db, cursor):
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    db.commit()


def create_table(cursor, unique_index=True, second_index=False):
    global _table_seq
    _table_seq += 1
    table = f"uniq_defer_{int(time.time() * 1000000)}_{_table_seq}"
    index = "UNIQUE INDEX" if unique_index else "INDEX"
    # An indexed column has to be NOT NULL and fit the 255-byte key limit
    name_column = "VARCHAR(63) NOT NULL" if second_index else "VARCHAR(64)"
    extra = ",\n            INDEX name_idx (name)" if second_index else ""
    cursor.execute(
        f'''CREATE TABLE ha_lineairdb_test.{table} (
            id INT NOT NULL,
            uval VARCHAR(63) NOT NULL,
            name {name_column},
            PRIMARY KEY (id),
            {index} uval_idx (uval){extra}
        ) ENGINE = LineairDB'''
    )
    return table


# One row buffers two ops (row write + index write), so a statement of this many
# rows crosses WRITE_BATCH_SIZE and the buffer auto-flushes mid-statement.
# This and the 1024/1025 boundary probes below assume WRITE_BATCH_SIZE = 1024
# (proxy/lineairdb_transaction.hh).
AUTO_FLUSH_ROWS = 1100


def bulk_rows(count, duplicate_of=None, duplicate_at=None):
    rows = [(i, f"k{i:06d}@example.com", f"n{i}") for i in range(1, count + 1)]
    if duplicate_of is not None:
        rows[duplicate_at - 1] = (duplicate_at, rows[duplicate_of - 1][1],
                                  f"n{duplicate_at}")
    return rows


def insert_sql(table, rows):
    values = ", ".join(f"({i}, '{v}', '{n}')" for i, v, n in rows)
    return f"INSERT INTO ha_lineairdb_test.{table} (id, uval, name) VALUES {values}"


def row_count(cursor, table):
    cursor.execute(f'SELECT COUNT(*) FROM ha_lineairdb_test.{table}')
    return cursor.fetchone()[0]


def index_ids(cursor, table, value):
    cursor.execute(
        f"SELECT id FROM ha_lineairdb_test.{table} "
        f"FORCE INDEX (uval_idx) WHERE uval = '{value}'"
    )
    return sorted(row[0] for row in cursor.fetchall())


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
    """Run BEGIN + statements + COMMIT; return (errno, failing statement).

    The failing statement is reported as its text so the caller can tell a
    statement-time rejection from a commit-time one.
    """
    for sql in ["BEGIN"] + statements + ["COMMIT"]:
        errno = run(cursor, sql)
        if errno is not None:
            run(cursor, "ROLLBACK")
            return errno, sql
    return None, None


def test_distinct_rows_commit(cursor):
    print("UNIQUE_CHECKS=0: DISTINCT KEYS TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor)

    values = ["a@example.com", "b@example.com", "c@example.com"]
    rows = [(i + 1, v, f"n{i + 1}") for i, v in enumerate(values)]
    errno = run(cursor, insert_sql(table, rows))
    if errno is not None:
        print(f"\tFailed: distinct-key insert rejected with {errno}")
        return 1

    total = row_count(cursor, table)
    by_index = [index_ids(cursor, table, v) for v in values]
    if total != len(rows) or by_index != [[1], [2], [3]]:
        print(f"\tFailed: primary key sees {total} rows, unique index sees {by_index}")
        return 1

    print("\tPassed!")
    return 0


def test_duplicate_within_statement(cursor):
    print("UNIQUE_CHECKS=0: DUPLICATE WITHIN ONE STATEMENT TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor)

    errno = run(cursor, insert_sql(
        table, [(1, 'dup@example.com', 'first'), (2, 'dup@example.com', 'second')]))
    if errno != 1213:
        print(f"\tFailed: expected 1213 on the statement, got {errno}")
        return 1

    total = row_count(cursor, table)
    stray = index_ids(cursor, table, 'dup@example.com')
    if total != 0 or stray != []:
        print(f"\tFailed: table has {total} rows and index entries {stray}, expected 0/[]")
        return 1

    print(f"\tPassed! (rejected with {errno})")
    return 0


def test_duplicate_of_committed_row(cursor):
    print("UNIQUE_CHECKS=0: DUPLICATE OF COMMITTED ROW TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor)

    errno = run(cursor, insert_sql(table, [(1, 'taken@example.com', 'first')]))
    if errno is not None:
        print(f"\tFailed: first insert rejected with {errno}")
        return 1

    errno = run(cursor, insert_sql(table, [(2, 'taken@example.com', 'second')]))
    if errno != 1213:
        print(f"\tFailed: expected 1213 on the statement, got {errno}")
        return 1

    total = row_count(cursor, table)
    hits = index_ids(cursor, table, 'taken@example.com')
    if total != 1 or hits != [1]:
        print(f"\tFailed: table has {total} rows and index entries {hits}, expected 1/[1]")
        return 1

    print(f"\tPassed! (rejected with {errno})")
    return 0


def test_duplicate_within_transaction(cursor):
    # The deferral is within a statement: an INSERT sends its own rows before
    # it returns, so the second one is rejected then, not on the COMMIT.
    print("UNIQUE_CHECKS=0: DUPLICATE INSIDE ONE TRANSACTION TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor)

    for sql in ["BEGIN", insert_sql(table, [(1, 'tx@example.com', 'first')])]:
        errno = run(cursor, sql)
        if errno is not None:
            run(cursor, "ROLLBACK")
            print(f"\tFailed: rejected with {errno} on {sql}")
            return 1

    errno = run(cursor, insert_sql(table, [(2, 'tx@example.com', 'second')]))
    run(cursor, "ROLLBACK")
    if errno is None:
        print("\tFailed: the second INSERT was accepted, expected a duplicate")
        return 1
    if errno != 1213:
        print(f"\tFailed: expected 1213 on the second INSERT, got {errno}")
        return 1

    total = row_count(cursor, table)
    hits = index_ids(cursor, table, 'tx@example.com')
    if total != 0 or hits != []:
        print(f"\tFailed: table has {total} rows and index entries {hits}, expected 0/[]")
        return 1

    print(f"\tPassed! (rejected with {errno} on the second INSERT)")
    return 0


def test_unique_checks_on_is_unchanged(cursor):
    print("UNIQUE_CHECKS=1: BASELINE UNCHANGED TEST")
    cursor.execute("SET SESSION unique_checks = 1")
    table = create_table(cursor)

    errno = run(cursor, insert_sql(
        table, [(1, 'strict@example.com', 'first'), (2, 'strict@example.com', 'second')]))
    if errno != 1213:
        print(f"\tFailed: expected 1213 on the statement, got {errno}")
        return 1

    errno = run(cursor, insert_sql(table, [(3, 'strict@example.com', 'third')]))
    if errno is not None:
        print(f"\tFailed: insert of a free key rejected with {errno}")
        return 1

    errno = run(cursor, insert_sql(table, [(4, 'strict@example.com', 'fourth')]))
    if errno != 1213:
        print(f"\tFailed: expected 1213 on the statement, got {errno}")
        return 1

    duplicate_sql = insert_sql(table, [(6, 'strict2@example.com', 'sixth')])
    errno, failed_at = run_transaction(cursor, [
        insert_sql(table, [(5, 'strict2@example.com', 'fifth')]),
        duplicate_sql,
    ])
    if errno != 1213 or failed_at != duplicate_sql:
        print(f"\tFailed: expected 1213 on the second INSERT, got {errno} on {failed_at}")
        return 1

    total = row_count(cursor, table)
    hits = index_ids(cursor, table, 'strict@example.com')
    if total != 1 or hits != [3]:
        print(f"\tFailed: table has {total} rows and index entries {hits}, expected 1/[3]")
        return 1

    print("\tPassed!")
    return 0


def test_insert_then_update(cursor, unique_checks, unique_index=True):
    # End state of an index entry an UPDATE moves in the same transaction that
    # inserted it: the new value is the only one the index answers with.
    kind = "UNIQUE" if unique_index else "NON-UNIQUE"
    print(f"UNIQUE_CHECKS={unique_checks}: INSERT THEN UPDATE, {kind} INDEX TEST")
    cursor.execute(f"SET SESSION unique_checks = {unique_checks}")
    table = create_table(cursor, unique_index)

    errno, failed_at = run_transaction(cursor, [
        insert_sql(table, [(1, 'before@example.com', 'first')]),
        f"UPDATE ha_lineairdb_test.{table} SET uval = 'after@example.com' WHERE id = 1",
    ])
    if errno is not None:
        print(f"\tFailed: rejected with {errno} on {failed_at}")
        return 1

    total = row_count(cursor, table)
    moved = index_ids(cursor, table, 'after@example.com')
    stale = index_ids(cursor, table, 'before@example.com')
    if total != 1 or moved != [1] or stale != []:
        print(f"\tFailed: {total} rows, unique index sees 'after'={moved} "
              f"'before'={stale}, expected 1/[1]/[]")
        return 1

    print("\tPassed!")
    return 0


def test_auto_flush_distinct(cursor):
    print("UNIQUE_CHECKS=0: AUTO-FLUSH BOUNDARY, DISTINCT KEYS TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor)

    rows = bulk_rows(AUTO_FLUSH_ROWS)
    errno = run(cursor, insert_sql(table, rows))
    if errno is not None:
        print(f"\tFailed: bulk insert rejected with {errno}")
        return 1

    total = row_count(cursor, table)
    cursor.execute(f"SELECT COUNT(*) FROM ha_lineairdb_test.{table} "
                   f"FORCE INDEX (uval_idx) WHERE uval >= '{rows[0][1]}' "
                   f"AND uval <= '{rows[-1][1]}'")
    by_index = cursor.fetchone()[0]
    boundary = {i: index_ids(cursor, table, rows[i - 1][1])
                for i in (1, 1024, 1025, AUTO_FLUSH_ROWS)}
    if (total != AUTO_FLUSH_ROWS or by_index != AUTO_FLUSH_ROWS
            or any(ids != [i] for i, ids in boundary.items())):
        print(f"\tFailed: {total} rows by primary key, {by_index} by unique index, "
              f"boundary lookups {boundary}, expected {AUTO_FLUSH_ROWS}")
        return 1

    print("\tPassed!")
    return 0


def test_auto_flush_duplicate_across_batches(cursor):
    print("UNIQUE_CHECKS=0: AUTO-FLUSH BOUNDARY, DUPLICATE ACROSS BATCHES TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor)

    # Row 5 leaves with an auto-flushed batch, its duplicate at row 1030 only
    # later, so the server has to reject a duplicate spanning two batches.
    rows = bulk_rows(AUTO_FLUSH_ROWS, duplicate_of=5, duplicate_at=1030)
    errno = run(cursor, insert_sql(table, rows))
    if errno != 1213:
        print(f"\tFailed: expected 1213 on the statement, got {errno}")
        return 1

    total = row_count(cursor, table)
    if total != 0:
        print(f"\tFailed: table has {total} rows, expected 0")
        return 1

    print(f"\tPassed! (rejected with {errno})")
    return 0


def test_auto_flush_distinct_two_indexes(cursor):
    # Three ops per row do not divide the batch, so rows straddle the flush
    # boundary. Every one of them still has to land.
    print("UNIQUE_CHECKS=0: AUTO-FLUSH BOUNDARY, TWO INDEXES, DISTINCT KEYS TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor, second_index=True)

    rows = bulk_rows(AUTO_FLUSH_ROWS)
    errno = run(cursor, insert_sql(table, rows))
    if errno is not None:
        print(f"\tFailed: bulk insert rejected with {errno}")
        return 1

    total = row_count(cursor, table)
    # 1024 ops is 341 whole rows plus one op, so row 342 is the split one
    boundary = {i: index_ids(cursor, table, rows[i - 1][1])
                for i in (1, 341, 342, 343, AUTO_FLUSH_ROWS)}
    if total != AUTO_FLUSH_ROWS or any(ids != [i] for i, ids in boundary.items()):
        print(f"\tFailed: {total} rows by primary key, boundary lookups "
              f"{boundary}, expected {AUTO_FLUSH_ROWS}")
        return 1

    print("\tPassed!")
    return 0


def test_auto_flush_duplicate_two_indexes(cursor):
    # Three ops per row, so a row's index writes land in the batch after its
    # own row write. The rejection still belongs to the statement.
    print("UNIQUE_CHECKS=0: AUTO-FLUSH BOUNDARY, TWO INDEXES TEST")
    cursor.execute("SET SESSION unique_checks = 0")
    table = create_table(cursor, second_index=True)

    rows = bulk_rows(AUTO_FLUSH_ROWS, duplicate_of=5, duplicate_at=1030)
    errno = run(cursor, insert_sql(table, rows))
    if errno != 1213:
        print(f"\tFailed: expected 1213 on the statement, got {errno}")
        return 1

    total = row_count(cursor, table)
    if total != 0:
        print(f"\tFailed: table has {total} rows, expected 0")
        return 1

    print(f"\tPassed! (rejected with {errno})")
    return 0


def start_statement(cursor, sql):
    """Run one statement on a worker thread. Returns (thread, outcome dict)."""
    outcome = {}

    def worker():
        outcome["errno"] = run(cursor, sql)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread, outcome


def test_cross_session_duplicate(cursor, user, password):
    print("UNIQUE_CHECKS=0: DUPLICATE FROM ANOTHER SESSION TEST")
    table = create_table(cursor)

    session_a = get_connection(user=user, password=password)
    session_b = get_connection(user=user, password=password)
    cursor_a = session_a.cursor()
    cursor_b = session_b.cursor()
    try:
        for cur in (cursor_a, cursor_b):
            cur.execute("SET SESSION unique_checks = 0")

        errno, _ = run_transaction(
            cursor_a, [insert_sql(table, [(1, 'cross@example.com', 'a')])])
        if errno is not None:
            print(f"\tFailed: session A rejected with {errno}")
            return 1

        duplicate_sql = insert_sql(table, [(2, 'cross@example.com', 'b')])
        errno, failed_at = run_transaction(cursor_b, [duplicate_sql])
        if errno != 1213 or failed_at != duplicate_sql:
            print(f"\tFailed: expected 1213 on session B's INSERT, got {errno} "
                  f"on {failed_at}")
            return 1
    finally:
        cursor_a.close()
        cursor_b.close()
        session_a.close()
        session_b.close()

    total = row_count(cursor, table)
    hits = index_ids(cursor, table, 'cross@example.com')
    if total != 1 or hits != [1]:
        print(f"\tFailed: table has {total} rows and index entries {hits}, "
              "expected the single committed row 1")
        return 1

    print(f"\tPassed! (session B rejected with {errno})")
    return 0


def test_concurrent_sessions(cursor, user, password):
    print("UNIQUE_CHECKS=0: TWO CONCURRENT SESSIONS TEST")
    table = create_table(cursor)
    # Read back over the index both sessions have written to, to check the
    # lookup itself does not reject either of them.
    lookup_sql = (f"SELECT id FROM ha_lineairdb_test.{table} "
                  "FORCE INDEX (uval_idx) WHERE uval = 'absent@example.com'")

    session_a = get_connection(user=user, password=password)
    session_b = get_connection(user=user, password=password)
    cursor_a = session_a.cursor()
    cursor_b = session_b.cursor()
    stalled = False
    try:
        for cur in (cursor_a, cursor_b):
            cur.execute("SET SESSION unique_checks = 0")
            cur.execute("BEGIN")

        errno = run(cursor_a, insert_sql(table, [(1, 'race@example.com', 'a')]))
        if errno is not None:
            print(f"\tFailed: session A insert rejected with {errno}")
            return 1
        errno = run(cursor_b, insert_sql(table, [(2, 'race@example.com', 'b')]))
        if errno is not None:
            print(f"\tFailed: session B insert rejected with {errno}")
            return 1

        # Each INSERT was flushed at its own statement end, so the server now
        # holds both duplicate index writes from two open transactions. Neither
        # is in conflict yet: only committing one makes them so.
        errno = run(cursor_a, lookup_sql)
        if errno is not None:
            print(f"\tFailed: session A lookup rejected with {errno}")
            return 1
        errno = run(cursor_b, lookup_sql)
        if errno is not None:
            print(f"\tFailed: session B lookup rejected with {errno}")
            return 1

        # A commits on its own thread: a plugin built with FENCE=true holds that
        # commit until B's transaction ends, so B has to run meanwhile. OCC lets
        # at most one of the two commit.
        thread_a, outcome_a = start_statement(cursor_a, "COMMIT")
        time.sleep(COMMIT_HEAD_START_SECONDS)

        errno = run(cursor_b, "COMMIT")
        run(cursor_b, "ROLLBACK")
        if errno is None:
            print("\tFailed: second committer unexpectedly succeeded")
            return 1
        if not is_conflict(errno):
            print(f"\tFailed: expected the COMMIT to fail as a duplicate, got "
                  f"{errno}: {_last_error_message}")
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

    total = row_count(cursor, table)
    hits = index_ids(cursor, table, 'race@example.com')
    if total != 1 or hits != [1]:
        print(f"\tFailed: table has {total} rows and index entries {hits}, "
              "expected the single committed row 1")
        return 1

    print(f"\tPassed! (second committer rejected with {errno})")
    return 0


def rpc_types_after(trace_path, offset, table):
    """Types of the RPCs the INSERTs on `table` issued past `offset`."""
    types = []
    with open(trace_path) as f:
        f.seek(offset)
        for line in f:
            tx = json.loads(line)
            inserts = {stmt["idx"] for stmt in tx["statements"]
                       if stmt["sql"].startswith("INSERT") and table in stmt["sql"]}
            types += [rpc["type"] for rpc in tx["rpcs"] if rpc["stmt"] in inserts]
    return types


def test_rpc_shape(user, password):
    # Every other case asserts an outcome the synchronous branch reaches too.
    # Only the RPCs tell the two branches apart, so this one reads the trace.
    print("UNIQUE_CHECKS=0: NO SYNCHRONOUS INDEX RPC TEST")
    trace_path = f"/tmp/uniq_defer_rpc_trace_{os.getpid()}.jsonl"
    # scripts/start_mysql.sh derives this path from the port it is given
    pid_path = f"/tmp/mysql_{RPC_TRACE_PORT}.pid"
    for path in (trace_path, pid_path):
        if os.path.exists(path):
            os.remove(path)

    done = subprocess.run(
        ["setsid", "-w", "./scripts/start_mysql.sh",
         "--mysqld-port", str(RPC_TRACE_PORT),
         "--server-host", "127.0.0.1", "--server-port", "9999"],
        cwd=ROOT, capture_output=True, text=True,
        env={**os.environ, "ENABLE_RPC_TRACE": "1",
             "ENABLE_RPC_TRACE_PATH": trace_path})
    pid = int(open(pid_path).read().strip()) if os.path.exists(pid_path) else None
    if done.returncode != 0 or pid is None:
        print(f"\tFailed: no mysqld on {RPC_TRACE_PORT}: {done.stdout[-300:]}")
        if pid is not None:
            os.kill(pid, signal.SIGKILL)
        return 1

    db = None
    try:
        db = mysql.connector.connect(host="127.0.0.1", port=RPC_TRACE_PORT,
                                     user=user, password=password)
        db.autocommit = True
        cursor = db.cursor()
        # DDL is per-mysqld, so this node creates the table it will write to
        cursor.execute("CREATE DATABASE IF NOT EXISTS ha_lineairdb_test")
        table = create_table(cursor)

        seen = {}
        for checks in (1, 0):
            cursor.execute(f"SET SESSION unique_checks = {checks}")
            offset = os.path.getsize(trace_path) if os.path.exists(trace_path) else 0
            first = 1 if checks == 1 else 1 + RPC_TRACE_ROWS
            rows = [(i, f"rpc{i:04d}@example.com", f"n{i}")
                    for i in range(first, first + RPC_TRACE_ROWS)]
            errno = run(cursor, insert_sql(table, rows))
            if errno is not None:
                print(f"\tFailed: the unique_checks={checks} insert was "
                      f"rejected with {errno}")
                return 1
            seen[checks] = rpc_types_after(trace_path, offset, table)

        sync_on = seen[1].count(SYNC_INDEX_RPC)
        if sync_on != RPC_TRACE_ROWS:
            print(f"\tFailed: unique_checks=1 sent {sync_on} {SYNC_INDEX_RPC}, "
                  f"expected one per row ({RPC_TRACE_ROWS})")
            return 1
        if SYNC_INDEX_RPC in seen[0]:
            print(f"\tFailed: unique_checks=0 sent "
                  f"{seen[0].count(SYNC_INDEX_RPC)} {SYNC_INDEX_RPC}, expected "
                  f"none; the index write was not deferred")
            return 1
        if BATCHED_WRITE_RPC not in seen[0]:
            print(f"\tFailed: unique_checks=0 never sent {BATCHED_WRITE_RPC}, "
                  f"only {sorted(set(seen[0]))}")
            return 1
    finally:
        if db is not None:
            db.close()
        os.kill(pid, signal.SIGKILL)

    print(f"\tPassed! ({SYNC_INDEX_RPC} x{sync_on} at unique_checks=1, none at 0)")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()

    reset(db, cursor)
    # Transactions are opened with an explicit BEGIN so that a lone statement
    # is its own transaction and every error arrives through the cursor.
    db.autocommit = True

    result = 0
    result |= test_distinct_rows_commit(cursor)
    result |= test_duplicate_within_statement(cursor)
    result |= test_duplicate_of_committed_row(cursor)
    result |= test_duplicate_within_transaction(cursor)
    result |= test_unique_checks_on_is_unchanged(cursor)
    result |= test_insert_then_update(cursor, 0)
    result |= test_insert_then_update(cursor, 1)
    result |= test_insert_then_update(cursor, 0, unique_index=False)
    result |= test_insert_then_update(cursor, 1, unique_index=False)
    result |= test_auto_flush_distinct(cursor)
    result |= test_auto_flush_duplicate_across_batches(cursor)
    result |= test_auto_flush_distinct_two_indexes(cursor)
    result |= test_auto_flush_duplicate_two_indexes(cursor)
    result |= test_cross_session_duplicate(cursor, args.user, args.password)
    result |= test_concurrent_sessions(cursor, args.user, args.password)
    result |= test_rpc_shape(args.user, args.password)

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
