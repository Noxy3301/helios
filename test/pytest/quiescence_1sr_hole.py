"""Columnar read view consistency across commit row installs (1SR regression).

1SR (one-copy serializability with real-time order) requires every read to
return a complete committed state and a read issued after a commit returns
to see that commit. Per-group write counters cannot provide this: they
return to even after every ROW install, so a reader scheduled between two
row installs of a multi-row commit observes a stable-looking half-applied
table. The bridge reads an epoch-fenced read view with before-image
resolution instead, and this test demands consistent results.

The test opens the install window wide and synchronizes on it instead of
racing:
  - lineairdb-server runs with the stateless_commit and silo_commit
    between_row_installs debug sync points set to sleep, so either commit
    path's install loop stays open between consecutive row installs (see
    LineairDB util/debug_sync.hpp)
  - mysqld runs with HELIOS_DUCKDB_BRIDGE=1; FORCED SELECTs take the
    columnar offload through the bridge
  - a writer thread signals right before COMMIT of a two-row-install
    transaction; the main thread then runs FORCED SELECTs while that
    COMMIT is in flight

Scenarios (each is one two-install transaction over a fresh table):
  update-update    both rows change value
  delete-update    one row disappears (a read view older than the commit
                   must resurrect it from its before-image)
  insert-update    a row appears (a read view older than the commit must
                   not see the fresh slot)

A pass requires, per scenario: secondary-engine engagement, a COMMIT that
actually paused (duration >= the sync-point sleep), at least one read
issued while COMMIT was in flight, every windowed read equal to the full
old or the full new state (never a mix), every read issued after COMMIT
returned equal to the new state (freshness), and primary/secondary
agreement on the final state.

Operational notes: the test assumes exclusive ownership of the local stack
(run_tests.py convention; the stop scripts kill every local instance). It
restarts lineairdb-server and mysqld itself because both need the special
environment, verifies the preflight stop actually emptied the stack
(start_server.sh treats "already running" as success), and stops the stack
afterwards so the sync point does not survive into later runs.
"""

import argparse
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.connection import get_connection

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
QUIET = "> /dev/null 2>&1"
PAUSE_MS = 1500
# The read-view-before-write scenario holds every bridge read open between
# its epoch fence and its scan; shorter than PAUSE_MS so the held read
# finishes while the writer's paused COMMIT is still in flight.
FENCE_HOLD_MS = 1000
# Both commit paths carry the install pause: prefetch transactions install
# through the stateless commit, while transactions with statements outside
# the prefetch shapes (the INSERT scenario) install through the native
# Silo path.
SYNC_POINT_ENV = (
    "LINEAIRDB_DEBUG_SYNC_STATELESS_COMMIT_BETWEEN_ROW_INSTALLS"
    f"=sleep:{PAUSE_MS} "
    "LINEAIRDB_DEBUG_SYNC_SILO_COMMIT_BETWEEN_ROW_INSTALLS"
    f"=sleep:{PAUSE_MS} "
    "LINEAIRDB_DEBUG_SYNC_PAX_READ_VIEW_AFTER_FENCE"
    f"=sleep:{FENCE_HOLD_MS}")

# The bridge registers tables in DuckDB under their bare names and the
# proxy strips qualifiers relative to the connection's default schema, so
# queries must run with USE <db> and unqualified table references.
DB = "ha_lineairdb_test"

STACK_PATTERNS = ("build/server/lineairdb-server",
                  "runtime_output_directory/mysqld")

# Each scenario: fresh table with BASE rows, then one transaction whose two
# statements produce two row installs with the pause between them. Every
# windowed read must equal `old` or `new`, nothing else.
BASE_ROWS = {1: 0, 2: 0}
SCENARIOS = [
    {
        "name": "update-update",
        "table": "probe_update",
        "statements": ["UPDATE {t} SET v = 1 WHERE id = 1",
                       "UPDATE {t} SET v = 1 WHERE id = 2"],
        "old": {1: 0, 2: 0},
        "new": {1: 1, 2: 1},
    },
    {
        "name": "delete-update",
        "table": "probe_delete",
        "statements": ["DELETE FROM {t} WHERE id = 1",
                       "UPDATE {t} SET v = 1 WHERE id = 2"],
        "old": {1: 0, 2: 0},
        "new": {2: 1},
    },
    {
        "name": "insert-update",
        "table": "probe_insert",
        "statements": ["INSERT INTO {t} VALUES (3, 1)",
                       "UPDATE {t} SET v = 1 WHERE id = 2"],
        "old": {1: 0, 2: 0},
        "new": {1: 0, 2: 1, 3: 1},
    },
]


def sh(cmd):
    return os.system(f"cd {ROOT} && {cmd}")


def ensure_stack_stopped(timeout_s=10):
    """Verify no stack process survives the preflight stop.

    start_server.sh treats an already-running server as success, so a
    leftover instance would silently run WITHOUT the sync point armed and
    the test would probe the wrong binary state.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        alive = [p for p in STACK_PATTERNS
                 if subprocess.run(["pgrep", "-f", p],
                                   capture_output=True).returncode == 0]
        if not alive:
            return
        if time.monotonic() > deadline:
            raise RuntimeError(f"stack processes still running: {alive}")
        time.sleep(0.5)


def start_stack_with_test_env():
    if sh(f"{SYNC_POINT_ENV} ./scripts/start_server.sh {QUIET}") != 0:
        raise RuntimeError("start_server.sh failed")
    time.sleep(2)
    if sh(f"HELIOS_DUCKDB_BRIDGE=1 ./scripts/start_mysql.sh "
          f"--mysqld-port 3307 --server-host 127.0.0.1 "
          f"--server-port 9999 {QUIET}") != 0:
        raise RuntimeError("start_mysql.sh failed")


def stop_stack():
    if sh(f"./scripts/stop_mysql.sh {QUIET}") != 0:
        print("\t[WARN] stop_mysql.sh returned non-zero")
    if sh(f"./scripts/stop_server.sh {QUIET}") != 0:
        print("\t[WARN] stop_server.sh returned non-zero")


def secondary_execution_count(cursor):
    cursor.execute("SHOW GLOBAL STATUS LIKE 'Secondary_engine_execution_count'")
    return int(cursor.fetchone()[1])


class Writer(threading.Thread):
    """BEGIN; two point statements; COMMIT - one txn with two row installs.

    UPDATE/DELETE point statements commit through the stateless (prefetch)
    path; a transaction containing an INSERT commits through the native
    silo path. Both install loops carry a between-row-installs sync point.
    commit_started is set immediately before COMMIT is issued;
    commit_done_at is stamped when it returns.
    """

    def __init__(self, user, password, table, statements):
        # daemon: a reader failure must not leave the process hanging on a
        # writer stuck inside the paused COMMIT.
        super().__init__(daemon=True)
        self.user = user
        self.password = password
        self.table = table
        self.statements = statements
        self.commit_started = threading.Event()
        self.commit_issued_at = None
        self.commit_done_at = None
        self.error = None

    def run(self):
        wdb = None
        try:
            wdb = get_connection(user=self.user, password=self.password)
            wdb.autocommit = True
            wcur = wdb.cursor()
            wcur.execute(f"USE {DB}")
            wcur.execute("BEGIN")
            for statement in self.statements:
                wcur.execute(statement.format(t=self.table))
            self.commit_issued_at = time.monotonic()
            self.commit_started.set()
            wcur.execute("COMMIT")
            self.commit_done_at = time.monotonic()
        except Exception as e:  # surfaced after join
            self.error = e
        finally:
            self.commit_started.set()
            if wdb is not None:
                try:
                    wdb.close()
                except Exception:
                    pass


def run_scenario(cursor, user, password, scenario):
    """No-torn-read regression over the open install window.

    Coverage note: the paused writer stays ONLINE at its commit epoch, so
    the windowed reader's read view fence waits for the paused COMMIT to
    finish installing and the read serializes after it (all-new). The old
    contract returned a torn result on this exact schedule; the
    before-image resolution itself is exercised by
    run_read_view_before_write, where the fence completes first.
    """
    table = scenario["table"]
    print(f"SCENARIO {scenario['name']}")
    cursor.execute(
        f"CREATE TABLE {table} (id INT PRIMARY KEY, v INT) "
        "ENGINE=LineairDB SECONDARY_ENGINE=LINEAIRDB_COLUMNAR"
    )
    values = ", ".join(f"({k}, {v})" for k, v in BASE_ROWS.items())
    cursor.execute(f"INSERT INTO {table} VALUES {values}")
    cursor.execute(f"ALTER TABLE {table} SECONDARY_LOAD")

    print("\twriter start (2-install txn, COMMIT pauses between installs)")
    writer = Writer(user, password, table, scenario["statements"])
    writer.start()
    try:
        if not writer.commit_started.wait(timeout=30):
            raise RuntimeError("writer never reached COMMIT")

        secondary_before = secondary_execution_count(cursor)
        cursor.execute("SET SESSION use_secondary_engine = FORCED")
        reads = []  # (issued_at, rows dict)
        mixed = None
        while writer.is_alive():
            issued_at = time.monotonic()
            cursor.execute(f"SELECT id, v FROM {table} ORDER BY id")
            rows = dict(cursor.fetchall())
            reads.append((issued_at, rows))
            if rows != scenario["old"] and rows != scenario["new"]:
                mixed = rows
                break
            time.sleep(0.05)

        writer.join()
        if writer.error:
            raise writer.error

        # Freshness probe: issued strictly after COMMIT returned, still on
        # the secondary engine.
        cursor.execute(f"SELECT id, v FROM {table} ORDER BY id")
        fresh_rows = dict(cursor.fetchall())
        secondary_after = secondary_execution_count(cursor)
        # Primary probe: OFF forbids the secondary engine (ON would still
        # allow it, and a wrong primary state could hide behind a correct
        # bridge); the counter must not advance across it.
        cursor.execute("SET SESSION use_secondary_engine = OFF")
        cursor.execute(f"SELECT id, v FROM {table} ORDER BY id")
        primary_rows = dict(cursor.fetchall())
        primary_probe_secondary_delta = (
            secondary_execution_count(cursor) - secondary_after)
        cursor.execute("SET SESSION use_secondary_engine = ON")

        commit_took = writer.commit_done_at - writer.commit_issued_at
        old_seen = sum(1 for _, r in reads if r == scenario["old"])
        new_seen = sum(1 for _, r in reads if r == scenario["new"])
        print(f"\t[DEBUG] reads={len(reads)} old={old_seen} new={new_seen} "
              f"mixed={mixed} commit_took={commit_took:.2f}s")
        print(f"\t[DEBUG] secondary executions: "
              f"{secondary_before} -> {secondary_after}")

        if secondary_after <= secondary_before:
            print("\tFailed: SELECTs did not execute on the secondary engine")
            return 1
        if commit_took < PAUSE_MS / 1000.0 * 0.9:
            print("\tFailed: COMMIT returned too fast; the install pause "
                  "did not engage (writes not on an instrumented commit "
                  "path?)")
            return 1
        windowed = sum(1 for issued_at, _ in reads
                       if issued_at < writer.commit_done_at)
        if windowed == 0:
            print("\tFailed: no read issued while COMMIT was in flight")
            return 1
        if mixed is not None:
            print(f"\tFailed: torn read {mixed}; a windowed read must see "
                  f"the full old or the full new state")
            return 1
        # Strict serializability: a read issued after the writer's COMMIT
        # returned must see the new state (no stale read).
        for issued_at, rows in reads:
            if issued_at > writer.commit_done_at and rows != scenario["new"]:
                print(f"\tFailed: stale read {rows} issued after COMMIT "
                      f"returned")
                return 1
        if fresh_rows != scenario["new"]:
            print(f"\tFailed: post-commit FORCED read {fresh_rows} != "
                  f"{scenario['new']} (stale read)")
            return 1
        if primary_probe_secondary_delta != 0:
            print("\tFailed: primary probe executed on the secondary "
                  "engine; primary/secondary agreement not established")
            return 1
        if primary_rows != scenario["new"]:
            print(f"\tFailed: primary state {primary_rows} != "
                  f"{scenario['new']} (writer did not commit)")
            return 1

        print("\tconsistent: every windowed read was all-old or all-new, "
              "post-commit reads are fresh")
        return 0
    finally:
        if writer.is_alive():
            writer.join(timeout=PAUSE_MS / 1000.0 + 30)


class Reader(threading.Thread):
    """One FORCED SELECT on its own connection, with result and end stamp.

    The armed pax_read_view.after_fence point holds the SELECT open between
    its epoch fence and its scan, so writes committed meanwhile land with
    epochs above the read view's cut.
    """

    def __init__(self, user, password, table):
        super().__init__(daemon=True)
        self.user = user
        self.password = password
        self.table = table
        self.rows = None
        self.done_at = None
        self.error = None

    def run(self):
        rdb = None
        try:
            rdb = get_connection(user=self.user, password=self.password)
            rdb.autocommit = True
            rcur = rdb.cursor()
            rcur.execute(f"USE {DB}")
            rcur.execute("SET SESSION use_secondary_engine = FORCED")
            rcur.execute(f"SELECT id, v FROM {self.table} ORDER BY id")
            self.rows = dict(rcur.fetchall())
            self.done_at = time.monotonic()
        except Exception as e:  # surfaced after join
            self.error = e
        finally:
            if rdb is not None:
                try:
                    rdb.close()
                except Exception:
                    pass


def run_read_view_before_write(cursor, user, password, scenario):
    """Fence first, write second: the scan must return the pre-write state.

    The reader's read view fence completes before the writer goes online, so
    the writer's commit epoch exceeds the read view cut and every row it
    installs must resolve through the version store: updates and deletes to
    their captured before-images (a deleted row must reappear), inserts to
    absence (a fresh slot must stay invisible). The old quiescence contract
    could never serve this schedule (it either returned torn bytes or
    aborted); returning the complete old state is the multi-version read's
    defining behavior.
    """
    table = "held_" + scenario["table"]
    print(f"SCENARIO read-view-before-write/{scenario['name']}")
    cursor.execute(
        f"CREATE TABLE {table} (id INT PRIMARY KEY, v INT) "
        "ENGINE=LineairDB SECONDARY_ENGINE=LINEAIRDB_COLUMNAR"
    )
    values = ", ".join(f"({k}, {v})" for k, v in BASE_ROWS.items())
    cursor.execute(f"INSERT INTO {table} VALUES {values}")
    cursor.execute(f"ALTER TABLE {table} SECONDARY_LOAD")

    old_state = scenario["old"]
    new_state = scenario["new"]
    statements = scenario["statements"]

    print("\treader start (read view held open after its fence)")
    reader = Reader(user, password, table)
    reader.start()
    # Let the SELECT pass its fence and enter the hold; the writer then
    # commits with a between-install pause that outlives the hold.
    time.sleep(FENCE_HOLD_MS / 1000.0 * 0.4)
    writer = Writer(user, password, table, statements)
    writer.start()

    reader.join(timeout=60)
    writer.join(timeout=60)
    if reader.is_alive() or writer.is_alive():
        print("\tFailed: reader or writer did not finish")
        return 1
    if reader.error:
        raise reader.error
    if writer.error:
        raise writer.error

    commit_took = writer.commit_done_at - writer.commit_issued_at
    print(f"\t[DEBUG] reader rows={reader.rows} commit_took={commit_took:.2f}s "
          f"reader_before_commit_done="
          f"{reader.done_at < writer.commit_done_at}")

    if commit_took < PAUSE_MS / 1000.0 * 0.9:
        print("\tFailed: COMMIT returned too fast; the install pause did "
              "not engage")
        return 1
    if reader.done_at >= writer.commit_done_at:
        print("\tFailed: reader finished after COMMIT returned; the hold "
              "did not overlap the write, timing invalid")
        return 1
    if reader.done_at <= writer.commit_issued_at:
        print("\tFailed: reader finished before COMMIT was issued; the "
              "before-image path was not exercised")
        return 1
    if reader.rows != old_state:
        print(f"\tFailed: read view returned {reader.rows} != pre-write state "
              f"{old_state}; before-image resolution broken")
        return 1

    cursor.execute("SET SESSION use_secondary_engine = FORCED")
    cursor.execute(f"SELECT id, v FROM {table} ORDER BY id")
    fresh_rows = dict(cursor.fetchall())
    if fresh_rows != new_state:
        cursor.execute("SET SESSION use_secondary_engine = ON")
        print(f"\tFailed: post-commit FORCED read {fresh_rows} != "
              f"{new_state} (stale read)")
        return 1
    # Primary agreement, with the secondary engine forbidden (see
    # run_scenario's primary probe).
    secondary_before_primary_probe = secondary_execution_count(cursor)
    cursor.execute("SET SESSION use_secondary_engine = OFF")
    cursor.execute(f"SELECT id, v FROM {table} ORDER BY id")
    primary_rows = dict(cursor.fetchall())
    primary_probe_secondary_delta = (
        secondary_execution_count(cursor) - secondary_before_primary_probe)
    cursor.execute("SET SESSION use_secondary_engine = ON")
    if primary_probe_secondary_delta != 0:
        print("\tFailed: primary probe executed on the secondary engine")
        return 1
    if primary_rows != new_state:
        print(f"\tFailed: primary state {primary_rows} != {new_state}")
        return 1

    print("\tconsistent: held read view returned the complete pre-write "
          "state, post-commit read is fresh, primary agrees")
    return 0


def run_probe(user, password):
    db = get_connection(user=user, password=password)
    db.autocommit = True
    try:
        cursor = db.cursor()

        print("SETUP")
        # The sync point sits in the stateless (prefetch) commit's install
        # loop; route writes through it.
        cursor.execute("SET GLOBAL lineairdb_prefetch_execution = ON")
        cursor.execute(f"DROP DATABASE IF EXISTS {DB}")
        cursor.execute(f"CREATE DATABASE {DB}")
        cursor.execute(f"USE {DB}")

        for scenario in SCENARIOS:
            if run_scenario(cursor, user, password, scenario):
                return 1
        for scenario in SCENARIOS:
            if run_read_view_before_write(cursor, user, password, scenario):
                return 1
        return 0
    finally:
        try:
            db.close()
        except Exception:
            pass


def main(user, password):
    # get_connection() prefers MYSQL_UNIX_PORT; this test owns its stack on
    # 127.0.0.1:3307 and must not follow an inherited socket elsewhere.
    os.environ.pop("MYSQL_UNIX_PORT", None)

    print(f"restarting stack with {PAUSE_MS}ms install sync point + bridge")
    sh(f"./scripts/stop_mysql.sh {QUIET}")
    sh(f"./scripts/stop_server.sh {QUIET}")
    ensure_stack_stopped()
    failed = 1
    try:
        start_stack_with_test_env()
        failed = run_probe(user, password)
    finally:
        print("stopping test stack (disarm the sync point)")
        stop_stack()

    if failed:
        print("\nTest failed")
        sys.exit(1)
    print("\nPassed!")
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Connect to MySQL")
    parser.add_argument("--user", metavar="user", type=str,
                        help="name of user", default="root")
    parser.add_argument("--password", metavar="pw", type=str,
                        help="password for the user", default="")
    args = parser.parse_args()
    main(args.user, args.password)
