"""Reproduce the columnar quiescence-or-abort 1SR hole (torn multi-row read).

The columnar read contract (capture write states -> lock-free scan ->
recheck) detects concurrent writes through per-group write counters, but a
counter returns to even after every ROW install. A multi-row commit
therefore has windows between two row installs where every counter is even
and stable while the table is half-applied. A reader scheduled inside such
a window passes the recheck and accepts a torn result.

This test opens the window wide and synchronizes on it instead of racing:
  - lineairdb-server runs with the stateless_commit.between_row_installs
    debug sync point set to sleep, so the commit install loop stays open
    between consecutive row installs (see LineairDB util/debug_sync.hpp)
  - mysqld runs with HELIOS_DUCKDB_BRIDGE=1; FORCED SELECTs take the
    columnar offload (bridge or query-block executor - both share the
    write-state contract under test)
  - a writer thread signals right before COMMIT of a two-row transaction;
    the main thread then retries FORCED SELECTs while that COMMIT is in
    flight and records the first torn result

A pass requires all of: a torn read whose SELECT completed before the
writer's COMMIT returned, secondary-engine engagement, a COMMIT that
actually paused (duration >= the sync-point sleep), and a fully
committed final state.
Exit 0 = the torn read was observed (the hole exists, current behavior).
After the multi-version read path lands, this branch must fail; flip the
assertion then so the test demands a consistent snapshot instead.

Operational notes: the test assumes exclusive ownership of the local stack
(run_tests.py convention; the stop scripts kill every local instance). It
restarts lineairdb-server and mysqld itself because both need the special
environment, verifies the preflight stop actually emptied the stack
(start_server.sh treats "already running" as success), and stops the stack
afterwards so the sync point does not survive into later runs. The
LineairDB sync point and this file must land together: run_tests.py globs
this directory, and the test fails without the sync point built in.
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
SYNC_POINT_ENV = ("LINEAIRDB_DEBUG_SYNC_STATELESS_COMMIT_BETWEEN_ROW_INSTALLS"
                  f"=sleep:{PAUSE_MS}")

# The bridge registers tables in DuckDB under their bare names and the
# proxy strips qualifiers relative to the connection's default schema, so
# queries must run with USE <db> and unqualified table references.
DB = "ha_lineairdb_test"
TABLE = "pause_probe"

STACK_PATTERNS = ("build/server/lineairdb-server",
                  "runtime_output_directory/mysqld")


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
    """BEGIN; two point UPDATEs; COMMIT - one txn with two row installs.

    Point updates keep the statements on the prefetch path (multi-range
    shapes are rejected by autogen). commit_started is set immediately
    before COMMIT is issued; commit_done_at is stamped when it returns.
    """

    def __init__(self, user, password):
        # daemon: a reader failure must not leave the process hanging on a
        # writer stuck inside the paused COMMIT.
        super().__init__(daemon=True)
        self.user = user
        self.password = password
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
            wcur.execute(f"UPDATE {TABLE} SET v = 1 WHERE id = 1")
            wcur.execute(f"UPDATE {TABLE} SET v = 1 WHERE id = 2")
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


def run_probe(user, password):
    db = get_connection(user=user, password=password)
    db.autocommit = True
    writer = None
    try:
        cursor = db.cursor()

        print("SETUP")
        # The sync point sits in the stateless (prefetch) commit's install
        # loop; route writes through it, matching the TPC-H measurement
        # conditions (tpch_setup step 4 also sets this on).
        cursor.execute("SET GLOBAL lineairdb_prefetch_execution = ON")
        cursor.execute(f"DROP DATABASE IF EXISTS {DB}")
        cursor.execute(f"CREATE DATABASE {DB}")
        cursor.execute(f"USE {DB}")
        cursor.execute(
            f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, v INT) "
            "ENGINE=LineairDB SECONDARY_ENGINE=LINEAIRDB_COLUMNAR"
        )
        cursor.execute(f"INSERT INTO {TABLE} VALUES (1, 0), (2, 0)")
        cursor.execute(f"ALTER TABLE {TABLE} SECONDARY_LOAD")

        print("WRITER START (2-row txn, COMMIT pauses between installs)")
        writer = Writer(user, password)
        writer.start()
        if not writer.commit_started.wait(timeout=30):
            raise RuntimeError("writer never reached COMMIT")

        print("READ WHILE COMMIT IN FLIGHT (FORCED)")
        secondary_before = secondary_execution_count(cursor)
        cursor.execute("SET SESSION use_secondary_engine = FORCED")
        torn = None
        torn_read_done_at = None
        attempts = 0
        while writer.is_alive():
            cursor.execute(f"SELECT id, v FROM {TABLE} ORDER BY id")
            rows = dict(cursor.fetchall())
            done_at = time.monotonic()
            attempts += 1
            if sorted(rows.values()) == [0, 1]:
                torn = rows
                torn_read_done_at = done_at
                break
            time.sleep(0.05)
        secondary_after = secondary_execution_count(cursor)
        cursor.execute("SET SESSION use_secondary_engine = ON")

        writer.join()
        if writer.error:
            raise writer.error

        commit_took = writer.commit_done_at - writer.commit_issued_at
        print(f"\t[DEBUG] attempts={attempts} torn={torn} "
              f"commit_took={commit_took:.2f}s")
        print(f"\t[DEBUG] secondary executions: "
              f"{secondary_before} -> {secondary_after}")

        cursor.execute(f"SELECT id, v FROM {TABLE} ORDER BY id")
        final_rows = dict(cursor.fetchall())
        print(f"\t[DEBUG] rows after commit: {final_rows}")

        if secondary_after <= secondary_before:
            print("\tFailed: SELECTs did not execute on the secondary engine")
            return 1
        if commit_took < PAUSE_MS / 1000.0 * 0.9:
            print("\tFailed: COMMIT returned too fast; the install pause "
                  "did not engage (writes not on the stateless path?)")
            return 1
        if final_rows != {1: 1, 2: 1}:
            print("\tFailed: writer did not commit both rows")
            return 1
        if torn is None:
            print("\tFailed: no torn read observed while COMMIT was in "
                  "flight; the hole did not reproduce")
            return 1
        if torn_read_done_at >= writer.commit_done_at:
            print("\tFailed: torn read finished after COMMIT returned; "
                  "timing invalid")
            return 1

        print("\tTorn read observed: one row new, one row old, and the "
              "write-state recheck accepted it while COMMIT was in flight.")
        print("\t1SR hole reproduced. "
              "(After the MV read path lands, this branch must FAIL.)")
        return 0
    finally:
        if writer is not None and writer.is_alive():
            writer.join(timeout=PAUSE_MS / 1000.0 + 30)
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
