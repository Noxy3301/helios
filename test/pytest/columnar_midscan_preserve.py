"""Columnar read view consistency for writes that land inside the scan.

quiescence_1sr_hole.py pins the two schedules whose write lands before the
scan starts: the fence waits for the paused COMMIT (all-new), or the fence
completes first and the whole write resolves through epoch images (all-old).
Neither reaches the schedule this file covers: an install landing after the
scan has claimed a group and started reading its slots. That is what the
per-chunk preserve audit exists for, so it needs its own regression.

The schedule is built out of the same two sync points:
  - pax_view.after_fence holds every bridge read between its epoch fence and
    its scan, so the writer's commit epoch is above the read view cut
  - silo_commit.between_row_installs pauses the writer between the two row
    installs of one transaction

The reader starts first and the writer is released so that its FIRST install
lands during the fence hold (an image already there when the group is
claimed) and its SECOND lands while the scan is running. The delay is derived
from a measured scan, and the timing the run actually achieved is asserted
afterwards, so a run that missed the window fails loudly instead of passing
vacuously.

The scan is made long enough to span the pause by a per-row LIKE over a wide
column and a single analytical thread (HELIOS_BRIDGE_THREADS=1). The query
aggregates, so a torn result is a wrong number rather than a wrong row order:
COUNT(*), SUM(v) and COUNT(n) must equal the full pre-write state, never a
mix. Column n is nullable and carries NULL on every even id, which pins the
validity bits: a chunk whose rewind emits fewer rows than the pass it rewinds
leaves output rows behind, and a group bulk-decoded into them afterwards
writes cells without writing validity, so a stale NULL bit would turn a
present value into a NULL and drop COUNT(n).

Scenarios (each one transaction over a fresh table, writing the slots the
scan is reading when the second install lands):
  update-update   two rows change value
  insert          a row appears (its fresh slot must stay invisible)
  delete          a row disappears (it must be returned from its image)

A paused commit lands its two installs at two instants, so it cannot prove
the per-chunk audit ran: the aimed install can still land before the group is
claimed and be resolved by the claim copy. The three paused scenarios verify
the returned rows, which pins the boundary cases (an install inside the fence
hold resolves through the claim copy, an insert stays invisible, a delete
reads from its image). The audit itself is pinned by the last scenario: a
writer updates scattered rows for the whole length of the scan, so installs
land in groups the scan is holding, and the run asserts that at least one
chunk audit had to redo a group.

Not covered here: an in-place DATE cell that names no calendar day. A cell a
writer tore and a date stored under a relaxed sql_mode both read that way,
and the chunk audit separates them (a group whose counter moved has the row
re-read, a group whose counter held fails the request). Reaching the stored
half needs a table loaded with sql_mode relaxed, which this file does not
build.

Operational notes: as in quiescence_1sr_hole.py, the test owns the local
stack, restarts it with the sync points armed, and stops it afterwards.
"""

import argparse
import glob
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.connection import get_connection

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
QUIET = "> /dev/null 2>&1"
# The writer's install pause, and the hold every bridge read takes between
# its epoch fence and its scan.
PAUSE_MS = 1500
FENCE_HOLD_MS = 2500
SERVER_ENV = (
    "HELIOS_DEBUG_SYNC_SILO_COMMIT_BETWEEN_ROW_INSTALLS"
    f"=sleep:{PAUSE_MS} "
    "HELIOS_DEBUG_SYNC_PAX_VIEW_AFTER_FENCE"
    f"=sleep:{FENCE_HOLD_MS} "
    # One analytical thread: the scan has to outlive the install pause.
    "HELIOS_BRIDGE_THREADS=1 "
    # The scan tallies, read back from the server log.
    "ENABLE_DUCKDB_BRIDGE_DEBUG=1 "
    # The table is loaded one row per transaction, because the install pause
    # fires between the row installs of any larger one.
    "HELIOS_COMMIT_DURABILITY=async")
# A fence takes one to two epochs; the writer delay allows for it.
FENCE_S = 0.06
# Where the second install should land inside the scan.
INSTALL_AT = 0.45

DB = "ha_helios_test"
STACK_PATTERNS = ("build/server/helios-storage",
                  "runtime_output_directory/mysqld")

# A per-row LIKE over a wide column costs about 1.4us, so this is a scan of
# a few hundred milliseconds on one thread - long enough to span an install.
ROWS = 180000
FILLER = "b" * 240 + "aaa"
QUERY = "SELECT COUNT(*), SUM(v), COUNT(n) FROM {t} WHERE s LIKE '%aaa%'"
# The written rows sit where the scan is when the second install lands, so
# the read view has claimed their group and not finished it.
TARGET = int(ROWS * INSTALL_AT)
# Column n holds NULL on every even id, so half the rows carry a value.
NON_NULL = ROWS // 2
# 1 when the row the delete scenario removes carries a value in n.
TARGET_NON_NULL = TARGET % 2

SCENARIOS = [
    {
        "name": "update-update",
        "table": "midscan_update",
        "statements": ["UPDATE {t} SET v = 1 WHERE id = {n1}",
                       "UPDATE {t} SET v = 1 WHERE id = {n}"],
        "new": (ROWS, 2, NON_NULL),
    },
    {
        "name": "insert",
        "table": "midscan_insert",
        "statements": ["INSERT INTO {t} VALUES ({n2}, 1, NULL, '{f}')",
                       "UPDATE {t} SET v = 1 WHERE id = {n}"],
        "new": (ROWS + 1, 2, NON_NULL),
    },
    {
        "name": "delete",
        "table": "midscan_delete",
        "statements": ["DELETE FROM {t} WHERE id = {n}",
                       "UPDATE {t} SET v = 1 WHERE id = {n1}"],
        "new": (ROWS - 1, 1, NON_NULL - TARGET_NON_NULL),
    },
]
OLD_STATE = (ROWS, 0, NON_NULL)


def sh(cmd):
    return os.system(f"cd {ROOT} && {cmd}")


def fill(statement, table):
    return statement.format(t=table, n=TARGET, n1=TARGET - 1, n2=ROWS + 1,
                            f=FILLER)


def ensure_stack_stopped(timeout_s=10):
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
    if sh(f"{SERVER_ENV} ./scripts/start_server.sh {QUIET}") != 0:
        raise RuntimeError("start_server.sh failed")
    time.sleep(2)
    if sh(f"./scripts/start_mysql.sh "
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


def create_table(cursor, table):
    """Loads the table one row per transaction.

    A multi-row INSERT installs several rows in one commit, and the armed
    install pause fires between every pair of them.
    """
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.execute(
        f"CREATE TABLE {table} "
        "(id INT PRIMARY KEY, v INT, n INT NULL, s VARCHAR(255)) "
        "ENGINE=Helios SECONDARY_ENGINE=HELIOS_COLUMNAR")
    started = time.monotonic()
    for i in range(1, ROWS + 1):
        n = "NULL" if i % 2 == 0 else str(i)
        cursor.execute(f"INSERT INTO {table} VALUES ({i}, 0, {n}, '{FILLER}')")
    cursor.execute(f"ALTER TABLE {table} SECONDARY_LOAD")
    print(f"\t[DEBUG] loaded {ROWS} rows in "
          f"{time.monotonic() - started:.0f}s")


class Writer(threading.Thread):
    """BEGIN; two statements; COMMIT - one txn with two row installs."""

    def __init__(self, user, password, table, statements, delay_s):
        super().__init__(daemon=True)
        self.user = user
        self.password = password
        self.table = table
        self.statements = statements
        self.delay_s = delay_s
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
            time.sleep(self.delay_s)
            wcur.execute("BEGIN")
            for statement in self.statements:
                wcur.execute(fill(statement, self.table))
            self.commit_issued_at = time.monotonic()
            wcur.execute("COMMIT")
            self.commit_done_at = time.monotonic()
        except Exception as e:  # surfaced after join
            self.error = e
        finally:
            if wdb is not None:
                try:
                    wdb.close()
                except Exception:
                    pass


class Reader(threading.Thread):
    """One FORCED aggregate over the whole table, with its own timestamps."""

    def __init__(self, user, password, table):
        super().__init__(daemon=True)
        self.user = user
        self.password = password
        self.table = table
        self.rows = None
        self.issued_at = None
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
            self.issued_at = time.monotonic()
            rcur.execute(fill(QUERY, self.table))
            row = rcur.fetchone()
            self.rows = (int(row[0]), int(row[1] or 0), int(row[2]))
            self.done_at = time.monotonic()
        except Exception as e:  # surfaced after join
            self.error = e
        finally:
            if rdb is not None:
                try:
                    rdb.close()
                except Exception:
                    pass


def last_scan_tally():
    """Scan tallies of the most recent bridge request, from the server log.

    Each request logs its statement and then one tally line per table, so the
    lines after the last statement belong to the read just finished.
    """
    logs = sorted(glob.glob(
        os.path.join(ROOT, "helios_logs", "helios_storage_*.log")))
    if not logs:
        return {}
    tally = {}
    with open(logs[-1], errors="replace") as log:
        for line in log:
            if line.startswith("[duckdb-ast] "):
                tally = {}
            elif line.startswith("[duckdb-scan] "):
                for part in line.split()[2:]:
                    if "=" not in part:
                        continue
                    key, value = part.split("=", 1)
                    tally[key] = tally.get(key, 0) + int(value)
    return tally


def measure_scan(cursor, table):
    """Scan time of one FORCED read, with the fence hold subtracted."""
    cursor.execute("SET SESSION use_secondary_engine = FORCED")
    started = time.monotonic()
    cursor.execute(fill(QUERY, table))
    cursor.fetchall()
    elapsed = time.monotonic() - started
    cursor.execute("SET SESSION use_secondary_engine = ON")
    return max(0.0, elapsed - FENCE_HOLD_MS / 1000.0)


def run_scenario(cursor, user, password, scenario):
    """One paused commit: the rows the scan returns must be the pre-write ones.

    This pins the boundary cases (an install inside the fence hold resolves
    through the claim copy, an insert stays invisible, a delete reads from its
    image). It does not pin the per-chunk audit: the second install can land
    before its group is claimed, and then the claim copy answers.
    """
    table = scenario["table"]
    print(f"SCENARIO {scenario['name']}")
    create_table(cursor, table)

    scan_s = measure_scan(cursor, table)
    print(f"\t[DEBUG] scan={scan_s * 1000:.0f}ms over {ROWS} rows")
    if scan_s < 0.10:
        print(f"\tFailed: scan of {scan_s * 1000:.0f}ms is too short to span "
              f"an install; raise ROWS")
        return 1
    # The first install lands during the fence hold, the second part of the
    # way into the scan that follows it.
    delay_s = (FENCE_HOLD_MS / 1000.0 + FENCE_S + INSTALL_AT * scan_s -
               PAUSE_MS / 1000.0)
    if delay_s <= 0.2:
        print("\tFailed: the fence hold is shorter than the install pause; "
              "the first install cannot land before the scan")
        return 1

    secondary_before = secondary_execution_count(cursor)
    reader = Reader(user, password, table)
    writer = Writer(user, password, table, scenario["statements"], delay_s)
    print(f"\treader start, writer released after {delay_s:.2f}s")
    reader.start()
    writer.start()
    reader.join(timeout=120)
    writer.join(timeout=120)
    if reader.is_alive() or writer.is_alive():
        print("\tFailed: reader or writer did not finish")
        return 1
    if reader.error:
        raise reader.error
    if writer.error:
        raise writer.error

    commit_took = writer.commit_done_at - writer.commit_issued_at
    # The scan ends when the reader returns and runs for about the measured
    # time, which places the second install inside it.
    into_scan = writer.commit_done_at - (reader.done_at - scan_s)
    secondary_after = secondary_execution_count(cursor)
    tally = last_scan_tally()
    print(f"\t[DEBUG] reader rows={reader.rows} "
          f"commit_took={commit_took:.2f}s "
          f"last_install_at={into_scan * 1000:.0f}ms of a "
          f"{scan_s * 1000:.0f}ms scan")
    print(f"\t[DEBUG] scan tally {tally}")
    print(f"\t[DEBUG] secondary executions: "
          f"{secondary_before} -> {secondary_after}")

    if secondary_after <= secondary_before:
        print("\tFailed: the SELECT did not execute on the secondary engine")
        return 1
    if commit_took < PAUSE_MS / 1000.0 * 0.9:
        print("\tFailed: COMMIT returned too fast; the install pause did not "
              "engage")
        return 1
    if into_scan <= 0:
        print("\tFailed: the last install landed before the scan began; the "
              "mid-scan schedule was not reached")
        return 1
    if reader.done_at <= writer.commit_done_at:
        print("\tFailed: the scan finished before the last install; the "
              "mid-scan schedule was not reached")
        return 1
    if tally.get("image_slots", 0) < 1:
        print(f"\tFailed: the read view resolved no slot through an epoch "
              f"image ({tally}); the write did not reach the scan")
        return 1
    if reader.rows != OLD_STATE:
        print(f"\tFailed: read view returned {reader.rows} != pre-write state "
              f"{OLD_STATE}; a mid-scan install reached the result")
        return 1

    cursor.execute("SET SESSION use_secondary_engine = FORCED")
    cursor.execute(fill(QUERY, table))
    fresh = cursor.fetchone()
    fresh_rows = (int(fresh[0]), int(fresh[1] or 0), int(fresh[2]))
    after_fresh = secondary_execution_count(cursor)
    cursor.execute("SET SESSION use_secondary_engine = OFF")
    cursor.execute(fill(QUERY, table))
    primary = cursor.fetchone()
    primary_rows = (int(primary[0]), int(primary[1] or 0), int(primary[2]))
    primary_delta = secondary_execution_count(cursor) - after_fresh
    cursor.execute("SET SESSION use_secondary_engine = ON")

    if fresh_rows != scenario["new"]:
        print(f"\tFailed: post-commit FORCED read {fresh_rows} != "
              f"{scenario['new']} (stale read)")
        return 1
    if primary_delta != 0:
        print("\tFailed: primary probe executed on the secondary engine")
        return 1
    if primary_rows != scenario["new"]:
        print(f"\tFailed: primary state {primary_rows} != {scenario['new']}")
        return 1

    print("\tconsistent: the scan spanned the install and returned the "
          "complete pre-write state, the next read is fresh, primary agrees")
    return 0


class Hammer(threading.Thread):
    """Single-row updates scattered over the table for a fixed window.

    Each is its own transaction with one row install, so the armed install
    pause does not fire: the installs land as fast as the writer can issue
    them, throughout the reader's scan.
    """

    # Coprime with ROWS, so the ids it walks are distinct and land in a
    # different group each step.
    STRIDE = 4099

    def __init__(self, user, password, table, delay_s, window_s, limit):
        super().__init__(daemon=True)
        self.user = user
        self.password = password
        self.table = table
        self.delay_s = delay_s
        self.window_s = window_s
        self.limit = limit
        self.updated = 0
        self.error = None

    def run(self):
        wdb = None
        try:
            wdb = get_connection(user=self.user, password=self.password)
            wdb.autocommit = True
            wcur = wdb.cursor()
            wcur.execute(f"USE {DB}")
            time.sleep(self.delay_s)
            deadline = time.monotonic() + self.window_s
            while self.updated < self.limit and time.monotonic() < deadline:
                row = 1 + (self.updated * self.STRIDE) % ROWS
                wcur.execute(
                    f"UPDATE {self.table} SET v = 1 WHERE id = {row}")
                self.updated += 1
        except Exception as e:  # surfaced after join
            self.error = e
        finally:
            if wdb is not None:
                try:
                    wdb.close()
                except Exception:
                    pass


def run_concurrent_scenario(cursor, user, password):
    """Installs throughout the scan: the only scenario that pins the audit.

    Installs land in groups the scan is holding, so chunk_redos >= 1 is
    asserted; the paused scenarios cannot reach that.
    """
    table = "midscan_stream"
    print("SCENARIO concurrent-updates")
    create_table(cursor, table)
    scan_s = measure_scan(cursor, table)
    print(f"\t[DEBUG] scan={scan_s * 1000:.0f}ms over {ROWS} rows")

    secondary_before = secondary_execution_count(cursor)
    reader = Reader(user, password, table)
    hammer = Hammer(user, password, table,
                    FENCE_HOLD_MS / 1000.0 + FENCE_S, scan_s * 1.5, 2000)
    reader.start()
    hammer.start()
    reader.join(timeout=180)
    hammer.join(timeout=180)
    if reader.is_alive() or hammer.is_alive():
        print("\tFailed: reader or writer did not finish")
        return 1
    if reader.error:
        raise reader.error
    if hammer.error:
        raise hammer.error

    tally = last_scan_tally()
    secondary_after = secondary_execution_count(cursor)
    print(f"\t[DEBUG] reader rows={reader.rows} updates={hammer.updated} "
          f"read_took={reader.done_at - reader.issued_at:.2f}s")
    print(f"\t[DEBUG] scan tally {tally}")

    if secondary_after <= secondary_before:
        print("\tFailed: the SELECT did not execute on the secondary engine")
        return 1
    if hammer.updated < 10:
        print(f"\tFailed: only {hammer.updated} updates ran during the scan")
        return 1
    if reader.rows != OLD_STATE:
        print(f"\tFailed: read view returned {reader.rows} != pre-write state "
              f"{OLD_STATE}; a mid-scan install reached the result")
        return 1
    if tally.get("chunk_redos", 0) < 1:
        print(f"\tFailed: no chunk audit found a group written under it "
              f"({tally}); the mid-scan schedule was not reached")
        return 1

    new_state = (ROWS, hammer.updated, NON_NULL)
    cursor.execute("SET SESSION use_secondary_engine = FORCED")
    cursor.execute(fill(QUERY, table))
    fresh = cursor.fetchone()
    fresh_rows = (int(fresh[0]), int(fresh[1] or 0), int(fresh[2]))
    after_fresh = secondary_execution_count(cursor)
    cursor.execute("SET SESSION use_secondary_engine = OFF")
    cursor.execute(fill(QUERY, table))
    primary = cursor.fetchone()
    primary_rows = (int(primary[0]), int(primary[1] or 0), int(primary[2]))
    primary_delta = secondary_execution_count(cursor) - after_fresh
    cursor.execute("SET SESSION use_secondary_engine = ON")

    if fresh_rows != new_state:
        print(f"\tFailed: post-commit FORCED read {fresh_rows} != "
              f"{new_state} (stale read)")
        return 1
    if primary_delta != 0:
        print("\tFailed: primary probe executed on the secondary engine")
        return 1
    if primary_rows != new_state:
        print(f"\tFailed: primary state {primary_rows} != {new_state}")
        return 1

    print(f"\tconsistent: {tally.get('chunk_redos')} chunk audits redid a "
          f"group written under the scan, and the read returned the complete "
          f"pre-write state")
    return 0


def run_probe(user, password):
    db = get_connection(user=user, password=password)
    db.autocommit = True
    try:
        cursor = db.cursor()
        print("SETUP")
        cursor.execute("SET GLOBAL helios_read_path = 'plan'")
        cursor.execute(f"DROP DATABASE IF EXISTS {DB}")
        cursor.execute(f"CREATE DATABASE {DB}")
        cursor.execute(f"USE {DB}")
        for scenario in SCENARIOS:
            if run_scenario(cursor, user, password, scenario):
                return 1
        return run_concurrent_scenario(cursor, user, password)
    finally:
        try:
            db.close()
        except Exception:
            pass


def main(user, password):
    os.environ.pop("MYSQL_UNIX_PORT", None)
    print(f"restarting stack with {PAUSE_MS}ms install sync point, "
          f"{FENCE_HOLD_MS}ms fence hold and one bridge thread")
    sh(f"./scripts/stop_mysql.sh {QUIET}")
    sh(f"./scripts/stop_server.sh {QUIET}")
    ensure_stack_stopped()
    failed = 1
    try:
        start_stack_with_test_env()
        failed = run_probe(user, password)
    finally:
        print("stopping test stack (disarm the sync points)")
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
