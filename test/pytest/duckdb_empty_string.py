"""A non-NULL empty string must not read back as NULL on the analytical path.

The Helios packed row format packs a zero-length field with one marker byte,
which a VARCHAR '' and a SQL NULL share. The null bitmap in field 0 of the row
is what separates them, and the row path reads it (proxy/row_codec.cc). This
file pins that the analytical path (SECONDARY_ENGINE=HELIOS_DUCKDB,
served by the DuckDB executor) agrees with it: WHERE c = '', WHERE c IS NULL,
COUNT(c), GROUP BY c and the select list must return the same answers as the
row path.

Column b is NOT NULL, so a and c hold non-adjacent null bits: a decode that
used the column ordinal as the bit index would read c through b's bit.

Two scan paths carry the same rows, and both are covered:
  - in place, from the PAX strip cells (the plain scenario)
  - through an epoch image, when a writer changes a row under an open read
    view (the concurrent scenario, which asserts image_slots >= 1)

The concurrent scenario uses the pax_view.after_fence sync point that holds
every OLAP read between its read view fence and its scan, so the writer
commits inside the hold and its rows resolve through images. The test owns
the local stack: it restarts it with the sync point armed and stops it
afterwards, as duckdb_midscan_preserve.py does.
"""

import argparse
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils import server_log
from utils.connection import get_connection
from utils.server_conf import write_conf

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
QUIET = "> /dev/null 2>&1"
# Bytes the server log held before this test started the stack.
LOG_OFFSET = 0
# The hold every OLAP read takes between its read view fence and its scan.
FENCE_HOLD_MS = 1000
# The debug sync points stay in the environment; the rest is configuration.
SERVER_ENV = ("HELIOS_DEBUG_SYNC_PAX_VIEW_AFTER_FENCE"
              f"=sleep:{FENCE_HOLD_MS}")
# The scan tallies, read back from the server log.
SERVER_CONF = {"olap_trace": 1}
# Where the writer's commit lands inside the fence hold; a fence takes one
# to two epochs, so this also clears it.
WRITE_AT_S = 0.35

DB = "ha_helios_test"
# One table per scenario: DROP TABLE leaves the rows in the storage, so a
# reused name collides on the primary key.
TABLES = ("empty_string_place", "empty_string_image")
TABLE = TABLES[0]
STACK_PATTERNS = ("build/server/helios-storage",
                  "runtime_output_directory/mysqld")

# (id, a, b, c). b is NOT NULL; a and c are nullable and each holds '', NULL
# and a non-empty value across the rows.
ROWS = [
    (1, "", "", "x"),
    (2, None, "x", ""),
    (3, "x", "", None),
    (4, "", "x", ""),
    (5, None, "", None),
    (6, "x", "x", "x"),
]

# The concurrent scenario's writer: '' becomes NULL and a value becomes '',
# so the images the reader resolves have to carry both distinctly.
WRITE = ["UPDATE {t} SET a = NULL WHERE id = 1",
         "UPDATE {t} SET a = '' WHERE id = 3"]
ROWS_AFTER = [(1, None, "", "x")] + ROWS[1:2] + [(3, "", "", None)] + ROWS[3:]


def counted(values, wanted):
    return sum(1 for value in values if value == wanted)


def group_counts(values):
    counts = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return sorted(counts.items(), key=lambda kv: (kv[0] is None, kv[0] or ""))


def checks(rows):
    """The queries and their expected rows, derived from `rows`."""
    a, b, c = [row[1] for row in rows], [row[2] for row in rows], \
        [row[3] for row in rows]
    return [
        ("SELECT COUNT(*) FROM {t} WHERE a = ''", [(counted(a, ""),)]),
        ("SELECT COUNT(*) FROM {t} WHERE a IS NULL", [(counted(a, None),)]),
        ("SELECT COUNT(a) FROM {t}",
         [(sum(1 for v in a if v is not None),)]),
        ("SELECT COUNT(*) FROM {t} WHERE b = ''", [(counted(b, ""),)]),
        ("SELECT COUNT(*) FROM {t} WHERE c = ''", [(counted(c, ""),)]),
        ("SELECT COUNT(*) FROM {t} WHERE c IS NULL", [(counted(c, None),)]),
        ("SELECT COUNT(c) FROM {t}",
         [(sum(1 for v in c if v is not None),)]),
        ("SELECT id, a, b, c FROM {t} ORDER BY id", [tuple(r) for r in rows]),
        ("SELECT c, COUNT(*) FROM {t} GROUP BY c", group_counts(c)),
    ]


def sh(cmd):
    return os.system(f"cd {ROOT} && {cmd}")


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
    global LOG_OFFSET
    conf = write_conf(**SERVER_CONF)
    # Every start appends to the one log; the tallies read back below are the
    # ones this start writes.
    LOG_OFFSET = server_log.size()
    if sh(f"{SERVER_ENV} ./scripts/start_server.sh --config {conf} {QUIET}") != 0:
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


def normalize(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    return value


def fetch(cursor, sql, analytical):
    """Runs one query on the named path and verifies it went there."""
    statement = sql.format(t=TABLE)
    before = secondary_execution_count(cursor)
    cursor.execute("SET SESSION use_secondary_engine = "
                   f"{'FORCED' if analytical else 'OFF'}")
    cursor.execute(statement)
    rows = [tuple(normalize(value) for value in row)
            for row in cursor.fetchall()]
    used_secondary = secondary_execution_count(cursor) > before
    cursor.execute("SET SESSION use_secondary_engine = ON")
    if used_secondary != analytical:
        raise RuntimeError(
            f"{statement} ran on the "
            f"{'primary' if analytical else 'secondary'} engine")
    if "GROUP BY" in statement:
        rows.sort(key=lambda row: (row[0] is None, row[0] or ""))
    return rows


def last_scan_tally():
    """Scan tallies of the most recent OLAP request, from the server log."""
    tally = {}
    for line in server_log.read_since(LOG_OFFSET).splitlines():
        if line.startswith("[duckdb-ast] "):
            tally = {}
        elif line.startswith("[duckdb-scan] "):
            for part in line.split()[2:]:
                if "=" not in part:
                    continue
                key, value = part.split("=", 1)
                tally[key] = tally.get(key, 0) + int(value)
    return tally


def create_table(cursor):
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cursor.execute(
        f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, a VARCHAR(32), "
        "b VARCHAR(32) NOT NULL, c VARCHAR(32)) "
        "ENGINE=Helios SECONDARY_ENGINE=HELIOS_DUCKDB")
    for row in ROWS:
        cursor.execute(f"INSERT INTO {TABLE} VALUES (%s, %s, %s, %s)", row)
    cursor.execute(f"ALTER TABLE {TABLE} SECONDARY_LOAD")


def compare(cursor, rows, label):
    """Every check on both paths: each must match, and match the other."""
    failed = 0
    for sql, expected in checks(rows):
        statement = sql.format(t=TABLE)
        analytical = fetch(cursor, sql, True)
        primary = fetch(cursor, sql, False)
        if primary != expected:
            print(f"\tFailed [{label}] row path: {statement}\n"
                  f"\t\tgot {primary}\n\t\twant {expected}")
            failed = 1
        if analytical != expected:
            print(f"\tFailed [{label}] analytical path: {statement}\n"
                  f"\t\tgot {analytical}\n\t\twant {expected}")
            failed = 1
    return failed


class Reader(threading.Thread):
    """One FORCED select of the whole table, held at the read view fence."""

    def __init__(self, user, password):
        super().__init__(daemon=True)
        self.user = user
        self.password = password
        self.rows = None
        self.error = None

    def run(self):
        db = None
        try:
            db = get_connection(user=self.user, password=self.password)
            db.autocommit = True
            cursor = db.cursor()
            cursor.execute(f"USE {DB}")
            self.rows = fetch(cursor, "SELECT id, a, b, c FROM {t} ORDER BY id",
                              True)
        except Exception as e:  # surfaced after join
            self.error = e
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass


class Writer(threading.Thread):
    """One transaction whose commit lands inside the reader's fence hold."""

    def __init__(self, user, password):
        super().__init__(daemon=True)
        self.user = user
        self.password = password
        self.committed_at = None
        self.error = None

    def run(self):
        db = None
        try:
            db = get_connection(user=self.user, password=self.password)
            db.autocommit = True
            cursor = db.cursor()
            cursor.execute(f"USE {DB}")
            time.sleep(WRITE_AT_S)
            cursor.execute("BEGIN")
            for statement in WRITE:
                cursor.execute(statement.format(t=TABLE))
            cursor.execute("COMMIT")
            self.committed_at = time.monotonic()
        except Exception as e:  # surfaced after join
            self.error = e
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass


def run_in_place(cursor):
    global TABLE
    TABLE = TABLES[0]
    print("SCENARIO in-place")
    create_table(cursor)
    if compare(cursor, ROWS, "in-place"):
        return 1
    print("\tconsistent: both paths separate '' from NULL in the strip cells")
    return 0


def run_concurrent(cursor, user, password):
    """The reader's rows come from epoch images, and must still separate them."""
    global TABLE
    TABLE = TABLES[1]
    print("SCENARIO epoch-image")
    create_table(cursor)

    reader = Reader(user, password)
    writer = Writer(user, password)
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

    tally = last_scan_tally()
    print(f"\t[DEBUG] reader rows={reader.rows}")
    print(f"\t[DEBUG] scan tally {tally}")
    if tally.get("image_slots", 0) < 1:
        print(f"\tFailed: the read view resolved no slot through an epoch "
              f"image ({tally}); the write did not land inside the hold")
        return 1
    if reader.rows != list(ROWS):
        print(f"\tFailed: read view returned {reader.rows} != pre-write state "
              f"{list(ROWS)}")
        return 1
    if compare(cursor, ROWS_AFTER, "post-write"):
        return 1
    print("\tconsistent: images carried '' and NULL apart, and the next read "
          "is fresh on both paths")
    return 0


def run_probe(user, password):
    db = get_connection(user=user, password=password)
    db.autocommit = True
    try:
        cursor = db.cursor()
        print("SETUP")
        cursor.execute(f"DROP DATABASE IF EXISTS {DB}")
        cursor.execute(f"CREATE DATABASE {DB}")
        cursor.execute(f"USE {DB}")
        if run_in_place(cursor):
            return 1
        return run_concurrent(cursor, user, password)
    finally:
        try:
            db.close()
        except Exception:
            pass


def main(user, password):
    os.environ.pop("MYSQL_UNIX_PORT", None)
    print(f"restarting stack with a {FENCE_HOLD_MS}ms fence hold")
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
