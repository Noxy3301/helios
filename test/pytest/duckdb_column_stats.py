"""The column statistics Helios hands DuckDB must not drop a row.

PAX keeps a per-column minimum and maximum, and the DuckDB table function
reports them through TableFunction::statistics (server/rpc/olap_executor.cc).
DuckDB is free to answer a filter from that range alone, so a range narrower
than the values stored, or a boundary converted into the wrong units, removes
rows from a query that still succeeds: no error and no warning appear.

What this file catches is a boundary that falls on the inside of the values
stored. A boundary that falls outside them costs a scan and no rows, which is
the direction the statistics are allowed to err in.

The three stored types needing a conversion each carry both boundaries: INT
is a signed binary value, DECIMAL(15,2) an integer scaled by the column
scale, and DATE a YYYYMMDD integer the scan turns into a calendar date. An
equality test sits on each boundary, so a scale or a date conversion that
shifts one of them inward shows up as a row count differing from the row
path.

The second scenario writes one row below every minimum and one above every
maximum, then asks for each of them by the boundary it moved. A boundary left
where the load put it prunes those queries to nothing.

The third scenario is the other way a stale boundary breaks a query. DuckDB
also drops a filter it can prove every row satisfies, so a maximum left
behind by a later write turns `WHERE c <= that maximum` into a scan with no
filter, and the row written above it comes back with the rest. This scenario
asks for each column by the maximum the load left, once a higher row exists.

Predicate literals stay non-negative. The plugin refuses to offload a
statement holding a unary minus, so the negative values live in columns whose
upper boundary is what the queries name, and columns p and q carry the
lower-boundary tests.
"""

import argparse
import os
import sys
from datetime import date
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.connection import get_connection

DB = "ha_helios_test"
TABLE = "column_stats"

# (id, n INT, d DECIMAL(15,2), t DATE, p INT, q DECIMAL(15,2))
ROWS = [
    (1, -40, Decimal("-12.25"), date(1996, 1, 2), 40, Decimal("1.25")),
    (2, 0, Decimal("0.00"), date(2007, 6, 15), 100, Decimal("9.50")),
    (3, 17, Decimal("9.50"), date(2015, 12, 31), 700, Decimal("99.99")),
    (4, 900, Decimal("1234.75"), date(2020, 2, 29), 900, Decimal("1234.75")),
]
# Written after SECONDARY_LOAD, one below every minimum and one above every
# maximum the load left behind.
LATE_ROWS = [
    (5, -1000, Decimal("-9999.99"), date(1970, 1, 1), 1, Decimal("0.25")),
    (6, 2000, Decimal("99999.99"), date(2099, 12, 31), 5000,
     Decimal("88888.88")),
]

# Column index, name, and whether its minimum can be written as a predicate
# literal. n and d hold negative values, and a statement with a unary minus
# is not offloaded, so only their maximum is asked for by value.
COLUMNS = (
    (1, "n", False),
    (2, "d", False),
    (3, "t", True),
    (4, "p", True),
    (5, "q", True),
)
# The separator for the two columns whose minimum stays unnamed, which keeps
# a query over their negative rows in the set.
ZERO = {"n": 0, "d": Decimal("0.00")}


def literal(value):
    return f"'{value}'" if isinstance(value, date) else str(value)


def checks(rows):
    """Queries whose answers the statistics must not change, with expectations.

    Each column sits its queries on the boundaries: past the maximum (empty),
    at the maximum (the rows holding it), and every row up to it. A column
    whose minimum can be written also gets below the minimum (empty) and at
    the minimum, which is what a boundary left behind by a later write prunes
    away.
    """
    out = []
    for index, column, low_usable in COLUMNS:
        values = sorted(row[index] for row in rows)
        low, high = values[0], values[-1]
        out += [
            (f"SELECT COUNT(*) FROM {{t}} WHERE {column} > {literal(high)}",
             [(0,)]),
            (f"SELECT COUNT(*) FROM {{t}} WHERE {column} = {literal(high)}",
             [(sum(1 for v in values if v == high),)]),
            (f"SELECT id FROM {{t}} WHERE {column} <= {literal(high)} "
             "ORDER BY id",
             [(row[0],) for row in sorted(rows)]),
        ]
        if low_usable:
            out += [
                (f"SELECT COUNT(*) FROM {{t}} WHERE {column} < {literal(low)}",
                 [(0,)]),
                (f"SELECT COUNT(*) FROM {{t}} WHERE {column} = {literal(low)}",
                 [(sum(1 for v in values if v == low),)]),
            ]
        else:
            zero = ZERO[column]
            out.append(
                (f"SELECT COUNT(*) FROM {{t}} WHERE {column} < "
                 f"{literal(zero)}",
                 [(sum(1 for v in values if v < zero),)]))
    return out


def stale_bound_checks(rows):
    """Queries a boundary left behind by a later write turns into wrong rows.

    Each literal is the maximum the load left. Once a row sits above it, a
    filter answered from that maximum is dropped as always true, and that row
    comes back with the rest.
    """
    out = []
    for index, column, _ in COLUMNS:
        loaded_high = max(row[index] for row in ROWS)
        kept = sum(1 for row in rows if row[index] <= loaded_high)
        out.append(
            (f"SELECT COUNT(*) FROM {{t}} WHERE {column} <= "
             f"{literal(loaded_high)}", [(kept,)]))
    return out


def secondary_execution_count(cursor):
    cursor.execute("SHOW GLOBAL STATUS LIKE 'Secondary_engine_execution_count'")
    return int(cursor.fetchone()[1])


def fetch(cursor, sql, analytical):
    """Runs one query on the named path and verifies it went there."""
    statement = sql.format(t=TABLE)
    before = secondary_execution_count(cursor)
    cursor.execute("SET SESSION use_secondary_engine = "
                   f"{'FORCED' if analytical else 'OFF'}")
    cursor.execute(statement)
    rows = [tuple(row) for row in cursor.fetchall()]
    used_secondary = secondary_execution_count(cursor) > before
    cursor.execute("SET SESSION use_secondary_engine = ON")
    if used_secondary != analytical:
        raise RuntimeError(
            f"{statement} ran on the "
            f"{'primary' if analytical else 'secondary'} engine")
    return rows


def compare(cursor, cases, label):
    """Every check on both paths: each must match, and match the other."""
    failed = 0
    for sql, expected in cases:
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


def create_table(cursor):
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cursor.execute(
        f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, n INT, "
        "d DECIMAL(15,2), t DATE, p INT, q DECIMAL(15,2)) "
        "ENGINE=Helios SECONDARY_ENGINE=HELIOS_DUCKDB")
    for row in ROWS:
        cursor.execute(
            f"INSERT INTO {TABLE} VALUES (%s, %s, %s, %s, %s, %s)", row)
    cursor.execute(f"ALTER TABLE {TABLE} SECONDARY_LOAD")


def main(user, password):
    db = get_connection(user=user, password=password)
    db.autocommit = True
    failed = 1
    try:
        cursor = db.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
        cursor.execute(f"USE {DB}")
        create_table(cursor)

        print("SCENARIO loaded rows")
        failed = compare(cursor, checks(ROWS), "loaded")
        if not failed:
            print("\tconsistent: both paths keep every row inside the bounds")

        print("SCENARIO rows past both ends of the reported range")
        for row in LATE_ROWS:
            cursor.execute(
                f"INSERT INTO {TABLE} VALUES (%s, %s, %s, %s, %s, %s)", row)
        if compare(cursor, checks(ROWS + LATE_ROWS), "late rows"):
            failed = 1
        elif not failed:
            print("\tconsistent: the boundaries followed the later writes")

        print("SCENARIO filter answered from a boundary the load left")
        if compare(cursor, stale_bound_checks(ROWS + LATE_ROWS),
                   "stale bound"):
            failed = 1
        elif not failed:
            print("\tconsistent: no row above the loaded maximum slipped in")
    finally:
        try:
            db.close()
        except Exception:
            pass

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
