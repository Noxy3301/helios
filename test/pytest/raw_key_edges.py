import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_helios_raw_key_edges_{int(time.time())}"

OPS = (">", ">=", "<", "<=")
INT_TYPES = [("TINYINT", -2**7, 2**7 - 1), ("SMALLINT", -2**15, 2**15 - 1),
             ("INT", -2**31, 2**31 - 1), ("BIGINT", -2**63, 2**63 - 1)]


def int_case(type_, lo, hi):
    """A signed type's edges on the primary key k and on index ks."""
    keys = sorted({lo, lo + 1, -2, -1, 0, 1, 2, 100, hi - 1, hi})
    edges = (lo, -1, 0, 1, hi)
    rows = "VALUES " + ",".join(f"({k},{edges[i % len(edges)]})"
                                for i, k in enumerate(keys))
    stmts = []
    for c, hint in (("k", " FORCE INDEX(PRIMARY)"), ("s", " FORCE INDEX(ks)")):
        q = f"SELECT k, s FROM {{t}}{hint}"
        stmts += [f"{q} WHERE {c} = {v}" for v in edges]
        stmts += [f"{q} WHERE {c} {op} {v}"
                  for op in OPS for v in (lo, -1, 0, hi - 1, hi)]
        stmts.append(f"{q} WHERE {c} BETWEEN -1 AND 1")
        for order in ("", " DESC"):
            by = f"{c}{order}" + (f", k{order}" if c == "s" else "")
            stmts.append(f"={q} ORDER BY {by}")
            stmts.append(f"={q} ORDER BY {by} LIMIT 3")
        stmts += [f"SELECT MIN({c}) FROM {{t}}{hint} WHERE {c} > {hi - 1}",
                  f"SELECT MIN({c}) FROM {{t}}{hint} WHERE {c} > {hi}",
                  f"SELECT MAX({c}) FROM {{t}}{hint} WHERE {c} < {lo + 1}"]
    return (type_.lower(), f"k {type_} NOT NULL PRIMARY KEY, "
            f"s {type_} NOT NULL, KEY ks (s)", rows, stmts)


def temporal_case(name, type_, keys, probes):
    """Point and range reads on a temporal primary key k."""
    rows = "VALUES " + ",".join(f"('{k}',{i})" for i, k in enumerate(keys))
    q = "SELECT k, n FROM {t} FORCE INDEX(PRIMARY)"
    stmts = [f"{q} WHERE k = '{p}'" for p in probes]
    stmts += [f"{q} WHERE k {op} '{p}'" for op in OPS for p in probes]
    stmts.append(f"{q} WHERE k BETWEEN '{probes[0]}' AND '{probes[-1]}'")
    stmts.append(f"={q} ORDER BY k DESC LIMIT 3")
    stmts.append(f"={q} WHERE k < '{probes[-1]}' ORDER BY k DESC LIMIT 2")
    return (name, f"k {type_} NOT NULL PRIMARY KEY, n INT NOT NULL", rows,
            stmts)


DAY_EDGE = ["2024-12-31 23:59:59.999999", "2025-01-01 00:00:00",
            "2025-01-01 00:00:00.000001", "2025-01-01 00:00:00.5"]
DATETIMES = DAY_EDGE + ["1000-01-01 00:00:00", "2024-12-31 23:59:59",
                        "2025-01-01 00:00:01", "9999-12-31 23:59:59.999999"]
TIMESTAMPS = DAY_EDGE + ["1970-01-01 00:00:01", "2024-12-31 23:59:59",
                         "2025-01-01 00:00:01", "2038-01-19 03:14:07.999999"]

# A composite key whose leading INT part packs raw and whose VARCHAR part
# keeps its header; "a = MAX" is a prefix of 0xff bytes without a successor.
MAX = 2**31 - 1
MIN = -2**31


def composite_case():
    rows = "VALUES " + ",".join(f"({a},'{v}',{b})"
                                for a in (MIN, -1, 0, MAX - 1, MAX)
                                for v in ("", "a", "b")
                                for b in (-1, 0, MAX))
    stmts = []
    for c, hint, order in (
            ("a", " FORCE INDEX(PRIMARY)", "a DESC, v DESC, b DESC"),
            ("b", " FORCE INDEX(ib)", "b DESC, v DESC, a DESC")):
        q = f"SELECT a, v, b FROM {{t}}{hint}"
        stmts += [f"{q} WHERE {c} = {MAX}",
                  f"{q} WHERE {c} = {MAX} AND v = 'a'",
                  f"{q} WHERE {c} = {MAX} AND v > 'a'",
                  f"{q} WHERE {c} = {MIN}",
                  f"{q} WHERE {c} = -1",
                  f"{q} WHERE {c} > {MAX - 1}",
                  f"{q} WHERE {c} >= {MAX}",
                  f"{q} WHERE {c} < {MAX}",
                  f"{q} WHERE {c} <= {MIN}",
                  f"{q} WHERE {c} BETWEEN -1 AND 0",
                  f"={q} WHERE {c} = {MAX} ORDER BY {order} LIMIT 2",
                  f"={q} WHERE {c} < {MAX} ORDER BY {order} LIMIT 2",
                  f"SELECT MIN({c}) FROM {{t}}{hint} WHERE {c} > {MAX - 1}",
                  f"SELECT MAX({c}) FROM {{t}}{hint} WHERE {c} < {MIN + 1}"]
    return ("composite", "a INT NOT NULL, v VARCHAR(10) NOT NULL, "
            "b INT NOT NULL, PRIMARY KEY (a, v, b), KEY ib (b, v)", rows,
            stmts)


# Each case: twin DDL, rows, statements. A result must match InnoDB's: rows as
# a multiset, or in order after "=".
CASES = [int_case(*t) for t in INT_TYPES] + [
    temporal_case("date", "DATE",
                  ["1000-01-01", "2024-02-28", "2024-02-29", "2024-03-01",
                   "2024-12-31", "2025-01-01", "9999-12-31"],
                  ["2024-02-29", "2024-12-31", "2025-01-01"]),
    temporal_case("datetime", "DATETIME(6)", DATETIMES, DAY_EDGE),
    temporal_case("timestamp", "TIMESTAMP(6)", TIMESTAMPS, DAY_EDGE),
    composite_case(),
]


def run(cursor, sql):
    """('rows', rows) on success, ('error', errno) else."""
    try:
        cursor.execute(sql)
        return "rows", cursor.fetchall()
    except mysql.connector.Error as e:
        return "error", e.errno


def twins(cursor, name, ddl, rows):
    for engine in ("Helios", "InnoDB"):
        t = f"{name}_{engine.lower()}"
        cursor.execute(f"CREATE TABLE {t} ({ddl}) ENGINE={engine}")
        cursor.execute(f"INSERT INTO {t} {rows}")


def test_cases(cursor):
    print("RAW KEYS: Helios against InnoDB twins")
    failed = total = 0
    for path in ("plan", "row"):
        cursor.execute(f"SET GLOBAL helios_read_path='{path}'")
        for name, ddl, rows, stmts in CASES:
            name = f"{name}_{path}"
            try:
                twins(cursor, name, ddl, rows)
            except mysql.connector.Error as e:
                failed += 1
                total += 1
                print(f"\tFAILED [{path}] {name}: setup: {e}")
                continue
            for stmt in stmts:
                ordered = stmt.startswith("=")
                stmt = stmt.lstrip("=")
                got = run(cursor, stmt.format(t=f"{name}_helios"))
                want = run(cursor, stmt.format(t=f"{name}_innodb"))
                ok = got[0] == want[0] == "rows" and (
                    got[1] == want[1] if ordered else
                    sorted(got[1], key=repr) == sorted(want[1], key=repr))
                total += 1
                if not ok:
                    failed += 1
                    print(f"\tFAILED [{path}] {name}: {stmt}\n"
                          f"\t\thelios {got}\n\t\tinnodb {want}")
    print(f"\t{total - failed}/{total} statements match")
    return 1 if failed else 0


def main():
    db = get_connection(user=args.user, password=args.password)
    db.autocommit = True
    cursor = db.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    # The TIMESTAMP edge values are the type's limits in UTC.
    cursor.execute("SET time_zone = '+00:00'")
    rc = 0
    try:
        rc = test_cases(cursor)
    except mysql.connector.Error as e:
        print(f"\tFAILED (unexpected mysql error): {e}")
        rc = 1
    finally:
        try:
            cursor.execute("SET GLOBAL helios_read_path='plan'")
            cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
            db.close()
        except mysql.connector.Error:
            pass
    sys.exit(rc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()
    main()
