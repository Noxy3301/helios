import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_helios_index_pk_columns_{int(time.time())}"

MAX = 2**31 - 1
MIN = -2**31
OPS = (">", ">=", "<", "<=")

DDL = ("a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, n INT NOT NULL, "
       "PRIMARY KEY (a, b), KEY ib (b), KEY ica (c, a), KEY iba (b, a)")
ALTER = "ALTER TABLE {t} ADD INDEX icb (c, b)"
ROWS = "VALUES " + ",".join(
    f"({a},{b},{(MIN, 0, MAX)[i % 3]},{i})"
    for i, (a, b) in enumerate((a, b) for a in (MIN, -1, 0, 1, MAX)
                               for b in (MIN, -1, 0, MAX)))

# Each index: name, its columns, then the key's order with the primary key.
INDEXES = [("ib", ("b",), "b, a"), ("ica", ("c", "a"), "c, a, b"),
           ("iba", ("b", "a"), "b, a"), ("icb", ("c", "b"), "c, b, a")]


def reads():
    stmts = []
    for name, cols, order in INDEXES:
        lead = cols[0]
        desc = ", ".join(f"{c} DESC" for c in order.split(", "))
        hint = f" FORCE INDEX({name})"
        q = f"SELECT a, b, c, n FROM {{t}}{hint}"
        cover = "a, b, c" if "c" in cols else "a, b"
        cover = f"SELECT {cover} FROM {{t}}{hint}"
        stmts += [f"{q} WHERE {lead} = {v}" for v in (MIN, -1, 0, MAX)]
        stmts += [f"{q} WHERE {lead} {op} {v}"
                  for op in OPS for v in (MIN, -1, 0, MAX)]
        stmts.append(f"{q} WHERE {lead} BETWEEN -1 AND 0")
        if len(cols) == 2:
            stmts += [f"{q} WHERE {lead} = {MAX} AND {cols[1]} = {MAX}",
                      f"{q} WHERE {lead} = 0 AND {cols[1]} > -1",
                      f"{q} WHERE {lead} = {MIN} AND {cols[1]} < 0",
                      f"SELECT MAX({cols[1]}) FROM {{t}}{hint} "
                      f"WHERE {lead} = 0"]
        stmts += [f"={q} ORDER BY {order}",
                  f"={q} ORDER BY {order} LIMIT 3",
                  f"={q} ORDER BY {desc}",
                  f"={q} ORDER BY {desc} LIMIT 3",
                  f"={q} WHERE {lead} = {MAX} ORDER BY {desc} LIMIT 2",
                  f"={q} WHERE {lead} < {MAX} ORDER BY {desc} LIMIT 2",
                  f"={cover} ORDER BY {order}",
                  f"={cover} WHERE {lead} >= 0 ORDER BY {desc} LIMIT 4",
                  f"{cover} WHERE {lead} = {MIN}",
                  f"SELECT COUNT(*) FROM {{t}}{hint}",
                  f"SELECT COUNT(*) FROM {{t}}{hint} WHERE {lead} = 0",
                  f"SELECT COUNT(*) FROM {{t}}{hint} WHERE {lead} > -1",
                  f"SELECT MIN({lead}), MAX({lead}) FROM {{t}}{hint}",
                  f"SELECT MIN({lead}) FROM {{t}}{hint} WHERE {lead} > 0",
                  f"SELECT MAX({lead}) FROM {{t}}{hint} WHERE {lead} < 0"]
    return stmts


# Writes that move rows within each index: a primary-key column, a secondary
# column, both, a delete, and inserts that reuse a deleted key.
WRITES = [
    "UPDATE {t} SET b = 7, c = 1 WHERE a = 1 AND b = -1",
    "UPDATE {t} SET a = a + 10 WHERE b = %d AND a BETWEEN -1 AND 1" % MIN,
    "UPDATE {t} SET c = -5 WHERE c = 0 AND a < 0",
    "DELETE FROM {t} WHERE b = 0 AND a > 0",
    "DELETE FROM {t} WHERE c = %d AND b = %d" % (MIN, MAX),
    "INSERT INTO {t} VALUES (2, %d, %d, 100), (%d, 1, 0, 101)"
    % (MIN, MAX, MAX),
    "INSERT INTO {t} VALUES (1, 0, 3, 102)",
]

# A result must match InnoDB's: rows as a multiset, or in order after "=";
# a write by affected rows.
STMTS = reads() + WRITES + reads()


def run(cursor, sql):
    """('rows', rows) or ('rowcount', n) on success, ('error', errno) else."""
    try:
        cursor.execute(sql)
        if cursor.with_rows:
            return "rows", cursor.fetchall()
        return "rowcount", cursor.rowcount
    except mysql.connector.Error as e:
        return "error", e.errno


def twins(cursor, name):
    for engine in ("Helios", "InnoDB"):
        t = f"{name}_{engine.lower()}"
        cursor.execute(f"CREATE TABLE {t} ({DDL}) ENGINE={engine}")
        cursor.execute(f"INSERT INTO {t} {ROWS}")
        cursor.execute(ALTER.format(t=t))


def test_cases(cursor):
    print("INDEX PK COLUMNS: Helios against InnoDB twins")
    failed = total = 0
    for path in ("plan", "row"):
        cursor.execute(f"SET GLOBAL helios_read_path='{path}'")
        name = f"pk_cols_{path}"
        try:
            twins(cursor, name)
        except mysql.connector.Error as e:
            print(f"\tFAILED [{path}] {name}: setup: {e}")
            failed += 1
            total += 1
            continue
        for stmt in STMTS:
            ordered = stmt.startswith("=")
            stmt = stmt.lstrip("=")
            got = run(cursor, stmt.format(t=f"{name}_helios"))
            want = run(cursor, stmt.format(t=f"{name}_innodb"))
            ok = got[0] == want[0] != "error" and (
                got[1] == want[1] if ordered or got[0] == "rowcount" else
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
