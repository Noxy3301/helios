import argparse
import operator
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_helios_reverse_range_{int(time.time())}"

# The index cursor reads INDEX_CURSOR_BATCH_SIZE (1024) rows per fetch; use a
# table larger than two fetches so reverse walks cross fetch boundaries.
ROWS = 2500

# Per key column: lowest key, highest key, and the lowest key the index tail
# fetch returns; a full secondary scan batch drops its lowest secondary key
# group.
BOUNDS = {
    "id": (1, ROWS, ROWS - 1023),
    "sk": (0, ROWS // 3, (ROWS - 1023) // 3 + 1),
}

OPS = {">=": operator.ge, ">": operator.gt, "<=": operator.le, "<": operator.lt}

LIMITS = (None, 1, 5, 1100)

# Without the hint the optimizer may read an sk range by a table scan and a
# filesort; the hint makes it read idx_sk backward.
SCANS = (("id", ""), ("sk", ""), ("sk", " FORCE INDEX(idx_sk)"))


def test_reverse_ranges(cursor, db):
    print("REVERSE RANGE: WHERE k <op> x ORDER BY k DESC [LIMIT n]")
    cursor.execute(
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, sk INT NOT NULL, "
        "KEY idx_sk (sk)) ENGINE=Helios"
    )
    for start in range(1, ROWS + 1, 500):
        rows = range(start, min(start + 500, ROWS + 1))
        cursor.execute("INSERT INTO t VALUES " +
                       ",".join(f"({i},{i // 3})" for i in rows))
    db.commit()
    total = failed = 0
    for path in ("plan", "row"):
        cursor.execute(f"SET GLOBAL helios_read_path='{path}'")
        for col, hint in SCANS:
            lo, hi, edge = BOUNDS[col]
            # Rows as (k, id), k descending.
            table = [(i if col == "id" else i // 3, i)
                     for i in range(ROWS, 0, -1)]
            cases = [
                (">=", edge, None), (">=", edge - 1, None), (">=", lo, None),
                (">=", hi, None), (">=", hi + 1, None),
                (">", edge, None), (">", edge - 1, None), (">", hi - 1, None),
                (">", hi, None),
                ("<=", edge, None), ("<=", lo, None), ("<=", lo - 1, None),
                ("<=", hi, None),
                ("<", edge, None), ("<", lo + 1, None), ("<", lo, None),
                ("BETWEEN", edge - 100, edge + 100), ("BETWEEN", lo, lo + 10),
                ("BETWEEN", hi - 10, hi), ("BETWEEN", hi + 1, hi + 100),
            ]
            for op, a, b in cases:
                if op == "BETWEEN":
                    cond = f"{col} BETWEEN {a} AND {b}"
                    want = [r for r in table if a <= r[0] <= b]
                else:
                    cond = f"{col} {op} {a}"
                    want = [r for r in table if OPS[op](r[0], a)]
                for limit in LIMITS:
                    sql = (f"SELECT {col}, id FROM t{hint} WHERE {cond} "
                           f"ORDER BY {col} DESC")
                    if limit:
                        sql += f" LIMIT {limit}"
                    total += 1
                    try:
                        cursor.execute(sql)
                        got = [tuple(r) for r in cursor.fetchall()]
                        db.commit()
                    except mysql.connector.Error as e:
                        print(f"\tFAILED [{path}] {sql}: {e}")
                        db.rollback()
                        failed += 1
                        continue
                    # Ties on sk may come in any id order, so the k sequence
                    # is compared and each row must be a distinct row of the
                    # range.
                    if ([k for k, _ in got] != [k for k, _ in want[:limit]]
                            or len(set(got)) != len(got)
                            or not set(got) <= set(want)):
                        print(f"\tFAILED [{path}] {sql}: got "
                              f"{len(got)} rows {got[:3]}..{got[-3:]}, want "
                              f"{len(want[:limit])} rows {want[:3]}..")
                        failed += 1
    print(f"\t{total - failed}/{total} queries correct")
    if failed:
        return 1
    print("\tPassed!")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    db.commit()
    try:
        rc = test_reverse_ranges(cursor, db)
    except mysql.connector.Error as e:
        print(f"\tFAILED (unexpected mysql error): {e}")
        db.rollback()
        rc = 1
    cursor.execute("SET GLOBAL helios_read_path='plan'")
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    db.close()
    sys.exit(rc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()
    main()
