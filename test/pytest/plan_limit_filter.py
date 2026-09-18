"""Check that a residual WHERE plus LIMIT returns the same rows on both read
paths. The plan path may stage a LIMIT into the storage scan, so a filter MySQL
evaluates above the scan must not lose rows."""

import argparse
import sys

from utils.connection import get_connection


DBNAME = "ha_lineairdb_plan_limit_filter"

# k = 1 for pk 1..40, flag = 1 only for pk 31..40, so the filtered rows sit at
# the far end of the index window a staged LIMIT would truncate.
ROWS = 50
K_ROWS = 40
FLAG_LO, FLAG_HI = 31, 40

QUERIES = [
    ("a", "SELECT pk FROM t WHERE k = 1 AND flag = 1 LIMIT 1", [31]),
    ("b", "SELECT pk FROM t WHERE k = 1 AND flag = 1 LIMIT 5",
     [31, 32, 33, 34, 35]),
    ("c", "SELECT pk FROM t WHERE k = 1 AND flag = 1 ORDER BY pk LIMIT 3",
     [31, 32, 33]),
    ("d", "SELECT pk FROM t WHERE k = 1 AND flag = 1 ORDER BY pk DESC LIMIT 3",
     [40, 39, 38]),
    ("e", "SELECT COUNT(*) FROM t WHERE k = 1 AND flag = 1", [10]),
]


def setup():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    cursor.execute(
        "CREATE TABLE t (pk INT NOT NULL PRIMARY KEY, k INT NOT NULL, "
        "flag INT NOT NULL, v VARCHAR(32), KEY k_idx(k)) ENGINE=LineairDB"
    )
    values = []
    for pk in range(1, ROWS + 1):
        k = 1 if pk <= K_ROWS else 2
        flag = 1 if FLAG_LO <= pk <= FLAG_HI else 0
        values.append(f"({pk},{k},{flag},'v{pk}')")
    cursor.execute(f"INSERT INTO t VALUES {','.join(values)}")
    db.commit()
    # ANALYZE settles the optimizer's plan before the two paths are compared.
    cursor.execute("ANALYZE TABLE t")
    cursor.fetchall()
    db.commit()
    cursor.close()
    db.close()


def set_read_path(value):
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    cursor.execute(f"SET GLOBAL lineairdb_read_path='{value}'")
    cursor.close()
    db.close()


def run_path(path, explain):
    set_read_path(path)
    # A fresh session, so nothing from the previous path is carried over.
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    cursor.execute(f"USE {DBNAME}")
    cursor.execute("SELECT @@GLOBAL.lineairdb_read_path")
    active = cursor.fetchone()[0]
    print(f"\n=== read path: requested {path}, global reports {active}")

    results = {}
    for name, sql, _ in QUERIES:
        if explain:
            cursor.execute(f"EXPLAIN FORMAT=TREE {sql}")
            tree = "\n".join(str(row[0]) for row in cursor.fetchall())
            print(f"\n--- ({name}) {sql}\n{tree}")
        cursor.execute(sql)
        results[name] = [row[0] for row in cursor.fetchall()]
    cursor.close()
    db.close()
    return results


def report(plan, row):
    failed = 0
    print("\n=== results (query | plan | row | expected)")
    for name, sql, expected in QUERIES:
        ok = plan[name] == row[name] == expected
        failed |= 0 if ok else 1
        print(f"({name}) {'OK  ' if ok else 'FAIL'} plan={plan[name]} "
              f"row={row[name]} expected={expected}")
    return failed


def main():
    setup()
    plan = run_path("plan", explain=True)
    row = run_path("row", explain=False)
    result = report(plan, row)
    # Leave the stack on the default path for whoever runs next.
    set_read_path("plan")

    if result == 0:
        print("\nALL TESTS PASSED!")
    else:
        print("\nSOME TESTS FAILED!")
    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Connect to MySQL")
    parser.add_argument("--user", type=str, default="root")
    parser.add_argument("--password", type=str, default="")
    args = parser.parse_args()
    main()
