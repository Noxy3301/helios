import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


DBNAME = f"ha_helios_string_key_collation_{int(time.time())}"

AI_ROWS = ("(id, v) VALUES (1,'abc'),(2,'ABC'),(3,'Abc'),(4,'ábc'),(5,'abd'),"
           "(6,'ABD'),(7,'b'),(8,'ab'),(9,'abc '),(10,'aBc'),(11,'')")

# Each case: twin DDL, rows, statements. A result must match InnoDB's: rows as
# a multiset, or in order after "=" (ORDER BY, by WEIGHT_STRING or HEX); DML by
# affected rows or errno. After "!", both only have to fail.
CASES = [
    ("ai_ci", "id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, "
     "n INT NOT NULL DEFAULT 0, KEY iv (v)", AI_ROWS, [
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v = 'abc'",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v IN ('ABC', 'b', 'abc ')",
         "SELECT WEIGHT_STRING(v), COUNT(*) FROM {t} GROUP BY v",
         "SELECT WEIGHT_STRING(v), COUNT(*) FROM {t} FORCE INDEX(iv) "
         "GROUP BY v",
         "SELECT COUNT(DISTINCT v) FROM {t} FORCE INDEX(iv)",
         "=SELECT WEIGHT_STRING(v) FROM {t} FORCE INDEX(iv) ORDER BY v",
         "=SELECT WEIGHT_STRING(v) FROM {t} FORCE INDEX(iv) ORDER BY v DESC",
         "SELECT WEIGHT_STRING(MIN(v)), WEIGHT_STRING(MAX(v)) FROM {t}",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v > 'abc' AND v <= 'b'",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v LIKE 'ab%'",
         "=SELECT WEIGHT_STRING(v) FROM {t} FORCE INDEX(iv) "
         "WHERE v LIKE 'AB%' ORDER BY v DESC",
         # Search keys whose weights exceed the storage key limit.
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v = REPEAT('ﷺ', 19)",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v > REPEAT('ﷺ', 19)",
         "=SELECT WEIGHT_STRING(v) FROM {t} FORCE INDEX(iv) "
         "WHERE v < REPEAT('ﷺ', 19) ORDER BY v DESC",
         "UPDATE {t} SET n = n + 1 WHERE v = 'abc'",
         "DELETE FROM {t} WHERE v = 'ABD'",
         "SELECT id, n FROM {t}",
     ]),
    ("pk", "v VARCHAR(20) PRIMARY KEY, n INT", "VALUES ('abc',1)", [
        "INSERT INTO {t} VALUES ('ABC', 2)",
        "INSERT INTO {t} VALUES ('ábc', 3)",
        "INSERT INTO {t} VALUES ('abc ', 4)",
        "SELECT n FROM {t} WHERE v = 'Abc'",
        "UPDATE {t} SET v = 'ABC' WHERE v = 'abc'",
        "=SELECT HEX(v), n FROM {t} ORDER BY v",
    ]),
    ("char_pk", "v CHAR(10) PRIMARY KEY", "VALUES ('abc')", [
        "INSERT INTO {t} VALUES ('ABC ')",
        "SELECT COUNT(*) FROM {t} WHERE v = 'aBC'",
    ]),
    ("unique", "id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, UNIQUE KEY uv (v)",
     "VALUES (1,'abc'),(3,'abd')", [
         "INSERT INTO {t} VALUES (2, 'ABC')",
         "UPDATE {t} SET v = 'ABD' WHERE id = 1",
         "UPDATE {t} SET v = 'ABC' WHERE id = 1",
         "SELECT id, HEX(v) FROM {t} WHERE v = 'abc'",
     ]),
    ("bin", "id INT PRIMARY KEY, v VARCHAR(20) COLLATE utf8mb4_0900_bin "
     "NOT NULL, KEY iv (v)",
     "VALUES (1,'abc'),(2,'ABC'),(3,'ábc'),(4,'ab'),(5,'abc '),(6,'b')", [
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v = 'abc'",
         "=SELECT HEX(v) FROM {t} FORCE INDEX(iv) ORDER BY v",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v LIKE 'ab%'",
     ]),
    ("bin_pk", "v VARCHAR(20) COLLATE utf8mb4_0900_bin PRIMARY KEY",
     "VALUES ('abc')", [
         "INSERT INTO {t} VALUES ('ABC')",
         "INSERT INTO {t} VALUES ('abc')",
         "=SELECT HEX(v) FROM {t} ORDER BY v",
     ]),
    ("varbinary", "id INT PRIMARY KEY, v VARBINARY(20) NOT NULL, KEY iv (v)",
     "VALUES (1,X'61'),(2,X'6100'),(3,X'610062'),(4,X'41'),(5,X''),(6,X'00'),"
     "(7,X'0000'),(8,X'6101')", [
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v = X'6100'",
         "=SELECT HEX(v) FROM {t} FORCE INDEX(iv) ORDER BY v",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v > X'61' AND v < X'6101'",
     ]),
    ("varbinary_pk", "v VARBINARY(20) PRIMARY KEY",
     "VALUES (X'61'),(X'6100')", [
         "INSERT INTO {t} VALUES (X'610000')",
         "INSERT INTO {t} VALUES (X'6100')",
         "=SELECT HEX(v) FROM {t} ORDER BY v",
     ]),
    ("pad_space", "id INT PRIMARY KEY, "
     "v VARCHAR(20) COLLATE utf8mb4_general_ci NOT NULL, KEY iv (v)",
     "VALUES (1,'abc'),(2,'ABC'),(3,'abc '),(4,'abc\\t'),(5,'abd'),"
     "(6,'ab')", [
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v = 'abc'",
         "=SELECT WEIGHT_STRING(v) FROM {t} FORCE INDEX(iv) ORDER BY v",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v < 'abc'",
     ]),
    ("pad_space_pk", "v VARCHAR(20) COLLATE utf8mb4_general_ci PRIMARY KEY",
     "VALUES ('abc')", [
         "INSERT INTO {t} VALUES ('ABC  ')",
         "INSERT INTO {t} VALUES ('abc\\t')",
     ]),
    ("composite", "a INT, v VARCHAR(10), b INT, PRIMARY KEY (a, v, b)",
     "VALUES (1,'abc',1),(1,'ABC',2),(1,'abd',1),(1,'ab',3),(2,'abc',1),"
     "(1,'Abc',0)", [
         "SELECT b FROM {t} WHERE a = 1 AND v = 'abc'",
         "SELECT b FROM {t} WHERE a = 1 AND v = 'abc' AND b >= 1",
         "=SELECT a, WEIGHT_STRING(v), b FROM {t} ORDER BY a, v, b",
         "SELECT b FROM {t} WHERE a = 1 AND v > 'abc'",
         "INSERT INTO {t} VALUES (1, 'aBc', 1)",
     ]),
    ("prefix", "id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, KEY iv (v(3))",
     "VALUES (1,'abcdef'),(2,'ABCxyz'),(3,'abd'),(4,'ab'),(5,'ÁBCdef'),"
     "(6,'abcDEF')", [
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v = 'abcdef'",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v LIKE 'abc%'",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v > 'abc'",
     ]),
    ("prefix_unique", "id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, "
     "UNIQUE KEY uv (v(3))", "VALUES (1,'abcX')", [
         "INSERT INTO {t} VALUES (2, 'ABCy')",
         "INSERT INTO {t} VALUES (3, 'abdX')",
     ]),
    ("backfill", "id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, "
     "n INT NOT NULL DEFAULT 0", AI_ROWS, [
         "CREATE INDEX iv ON {t} (v)",
         "SELECT id FROM {t} FORCE INDEX(iv) WHERE v = 'abc'",
         "=SELECT WEIGHT_STRING(v) FROM {t} FORCE INDEX(iv) ORDER BY v",
         "!ALTER TABLE {t} ADD UNIQUE KEY uv (v)",
     ]),
]

# U+FDFA weighs 16 bytes, so 20 of them exceed the 255-byte key limit.
LONG = "REPEAT('\uFDFA', 20)"
LONG_KEY_CASES = [
    ("long_sk", "id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, KEY iv (v)",
     f"INSERT INTO {{t}} VALUES (2, {LONG})"),
    ("long_update", "id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, KEY iv (v)",
     f"UPDATE {{t}} SET v = {LONG} WHERE id = 1"),
    ("long_pk", "id INT, v VARCHAR(20), PRIMARY KEY (v, id)",
     f"INSERT INTO {{t}} VALUES (2, {LONG})"),
]


def run(cursor, sql):
    """('rows', rows) or ('rowcount', n) on success, ('error', errno) else."""
    try:
        cursor.execute(sql)
        if cursor.with_rows:
            return "rows", [tuple(bytes(c) if isinstance(c, bytearray) else c
                                  for c in r) for r in cursor.fetchall()]
        return "rowcount", cursor.rowcount
    except mysql.connector.Error as e:
        return "error", e.errno


def twins(cursor, name, ddl, rows):
    for engine in ("Helios", "InnoDB"):
        t = f"{name}_{engine.lower()}"
        cursor.execute(f"CREATE TABLE {t} ({ddl}) ENGINE={engine}")
        if rows:
            cursor.execute(f"INSERT INTO {t} {rows}")


def test_cases(cursor):
    print("STRING KEYS: Helios against InnoDB twins")
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
                fails_only = stmt.startswith("!")
                stmt = stmt.lstrip("=!")
                got = run(cursor, stmt.format(t=f"{name}_helios"))
                want = run(cursor, stmt.format(t=f"{name}_innodb"))
                if fails_only:
                    ok = got[0] == want[0] == "error"
                elif got[0] == want[0] == "rows" and not ordered:
                    ok = sorted(got[1], key=repr) == sorted(want[1], key=repr)
                else:
                    ok = got == want
                total += 1
                if not ok:
                    failed += 1
                    print(f"\tFAILED [{path}] {name}: {stmt}\n"
                          f"\t\thelios {got}\n\t\tinnodb {want}")
    print(f"\t{total - failed}/{total} statements match")
    return 1 if failed else 0


def test_long_keys(cursor):
    print("STRING KEYS: a key above the limit fails")
    failed = 0
    for name, ddl, stmt in LONG_KEY_CASES:
        twins(cursor, name, ddl, "VALUES (1, 'a')")
        got = run(cursor, stmt.format(t=f"{name}_helios"))
        want = run(cursor, stmt.format(t=f"{name}_innodb"))
        rows = run(cursor, f"SELECT id, v FROM {name}_helios")
        if got != ("error", 1235) or want != ("rowcount", 1) or rows != (
                "rows", [(1, "a")]):
            failed += 1
            print(f"\tFAILED {name}: helios {got} rows {rows}, innodb {want}")
    if failed:
        return 1
    print("\tPassed!")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    db.autocommit = True
    cursor = db.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cursor.execute(f"CREATE DATABASE {DBNAME}")
    cursor.execute(f"USE {DBNAME}")
    rc = 0
    try:
        rc |= test_cases(cursor)
        rc |= test_long_keys(cursor)
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
