import argparse
import sys

from utils.connection import get_connection


ROW_COUNT = 2505  # crosses the 1024-row cursor boundary twice


def test_index_cursor(db, cursor):
    cursor.execute("DROP DATABASE IF EXISTS ha_lineairdb_test")
    cursor.execute("CREATE DATABASE ha_lineairdb_test")
    cursor.execute(
        """
        CREATE TABLE ha_lineairdb_test.index_cursor (
            pk INT NOT NULL PRIMARY KEY,
            sk INT NOT NULL,
            payload VARCHAR(20) NOT NULL,
            KEY idx_sk (sk)
        ) ENGINE=LineairDB
        """
    )

    rows = [(i, i // 3, f"v{i}") for i in range(ROW_COUNT)]
    cursor.executemany(
        "INSERT INTO ha_lineairdb_test.index_cursor VALUES (%s, %s, %s)",
        rows,
    )
    db.commit()

    cursor.execute(
        "SELECT MIN(pk), MAX(pk) FROM ha_lineairdb_test.index_cursor"
    )
    if cursor.fetchone() != (0, ROW_COUNT - 1):
        print("MIN/MAX did not return the index endpoints")
        return 1

    cursor.execute("SELECT MAX(sk) FROM ha_lineairdb_test.index_cursor")
    if cursor.fetchone() != ((ROW_COUNT - 1) // 3,):
        print("secondary-index MAX did not return the index tail")
        return 1

    cursor.execute("SELECT MIN(sk) FROM ha_lineairdb_test.index_cursor")
    if cursor.fetchone() != (0,):
        print("secondary-index MIN did not return the index head")
        return 1

    cursor.execute(
        "SELECT pk FROM ha_lineairdb_test.index_cursor "
        "FORCE INDEX(PRIMARY) ORDER BY pk"
    )
    ascending = [row[0] for row in cursor.fetchall()]
    if ascending != list(range(ROW_COUNT)):
        print(f"forward primary cursor mismatch: {len(ascending)} rows")
        return 1

    cursor.execute(
        "SELECT pk FROM ha_lineairdb_test.index_cursor "
        "FORCE INDEX(PRIMARY) ORDER BY pk DESC"
    )
    descending = [row[0] for row in cursor.fetchall()]
    if descending != list(range(ROW_COUNT - 1, -1, -1)):
        print(f"reverse primary cursor mismatch: {len(descending)} rows")
        return 1

    cursor.execute(
        "SELECT sk FROM ha_lineairdb_test.index_cursor "
        "FORCE INDEX(idx_sk) ORDER BY sk DESC"
    )
    secondary = [row[0] for row in cursor.fetchall()]
    expected_secondary = sorted((i // 3 for i in range(ROW_COUNT)), reverse=True)
    if secondary != expected_secondary:
        print(f"reverse secondary cursor mismatch: {len(secondary)} rows")
        return 1

    print("INDEX CURSOR TEST PASSED")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    try:
        sys.exit(test_index_cursor(db, cursor))
    finally:
        cursor.close()
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LineairDB index cursor test")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()
    main()
