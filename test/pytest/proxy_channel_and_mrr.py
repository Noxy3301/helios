"""RPC channel recovery and rowid reads of batched MRR rows.

The restart case needs its own stack, so it borrows hidden_primary_key's
private-stack helpers instead of stop_server.sh, which kills by command-line
substring. The MRR case runs on the caller's stack.
"""
import argparse
import sys
import tempfile

import mysql.connector

import hidden_primary_key as hpk
from utils.connection import get_connection

DATABASE = "ha_helios_test"
MRR_ROWS = 50
DELETE_ROWS = 5
WIDE_COLUMN = 512  # the utf8mb4 width at the PAX cell limit


def channel_table(cursor, table):
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
    cursor.execute(f"CREATE DATABASE {DATABASE}")
    cursor.execute(
        f"""CREATE TABLE {DATABASE}.{table} (
            pk INT PRIMARY KEY,
            c VARCHAR(64) NOT NULL
        ) ENGINE = Helios""")


def test_channel_reopens_after_storage_restart(user, password):
    """A transport error must not leave the channel unusable.

    The failed exchange can have consumed part of a response, so the socket is
    closed; the next transaction on the same MySQL connection has to reconnect.
    """
    print("PROXY CHANNEL: STORAGE SERVER RESTART TEST")
    port = hpk.RESTART_MYSQLD_PORTS[0]

    if hpk.port_is_open(hpk.RESTART_SERVER_PORT):
        print(f"\tFailed: port {hpk.RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="proxy_channel_")
    server = None
    started = False
    connection = None
    try:
        # Recovery on, so the rows written before the restart are still there
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1
        started = True
        if not hpk.start_mysqld(port, hpk.RESTART_SERVER_PORT):
            return 1

        table = hpk.unique_table("channel_restart")
        connection = hpk.connection_on(port, user, password)
        connection.autocommit = True
        cursor = connection.cursor()
        channel_table(cursor, table)
        for pk in range(3):
            cursor.execute(
                f"INSERT INTO {DATABASE}.{table} VALUES ({pk}, 'before')")
        cursor.execute(f"SELECT pk FROM {DATABASE}.{table} ORDER BY pk")
        if [row[0] for row in cursor.fetchall()] != [0, 1, 2]:
            print("\tFailed: the rows written before the restart are missing")
            return 1

        hpk.stop_storage_server(server)
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1

        # The statement that meets the dead socket. It may also survive, when
        # the first RPC to fail is one whose caller does not end the statement.
        seen = None
        try:
            cursor.execute(f"SELECT pk FROM {DATABASE}.{table}")
            cursor.fetchall()
        except mysql.connector.Error as err:
            seen = err.errno
            print(f"\tThe statement after the restart failed with {err.errno}: "
                  f"{err.msg}")
        if seen is not None and seen not in (2013, 1030, 1296):
            print(f"\tFailed: unexpected error {seen} after the restart")
            return 1

        # Strict part: the next transaction on the same connection must work.
        cursor.execute(f"SELECT pk, c FROM {DATABASE}.{table} ORDER BY pk")
        rows = cursor.fetchall()
        if rows != [(0, 'before'), (1, 'before'), (2, 'before')]:
            print(f"\tFailed: the transaction after the reconnect read {rows}")
            return 1
        cursor.execute(f"INSERT INTO {DATABASE}.{table} VALUES (3, 'after')")
        cursor.execute(f"SELECT c FROM {DATABASE}.{table} WHERE pk = 3")
        if cursor.fetchall() != [('after',)]:
            print("\tFailed: the write after the reconnect is not readable")
            return 1

        print("\tPassed!")
        return 0
    finally:
        if connection is not None:
            connection.close()
        if started:
            hpk.stop_mysqld(port)
        hpk.stop_storage_server(server)


def test_batch_mrr_rowid_sort(user, password):
    """Rows delivered from the MRR batch must be addressable by rowid.

    position() stores the last fetched primary key, and rnd_pos() reads it
    back. A plain ORDER BY keeps the row in the sort buffer and never asks, so
    the DELETE below is what forces the rowid sort (sql_delete.cc passes
    force_sort_rowids).
    """
    print("BATCH MRR: ROWID SORT TEST")
    setup = get_connection(user, password)
    setup.autocommit = True
    setup_cursor = setup.cursor()
    setup_cursor.execute("SET GLOBAL helios_read_path = 'row'")
    setup_cursor.close()
    setup.close()

    # A new session keeps the restart scenario apart from the setup connection.
    connection = get_connection(user, password)
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        table = hpk.unique_table("mrr_rowid")
        cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
        cursor.execute(f"CREATE DATABASE {DATABASE}")
        cursor.execute(
            f"""CREATE TABLE {DATABASE}.{table} (
                pk INT PRIMARY KEY,
                c VARCHAR({WIDE_COLUMN}) NOT NULL
            ) ENGINE = Helios""")

        # c descends while pk ascends, so ORDER BY c reverses the batch order
        payload = {pk: f"{MRR_ROWS - pk:04d}-" + "x" * 64
                   for pk in range(MRR_ROWS)}
        for pk, value in payload.items():
            cursor.execute(
                f"INSERT INTO {DATABASE}.{table} VALUES ({pk}, '{value}')")

        wanted = list(range(0, MRR_ROWS, 2))
        keys = ",".join(str(pk) for pk in wanted)
        query = (f"SELECT pk, c FROM {DATABASE}.{table} "
                 f"WHERE pk IN ({keys}) ORDER BY c")

        cursor.execute(f"EXPLAIN {query}")
        plan = cursor.fetchall()
        print(f"\tEXPLAIN: {plan[0][-1]}")
        if "MRR" not in plan[0][-1]:
            print("\tFailed: the batch MRR path is not taken")
            return 1

        cursor.execute(query)
        rows = cursor.fetchall()
        expected = sorted(((pk, payload[pk]) for pk in wanted),
                          key=lambda row: row[1])
        if rows != expected:
            print(f"\tFailed: {len(rows)} rows, expected {len(expected)}")
            if rows:
                print(f"\tfirst pk {rows[0][0]}, expected {expected[0][0]}")
            return 1

        # The rowid sort: the ordered DELETE re-reads each row through
        # rnd_pos(), and an unset ref makes every one of them miss.
        cursor.execute(f"DELETE FROM {DATABASE}.{table} "
                       f"WHERE pk IN ({keys}) ORDER BY c LIMIT {DELETE_ROWS}")
        if cursor.rowcount != DELETE_ROWS:
            print(f"\tFailed: the ordered DELETE removed {cursor.rowcount} rows")
            return 1

        # Smallest c is largest pk, so the top DELETE_ROWS even keys go
        gone = set(wanted[-DELETE_ROWS:])
        cursor.execute(f"SELECT pk FROM {DATABASE}.{table} ORDER BY pk")
        left = [row[0] for row in cursor.fetchall()]
        if left != [pk for pk in range(MRR_ROWS) if pk not in gone]:
            print(f"\tFailed: the ordered DELETE left {left}")
            return 1

        print("\tPassed!")
        return 0
    finally:
        cursor.execute("SET GLOBAL helios_read_path = 'plan'")
        cursor.close()
        connection.close()


def main():
    result = 0
    result |= test_batch_mrr_rowid_sort(args.user, args.password)
    result |= test_channel_reopens_after_storage_restart(args.user,
                                                         args.password)
    if result == 0:
        print("\nALL TESTS PASSED!")
    else:
        print("\nSOME TESTS FAILED!")
    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str, default="root")
    parser.add_argument('--password', metavar='pw', type=str, default="")
    args = parser.parse_args()
    main()
