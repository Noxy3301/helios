"""A transaction whose storage channel closed aborts at COMMIT.

The optimizer-statistics RPC does not end the transaction when its exchange
fails, so a transaction can reach a restarted storage server over a new channel
and carry reads from the previous run into its commit. That commit must fail as
a lost race, and a retry of the same transaction must commit. The case runs on
hidden_primary_key's private stack, which it restarts by pid.
"""
import argparse
import shutil
import sys
import tempfile

import mysql.connector

import hidden_primary_key as hpk

DATABASE = "ha_helios_test"


def read_v(cursor, table):
    cursor.execute(f"SELECT v FROM {DATABASE}.{table} WHERE pk = 1")
    return cursor.fetchall()[0][0]


def copy_to_b(cursor, value):
    """Reads b, the first read of a fresh table share, which fetches the
    optimizer statistics before the row, then writes value + 1 to b and
    commits."""
    read_v(cursor, "b")
    cursor.execute(f"UPDATE {DATABASE}.b SET v = {value + 1} WHERE pk = 1")
    cursor.execute("COMMIT")


def test_commit_after_storage_restart(user, password):
    print("TRANSACTION ACROSS A STORAGE SERVER RESTART TEST")
    port = hpk.RESTART_MYSQLD_PORTS[0]

    if hpk.port_is_open(hpk.RESTART_SERVER_PORT):
        print(f"\tFailed: port {hpk.RESTART_SERVER_PORT} is in use; set "
              "HELIOS_TEST_SERVER_PORT to a free one")
        return 1

    work_dir = tempfile.mkdtemp(prefix="tx_restart_")
    server = None
    started = False
    connection = None
    try:
        # Recovery on, so the restarted server holds the rows read before it
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1
        started = True
        if not hpk.start_mysqld(port, hpk.RESTART_SERVER_PORT):
            return 1

        connection = hpk.connection_on(port, user, password)
        connection.autocommit = True
        cursor = connection.cursor()
        hpk.reset_schema(cursor)
        for table in ("a", "b"):
            cursor.execute(f"CREATE TABLE {DATABASE}.{table} "
                           "(pk INT PRIMARY KEY, v INT) ENGINE = Helios")
            cursor.execute(f"INSERT INTO {DATABASE}.{table} VALUES (1, 10)")

        # Drops the table shares, so the first read of each table fetches
        # statistics
        cursor.execute("FLUSH TABLES")
        cursor.execute("BEGIN")
        value = read_v(cursor, "a")
        hpk.stop_storage_server(server)
        server = hpk.start_storage_server(work_dir)
        if server is None:
            return 1
        try:
            copy_to_b(cursor, value)
            print("\tFailed: COMMIT succeeded after the channel closed")
            return 1
        except mysql.connector.Error as err:
            if err.errno != 1180 or "Got error 149" not in err.msg:
                print(f"\tFailed: expected 1180 wrapping 149, got {err.errno}: "
                      f"{err.msg}")
                return 1
            print(f"\tCOMMIT failed with {err.errno}: {err.msg}")
        if read_v(cursor, "b") != 10:
            print("\tFailed: the aborted transaction wrote b")
            return 1

        cursor.execute("FLUSH TABLES")
        cursor.execute("BEGIN")
        copy_to_b(cursor, read_v(cursor, "a"))
        if read_v(cursor, "b") != 11:
            print("\tFailed: the retry did not write b")
            return 1

        print("\tPassed!")
        return 0
    finally:
        if connection is not None:
            try:
                connection.close()
            except mysql.connector.Error:
                pass
        if started and not hpk.stop_mysqld(port):
            print(f"\tWarning: the instance on port {port} is still running")
        hpk.stop_storage_server(server)
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str, default="root")
    parser.add_argument('--password', metavar='pw', type=str, default="")
    args = parser.parse_args()
    sys.exit(test_commit_after_storage_restart(args.user, args.password))
