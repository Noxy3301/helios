"""DOUBLE literals and folded numeric bounds preserve InnoDB query results."""

import argparse
from decimal import Decimal

import mysql.connector

from utils.connection import get_connection


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', default='root')
    parser.add_argument('--password', default='')
    args = parser.parse_args()
    connection = get_connection(args.user, args.password)
    connection.autocommit = True
    cursor = connection.cursor()
    rows = [(i, Decimal(i) / 100, i % 4) for i in range(100)]
    rows.append((100, None, 0))
    try:
        cursor.execute('DROP DATABASE IF EXISTS ha_helios_double')
        cursor.execute('CREATE DATABASE ha_helios_double')
        cursor.execute('USE ha_helios_double')
        for table, engine in [('h', 'HELIOS'), ('i', 'InnoDB')]:
            cursor.execute(f'CREATE TABLE {table} '
                           '(id INT PRIMARY KEY, d DECIMAL(15,2), g INT) '
                           f'ENGINE={engine}')
            cursor.executemany(f'INSERT INTO {table} VALUES (%s,%s,%s)', rows)
        cursor.execute('ALTER TABLE h SECONDARY_ENGINE=HELIOS_DUCKDB')
        cursor.execute('ALTER TABLE h SECONDARY_LOAD')
        cursor.fetchall()
        queries = []
        for discount in range(2, 10):
            for op in ['BETWEEN', 'NOT BETWEEN']:
                queries.append('SELECT id FROM {t} WHERE d ' + op +
                               f" '0.{discount:02}' - 0.01 AND "
                               f"'0.{discount:02}' + 0.01 ORDER BY id")
        for op in ['=', '<>', '<', '<=', '>', '>=']:
            queries.append(f'SELECT id FROM {{t}} WHERE d {op} 6.0E-2 ORDER BY id')
        queries += [
            'SELECT id FROM {t} WHERE d BETWEEN 0.02 AND 6.0E-2 ORDER BY id',
            'SELECT id FROM {t} WHERE d BETWEEN 2.0E-2 AND 0.06 ORDER BY id',
            'SELECT id FROM {t} WHERE d * 2.0E-4 > 5.0E-5 ORDER BY id',
            'SELECT id FROM {t} WHERE 2.0E-4 * d > 5.0E-5 ORDER BY id',
            'SELECT id FROM {t} WHERE d + 2.0E-2 < 0.09 ORDER BY id',
            'SELECT id FROM {t} WHERE d - 2.0E-2 < 0.09 ORDER BY id',
            'SELECT id FROM {t} WHERE d / 2.0E-2 < 3 ORDER BY id',
            'SELECT id FROM {t} WHERE (d * 2.0E-4) IS NULL ORDER BY id',
            'SELECT id FROM {t} WHERE d * 1.0E0 IN (2.0E-2,6.0E-2) ORDER BY id',
            'SELECT id, d * 2.0E-1 FROM {t} ORDER BY id',
            'SELECT id, d % 0E0 FROM {t} ORDER BY id',
        ]
        # Refused by the request builder: ON runs them on the primary engine.
        primary = [
            'SELECT g, SUM(d * 1.0E-1) FROM {t} GROUP BY g ORDER BY g',
            'SELECT g, SUM(d) FROM {t} GROUP BY g HAVING SUM(d) > '
            '(SELECT SUM(d) * 2.0E-1 FROM {t}) ORDER BY g',
            'SELECT id FROM {t} WHERE d > '
            '(SELECT AVG(d * 2.0E-1) FROM {t}) ORDER BY id',
        ]
        for query, mode in ([(q, 'FORCED') for q in queries] +
                            [(q, 'ON') for q in primary]):
            cursor.execute('SET SESSION use_secondary_engine=OFF')
            cursor.execute(query.format(t='i'))
            expected = cursor.fetchall()
            cursor.execute(f'SET SESSION use_secondary_engine={mode}')
            cursor.execute(query.format(t='h'))
            actual = cursor.fetchall()
            assert actual == expected, (query, actual, expected)
        # The request builder refuses these, which FORCED turns into an error.
        cursor.execute('SET SESSION use_secondary_engine=FORCED')
        for query in primary:
            try:
                cursor.execute(query.format(t='h'))
                cursor.fetchall()
            except mysql.connector.Error:
                pass
            else:
                raise AssertionError(f'offloaded: {query}')
        for engine, table in [('OFF', 'i'), ('FORCED', 'h')]:
            cursor.execute(f'SET SESSION use_secondary_engine={engine}')
            try:
                cursor.execute(f'SELECT id FROM {table} WHERE '
                               '(d * 1.0E308) * 1.0E308 > 0')
                cursor.fetchall()
            except mysql.connector.Error as error:
                assert 'out of range' in error.msg.lower(), error
            else:
                raise AssertionError(f'{engine}: overflow succeeded')
        print(f'{len(queries) + len(primary)} InnoDB comparisons and '
              'overflow checks passed')
    finally:
        cursor.execute('SET SESSION use_secondary_engine=OFF')
        cursor.execute('DROP DATABASE IF EXISTS ha_helios_double')
        cursor.close()
        connection.close()


if __name__ == '__main__':
    main()
