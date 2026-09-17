import mysql.connector

def reset (db, cursor) :
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('CREATE TABLE ha_lineairdb_test.items (\
        title VARCHAR(50) NOT NULL,\
        content VARCHAR(255),\
        content2 VARCHAR(255),\
        content3 VARCHAR(255),\
        content4 VARCHAR(255),\
        content5 VARCHAR(255),\
        content6 VARCHAR(255),\
        content7 VARCHAR(255),\
        content8 VARCHAR(255),\
        content9 VARCHAR(255),\
        INDEX title_idx (title)\
    )ENGINE = LineairDB')
    db.commit()