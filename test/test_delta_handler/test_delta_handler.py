# -*- coding: utf-8 -*-
print(
    """
#-------------------------------------------------------------------------
# Author: Juan José Albán, Sumz
# github: elcubonegro, sumzcol
# CD: 2021-14-10
# LUD: 2021-09-08
# Description: testing for delta_retrieval() function
# Test 1: simple connection
# Test 2: simple new rows retrieval
# Test 3: simple updated rows retrieval
# Test 4: simple deleted rows retrieval
# Test 5: no dated rows retrieval
# Test 6: no dated updates retrieval
# Test 7: concatenation in log_table

# Use: console execute `pytest test_delta_handler.py` 
#     modify delta_handler function so it generates a database table 
#     with the delta (i.e. inserts, updates, or deletes) of a table queried 
#     in two moments.
#     packages needed
#         faker
#         pytest
#         pytest-cov
#         sqlite3
#         datetime
#         fdb
#     Needed servers (for testing)
#         firebird (local)
#         postgresql (local)
#         mysql (local)
# 
#     USER MUST HAVE WRITING AND DATABASE CREATION PERMISSIONS, OTHERWISE IT WILL NOT WORK,
#     the secrets will remain hardcoded as is intended to be used in a local environment
#     
#     Example
# 
#     Table 1 in moment 1
#     client_id | client_name | value
#         1     |   john      |  100
#         2     |   paul      |  200
#
#
#     Table 1 in moment 2
#     client_id | client_name | value
#         2     |   paul      |  250
#         3     |   mike      |  300
#
#
#     delta_handler should identify and create the following table
#     
#     Table 1 in moment 2
#     client_id | client_name | value    | type_of_crude
#         1     |   john      |  100     |  delete
#         2     |   paul      |  200     |  update
#         3     |   mike      |  300     |  insert
#
# v1
# Modification:
# Description: first published version, waiting to stablish contracts
#-------------------------------------------------------------------------
# 
"""
)

# imports

from fdb.fbcore import Connection, Cursor
from faker import Faker
import datetime
import logging as log
import pandas as pd

from delta_handler import delta_handler

import fdb as firebird
import mysql.connector as mysql_connector
import psycopg2 as postgresql

# logging level

log.basicConfig(level=log.DEBUG, filename="logs/test_delta_handler.log", filemode="w")

# Environment creation

TEST1_TABLE_NAME = "test_table"

FIREBIRD_DATABASE_HOST = "localhost"
FIREBIRD_DATABASE_USER = "SYSDBA"
FIREBIRD_DATABASE_PASSWORD = 'ConTErcardya"'
FIREBIRD_DATABASE_PATH = "test_firebird.fdb"

MYSQL_DATABASE_HOST = "localhost"
MYSQL_DATABASE_USER = "root"
MYSQL_DATABASE_PASSWORD = 'ConTErcardya"'
MYSQL_DATABASE_PATH = "test_mysql"

POSTGRES_DATABASE_HOST = "localhost"
POSTGRES_DATABASE_USER = "SYSDBA"
POSTGRES_DATABASE_PASSWORD = 'ConTErcardya"'
POSTGRES_DATABASE_PATH = "test_postgres"


# 1.1.1 Firebird environment creation
def firebird_database_connection() -> Connection:
    """
    Function to create a connection to a firebird database in case it exists
    Args:
        None
    Returns:
        cursor: cursor to the database
    """
    connection = firebird.connect(
        host=FIREBIRD_DATABASE_HOST,
        database=FIREBIRD_DATABASE_PATH,
        user=FIREBIRD_DATABASE_USER,
        password=FIREBIRD_DATABASE_PASSWORD,
        connection_class=firebird.ConnectionWithSchema,
    )
    log.info("connected to firebird database")
    return connection


firebird_connection = firebird_database_connection()

# 1.1.2 MySQL environment creation
def connect_mysql_database() -> Connection:
    """Returns a cursor with the mysql connection to the test_mysql database
    Args:
        None
    Returns:
        cursor: cursor to the database
    """
    connection = mysql_connector.connect(
        host=MYSQL_DATABASE_HOST,
        user=MYSQL_DATABASE_USER,
        passwd=MYSQL_DATABASE_PASSWORD,
        database=MYSQL_DATABASE_PATH,
    )
    log.info("connected to mysql database")
    return connection


mysql_connection = connect_mysql_database()

# 1.1.3 PostgreSQL environment creation
def postgress_connection(with_db_name=True) -> Connection:
    """
    Function to create a connection to a postgresql database in case it exists
    Args:
        None
    Returns:
        cursor: cursor to the database
    """
    log.info("connecting to database at localhost:'/var/lib/pgsql/data")
    connection_parameters = {
        "user": POSTGRES_DATABASE_USER,
        "password": POSTGRES_DATABASE_PASSWORD,
        "host": POSTGRES_DATABASE_HOST,
    }
    if with_db_name:
        connection_parameters["database"] = POSTGRES_DATABASE_PATH
    connection = postgresql.connect(**connection_parameters)
    log.info("connected to postgresql database")
    return connection


postgresql_connection = postgress_connection()

# 1.2 Clean environments
log.info("cleaning environments")

try:
    firebird_connection.drop_database()
except Exception as e:
    log.exception(e)

try:
    mysql_connection.drop_database()
except Exception as e:
    log.exception(e)
try:
    postgresql_connection = postgress_connection()
    postgresql_connection.autocommit = 1
    postgresql_cursor = postgresql_connection.cursor()
    postgresql_cursor.execute(
        "SELECT pg_terminate_backend (PID) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{}'".format(
            POSTGRES_DATABASE_PATH
        )
    )

    postgresql_connection = postgress_connection()
    postgresql_connection.autocommit = 1
    postgresql_cursor = postgresql_connection.cursor()
    postgresql_cursor.execute(
        "DROP DATABASE IF EXISTS {}".format(POSTGRES_DATABASE_PATH)
    )
    postgresql_connection.close()
except Exception as e:
    log.exception(e)

firebird_connection = firebird.create_database(
    "create database 'localhost:{path}' user '{user}' password '{password}'".format(
        path=FIREBIRD_DATABASE_PATH,
        user=FIREBIRD_DATABASE_USER,
        password=FIREBIRD_DATABASE_PASSWORD,
    )
)
firebird_cursor = firebird_connection.cursor()

mysql_connection = mysql_connector.connect(
    host=MYSQL_DATABASE_HOST,
    user=MYSQL_DATABASE_USER,
    passwd=MYSQL_DATABASE_PASSWORD,
)
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute("DROP DATABASE IF EXISTS {}".format(MYSQL_DATABASE_PATH))
mysql_cursor.execute("CREATE DATABASE {}".format(MYSQL_DATABASE_PATH))
mysql_cursor.execute("USE {}".format(MYSQL_DATABASE_PATH))

# 1.4 Create test databases
log.info("creating test tables")

firebird_cursor.execute(
    """
    create table {0} (
        id integer,
        city varchar(20),
        state varchar(20),
        real_value float,
        visit_date date
    )""".format(
        TEST1_TABLE_NAME
    )
)

mysql_cursor.execute(
    """
    create table {0} (
        id integer,
        city varchar(20),
        state varchar(20),
        real_value float,
        visit_date date
    )""".format(
        TEST1_TABLE_NAME
    )
)

postgresql_connection = postgress_connection()
postgresql_cursor = postgresql_connection.cursor()
postgresql_cursor.execute(
    """
    create table {0} (
        id integer,
        city varchar(20),
        state varchar(20),
        real_value float,
        visit_date date
    )""".format(
        TEST1_TABLE_NAME
    )
)

# 1.5 Charge dummy data

dummy_data = [
    (1, "Atlanta", "Georgia", 10.00, "2020-01-01"),
    (2, "Boston", "Massachusetts", 20.00, "2020-01-02"),
    (3, "Chicago", "Illinois", 30.00, "2020-01-03"),
    (4, "Dallas", "Texas", 40.00, "2020-01-04"),
    (5, "Denver", "Colorado", 50.00, "2020-01-05"),
    (6, "Houston", "Texas", 60.00, "2020-01-06"),
    (7, "Los Angeles", "California", 70.00, "2020-01-07"),
    (8, "Miami", "Florida", 80.00, "2020-01-08"),
    (9, "New York", "New York", 90.00, "2020-01-09"),
    (10, "Seattle", "Washington", 100.00, "2020-01-10"),
    (11, "Washington", "District of Columbia", 110.00, "2020-01-11"),
    (12, "Wichita", "Kansas", 120.00, "2020-01-12"),
    (13, "Chicago", "Illinois", 130.00, "2020-01-13"),
    (14, "Boston", "Massachusetts", 140.00, "2020-01-14"),
    (15, "Los Angeles", "California", 150.00, "2020-01-15"),
    (16, "New York", "New York", 160.00, "2020-01-16"),
    (17, "Seattle", "Washington", 170.00, "2020-01-17"),
    (18, "Washington", "District of Columbia", 180.00, "2020-01-18"),
    (19, "Wichita", "Kansas", 190.00, "2020-01-19"),
    (20, "Chicago", "Illinois", 200.00, "2020-01-20"),
    (21, "Boston", "Massachusetts", 210.00, "2020-01-21"),
    (22, "Los Angeles", "California", 220.00, "2020-01-22"),
]

firebird_cursor.executemany(
    "INSERT INTO {} (id, city, state, real_value, visit_date) VALUES(?, ?, ?, ?, ?) returning id".format(
        TEST1_TABLE_NAME
    ),
    dummy_data,
)

# firebird_checking_query = """
#    SELECT *
#    FROM RDB$RELATIONS a
#    WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0 AND RDB$RELATION_TYPE = 0
#    """

# response = firebird_cursor.execute(firebird_checking_query)
# print(response.fetchall())

# postgresql_fixture_query = """
#    CREATE TABLE
# """
# mysql_fixture_query = """  """
