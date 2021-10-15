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

# -------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------

from fdb.fbcore import Connection, Cursor
import sqlite3 as sq3
import fdb as firebird
import psycopg2 as pg
from faker import Faker
import logging as log
import pandas as pd
import os
import sys
import inspect

from kedro_devops.common.deltas_handler import DeltaHandler
from kedro.config import ConfigLoader, MissingConfigException

# -------------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------------

log.basicConfig(
    level=log.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="delta_handler_test.log",
    filemode="w",
)

# -------------------------------------------------------------------------
# Constants and parameters
# -------------------------------------------------------------------------

conf_paths = ["../conf/base", "../conf/local"]
conf_loader = ConfigLoader(conf_paths)
parameters = conf_loader.get("parameters*", "parameters*/**")
catalog = conf_loader.get("catalog*", "catalog*/**")
logging = conf_loader.get("logging*", "logging*/**")
credentials = conf_loader.get("credentials*", "credentials*/**")

print("credentials: ", credentials)

TEST1_TABLE_NAME = parameters.test_environment.TEST1_TABLE_NAME

FIREBIRD_TEST_DATABASE_HOST = parameters.firebird.DATABASE_HOST
FIREBIRD_TEST_DATABASE_USER = credentials.firebird.DATABASE_USER
FIREBIRD_TEST_DATABASE_PASSWORD = credentials.firebird.DATABASE_PASSWORD
FIREBIRD_TEST_DATABASE_PATH = parameters.firebird.DATABASE_PATH

MYSQL_TEST_DATABASE_HOST = parameters.mysql.DATABASE_HOST
MYSQL_TEST_DATABASE_USER = credentials.mysql.DATABASE_USER
MYSQL_TEST_DATABASE_PASSWORD = credentials.mysql.DATABASE_PASSWORD
MYSQL_TEST_DATABASE_PATH = parameters.mysql.DATABASE_PATH

POSTGRES_TEST_DATABASE_HOST = parameters.postgres.DATABASE_HOST
POSTGRES_TEST_DATABASE_USER = credentials.postgres.DATABASE_USER
POSTGRES_TEST_DATABASE_PASSWORD = credentials.postgres.DATABASE_PASSWORD
POSTGRES_TEST_DATABASE_PATH = parameters.postgres.DATABASE_PATH

# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------
