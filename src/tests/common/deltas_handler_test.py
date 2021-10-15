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
