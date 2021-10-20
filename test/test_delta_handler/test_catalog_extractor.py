# -*- coding: utf-8 -*-
print(
    """
#-------------------------------------------------------------------------
# Author: Juan José Albán, Sumz
# github: elcubonegro, sumzcol
# CD: 2021-10-20
# LUD: 2021-10-20
# Description: testing for catalog_extractor() function
# Test 1: The method retrieves all the data from a catalog object
# Test 2: The method executes a function that transforms the object in a expected way.
# Test 3: If the object doesn't have the resources, it will return a ParameterNotFound exception pointing the missing 
# parameter on the transformation function.

# Use: console execute `pytest test_catalog_extractor.py` 
#     Given a catalog entry name it retrieves the catalog entry information
#     packages needed
#         faker
#         pytest
#         pytest-cov
#         kedro
# 
# Example:
# json = catalog_extractor(source_cdm_usuarios)
# print(json)
# >> {
#   "type": "spark.SparkJDBCDataSet",
#   "table": "(SELECT *, CAST(created_at AS date) AS "creation_date" FROM profiles) AS profiles",
#   "credentials": "decameron_spark",
#   "url": "jdbc:postgresql://cocnaodcd001s.decameron.com:5432/cdm",
#   "load_args": {
#       "properties":{
#           "fetchSize": "1000",
#           "driver": "org.postgresql.Driver",
#           "partitionColumn": "creation_date",
#           "lowerBound": "2021-09-01",
#           "upperBound": "2021-10-01",
#           "numPartitions": "200",
#       }
#   },
#   "layer": "00_source"
# }
# v1
# Modification:
# Description: first published version, waiting to establish contracts
#-------------------------------------------------------------------------
# 
"""
)

import logging as log
import kedro
from Aldebaran_Learning.common import catalog_extractor

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

class test_catalog_extractor:
