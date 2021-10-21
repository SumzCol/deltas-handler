# -*- coding: utf-8 -*-
import logging
from typing import Tuple
import logging as log
from Aldebaran_Learning.common.catalog_extractor import catalog_extractor
import pytest

# import kedro

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
# Case 1: The function should retrieve the following information on the source_cdm_usuarios
#
# def ldx(x):
#   return lambda x: x
#
# json = catalog_extractor("source_cdm_usuarios", ldx)
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
# -------------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------------

log.basicConfig(
    level=log.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="./delta_handler_test.log",
    filemode="w",
)


def compare_dictionaries(
    expected_dictionary: dict = {}, given_dictionary: dict = {}, path=""
) -> Tuple[bool, dict]:
    """
    Returns a boolean that ensures the dictionaries have the same keys and values

    Args:
        expected_dictionary (dict) A dictionary
        given_dictionary (dict) A dictionary
        path (str) The key-value path (for recursion only), baseCase = ""

    Returns:
        (Tuple) [True, []] if the dictionaries have the same keys and values\
        (Tuple) [False, [differences]] if the dictionaries have differences

    """
    differences = []
    result = []

    assert expected_dictionary, "dictionary 1 don't exist, passed %s instead".format(
        expected_dictionary
    )
    assert given_dictionary, "dictionary 1 don't exist, passed %s instead".format(
        given_dictionary
    )

    for key in expected_dictionary:
        if key in given_dictionary:
            if type(expected_dictionary[key]) is dict:
                compare_dictionaries(
                    expected_dictionary[key],
                    given_dictionary[key],
                    "%s -> %s" % (path, key) if path else key,
                )
            if expected_dictionary[key] != given_dictionary[key]:
                differences.append(
                    [
                        "%s: " % path,
                        " - %s : %s" % (key, expected_dictionary[key]),
                        " + %s : %s" % (key, given_dictionary[key]),
                    ]
                )
        else:
            differences.append(
                ["%s%s as key not in d2\n" % ("%s: " % path if path else "", key)]
            )
    if len(differences) == 0:
        result[0] = True
        result[1] = []
    else:
        result[0] = False
        result[1] = differences
    return result


def case_1() -> Tuple[str, dict]:
    """
    Sets the assertion data for the catalog_extractor test

    """
    catalog_source = "source_cdm_usuarios"
    source_cdm_usuarios = {
        "type": "spark.SparkJDBCDataSet",
        "table": '(SELECT *, CAST(created_at AS date) AS "creation_date" FROM profiles) AS profiles',
        "credentials": "decameron_spark",
        "url": "jdbc:postgresql://cocnaodcd001s.decameron.com:5432/cdm",
        "load_args": {
            "properties": {
                "fetchSize": "1000",
                "driver": "org.postgresql.Driver",
                "partitionColumn": "creation_date",
                "lowerBound": "2021-09-01",
                "upperBound": "2021-10-01",
                "numPartitions": "200",
            }
        },
        "layer": "00_source",
    }
    return [catalog_source, source_cdm_usuarios]


class TestCatalogExtractor:
    #   @pytest.fixture(scope="module")

    def test_catalog_extractor(self):
        """
        Set the values and tests all the possibilities .
        """

        # Set case 1f
        case1_data = case_1()
        case1_input = case1_data[0]
        case1_expected_outputs = case1_data[1]
        case1_output = catalog_extractor(case1_input)
        dictionaries_comparison = compare_dictionaries(
            case1_expected_outputs, case1_output
        )

        assert dictionaries_comparison[
            0
        ], "the expected output %d differs from %d in %d".format(
            case1_expected_outputs, case1_output, dictionaries_comparison[1]
        )
