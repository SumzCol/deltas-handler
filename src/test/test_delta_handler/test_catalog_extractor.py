# -*- coding: utf-8 -*-
import logging
from typing import Tuple
import logging as log
from Aldebaran_Learning.common.catalog_extractor import catalog_extractor
import pytest


from pathlib import Path
import pytest
from kedro.framework.context import KedroContext

# import kedro

print(
    """
#-------------------------------------------------------------------------
# Author: Juan José Albán, Sumz
# github: elcubonegro, sumzcol
# CD: 2021-10-20
# LUD: 2021-10-20
# Description: testing for catalog_extractor() function and helper functions
# Test 1: The method compare_dictionaries returns [True, []] when a dict is
#   compared to itself
# Test 2: The method compare_dictionaries returns [False, differences_dictionary]
#   with a expected difference when a dict is compared to a modified version

# Test 2: The method executes a function that transforms the object in a expected way.
# Test 3: If the object doesn't have the resources, it will return a ParameterNotFound
    exception pointing the missing
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


@pytest.fixture
def project_context():
    return KedroContext(package_name="Aldebaran_Learning", project_path=Path.cwd())


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
    result = [True, []]

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


def case_1() -> Tuple[Tuple[dict, dict], Tuple[True, dict]]:
    """
    Sets the helper function to compare a dictionary with itself

    Returns
        [[dict1, dict1], [True, []]]]) a data arrangement with the information to the test

    """

    test_dictionary = {
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

    return [test_dictionary, test_dictionary, [True, []]]


def case_2() -> Tuple[Tuple[dict, dict], Tuple[False, dict]]:
    """
    Sets the second case assertion values.

    Sets the assertion data for the compare_dictionaries test when there are differences between the
    given dictionaries, it returns a list with the two dictionaries [dictA, dictB] that were compared
    and a tuple with [False, dictA diff dictB]

    Returns
        (Tuple[Tuple[dict, dict], Tuple[bool, dict]]) a data arrangement with the information to the test
    """
    test_dictionary_1 = {
        "name": "testname",
        "properties": {
            "propertie1": "1",
            "propertie2": "2",
            "propertie_delete_this_one": "3",
            "propertie_delete_first_child": {
                "propertie_delete_first_child_1": "4",
                "propertie_keepthis": "5",
            },
        },
    }

    test_dictionary_2 = {
        "name": "testname",
        "properties": {
            "propertie1": "1",
            "propertie2": "2",
            "propertie4_addedthisone": "4",
            "propertie_delete_first_child": {"propertie_keepthis": "5"},
        },
    }

    differences = {
        "properties": {
            "propertie_delete_this_one": "3",
            "propertie4_addedthisone": "4",
            "propertie_delete_first_child": {
                "propertie_delete_first_child_1": "4",
            },
        }
    }

    return [[test_dictionary_1, test_dictionary_2], [False, differences]]


def case_3() -> Tuple[str, dict]:
    """
    Sets the third case test values.
    Sets the assertion data for the catalog_extractor test with a simple retrieval of information

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
    def test_catalog_extractor(self):
        """
        Set the values and tests all the possibilities .
        """

        # Set case 2
        case3_data = case_3()
        case3_input = case3_data[0]
        case3_expected_outputs = case3_data[1]
        case3_output = catalog_extractor(case3_input)
        dictionaries_comparison = compare_dictionaries(
            case3_expected_outputs, case3_output
        )

        assert not dictionaries_comparison[
            0
        ], "the expected output %d differs from %d in %d".format(
            case3_expected_outputs, case3_output, dictionaries_comparison[1]
        )
