# -*- coding: utf-8 -*-
import logging as log
import os

import kedro.framework.context as kedrocontexttools
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline, node

from Aldebaran_Learning.common.persist import persist


def get_catalog(path: str) -> DataCatalog:
    """
    Returns the context catalog of the project.

    Args:
        path: Path to the root of the project.

    Returns:
        (kedrocontexttools.Catalog) The catalog of the project.
    """
    context = kedrocontexttools.load_context(path)
    return context.catalog


catalog = get_catalog(os.getcwd())


def create_nodes_list(identify: str, catalog=catalog) -> list:
    """
    Creates a list with the replication nodes for all the sources in the catalog
    that have the same identify string

    Args:
        (str) identify: The identify string of the sources in the catalog
        (kedrocontexttools.Catalog) catalog: The catalog of the project

    Returns:
        (list) A list with the replication nodes
    """
    list_nodes = []
    for table_name in catalog.list():
        if "blob" in table_name and identify in table_name:
            blob = table_name
            source = table_name.replace("blob", "source")
            node_name = "persist_" + identify + "_" + table_name
            nd = node(
                func=persist,
                inputs=source,
                outputs=[blob],
                name=node_name,
                tags=["persistence", identify],
            )
            list_nodes.append(nd)
    return list_nodes


def create_pipeline() -> Pipeline:
    """
    Wraps the replication nodes in a pipeline.

    Returns:
        (kedro.pipeline.Pipeline) The pipeline with the replication nodes.
    """
    hodeline_web = create_nodes_list("hw")
    cdm = create_nodes_list("cdm")
    nodes_list = hodeline_web + cdm

    log.debug("Creating pipeline with %s", nodes_list)
    log.info("Creating pipeline with %s nodes", len(nodes_list))
    return Pipeline(nodes_list)
