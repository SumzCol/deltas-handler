# -*- coding: utf-8 -*-
"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline

import Aldebaran_Learning.pipelines.first_extraction_pipeline as first_extraction


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """
    first_extraction_pipeline = first_extraction.create_pipeline()

    return {
        "de": first_extraction_pipeline,
        "__default__": first_extraction_pipeline,
    }
