# -*- coding: utf-8 -*-
from typing import Tuple

from pandas import DataFrame


def persist(*args: DataFrame) -> Tuple[DataFrame, ...]:
    """
    Return every dataframe in args as a tuple
    Args:
        *args: DataFrame
    Returns:
        Tuple that cointains every DataFrame in args
    """
    return tuple(args)
