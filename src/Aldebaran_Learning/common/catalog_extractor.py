# -*- coding: utf-8 -*-
def catalog_extractor(catalog_identifyer: str) -> dict:
    """
    Returns a dictionary with the catalog identifyer building materials.

    Args:
        (str) catalog_identifyer: the name that is given at the catalog

    Returns:
        (Dict) A dictionary with all de building materials of the catalog
    """
    _dict = {"catalog_identifyer": catalog_identifyer}
    return _dict
