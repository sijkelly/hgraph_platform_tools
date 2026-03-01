"""
fpml_xsd_reference_files package.

Contains FpML 5.12 XSD schema files and tooling to parse them into a
structured JSON dictionary.

Usage:
    from hgraph_trade.fpml_xsd_reference_files import get_fpml_tags

    tags = get_fpml_tags()            # lazy-loaded from fpml_tags.json
    bullion = tags["bullionPhysicalLeg"]

If fpml_tags.json does not exist, run the parser first:
    python -m hgraph_trade.fpml_xsd_reference_files.fpml_XSD_parser
"""

import json
import os
from typing import Any, Dict, Optional

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_JSON_PATH = os.path.join(_THIS_DIR, "fpml_tags.json")
_CACHE: Optional[Dict[str, Any]] = None


def get_fpml_tags() -> Dict[str, Any]:
    """
    Lazy-load and return the FpML tags dictionary from the generated JSON file.

    The result is cached after the first call so subsequent calls are instant.

    :return: Dictionary mapping FpML element names to their parsed structure.
    :raises FileNotFoundError: If fpml_tags.json has not been generated yet.
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    if not os.path.exists(_JSON_PATH):
        raise FileNotFoundError(
            f"FpML tags JSON not found at {_JSON_PATH}. "
            f"Run the parser first:\n"
            f"  python -m hgraph_trade.fpml_xsd_reference_files.fpml_XSD_parser"
        )

    with open(_JSON_PATH, "r", encoding="utf-8") as fh:
        _CACHE = json.load(fh)

    return _CACHE
