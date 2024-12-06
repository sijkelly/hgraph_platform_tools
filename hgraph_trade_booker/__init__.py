"""
hgraph_trade_booker package initialization.

This package provides functionality to convert executed trades and filled orders
from upstream systems into a bookable trade message (e.g., in FpML format) for
downstream trade processing applications.
"""

from .message_wrapper import (
    create_message_header,
    create_message_footer,
    wrap_message_with_headers_and_footers,
)
from hgraph_trade_model.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_instrument_type,
    map_sub_instrument_type,
    map_hgraph_to_fpml,
)

__all__ = [
    # Message wrapper functions
    "create_message_header",
    "create_message_footer",
    "wrap_message_with_headers_and_footers",

    # FpML mappings
    "get_global_mapping",
    "get_instrument_mapping",
    "map_instrument_type",
    "map_sub_instrument_type",
    "map_hgraph_to_fpml",
]