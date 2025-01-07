"""
hgraph_trade_booker package initialization.

This package provides functionality to convert executed trades and filled orders
from upstream systems into a bookable trade message (e.g., FpML format) for
downstream trade processing applications.
"""

from .message_wrapper import (
    create_message_header,
    create_message_footer,
    wrap_message_with_headers_and_footers,
)
from hgraph_trade.hgraph_trade_mapping import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
    map_pricing_instrument
)
from .decomposition import decompose_instrument  # Add this line if you want it publicly available

__all__ = [
    "create_message_header",
    "create_message_footer",
    "wrap_message_with_headers_and_footers",
    "get_global_mapping",
    "get_instrument_mapping",
    "map_hgraph_to_fpml",
    "map_pricing_instrument",
    "decompose_instrument"
]