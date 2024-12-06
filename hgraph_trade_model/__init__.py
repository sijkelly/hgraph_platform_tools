"""
This package provides components and utilities to construct trades in an FpML-like
format from raw hgraph-sourced trade data. Each module focuses on a specific aspect
of the trade (e.g., swaps, options, forwards, headers, footers) and can create
fully structured sections that are combined into a complete trade message.

Modules within this package:
- swap.py: Create commodity swap data structures.
- option.py: Create commodity option data structures.
- forward.py: Create commodity forward data structures.
- future.py: Create commodity future data structures.
- physical.py: Create commodity physical trade data structures.
- swaption.py: Create commodity swaption data structures.
- fx.py: Create foreign exchange (FX) trade data structures.
- trade_header.py: Create the trade header section.
- trade_footer.py: Create the trade footer section.
- fpml_mappings.py: Provide mappings and utility functions to translate hgraph fields
  to FpML fields.
"""

# Import individual model components
from .swap import create_commodity_swap
from .option import create_commodity_option
from .forward import create_commodity_forward
from .future import create_commodity_future
from .physical import create_commodity_physical
from .swaption import create_commodity_swaption
from .fx import create_fx_trade
from .trade_header import create_trade_header
from .trade_footer import create_trade_footer
from .fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_instrument_type,
    map_hgraph_to_fpml,
    map_sub_instrument_type
)

# Define all exports for this module
__all__ = [
    # Commodity-related creators
    "create_commodity_swap",
    "create_commodity_option",
    "create_commodity_forward",
    "create_commodity_future",
    "create_commodity_physical",
    "create_commodity_swaption",
    "create_fx_trade",

    # Trade structure components
    "create_trade_header",
    "create_trade_footer",

    # Mappings
    "get_global_mapping",
    "get_instrument_mapping",
    "map_instrument_type",
    "map_hgraph_to_fpml",
    "map_sub_instrument_type",
]