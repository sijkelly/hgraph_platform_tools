"""
This package provides components and utilities to construct trades in an FpML-like
format from raw hgraph-sourced trade data. Each module focuses on a specific aspect
of the trade.

Modules:
- swap.py
- option.py
- forward.py
- future.py
- physical.py
- swaption.py
- fx.py
- trade_header.py
- trade_footer.py
"""

from .swap import create_commodity_swap
from .option import create_commodity_option
from .forward import create_commodity_forward
from .future import create_commodity_future
from .physical import create_commodity_physical
from .swaption import create_commodity_swaption
from .fx import create_fx_trade
from .trade_header import create_trade_header
from .trade_footer import create_trade_footer
from hgraph_trade.hgraph_trade_mapping import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
    map_pricing_instrument
)

__all__ = [
    "create_commodity_swap",
    "create_commodity_option",
    "create_commodity_forward",
    "create_commodity_future",
    "create_commodity_physical",
    "create_commodity_swaption",
    "create_fx_trade",
    "create_trade_header",
    "create_trade_footer",
    "get_global_mapping",
    "get_instrument_mapping",
    "map_hgraph_to_fpml",
    "map_pricing_instrument",
]