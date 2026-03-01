"""
config.py

DEPRECATED — This module was the original entry point for trade booking.
It has been superseded by main.py, which includes batch processing,
structured error reporting, and dead-letter quarantine.

This file is kept only for backwards compatibility. New code should
use main.py instead.
"""

import warnings

warnings.warn(
    "config.py is deprecated. Use main.py as the pipeline entry point.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export main for backwards compatibility
from hgraph_trade.hgraph_trade_booker.main import main  # noqa: F401
