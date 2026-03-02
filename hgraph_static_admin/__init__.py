"""
hgraph_static_admin package initialization.

This package provides static reference data management for the hgraph platform.
It handles fetching, processing, and storing counterparty data and other
static reference information through a fetch-process-store pipeline.

Modules:
- example_code.py: Core static data management including database operations,
  API fetching, data processing, and CLI interface.
"""

from .example_code import (
    init_db,
    fetch_static_data,
    process_counterparty_data,
    store_counterparty_data,
    run_pipeline,
)

__all__ = (
    "init_db",
    "fetch_static_data",
    "process_counterparty_data",
    "store_counterparty_data",
    "run_pipeline",
)
