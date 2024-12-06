import os
import logging
from typing import Dict
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping

# Configure logging for config-related issues
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Base directory for the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directories for test trades and outputs
TEST_TRADES_DIR = os.path.join(BASE_DIR, "test_trades")
TEST_OUTPUTS_DIR = os.path.join(BASE_DIR, "test_trades_booking_outputs")

# Ensure directories exist
if not os.path.exists(TEST_TRADES_DIR):
    logging.warning(f"Test trades directory does not exist: {TEST_TRADES_DIR}")
if not os.path.exists(TEST_OUTPUTS_DIR):
    logging.warning(f"Test outputs directory does not exist: {TEST_OUTPUTS_DIR}")

# File paths
SAMPLE_TRADE_FILE = os.path.join(TEST_TRADES_DIR, "fixed_float_BM_swap_001.txt")

# Mappings
GLOBAL_MAPPING: Dict[str, str] = get_global_mapping()
INSTRUMENT_MAPPINGS: Dict[str, Dict[str, str]] = {
    instrument: get_instrument_mapping(instrument)
    for instrument in ["swap", "option", "forward", "future", "swaption", "fx", "physical"]
}

# Environment settings
ENVIRONMENT = os.getenv("APP_ENV", "development")  # Use "development" as default environment

# Example of additional environment-based settings
DEBUG_MODE = ENVIRONMENT == "development"

# Logging environment settings
logging.info(f"Running in {ENVIRONMENT} mode. Debug mode is {'on' if DEBUG_MODE else 'off'}.")

# Configuration dictionary for external usage
CONFIG = {
    "BASE_DIR": BASE_DIR,
    "TEST_TRADES_DIR": TEST_TRADES_DIR,
    "TEST_OUTPUTS_DIR": TEST_OUTPUTS_DIR,
    "SAMPLE_TRADE_FILE": SAMPLE_TRADE_FILE,
    "GLOBAL_MAPPING": GLOBAL_MAPPING,
    "INSTRUMENT_MAPPINGS": INSTRUMENT_MAPPINGS,
    "ENVIRONMENT": ENVIRONMENT,
    "DEBUG_MODE": DEBUG_MODE,
}

# Example usage of config
if __name__ == "__main__":
    print(f"Base Directory: {CONFIG['BASE_DIR']}")
    print(f"Sample Trade File: {CONFIG['SAMPLE_TRADE_FILE']}")
    print(f"Global Mappings: {CONFIG['GLOBAL_MAPPING']}")
    print(f"Instrument Mappings: {CONFIG['INSTRUMENT_MAPPINGS']}")