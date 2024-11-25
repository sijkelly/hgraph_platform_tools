import os

# Base directory for the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directories for test trades and outputs
TEST_TRADES_DIR = os.path.join(BASE_DIR, "test_trades")
TEST_OUTPUTS_DIR = os.path.join(BASE_DIR, "test_trades_booking_outputs")

# File paths
SAMPLE_TRADE_FILE = os.path.join(TEST_TRADES_DIR, "fixed_float_BM_swap_001.txt")