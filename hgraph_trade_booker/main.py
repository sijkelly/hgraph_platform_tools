from trade_loader import load_trade_from_file
from trade_mapper import map_trade_to_model
from trade_booker import book_trade

# Paths for testing
TRADE_INPUT_PATH = "/Users/simonkelly/Documents/GitHub/hgraph_platform_tools/test_trades/fixed_float_BM_swap_001.txt"
TRADE_OUTPUT_FILENAME = "fixed_float_BM_swap_001_bookable.json"  # Save as JSON for clarity
TRADE_OUTPUT_PATH = "/Users/simonkelly/Documents/GitHub/hgraph_platform_tools/test_trades_booking_outputs"

if __name__ == "__main__":
    # Load trade from the specified local file
    trade_data = load_trade_from_file(TRADE_INPUT_PATH)

    # Map trade to the appropriate model
    trade_model = map_trade_to_model(trade_data)

    # Book the trade and save it to the output path
    book_trade(trade_model, TRADE_OUTPUT_FILENAME)