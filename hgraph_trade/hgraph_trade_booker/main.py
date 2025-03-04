"""
main.py

This script acts as the entry point for processing and booking trades from the command line.
It:
1. Parses command-line arguments for input file, output directory, and output file name.
2. Loads and validates the trade data from a file.
3. Maps the trade data to a model appropriate for downstream systems.
4. Books the trade by saving it as a JSON file in the specified output location.

Exit codes:
- 0: Success
- 1: Errors related to file not found, invalid data, or unexpected exceptions.
"""

import sys
import argparse
import logging

from trade_loader import load_trade_from_file
from trade_mapper import map_trade_to_model
from trade_booker import book_trade

from hgraph_trade.logging_config import setup_logging  # Ensure logging_config is also updated if moved


def main():
    """
    Main function to handle trade booking from the command line.
    Parses arguments, configures logging, loads and validates trade data,
    maps it to the correct model, and books the trade.
    """
    parser = argparse.ArgumentParser(
        description="Process and book a trade using specified input and output paths."
    )

    # Command-line arguments
    parser.add_argument(
        "--input_file",
        type=str,
        required=True,
        help="Path to the input trade file (e.g., JSON or TXT)."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Directory where the output file will be saved."
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default="booked_trade.json",
        help="Name of the output file (default: booked_trade.json)."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debugging output."
    )

    args = parser.parse_args()

    # Initialize logging using the centralized setup
    setup_logging()

    # Adjust the logging level if verbose mode is enabled
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        logging.info("Loading trade data from file: %s", args.input_file)
        trade_data = load_trade_from_file(args.input_file)
        logging.debug("Loaded trade data: %s", trade_data)

        # Check for essential fields (removed 'sub_instrument')
        required_keys = {"instrument", "tradeType"}
        missing_keys = required_keys - trade_data.keys()
        if missing_keys:
            raise ValueError(f"Missing required keys in trade data: {missing_keys}")

        logging.debug("Detected instrument: %s", trade_data.get("instrument", ""))

        logging.info("Mapping trade data to model.")
        trade_model = map_trade_to_model(trade_data)

        logging.info("Booking trade and saving to file: %s", args.output_file)
        book_trade(trade_model, args.output_file, output_dir=args.output_dir)

        logging.info("Trade processing and booking completed successfully.")
        sys.exit(0)

    except FileNotFoundError as e:
        logging.error("File not found: %s", e)
        sys.exit(1)
    except ValueError as e:
        if "Unsupported instrument type" in str(e) or "Unsupported sub-instrument type" in str(e):
            logging.error("Instrument or sub-instrument type error: %s", e)
        else:
            logging.error("Invalid trade data: %s", e)
        sys.exit(1)
    except Exception as e:
        logging.exception("Unexpected error encountered.")
        sys.exit(1)


if __name__ == "__main__":
    main()