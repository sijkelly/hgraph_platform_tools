import sys
import os
import argparse
import logging
from trade_loader import load_trade_from_file
from trade_mapper import map_trade_to_model
from trade_booker import book_trade


def main():
    """
    Main function to handle trade booking from the command line.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Process and book a trade using specified input and output paths."
    )

    # Define command-line arguments
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

    # Parse arguments
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        # Load the trade data
        logging.info("Loading trade data from file: %s", args.input_file)
        trade_data = load_trade_from_file(args.input_file)
        logging.debug("Loaded trade data: %s", trade_data)

        # Ensure required fields are present
        required_keys = {"instrument", "sub_instrument", "tradeType"}
        missing_keys = required_keys - trade_data.keys()
        if missing_keys:
            raise ValueError(f"Missing required keys in trade data: {missing_keys}")

        # Log detected instrument and sub-instrument
        logging.debug("Detected instrument: %s", trade_data.get("instrument", ""))
        logging.debug("Detected sub-instrument: %s", trade_data.get("sub_instrument", ""))

        # Map trade data to model
        logging.info("Mapping trade data to model.")
        trade_model = map_trade_to_model(trade_data)

        # Book the trade
        logging.info("Booking trade and saving to file: %s", args.output_file)
        book_trade(trade_model, args.output_file, output_dir=args.output_dir)

        logging.info("Trade processing and booking completed successfully.")

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
        logging.error("Unexpected error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()