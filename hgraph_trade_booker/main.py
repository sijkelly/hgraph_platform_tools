import sys
import os
import argparse

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

    # Parse arguments
    args = parser.parse_args()

    # Process the trade
    trade_data = load_trade_from_file(args.input_file)
    trade_model = map_trade_to_model(trade_data)
    book_trade(trade_model, args.output_file, output_dir=args.output_dir)


if __name__ == "__main__":
    main()