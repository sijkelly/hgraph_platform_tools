#!/usr/bin/env python3
"""
parse_trade.py

This script reads a JSON file representing trade data and returns a dictionary
of key-value pairs for further processing (e.g., rendering Jinja2 templates).

Usage:
    python parse_trade.py --file /path/to/fixed_float_BM_swap_001.txt
"""

import argparse
import json
from typing import Dict, Any


def parse_trade_data(file_path: str) -> Dict[str, Any]:
    """
    Parse a JSON trade file and extract key-value pairs into a dictionary.

    Args:
        file_path (str): Full path to the trade data file (JSON format).

    Returns:
        Dict[str, Any]: A dictionary containing parsed trade details.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        trade_data = json.load(file)
    return trade_data


def main() -> None:
    """
    Main function that sets up command-line argument parsing
    and prints out the trade data dictionary for testing.
    """
    parser = argparse.ArgumentParser(description="Parse a JSON trade file.")
    parser.add_argument("--file", required=True, help="Path to the trade file.")
    args = parser.parse_args()

    trade_info = parse_trade_data(args.file)
    print("Parsed Trade Data:")
    for key, value in trade_info.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()