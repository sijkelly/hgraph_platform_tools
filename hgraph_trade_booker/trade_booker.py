"""
trade_booker.py

This module provides functionality to finalize (or "book") a trade by writing the
trade data to a specified output directory. The resulting file can then be used
by downstream systems for further processing or confirmation.
"""

import os
import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_instrument_type


def book_trade(trade_data: Dict[str, Any], output_file: str, output_dir: str) -> None:
    """
    Book the trade by saving it to a specified output directory in JSON format.

    Booking a trade typically means finalizing it in a downstream system. Here, this
    step involves writing the trade data to a file so that it can be picked up by
    other systems or processes for further action.

    :param trade_data: The fully mapped and validated trade data dictionary.
    :param output_file: The name of the output file (e.g., "booked_trade.json").
    :param output_dir: The directory where the file will be saved. Created if it doesn't exist.
    :raises IOError: If an error occurs while writing to the specified output path.
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_file)

    try:
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(trade_data, file, indent=4)
    except IOError as e:
        # In a production environment, consider logging or handling the error more gracefully.
        raise IOError(f"Error writing to {output_path}: {e}") from e