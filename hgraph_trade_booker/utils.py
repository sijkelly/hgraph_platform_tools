"""
utils.py

This module provides utility functions for file operations, directory management,
and applying global mappings. These utilities are intended to support the process
of preparing and saving trade data for downstream systems.
"""

import os
import json
from typing import Dict, Any, Optional
from hgraph_trade_model.fpml_mappings import get_global_mapping, map_instrument_type


def ensure_directory_exists(directory: str) -> None:
    """
    Ensure that the specified directory exists, creating it if necessary.

    :param directory: The path to the directory to ensure exists.
    :raises RuntimeError: If the directory cannot be created.
    """
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError as e:
        raise RuntimeError(f"Failed to ensure directory exists: {directory}") from e


def save_trade_to_file(trade_data: Dict[str, Any], output_file: str, output_dir: str) -> None:
    """
    Save trade data to a JSON file in the specified output directory.

    The function ensures that the output directory exists before writing the file.

    :param trade_data: The trade data dictionary to save as JSON.
    :param output_file: The name of the output JSON file (e.g., "booked_trade.json").
    :param output_dir: The directory where the file will be saved.
    :raises RuntimeError: If there is an error writing to the file.
    """
    ensure_directory_exists(output_dir)
    output_path = os.path.join(output_dir, output_file)

    try:
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(trade_data, file, indent=4)
        # If you prefer to log or silently succeed, omit or adjust the below line.
        # For production, consider using logging.debug or logging.info:
        # logging.info(f"Trade data successfully saved to {output_path}")
    except IOError as e:
        raise RuntimeError(f"Failed to write to file: {output_path}") from e


def apply_global_mappings(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply global mappings to the provided trade data.

    Global mappings can modify or enrich the trade data keys to align with a standard format.

    :param trade_data: The original trade data dictionary.
    :return: A new dictionary with global mappings applied.
    """
    global_mapping = get_global_mapping()
    return {**trade_data, **global_mapping}


def map_trade_type(hgraph_type: str) -> Optional[str]:
    """
    Map the trade type from an hgraph-defined type to an FpML-compatible type.

    :param hgraph_type: The hgraph trade type string.
    :return: The corresponding FpML trade type, or None if no mapping exists.
    """
    return map_instrument_type(hgraph_type)