#!/usr/bin/env python3
"""
trade_loader.py

This module handles the loading and basic validation of trade data from a file.
It uses regex-based checks before parsing JSON to ensure the presence of required keys,
as well as post-parsing validations (e.g., checking instrument types, date formats).
The validated trade data is then ready for further processing or mapping.
"""

import os
import json
import re
import logging
from typing import Dict, Any

from hgraph_trade_model.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_instrument_type,
    map_sub_instrument_type
)

def validate_required_keys(file_content: str, required_keys: Dict[str, Any]) -> None:
    """
    Validate that all required top-level keys are present in the file content.

    :param file_content: Raw file content as a string.
    :param required_keys: A list or dict of required keys as strings.
    :raises ValueError: If any required key is missing.
    """
    for key in required_keys:
        if not re.search(rf'"{key}"\s*:', file_content):
            raise ValueError(f"Missing required key: {key}")

def validate_nested_fields(file_content: str, nested_keys: Dict[str, list]) -> None:
    """
    Validate that certain parent keys contain the specified nested child keys.

    :param file_content: Raw file content as a string.
    :param nested_keys: Dictionary where keys are parent fields and values are lists of required children.
    :raises ValueError: If any required nested key is missing.
    """
    for parent, children in nested_keys.items():
        for child in children:
            pattern = rf'"{parent}"\s*:\s*{{[^}}]*"{child}"\s*:'
            if not re.search(pattern, file_content):
                raise ValueError(f"Missing nested key: {parent}.{child}")

def validate_trade_file_with_regex(file_content: str) -> None:
    """
    Validate the trade file's structure using regex before JSON parsing.

    Ensures required keys and nested structures are present in the raw file content.

    :param file_content: Content of the trade file as a string.
    :raises ValueError: If validation fails (e.g., missing keys).
    """
    required_keys = ["tradeType", "instrument", "sub_instrument", "trade_id", "counterparty", "portfolio", "traders"]
    nested_keys = {
        "traders": ["internal", "external"],
        "counterparty": ["internal", "external"],
        "portfolio": ["internal", "external"]
    }

    validate_required_keys(file_content, required_keys)
    validate_nested_fields(file_content, nested_keys)

def validate_instrument_types(trade_data: Dict[str, Any]) -> None:
    """
    Validate that the instrument and sub-instrument are supported according to known mappings.

    :param trade_data: Parsed trade data dictionary.
    :raises ValueError: If the instrument or sub-instrument is not supported.
    """
    raw_instrument = trade_data.get("instrument", "UnknownInstrument")
    raw_sub_instrument = trade_data.get("sub_instrument", "UnknownSubInstrument")

    instrument_type = map_instrument_type(raw_instrument)
    sub_instrument_type = map_sub_instrument_type(raw_sub_instrument)

    logging.debug(f"Instrument type: {instrument_type}, Sub-instrument type: {sub_instrument_type}")

    if instrument_type == "UnknownInstrument":
        raise ValueError(f"Unsupported instrument type: {raw_instrument}")
    if sub_instrument_type == "UnknownSubInstrument":
        raise ValueError(f"Unsupported sub-instrument type: {raw_sub_instrument}")

def validate_field_with_regex(field_value: str, pattern: str, field_name: str) -> None:
    """
    Validate a specific field's value against a regex pattern.

    :param field_value: The value of the field to validate.
    :param pattern: The regex pattern the field must match.
    :param field_name: The name of the field for error reporting.
    :raises ValueError: If the field does not match the pattern.
    """
    if not re.match(pattern, field_value):
        raise ValueError(f"Invalid format for {field_name}: {field_value}")

def additional_validations(trade_data: Dict[str, Any]) -> None:
    """
    Perform additional regex validations after JSON parsing.

    For example, ensure date formats are correct.

    :param trade_data: Parsed trade data dictionary.
    :raises ValueError: If any post-parse validation fails.
    """
    if "trade_date" in trade_data:
        validate_field_with_regex(
            field_value=str(trade_data["trade_date"]),
            pattern=r'^\d{4}-\d{2}-\d{2}$',
            field_name="trade_date"
        )

def load_trade_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load trade data from a local JSON file, performing regex validation and field checks.

    :param file_path: Path to the trade file.
    :return: Parsed and validated trade data as a dictionary.
    :raises FileNotFoundError: If the file does not exist.
    :raises ValueError: If validation fails or JSON is invalid.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Trade file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as file:
        file_content = file.read()

    # Validate structure before parsing JSON
    validate_trade_file_with_regex(file_content)

    try:
        trade_data = json.loads(file_content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in file: {file_path}") from e

    # Validate instrument types and other fields
    validate_instrument_types(trade_data)
    additional_validations(trade_data)

    return trade_data

def fetch_trade_from_hgraph(trade_id: str) -> Dict[str, Any]:
    """
    Placeholder function to fetch trade data from the hgraph database.

    :param trade_id: ID of the trade to fetch.
    :return: Trade data from the hgraph database (currently an empty dictionary).
    """
    # Implement actual database fetch logic here if required
    return {}

# Example usage (for demonstration or testing)
if __name__ == "__main__":
    file_path = "../test_trades/fixed_float_BM_swap_001.txt"
    try:
        trade_data = load_trade_from_file(file_path)
        logging.info(json.dumps(trade_data, indent=4))  # Using logging instead of print
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Error: {e}")