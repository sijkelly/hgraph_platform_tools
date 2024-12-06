import os
import json
import re
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_instrument_type,
    map_sub_instrument_type
)

# Debugging print statement after imports
print(f"DEBUG: map_sub_instrument_type in trade_loader: {map_sub_instrument_type}")

def validate_trade_file_with_regex(file_content: str) -> None:
    """
    Validate the trade file's content using regex to ensure it follows the expected structure.

    :param file_content: Content of the trade file as a string.
    :raises ValueError: If the validation fails.
    """
    # Example: Ensure the file contains essential keys like "tradeType", "instrument", and "sub_instrument"
    required_keys = ["tradeType", "instrument", "sub_instrument", "trade_id", "counterparty", "portfolio", "traders"]

    for key in required_keys:
        if not re.search(rf'\"{key}\"[ ]*:', file_content):
            raise ValueError(f"Missing required key: {key}")

    # Check for nested fields like `internal` and `external` in `traders`, `counterparty`, and `portfolio`
    nested_keys = {
        "traders": ["internal", "external"],
        "counterparty": ["internal", "external"],
        "portfolio": ["internal", "external"]
    }

    for parent, children in nested_keys.items():
        for child in children:
            if not re.search(rf'\"{parent}\"[ ]*:[^{{]*{{[^}}]*\"{child}\"[ ]*:', file_content):
                raise ValueError(f"Missing nested key: {parent}.{child}")


def validate_instrument_types(trade_data: Dict[str, Any]) -> None:
    raw_instrument = trade_data.get("instrument", "UnknownInstrument")
    raw_sub_instrument = trade_data.get("sub_instrument", "UnknownSubInstrument")

    print(f"DEBUG: raw_instrument={raw_instrument}, raw_sub_instrument={raw_sub_instrument}")

    instrument_type = map_instrument_type(raw_instrument)
    sub_instrument_type = map_sub_instrument_type(raw_sub_instrument)

    print(f"DEBUG: instrument_type={instrument_type}, sub_instrument_type={sub_instrument_type}")

    if instrument_type == "UnknownInstrument":
        raise ValueError(f"Unsupported instrument type: {raw_instrument}")
    if sub_instrument_type == "UnknownSubInstrument":
        raise ValueError(f"Unsupported sub-instrument type: {raw_sub_instrument}")


def load_trade_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load trade data from a local JSON file, with additional regex validation.

    :param file_path: Path to the trade file.
    :return: Parsed trade data as a dictionary.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Trade file not found: {file_path}")

    with open(file_path, 'r') as file:
        file_content = file.read()

    # Validate the file content using regex
    validate_trade_file_with_regex(file_content)

    # Parse the JSON content
    try:
        trade_data = json.loads(file_content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in file: {file_path}") from e

    # Validate instrument and sub-instrument types
    validate_instrument_types(trade_data)

    return trade_data


def fetch_trade_from_hgraph(trade_id: str) -> Dict[str, Any]:
    """
    Placeholder function to fetch trade data from hgraph database.

    :param trade_id: ID of the trade to fetch.
    :return: Trade data from the hgraph database.
    """
    # Implement database fetch logic here
    return {}


# Example usage
if __name__ == "__main__":
    # Example file path (replace with dynamic paths in production)
    file_path = "../test_trades/fixed_float_BM_swap_001.txt"

    try:
        trade_data = load_trade_from_file(file_path)
        print(json.dumps(trade_data, indent=4))  # Print loaded trade data for verification
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")