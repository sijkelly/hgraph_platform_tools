import os
import json
import re
from typing import Dict, Any


def validate_trade_file_with_regex(file_content: str) -> None:
    """
    Validate the trade file's content using regex to ensure it follows the expected structure.

    :param file_content: Content of the trade file as a string.
    :raises ValueError: If the validation fails.
    """
    # Example: Ensure the file contains essential keys like "tradeType" and "instrument"
    required_keys = ["tradeType", "instrument", "trade_id"]
    for key in required_keys:
        if not re.search(rf'\"{key}\"[ ]*:', file_content):
            raise ValueError(f"Missing required key: {key}")


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