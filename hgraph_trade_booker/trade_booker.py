import os
import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_instrument_type


def book_trade(trade_data: Dict[str, Any], output_file: str, output_dir: str):
    """
    Book the trade and save it to the specified output directory.

    :param trade_data: The trade data to be booked.
    :param output_file: The name of the output file.
    :param output_dir: The directory where the file will be saved.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Save the trade data to the specified file
    output_path = os.path.join(output_dir, output_file)
    try:
        with open(output_path, "w") as file:
            json.dump(trade_data, file, indent=4)
        print(f"Booking message saved to {output_path}")
    except IOError as e:
        print(f"Error writing to {output_path}: {e}")
        raise