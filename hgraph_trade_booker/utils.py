import os
import json
from typing import Dict, Any, Optional
from hgraph_trade_model.fpml_mappings import get_global_mapping, map_instrument_type


def ensure_directory_exists(directory: str):
    """
    Ensure that the specified directory exists, creating it if necessary.

    :param directory: Path to the directory to ensure exists.
    """
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError as e:
        raise RuntimeError(f"Failed to ensure directory exists: {directory}") from e


def save_trade_to_file(trade_data: Dict[str, Any], output_file: str, output_dir: str):
    """
    Save trade data to a JSON file in the specified output directory.

    :param trade_data: Trade data to save.
    :param output_file: The name of the output file.
    :param output_dir: The directory where the file will be saved.
    """
    ensure_directory_exists(output_dir)
    output_path = os.path.join(output_dir, output_file)

    try:
        with open(output_path, "w") as file:
            json.dump(trade_data, file, indent=4)
        print(f"Trade data successfully saved to {output_path}")
    except IOError as e:
        raise RuntimeError(f"Failed to write to file: {output_path}") from e


def apply_global_mappings(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply global mappings to the provided trade data.

    :param trade_data: The original trade data.
    :return: Trade data with global mappings applied.
    """
    global_mapping = get_global_mapping()
    return {**trade_data, **global_mapping}


def map_trade_type(hgraph_type: str) -> Optional[str]:
    """
    Map the trade type from hgraph to FpML format.

    :param hgraph_type: The hgraph trade type.
    :return: The corresponding FpML trade type, or None if no mapping exists.
    """
    return map_instrument_type(hgraph_type)