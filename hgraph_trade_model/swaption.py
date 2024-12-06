"""
swaption.py

This module provides functionality to create the commodity swaption section of a trade message.
It uses global and instrument-specific mappings to convert hgraph keys to FpML-like fields,
and constructs a standardized commoditySwaption dictionary. As the specification evolves,
additional fields can be included.
"""

import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_hgraph_to_fpml

def create_commodity_swaption(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity swaption trade section of the trade data.

    This function applies global and swaption-specific mappings to translate hgraph
    fields into FpML-compatible fields and then constructs a commoditySwaption structure.
    Additional fields can be added as the swaption specification evolves.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity swaption trade section.
    """
    global_mapping = get_global_mapping()
    swaption_mapping = get_instrument_mapping("comm_swaption")
    combined_mapping = {**global_mapping, **swaption_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Define the swaption trade structure
    swaption_trade = {
        "placeholderField1": fpml_data.get("placeholderField1", ""),
        "placeholderField2": fpml_data.get("placeholderField2", ""),
        # Future expansions for commodity swaption fields can go here
    }

    return {
        "commoditySwaption": swaption_trade,
        "metadata": {
            "type": "commoditySwaption",
            "version": "1.0"
        }
    }

# Example usage (commented out for production; can be tested separately)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "placeholderField1": "Value1",
#         "placeholderField2": "Value2"
#     }
#
#     commodity_swaption = create_commodity_swaption(sample_trade_data)
#     print(json.dumps(commodity_swaption, indent=4))