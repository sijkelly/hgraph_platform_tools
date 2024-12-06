"""
physical.py

This module provides functionality to create the commodity physical trade section
of a trade message. It maps hgraph trade data to FpML-compatible keys and constructs
a standardized commodityPhysical structure. As the specification evolves, more fields
can be added and mapped here.
"""

import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_hgraph_to_fpml

def create_commodity_physical(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity physical trade section of the trade data.

    This function retrieves global and physical-specific mappings, applies them to
    the hgraph input data, and constructs a commodityPhysical dictionary compatible
    with downstream systems. Additional fields can be added as needed.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity physical trade section.
    """
    global_mapping = get_global_mapping()
    physical_mapping = get_instrument_mapping("comm_physical")
    combined_mapping = {**global_mapping, **physical_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Define the physical trade structure
    physical_trade = {
        "placeholderField1": fpml_data.get("placeholderField1", ""),
        "placeholderField2": fpml_data.get("placeholderField2", ""),
        # Future expansions can add more fields here
    }

    return {
        "commodityPhysical": physical_trade,
        "metadata": {
            "type": "commodityPhysical",
            "version": "1.0"
        }
    }

# Example usage (commented out for production; can be used for testing)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "placeholderField1": "Value1",
#         "placeholderField2": "Value2"
#     }
#
#     commodity_physical = create_commodity_physical(sample_trade_data)
#     print(json.dumps(commodity_physical, indent=4))