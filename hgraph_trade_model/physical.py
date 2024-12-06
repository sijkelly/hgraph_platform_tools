import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_instrument_type


def create_commodity_physical(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity physical trade section.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity physical trade.
    """
    # Retrieve the global and physical-specific mappings
    global_mapping = get_global_mapping()
    physical_mapping = get_instrument_mapping("comm_physical")
    combined_mapping = {**global_mapping, **physical_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Define the physical trade structure
    physical_trade = {
        "placeholderField1": fpml_data.get("placeholderField1", ""),
        "placeholderField2": fpml_data.get("placeholderField2", ""),
        # Add more fields as the commodity physical structure evolves
    }

    return {
        "commodityPhysical": physical_trade,
        "metadata": {
            "type": "commodityPhysical",
            "version": "1.0"
        }
    }


# Example usage (isolated)
if __name__ == "__main__":
    # Sample trade data for testing
    sample_trade_data = {
        "placeholderField1": "Value1",
        "placeholderField2": "Value2"
    }

    # Generate the commodity physical trade
    commodity_physical = create_commodity_physical(sample_trade_data)

    # Print the generated commodity physical trade
    print(json.dumps(commodity_physical, indent=4))