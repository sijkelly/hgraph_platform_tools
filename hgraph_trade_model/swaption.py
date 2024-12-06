import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_instrument_type


def create_commodity_swaption(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity swaption trade section.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity swaption trade.
    """
    # Retrieve the global and swaption-specific mappings
    global_mapping = get_global_mapping()
    swaption_mapping = get_instrument_mapping("comm_swaption")
    combined_mapping = {**global_mapping, **swaption_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Define the swaption trade structure
    swaption_trade = {
        "placeholderField1": fpml_data.get("placeholderField1", ""),
        "placeholderField2": fpml_data.get("placeholderField2", ""),
        # Add more fields as the commodity swaption structure evolves
    }

    return {
        "commoditySwaption": swaption_trade,
        "metadata": {
            "type": "commoditySwaption",
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

    # Generate the commodity swaption trade
    commodity_swaption = create_commodity_swaption(sample_trade_data)

    # Print the generated commodity swaption trade
    print(json.dumps(commodity_swaption, indent=4))