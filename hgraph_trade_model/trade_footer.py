import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping


def map_hgraph_to_fpml_footer(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade footer data keys to their corresponding fpml keys.

    :param trade_data: Dictionary containing hgraph trade footer data.
    :param mapping: Mapping dictionary for hgraph to fpml keys.
    :return: Dictionary with keys converted to fpml format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)  # Default to the original key if no mapping exists
        mapped_data[fpml_key] = value
    return mapped_data


def create_trade_footer(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the trade footer section.

    :param trade_data: Dictionary containing hgraph trade footer data.
    :return: A dictionary representing the trade footer.
    """
    # Retrieve the global and footer-specific mappings
    global_mapping = get_global_mapping()
    footer_mapping = get_instrument_mapping("trade_footer")
    combined_mapping = {**global_mapping, **footer_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml_footer(trade_data, combined_mapping)

    return {
        "tradeFooter": {
            "placeholderField1": fpml_data.get("placeholderField1", ""),
            "placeholderField2": fpml_data.get("placeholderField2", ""),
        },
        "metadata": {
            "type": "tradeFooter",
            "version": "1.0"
        }
    }


# Example usage for testing
if __name__ == "__main__":
    # Sample footer data for testing
    sample_footer_data = {
        "placeholderField1": "Value1",
        "placeholderField2": "Value2"
    }

    # Generate the trade footer
    trade_footer = create_trade_footer(sample_footer_data)

    # Print the generated trade footer
    print(json.dumps(trade_footer, indent=4))