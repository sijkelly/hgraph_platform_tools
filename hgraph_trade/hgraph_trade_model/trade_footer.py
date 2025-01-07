"""
trade_footer.py

This module provides functionality to create the trade footer section of a trade message.
It maps hgraph trade footer data into FpML-compliant fields, producing a standardized
tradeFooter structure with associated metadata.
"""

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import get_global_mapping, get_instrument_mapping

def map_hgraph_to_fpml_footer(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade footer data keys to their corresponding FpML keys.

    :param trade_data: Dictionary containing hgraph trade footer data.
    :param mapping: A dictionary mapping hgraph keys to FpML keys.
    :return: A dictionary with keys converted to FpML format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)  # Use original key if no mapping exists
        mapped_data[fpml_key] = value
    return mapped_data

def create_trade_footer(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the trade footer section of the trade data.

    This function applies global and footer-specific mappings to the hgraph trade footer data,
    translating keys into FpML format. It then constructs a tradeFooter dictionary that can be
    integrated into a complete trade message.

    :param trade_data: Dictionary containing hgraph trade footer data.
    :return: A dictionary representing the tradeFooter section and its metadata.
    """
    global_mapping = get_global_mapping()
    footer_mapping = get_instrument_mapping("trade_footer")
    combined_mapping = {**global_mapping, **footer_mapping}

    # Map hgraph keys to FpML keys
    fpml_data = map_hgraph_to_fpml_footer(trade_data, combined_mapping)

    return {
        "tradeFooter": {
            "placeholderField1": fpml_data.get("placeholderField1", ""),
            "placeholderField2": fpml_data.get("placeholderField2", "")
        },
        "metadata": {
            "type": "tradeFooter",
            "version": "1.0"
        }
    }

# Example usage (commented out for production; can be tested separately)
# if __name__ == "__main__":
#     sample_footer_data = {
#         "placeholderField1": "Value1",
#         "placeholderField2": "Value2"
#     }
#
#     trade_footer = create_trade_footer(sample_footer_data)
#     print(json.dumps(trade_footer, indent=4))