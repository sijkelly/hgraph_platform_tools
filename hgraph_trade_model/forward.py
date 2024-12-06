"""
forward.py

This module provides functionality to create a commodity forward trade section
of a message. It maps raw hgraph trade data to FpML-like fields using global and
instrument-specific mappings, and then constructs a standardized commodityForward
structure.
"""

import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping


def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade data keys to corresponding FpML keys based on the provided mapping.

    :param trade_data: Dictionary containing hgraph trade data.
    :param mapping: A dictionary mapping hgraph keys to FpML keys.
    :return: A dictionary with hgraph keys replaced by their FpML equivalents.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)  # Default to the original key if no mapping exists
        mapped_data[fpml_key] = value
    return mapped_data


def create_commodity_forward(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity forward section of the trade data.

    This function retrieves global and forward-specific mappings to translate hgraph
    fields to FpML keys, then constructs a commodityForward dictionary with the
    standardized fields.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity forward section.
    """
    global_mapping = get_global_mapping()
    forward_mapping = get_instrument_mapping("comm_forward")
    combined_mapping = {**global_mapping, **forward_mapping}

    # Map hgraph keys to FpML keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Build the forward data structure
    forward = {
        "buySell": fpml_data.get("buySell", ""),
        "tradeDate": fpml_data.get("tradeDate", ""),
        "effectiveDate": fpml_data.get("effectiveDate", ""),
        "terminationDate": fpml_data.get("terminationDate", ""),
        "underlyer": fpml_data.get("underlyer", ""),
        "notionalQuantity": {
            "quantity": fpml_data.get("quantity", ""),
            "unit": fpml_data.get("unit", "")
        },
        "paymentCurrency": fpml_data.get("paymentCurrency", ""),
        "priceUnit": fpml_data.get("priceUnit", ""),
        "fixedPrice": fpml_data.get("fixedPrice", ""),
        "deliveryLocation": fpml_data.get("deliveryLocation", "")
    }

    return {"commodityForward": forward}


# Example usage (commented out for production; move to separate tests or examples if needed)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "tradeId": "FORWARD-001",
#         "tradeDate": "2024-11-20",
#         "buySell": "Buy",
#         "effectiveDate": "2024-12-01",
#         "terminationDate": "2025-12-01",
#         "underlyer": "Crude Oil",
#         "notionalQuantity": 500,
#         "notionalUnit": "barrels",
#         "currency": "USD",
#         "priceUnit": "USD per barrel",
#         "fixedPrice": 75.00,
#         "deliveryLocation": "Cushing, OK"
#     }
#     commodity_forward = create_commodity_forward(sample_trade_data)
#     print(json.dumps(commodity_forward, indent=4))