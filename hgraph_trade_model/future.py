"""
future.py

This module provides functionality to create the commodity future section of a trade message.
It uses global and instrument-specific mappings to convert hgraph keys to FpML-like keys
and then constructs a standardized commodityFuture dictionary.
"""

import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping

def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade data keys to their corresponding FpML keys.

    :param trade_data: Dictionary containing hgraph trade data.
    :param mapping: A mapping dictionary for hgraph to FpML keys.
    :return: Dictionary with keys converted to FpML format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)  # Default to the original key if no mapping exists
        mapped_data[fpml_key] = value
    return mapped_data

def create_commodity_future(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity future section of the trade data.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodityFuture section.
    """
    global_mapping = get_global_mapping()
    future_mapping = get_instrument_mapping("comm_future")
    combined_mapping = {**global_mapping, **future_mapping}

    # Map hgraph keys to FpML keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Build the future data structure
    future = {
        "buySell": fpml_data.get("buySell", ""),
        "tradeDate": fpml_data.get("tradeDate", ""),
        "expiryDate": fpml_data.get("expiryDate", ""),
        "underlyer": fpml_data.get("underlyer", ""),
        "notionalQuantity": {
            "quantity": fpml_data.get("quantity", ""),
            "unit": fpml_data.get("unit", "")
        },
        "paymentCurrency": fpml_data.get("paymentCurrency", ""),
        "priceUnit": fpml_data.get("priceUnit", ""),
        "contractPrice": fpml_data.get("contractPrice", ""),
        "exchange": fpml_data.get("exchange", ""),
        "deliveryLocation": fpml_data.get("deliveryLocation", "")
    }

    return {"commodityFuture": future}

# Example usage (commented out for production; can be moved to tests or examples)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "tradeId": "FUTURE-001",
#         "tradeDate": "2024-11-20",
#         "buySell": "Buy",
#         "expiryDate": "2025-12-01",
#         "underlyer": "Crude Oil",
#         "notionalQuantity": 500,
#         "notionalUnit": "barrels",
#         "currency": "USD",
#         "priceUnit": "USD per barrel",
#         "contractPrice": 75.00,
#         "exchange": "NYMEX",
#         "deliveryLocation": "Cushing, OK"
#     }
#
#     commodity_future = create_commodity_future(sample_trade_data)
#     print(json.dumps(commodity_future, indent=4))