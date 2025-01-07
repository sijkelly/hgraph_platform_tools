"""
forward.py

This module provides functionality to create a commodity forward trade section
of a message. It maps raw hgraph trade data to FpML-like fields using global and
instrument-specific mappings, and then constructs a standardized commodityForward
structure.
"""

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import get_global_mapping, get_instrument_mapping

def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)
        mapped_data[fpml_key] = value
    return mapped_data

def create_commodity_forward(trade_data: Dict[str, Any]) -> list:
    global_mapping = get_global_mapping()
    forward_mapping = get_instrument_mapping("comm_forward")
    combined_mapping = {**global_mapping, **forward_mapping}
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

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

    return [{"commodityForward": forward}]


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