import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_instrument_type


def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade data keys to their corresponding fpml keys.

    :param trade_data: Dictionary containing hgraph trade data.
    :param mapping: Mapping dictionary for hgraph to fpml keys.
    :return: Dictionary with keys converted to fpml format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)  # Default to the original key if no mapping exists
        mapped_data[fpml_key] = value
    return mapped_data


def create_commodity_future(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity future section.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity future.
    """
    # Retrieve the global and future-specific mappings
    global_mapping = get_global_mapping()
    future_mapping = get_instrument_mapping("comm_future")
    combined_mapping = {**global_mapping, **future_mapping}

    # Map hgraph keys to fpml keys
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


# Example usage (isolated)
if __name__ == "__main__":
    # Sample trade data for testing
    sample_trade_data = {
        "tradeId": "FUTURE-001",
        "tradeDate": "2024-11-20",
        "buySell": "Buy",
        "expiryDate": "2025-12-01",
        "underlyer": "Crude Oil",
        "notionalQuantity": 500,
        "notionalUnit": "barrels",
        "currency": "USD",
        "priceUnit": "USD per barrel",
        "contractPrice": 75.00,
        "exchange": "NYMEX",
        "deliveryLocation": "Cushing, OK"
    }

    # Generate the commodity future
    commodity_future = create_commodity_future(sample_trade_data)

    # Print the generated commodity future
    print(json.dumps(commodity_future, indent=4))