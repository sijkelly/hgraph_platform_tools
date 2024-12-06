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


def create_commodity_forward(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity forward section.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity forward.
    """
    # Retrieve the global and forward-specific mappings
    global_mapping = get_global_mapping()
    forward_mapping = get_instrument_mapping("comm_forward")
    combined_mapping = {**global_mapping, **forward_mapping}

    # Map hgraph keys to fpml keys
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


# Example usage (isolated)
if __name__ == "__main__":
    # Sample trade data for testing
    sample_trade_data = {
        "tradeId": "FORWARD-001",
        "tradeDate": "2024-11-20",
        "buySell": "Buy",
        "effectiveDate": "2024-12-01",
        "terminationDate": "2025-12-01",
        "underlyer": "Crude Oil",
        "notionalQuantity": 500,
        "notionalUnit": "barrels",
        "currency": "USD",
        "priceUnit": "USD per barrel",
        "fixedPrice": 75.00,
        "deliveryLocation": "Cushing, OK"
    }

    # Generate the commodity forward
    commodity_forward = create_commodity_forward(sample_trade_data)

    # Print the generated commodity forward
    print(json.dumps(commodity_forward, indent=4))