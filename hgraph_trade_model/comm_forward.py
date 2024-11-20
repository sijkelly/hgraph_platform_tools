import json
from typing import Dict, Any


def create_commodity_forward(trade_data: dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity forward section.

    :param trade_data: Dictionary containing trade data for the commodity forward.
        Expected keys:
        - buySell (str): Buy or sell indicator.
        - effectiveDate (str): Start date of the forward.
        - terminationDate (str): End date of the forward.
        - underlyer (str): Underlying asset.
        - notionalQuantity (float): Quantity of the commodity.
        - notionalUnit (str): Unit of the notional quantity.
        - currency (str): Payment currency.
        - priceUnit (str): Unit for pricing.
        - fixedPrice (float): Fixed price for the forward.
        - deliveryLocation (str): Location of delivery (optional).
    :return: A dictionary representing the commodity forward.
    """
    forward = {
        "buySell": trade_data.get("buySell", ""),
        "effectiveDate": trade_data.get("effectiveDate", ""),
        "terminationDate": trade_data.get("terminationDate", ""),
        "underlyer": trade_data.get("underlyer", ""),
        "notionalQuantity": {
            "quantity": trade_data.get("notionalQuantity", ""),
            "unit": trade_data.get("notionalUnit", "")
        },
        "paymentCurrency": trade_data.get("currency", ""),
        "priceUnit": trade_data.get("priceUnit", ""),
        "fixedPrice": trade_data.get("fixedPrice", ""),
        "deliveryLocation": trade_data.get("deliveryLocation", "")
    }

    return {"commodityForward": forward}


# Example usage
sample_trade_data = {
    "tradeId": "FORWARD-001",
    "tradeDate": "2024-11-20",
    "partyReference": "Party1",
    "buySell": "Buy",
    "effectiveDate": "2024-12-01",
    "terminationDate": "2025-12-01",
    "underlyer": "Silver",
    "notionalQuantity": 10000,
    "notionalUnit": "ounces",
    "currency": "USD",
    "priceUnit": "USD per ounce",
    "fixedPrice": 23.50,
    "deliveryLocation": "New York"
}

# Generate the commodity forward
commodity_forward = create_commodity_forward(sample_trade_data)

# Print the generated commodity forward
print(json.dumps(commodity_forward, indent=4))