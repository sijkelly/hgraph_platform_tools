import json
from typing import Dict, Any


def create_commodity_future(trade_data: dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity future section.

    :param trade_data: Dictionary containing trade data for the commodity future.
        Expected keys:
        - buySell (str): Buy or sell indicator.
        - tradeDate (str): Trade date of the future.
        - expiryDate (str): Expiry date of the future contract.
        - underlyer (str): Underlying asset.
        - notionalQuantity (float): Quantity of the commodity.
        - notionalUnit (str): Unit of the notional quantity.
        - currency (str): Payment currency.
        - priceUnit (str): Unit for pricing.
        - contractPrice (float): Agreed price for the future.
        - exchange (str): Exchange where the future is traded.
        - deliveryLocation (str): Location of delivery (optional).
    :return: A dictionary representing the commodity future.
    """
    future = {
        "buySell": trade_data.get("buySell", ""),
        "tradeDate": trade_data.get("tradeDate", ""),
        "expiryDate": trade_data.get("expiryDate", ""),
        "underlyer": trade_data.get("underlyer", ""),
        "notionalQuantity": {
            "quantity": trade_data.get("notionalQuantity", ""),
            "unit": trade_data.get("notionalUnit", "")
        },
        "paymentCurrency": trade_data.get("currency", ""),
        "priceUnit": trade_data.get("priceUnit", ""),
        "contractPrice": trade_data.get("contractPrice", ""),
        "exchange": trade_data.get("exchange", ""),
        "deliveryLocation": trade_data.get("deliveryLocation", "")
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