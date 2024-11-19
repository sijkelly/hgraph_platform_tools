import json
from typing import Dict, Any

def create_commodity_swap(trade_data: dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity swap section based on the swap type.
    """
    if trade_data["swapType"] == "fixedFloat":
        # Handling fixed-float swaps (fixed leg + floating leg)
        swap = {
            "buySell": trade_data["buySell"],
            "effectiveDate": trade_data["effectiveDate"],
            "terminationDate": trade_data["terminationDate"],
            "underlyer": trade_data["underlyer"],
            "notionalQuantity": {
                "quantity": trade_data["notionalQuantity"],
                "unit": trade_data["notionalUnit"]
            },
            "paymentCurrency": trade_data["currency"],
            "fixedLeg": {
                "fixedPrice": trade_data["fixedLegPrice"],
                "priceUnit": trade_data["priceUnit"]
            },
            "floatLeg": {
                "referencePrice": trade_data["floatLegReference"],
                "resetDates": trade_data["resetDates"]
            }
        }
    elif trade_data["swapType"] == "floatFloat":
        # Handling float-float swaps (basis swaps)
        swap = {
            "buySell": trade_data["buySell"],
            "notionalQuantity": {
                "quantity": trade_data["notionalQuantity"],
                "unit": trade_data["notionalUnit"]
            },
            "paymentCurrency": trade_data["currency"],
            "floatLeg1": {
                "effectiveDate": trade_data["effectiveDate"],
                "terminationDate": trade_data["terminationDate"],
                "underlyer": trade_data["underlyer"],"referencePrice": trade_data["floatLeg1Reference"],
                "resetDates": trade_data["floatLeg1ResetDates"],
            },
            "floatLeg2": {
                "effectiveDate": trade_data["effectiveDate"],
                "terminationDate": trade_data["terminationDate"],
                "underlyer": trade_data["underlyer"],"referencePrice": trade_data["floatLeg2Reference"],
                "resetDates": trade_data["floatLeg2ResetDates"],
                "spread": trade_data["floatLeg2Spread"]
            }
        }
    else:
        raise ValueError(f"Unsupported swap type: {trade_data['swapType']}")

    return {"commoditySwap": swap}

def generate_trade_json(trade_data: Dict[str, Any]) -> str:
    """
    Generate a complete trade JSON with trade header and commodity swap.
    """
    trade_header = create_trade_header(trade_data)
    commodity_swap = create_commodity_swap(trade_data)

    # Combine both sections into a single JSON structure
    trade = {**trade_header, **commodity_swap}
    return json.dumps(trade, indent=4)


# Sample trade data for a fixed-float swap
sample_trade_data = {
    "tradeId": "SWAP-003",
    "tradeDate": "2024-11-10",
    "partyReference": "Party1",
    "party1": {"id": "Party1", "name": "Trader A"},
    "party2": {"id": "Party2", "name": "Trader B"},
    "buySell": "Buy",
    "effectiveDate": "2024-12-01",
    "terminationDate": "2025-12-01",
    "underlyer": "WTI",
    "notionalQuantity": 10000,
    "notionalUnit": "barrels",
    "currency": "USD",
    "priceUnit": "USD per barrel",

    # For Fixed-Float Swap
    "swapType": "fixedFloat",
    "fixedLegPrice": 85.50,
    "floatLegReference": "Brent",
    "resetDates": ["2024-12-15", "2025-01-15"]
}

# Generate the JSON for the trade
trade_json = generate_trade_json(sample_trade_data)
print(trade_json)