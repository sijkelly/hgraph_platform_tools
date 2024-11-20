import json
from typing import Dict, Any

# Define the mapping between hgraph tags and fpml tags
HGRAPH_TO_FPML_SWAP_MAPPING = {
    "buy_sell": "buySell",
    "swap_type": "instrument",
    "effective_date": "effectiveDate",
    "termination_date": "terminationDate",
    "underlyer": "underlyer",
    "notional_quantity": "notionalQuantity",
    "notional_unit": "notionalUnit",
    "currency": "paymentCurrency",
    "price_unit": "priceUnit",
    "fixed_leg_price": "fixedPrice",
    "float_leg_reference": "referencePrice",
    "reset_dates": "resetDates",
    "float_leg1_reference": "floatLeg1Reference",
    "float_leg1_reset_dates": "floatLeg1ResetDates",
    "float_leg2_reference": "floatLeg2Reference",
    "float_leg2_reset_dates": "floatLeg2ResetDates",
    "float_leg2_spread": "spread",
}

# Add a mapping for instrument type specifically
INSTRUMENT_TYPE_MAPPING = {
    "fixed_float": "fixedFloat",
    "float_float": "floatFloat",
}

def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map hgraph trade data keys to their corresponding fpml keys.

    :param trade_data: Dictionary containing hgraph trade data.
    :param mapping: Mapping dictionary for hgraph to fpml keys.
    :return: Dictionary with keys converted to fpml format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        if key == "instrument":  # Handle instrument mapping separately
            mapped_data[mapping.get(key, key)] = INSTRUMENT_TYPE_MAPPING.get(value, value)
        else:
            mapped_data[mapping.get(key, key)] = value
    return mapped_data


def create_commodity_swap(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity swap section based on the swap type.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity swap.
    """
    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, HGRAPH_TO_FPML_SWAP_MAPPING)

    swap = {"buySell": fpml_data.get("buySell", "")}

    # Handle different swap types
    instrument_type = fpml_data.get("instrument")
    if instrument_type == "fixedFloat":
        # Fixed-Float Swap
        swap.update({
            "effectiveDate": fpml_data.get("effectiveDate", ""),
            "terminationDate": fpml_data.get("terminationDate", ""),
            "underlyer": fpml_data.get("underlyer", ""),
            "notionalQuantity": {
                "quantity": fpml_data.get("notionalQuantity", ""),
                "unit": fpml_data.get("notionalUnit", "")
            },
            "paymentCurrency": fpml_data.get("paymentCurrency", ""),
            "fixedLeg": {
                "fixedPrice": fpml_data.get("fixedPrice", ""),
                "priceUnit": fpml_data.get("priceUnit", "")
            },
            "floatLeg": {
                "referencePrice": fpml_data.get("referencePrice", ""),
                "resetDates": fpml_data.get("resetDates", [])
            }
        })
    elif instrument_type == "floatFloat":
        # Float-Float Swap (Basis Swap)
        swap.update({
            "notionalQuantity": {
                "quantity": fpml_data.get("notionalQuantity", ""),
                "unit": fpml_data.get("notionalUnit", "")
            },
            "paymentCurrency": fpml_data.get("paymentCurrency", ""),
            "floatLeg1": {
                "effectiveDate": fpml_data.get("effectiveDate", ""),
                "terminationDate": fpml_data.get("terminationDate", ""),
                "underlyer": fpml_data.get("underlyer", ""),
                "referencePrice": fpml_data.get("floatLeg1Reference", ""),
                "resetDates": fpml_data.get("floatLeg1ResetDates", [])
            },
            "floatLeg2": {
                "effectiveDate": fpml_data.get("effectiveDate", ""),
                "terminationDate": fpml_data.get("terminationDate", ""),
                "underlyer": fpml_data.get("underlyer", ""),
                "referencePrice": fpml_data.get("floatLeg2Reference", ""),
                "resetDates": fpml_data.get("floatLeg2ResetDates", []),
                "spread": fpml_data.get("spread", "")
            }
        })
    else:
        raise ValueError(f"Unsupported instrument type: {instrument_type}")

    return {"commoditySwap": swap}


# Example usage
if __name__ == "__main__":
    sample_trade_data = {
        "buy_sell": "Buy",
        "swap_type": "fixed_float",
        "effective_date": "2024-12-01",
        "termination_date": "2025-12-01",
        "underlyer": "WTI",
        "notional_quantity": 10000,
        "notional_unit": "barrels",
        "currency": "USD",
        "price_unit": "USD per barrel",
        "fixed_leg_price": 85.50,
        "float_leg_reference": "Brent",
        "reset_dates": ["2024-12-15", "2025-01-15"]
    }

    commodity_swap = create_commodity_swap(sample_trade_data)
    print(json.dumps(commodity_swap, indent=4))