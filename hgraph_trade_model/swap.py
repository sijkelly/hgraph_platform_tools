import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
    map_sub_instrument_type
)


def create_commodity_swap(trade_data: Dict[str, Any], sub_instrument_type: str) -> Dict[str, Any]:
    """
    Create the commodity swap section based on the sub-instrument type.

    :param trade_data: Dictionary containing hgraph trade data.
    :param sub_instrument_type: Mapped sub-instrument type for the trade (e.g., "fixedFloat").
    :return: A dictionary representing the commodity swap.
    """
    # Retrieve mappings
    global_mapping = get_global_mapping()
    swap_mapping = get_instrument_mapping("swap")
    combined_mapping = {**global_mapping, **swap_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Debugging
    print(f"DEBUG: sub_instrument_type passed to create_commodity_swap: {sub_instrument_type}")
    print(f"DEBUG: fpml_data after mapping: {json.dumps(fpml_data, indent=4)}")

    # Initialize the swap structure
    swap = {"buySell": fpml_data.get("buySell", "")}

    if sub_instrument_type == "fixedFloat":
        # Fixed-Float Swap
        swap.update({
            "effectiveDate": fpml_data.get("effectiveDate", ""),
            "terminationDate": fpml_data.get("terminationDate", ""),
            "underlyer": fpml_data.get("underlyer", ""),
            "notionalQuantity": {
                "quantity": fpml_data.get("quantity", ""),
                "unit": fpml_data.get("unit", "")
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
    elif sub_instrument_type == "floatFloat":
        # Float-Float Swap
        swap.update({
            "notionalQuantity": {
                "quantity": fpml_data.get("quantity", ""),
                "unit": fpml_data.get("unit", "")
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
        raise ValueError(
            f"Unsupported sub-instrument type: {sub_instrument_type}. "
            f"Ensure `sub_instrument` is one of: ['fixedFloat', 'floatFloat']"
        )

    return {"commoditySwap": swap}


# Example usage for testing
if __name__ == "__main__":
    sample_trade_data = {
        "tradeType": "newTrade",
        "instrument": "CommoditySwap",
        "sub_instrument": "fixed_float",
        "buy_sell": "Buy",
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

    # Map sub-instrument type
    mapped_sub_instrument = map_sub_instrument_type(sample_trade_data["sub_instrument"])

    # Generate the commodity swap
    commodity_swap = create_commodity_swap(sample_trade_data, mapped_sub_instrument)
    print(json.dumps(commodity_swap, indent=4))