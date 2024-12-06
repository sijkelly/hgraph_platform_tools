"""
swap.py

This module provides functionality to create the commodity swap section of a trade message.
It uses global and instrument-specific mappings to translate hgraph trade data into
FpML-like keys. Depending on the sub-instrument type (e.g., fixedFloat, floatFloat),
it constructs a standardized commoditySwap dictionary suitable for downstream processing.
"""

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
    Create the commodity swap section based on the given sub-instrument type.

    This function applies global and swap-specific mappings to translate hgraph
    keys into FpML-compatible keys, and then constructs a commoditySwap dictionary
    appropriate for the specified sub-instrument type.

    :param trade_data: Dictionary containing hgraph trade data.
    :param sub_instrument_type: The mapped sub-instrument type (e.g., "fixedFloat", "floatFloat").
    :return: A dictionary representing the commoditySwap section.
    :raises ValueError: If the sub-instrument type is not supported.
    """
    global_mapping = get_global_mapping()
    swap_mapping = get_instrument_mapping("swap")
    combined_mapping = {**global_mapping, **swap_mapping}

    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Initialize the swap structure with a buySell field
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

# Example usage (commented out for production; move to tests or separate examples file if desired)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "tradeType": "newTrade",
#         "instrument": "CommoditySwap",
#         "sub_instrument": "fixed_float",
#         "buy_sell": "Buy",
#         "effective_date": "2024-12-01",
#         "termination_date": "2025-12-01",
#         "underlyer": "WTI",
#         "notional_quantity": 10000,
#         "notional_unit": "barrels",
#         "currency": "USD",
#         "price_unit": "USD per barrel",
#         "fixed_leg_price": 85.50,
#         "float_leg_reference": "Brent",
#         "reset_dates": ["2024-12-15", "2025-01-15"]
#     }
#
#     mapped_sub_instrument = map_sub_instrument_type(sample_trade_data["sub_instrument"])
#     commodity_swap = create_commodity_swap(sample_trade_data, mapped_sub_instrument)
#     print(json.dumps(commodity_swap, indent=4))