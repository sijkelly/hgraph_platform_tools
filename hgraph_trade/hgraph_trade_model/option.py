"""
option.py

This module provides functionality to create the commodity option section of a trade message.
It maps raw hgraph trade data to FpML-compatible keys and constructs a standardized
commodityOption dictionary. The code also handles exercise styles (European, American,
Bermudan) and calculates premiums if they are not explicitly provided.
"""

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import get_global_mapping, get_instrument_mapping

exercise_styles = ["European", "American", "Bermudan"]

def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    mapped_data = {}
    for key, value in trade_data.items():
        fpml_key = mapping.get(key, key)
        mapped_data[fpml_key] = value
    return mapped_data

def create_commodity_option(trade_data: Dict[str, Any]) -> list:
    global_mapping = get_global_mapping()
    option_mapping = get_instrument_mapping("comm_option")
    combined_mapping = {**global_mapping, **option_mapping}

    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    total_premium = fpml_data.get("totalPremium") or (
        fpml_data.get("premiumPerUnit", 0) * fpml_data.get("quantity", 0)
    )

    if fpml_data.get("exerciseStyle") not in exercise_styles:
        raise ValueError(
            f"Invalid exercise style: {fpml_data.get('exerciseStyle')}. "
            f"Expected one of {exercise_styles}."
        )

    option = {
        "buySell": fpml_data.get("buySell", ""),
        "effectiveDate": fpml_data.get("effectiveDate", ""),
        "expirationDate": fpml_data.get("expirationDate", ""),
        "underlyer": fpml_data.get("underlyer", ""),
        "notionalQuantity": {
            "quantity": fpml_data.get("quantity", ""),
            "unit": fpml_data.get("unit", "")
        },
        "paymentCurrency": fpml_data.get("paymentCurrency", ""),
        "priceUnit": fpml_data.get("priceUnit", ""),
        "strikePrice": fpml_data.get("strikePrice", ""),
        "premium": {
            "premiumPaymentDate": fpml_data.get("premiumPaymentDate", ""),
            "premiumPerUnit": fpml_data.get("premiumPerUnit", ""),
            "totalPremium": total_premium
        },
        "exerciseStyle": fpml_data.get("exerciseStyle", "")
    }

    if fpml_data.get("exerciseStyle") == "Bermudan":
        option["exerciseDates"] = fpml_data.get("exerciseDates", [])

    return [{"commodityOption": option}]

# Example usage (commented out for production; move to tests or examples)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "tradeId": "OPT-002",
#         "tradeDate": "2024-11-15",
#         "partyReference": "Party1",
#         "buySell": "Buy",
#         "optionType": "Bermudan",
#         "effectiveDate": "2024-12-01",
#         "expirationDate": "2025-12-01",
#         "underlyer": "Gold",
#         "notionalQuantity": 5000,
#         "notionalUnit": "ounces",
#         "currency": "USD",
#         "priceUnit": "USD per ounce",
#         "strikePrice": 1900.00,
#         "premiumPaymentDate": "2024-12-05",
#         "premiumPerUnit": 25.50,
#         "exerciseDates": ["2025-06-01", "2025-09-01", "2025-12-01"]
#     }
#
#     commodity_option = create_commodity_option(sample_trade_data)
#     print(json.dumps(commodity_option, indent=4))