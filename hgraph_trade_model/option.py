import json
from typing import Dict, Any
from hgraph_trade_model.fpml_mappings import get_global_mapping, get_instrument_mapping, map_instrument_type

# Placeholder for exercise styles until the FPML ENUM file is ready
exercise_styles = ["European", "American", "Bermudan"]


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


def create_commodity_option(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity option section based on the option type.

    :param trade_data: Dictionary containing hgraph trade data.
    :return: A dictionary representing the commodity option.
    """
    # Retrieve the global and option-specific mappings
    global_mapping = get_global_mapping()
    option_mapping = get_instrument_mapping("comm_option")
    combined_mapping = {**global_mapping, **option_mapping}

    # Map hgraph keys to fpml keys
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Calculate total premium if not explicitly provided
    total_premium = fpml_data.get("totalPremium") or (
        fpml_data.get("premiumPerUnit", 0) * fpml_data.get("quantity", 0)
    )

    # Validate exercise style
    if fpml_data.get("exerciseStyle") not in exercise_styles:
        raise ValueError(
            f"Invalid exercise style: {fpml_data.get('exerciseStyle')}. "
            f"Expected one of {exercise_styles}."
        )

    # Create the option structure
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

    # Handle Bermudan option-specific fields
    if fpml_data.get("exerciseStyle") == "Bermudan":
        option["exerciseDates"] = fpml_data.get("exerciseDates", [])

    return {"commodityOption": option}


# Example usage (isolated to prevent execution during import)
if __name__ == "__main__":
    # Sample trade data for testing
    sample_trade_data = {
        "tradeId": "OPT-002",
        "tradeDate": "2024-11-15",
        "partyReference": "Party1",
        "buySell": "Buy",
        "optionType": "Bermudan",
        "effectiveDate": "2024-12-01",
        "expirationDate": "2025-12-01",
        "underlyer": "Gold",
        "notionalQuantity": 5000,
        "notionalUnit": "ounces",
        "currency": "USD",
        "priceUnit": "USD per ounce",
        "strikePrice": 1900.00,
        "premiumPaymentDate": "2024-12-05",
        "premiumPerUnit": 25.50,
        "exerciseDates": ["2025-06-01", "2025-09-01", "2025-12-01"]
    }

    # Generate the commodity option
    commodity_option = create_commodity_option(sample_trade_data)

    # Print the generated commodity option
    print(json.dumps(commodity_option, indent=4))