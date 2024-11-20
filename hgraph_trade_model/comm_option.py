import json
from typing import Dict, Any

# Uncomment the lines below and adjust when the FPML ENUM file is ready to be used
# import xmlschema
# def load_exercise_style_enum(xsd_file: str) -> list[str]:
#     """
#     Load ExerciseStyleEnum values from the FPML XSD file.
#     """
#     schema = xmlschema.XMLSchema(xsd_file)
#     enums = schema.types['ExerciseStyleEnum'].facets['enumeration']
#     return [enum.value for enum in enums]

# Temporarily disable loading exercise styles from the FPML ENUM file
# exercise_styles = load_exercise_style_enum("/mnt/data/fpml-enum-5-12.xsd")
exercise_styles = ["European", "American", "Bermudan"]  # Placeholder values


def create_commodity_option(trade_data: dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity option section based on the option type.

    :param trade_data: Dictionary containing trade data.
        Expected keys vary based on optionType:
        - optionType (str): "European", "American", or "Bermudan".
        - buySell (str): Buy or sell indicator.
        - effectiveDate (str): Option start date.
        - expirationDate (str): Option expiration date.
        - underlyer (str): Underlying asset.
        - notionalQuantity (float): Quantity of the commodity.
        - notionalUnit (str): Unit of the notional quantity.
        - currency (str): Payment currency.
        - priceUnit (str): Unit for pricing.
        - strikePrice (float): Strike price of the option.
        - exerciseDates (list[str]): Exercise dates (only for Bermudan).
        - premiumPaymentDate (str): Date of premium payment.
        - premiumPerUnit (float): Premium per unit.
    :return: A dictionary representing the commodity option.
    """
    # Calculate total premium if not explicitly provided
    total_premium = trade_data.get("totalPremium") or (
        trade_data.get("premiumPerUnit", 0) * trade_data.get("notionalQuantity", 0)
    )

    # Validate exercise style
    if trade_data.get("optionType") not in exercise_styles:
        raise ValueError(
            f"Invalid exercise style: {trade_data.get('optionType')}. "
            f"Expected one of {exercise_styles}."
        )

    option = {
        "buySell": trade_data.get("buySell", ""),
        "effectiveDate": trade_data.get("effectiveDate", ""),
        "expirationDate": trade_data.get("expirationDate", ""),
        "underlyer": trade_data.get("underlyer", ""),
        "notionalQuantity": {
            "quantity": trade_data.get("notionalQuantity", ""),
            "unit": trade_data.get("notionalUnit", "")
        },
        "paymentCurrency": trade_data.get("currency", ""),
        "priceUnit": trade_data.get("priceUnit", ""),
        "strikePrice": trade_data.get("strikePrice", ""),
        "premium": {
            "premiumPaymentDate": trade_data.get("premiumPaymentDate", ""),
            "premiumPerUnit": trade_data.get("premiumPerUnit", ""),
            "totalPremium": total_premium
        },
        "exerciseStyle": trade_data.get("optionType", "")
    }

    # Handle Bermudan option-specific fields
    if trade_data.get("optionType") == "Bermudan":
        option["exerciseDates"] = trade_data.get("exerciseDates", [])

    return {"commodityOption": option}


# Example usage
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