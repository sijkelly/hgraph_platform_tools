import json
from typing import Dict, Any

def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Map HGraph trade data keys to their corresponding FpML keys.

    :param trade_data: Dictionary containing HGraph trade data.
    :param mapping: A mapping dictionary for HGraph to FpML keys.
    :return: Dictionary with keys converted to FpML format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        # Debugging key-value pairs being processed
        print(f"DEBUG: Processing key={key}, value={value}")

        # Check if the value itself is a nested dictionary
        if isinstance(value, dict):
            fpml_key = mapping.get(key, key)
            mapped_data[fpml_key] = map_hgraph_to_fpml(value, mapping)  # Recursive mapping for nested dicts
        else:
            fpml_key = mapping.get(key, key)  # Default to the original key if no mapping exists
            mapped_data[fpml_key] = value

    # Debugging full mapped data
    print(f"DEBUG: Mapped Data: {json.dumps(mapped_data, indent=4)}")
    return mapped_data

# Global field mappings
HGRAPH_TO_FPML_GLOBAL_MAPPING = {
    "buy_sell": "buySell",
    "currency": "priceUnit",
    "price_unit": "priceUnit",
    "effective_date": "effectiveDate",
    "termination_date": "terminationDate",
    "qty": "quantity",
    "unit": "unit",
    "commodity": "underlyer",
    "asset": "asset",
    "trade_date": "tradeDate",
    "trade_id": "tradeId",
    "internal_party": "internalParty",
    "external_party": "externalParty",
    "internal_portfolio": "internalPortfolio",
    "external_portfolio": "externalPortfolio",
    "internal_trader": "internalTrader",
    "external_trader": "externalTrader"
}

# Instrument-specific mappings
HGRAPH_TO_FPML_INSTRUMENT_MAPPING = {
    "swap": {
        "fixed_leg_price": "fixedPrice",
        "float_leg_reference": "referencePrice",
        "reset_dates": "resetDates",
        "float_leg1_reference": "floatLeg1Reference",
        "float_leg1_reset_dates": "floatLeg1ResetDates",
        "float_leg2_reference": "floatLeg2Reference",
        "float_leg2_reset_dates": "floatLeg2ResetDates",
        "float_leg2_spread": "spread",
    },
    "option": {
        "strike_price": "strikePrice",
        "exercise_dates": "exerciseDates",
        "premium_payment_date": "premiumPaymentDate",
        "premium_per_unit": "premiumPerUnit",
        "total_premium": "totalPremium",
        "option_type": "exerciseStyle",
    },
    "forward": {
        "fixed_price": "fixedPrice",
        "delivery_location": "deliveryLocation",
    },
    "future": {
        "contract_price": "contractPrice",
        "expiry_date": "expiryDate",
        "exchange": "exchange",
        "delivery_location": "deliveryLocation",
    },
    "physical": {
        "placeholder_field1": "placeholderField1",
        "placeholder_field2": "placeholderField2",
    },
    "swaption": {
        "placeholder_field1": "placeholderField1",
        "placeholder_field2": "placeholderField2",
    },
    "fx": {
        "trade_date": "tradeDate",
        "settlement_date": "settlementDate",
        "currency_pair": "currencyPair",
        "notional_amount": "notionalAmount",
        "rate": "rate",
    }
}

# Instrument type mapping
INSTRUMENT_TYPE_MAPPING = {
    "swap": "swap",
    "option": "option",
    "forward": "forward",
    "future": "future",
    "physical": "physical",
    "swaption": "swaption",
    "fx": "fx",
}

# Sub-instrument type mapping
SUB_INSTRUMENT_TYPE_MAPPING = {
    "fixed_float": "fixedFloat",
    "float_float": "floatFloat",
}

def get_global_mapping() -> Dict[str, str]:
    """
    Retrieve the global field mapping.
    """
    return HGRAPH_TO_FPML_GLOBAL_MAPPING

def get_instrument_mapping(instrument: str) -> Dict[str, str]:
    """
    Retrieve the mapping specific to an instrument.
    :param instrument: The name of the instrument (e.g., "comm_swap").
    :return: The instrument-specific mapping.
    """
    return HGRAPH_TO_FPML_INSTRUMENT_MAPPING.get(instrument, {})

def map_instrument_type(hgraph_type: str) -> str:
    """
    Map instrument types from hgraph to FpML.
    :param hgraph_type: The hgraph instrument type.
    :return: The FpML instrument type.
    """
    return INSTRUMENT_TYPE_MAPPING.get(hgraph_type, hgraph_type)

def map_sub_instrument_type(hgraph_sub_type: str) -> str:
    """
    Map sub-instrument types from hgraph to FpML.
    :param hgraph_sub_type: The hgraph sub-instrument type.
    :return: The FpML sub-instrument type.
    """
    # Normalize the sub-instrument key to lowercase for mapping consistency
    normalized_key = hgraph_sub_type.strip().lower()
    return SUB_INSTRUMENT_TYPE_MAPPING.get(normalized_key, normalized_key)