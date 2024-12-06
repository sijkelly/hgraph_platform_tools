"""
fpml_mappings.py

This module defines mappings from hgraph-specific trade data fields to FpML-compliant keys
and provides utility functions for converting raw trade data into an FpML-like structure.
These mappings ensure consistency and facilitate easier integration with downstream systems
expecting standardized FpML fields.

Key functionalities:
- map_hgraph_to_fpml: Recursively maps hgraph trade data keys to FpML keys.
- get_global_mapping: Retrieves a global mapping for commonly used keys across multiple instruments.
- get_instrument_mapping: Retrieves instrument-specific mappings.
- map_instrument_type and map_sub_instrument_type: Convert hgraph instrument and sub-instrument
  identifiers to the corresponding FpML types.
"""

import json
from typing import Dict, Any

def map_hgraph_to_fpml(trade_data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Recursively map HGraph trade data keys to their corresponding FpML keys using the provided mapping.

    :param trade_data: Dictionary containing HGraph trade data.
    :param mapping: A dictionary mapping hgraph keys to FpML keys.
    :return: A dictionary with keys converted to FpML format.
    """
    mapped_data = {}
    for key, value in trade_data.items():
        # If debugging is needed, consider using logging.debug() instead of print
        # logging.debug(f"Processing key={key}, value={value}")
        if isinstance(value, dict):
            fpml_key = mapping.get(key, key)
            mapped_data[fpml_key] = map_hgraph_to_fpml(value, mapping)  # Recursive mapping for nested dicts
        else:
            fpml_key = mapping.get(key, key)
            mapped_data[fpml_key] = value

    # If debugging is needed, consider using logging.debug()
    # logging.debug("Mapped Data: %s", json.dumps(mapped_data, indent=4))
    return mapped_data

# Global field mappings: apply to all instruments
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

# Instrument-specific mappings: apply only to particular instruments
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

# Instrument type mapping: maps hgraph instrument strings to known instrument categories
INSTRUMENT_TYPE_MAPPING = {
    "swap": "swap",
    "option": "option",
    "forward": "forward",
    "future": "future",
    "physical": "physical",
    "swaption": "swaption",
    "fx": "fx",
}

# Sub-instrument type mapping: maps hgraph sub-instrument strings to known sub-instrument categories
SUB_INSTRUMENT_TYPE_MAPPING = {
    "fixed_float": "fixedFloat",
    "float_float": "floatFloat",
}

def get_global_mapping() -> Dict[str, str]:
    """
    Retrieve the global field mapping that applies to all instruments.
    """
    return HGRAPH_TO_FPML_GLOBAL_MAPPING

def get_instrument_mapping(instrument: str) -> Dict[str, str]:
    """
    Retrieve the mapping specific to an instrument.

    :param instrument: The name of the instrument (e.g., "swap", "option").
    :return: The instrument-specific mapping dictionary.
    """
    return HGRAPH_TO_FPML_INSTRUMENT_MAPPING.get(instrument, {})

def map_instrument_type(hgraph_type: str) -> str:
    """
    Map hgraph instrument types to FpML instrument types.

    :param hgraph_type: The hgraph instrument type.
    :return: The corresponding FpML instrument type or the original if unmapped.
    """
    return INSTRUMENT_TYPE_MAPPING.get(hgraph_type, hgraph_type)

def map_sub_instrument_type(hgraph_sub_type: str) -> str:
    """
    Map hgraph sub-instrument types to FpML sub-instrument types.

    :param hgraph_sub_type: The hgraph sub-instrument type.
    :return: The corresponding FpML sub-instrument type or the original if unmapped.
    """
    normalized_key = hgraph_sub_type.strip().lower()
    return SUB_INSTRUMENT_TYPE_MAPPING.get(normalized_key, normalized_key)