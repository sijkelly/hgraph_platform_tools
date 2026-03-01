"""
swaption.py

This module provides functionality to create the commodity swaption section of a trade message.
A swaption is an option to enter into a commodity swap. It uses global and instrument-specific
mappings to convert hgraph keys to FpML-like fields, and constructs a standardized
commoditySwaption dictionary.

The structure includes:
- Premium details (payment date, per-unit premium, total premium)
- Exercise style (European, American, Bermudan) with exercise dates
- The underlying swap definition (fixed/float or float/float)
"""

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
)

EXERCISE_STYLES = ["European", "American", "Bermudan"]


def create_commodity_swaption(trade_data: Dict[str, Any]) -> list:
    """
    Create a commodity swaption trade structure from raw hgraph trade data.

    :param trade_data: Dictionary containing raw hgraph trade data.
    :return: A list containing a single dictionary with the commoditySwaption structure.
    """
    global_mapping = get_global_mapping()
    swaption_mapping = get_instrument_mapping("swaption")
    combined_mapping = {**global_mapping, **swaption_mapping}

    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    # Validate exercise style
    exercise_style = fpml_data.get("exerciseStyle", "European")
    if exercise_style not in EXERCISE_STYLES:
        raise ValueError(
            f"Invalid exercise style: {exercise_style}. "
            f"Expected one of {EXERCISE_STYLES}."
        )

    # Calculate total premium if not provided
    total_premium = fpml_data.get("totalPremium") or (
        fpml_data.get("premiumPerUnit", 0) * fpml_data.get("quantity", 0)
    )

    # Build premium structure
    premium = {
        "premiumPaymentDate": fpml_data.get("premiumPaymentDate", ""),
        "premiumPerUnit": fpml_data.get("premiumPerUnit", ""),
        "totalPremium": total_premium,
        "priceCurrency": fpml_data.get("premiumCurrency", ""),
        "priceUnit": fpml_data.get("priceUnit", ""),
    }

    # Build exercise structure
    exercise = {
        "exerciseStyle": exercise_style,
        "expirationDate": fpml_data.get("expirationDate", ""),
    }

    if exercise_style == "Bermudan":
        exercise["exerciseDates"] = fpml_data.get("exerciseDates", [])

    if exercise_style == "American":
        exercise["commencementDate"] = fpml_data.get("commencementDate", "")

    # Build the underlying swap structure
    underlying_swap = _build_underlying_swap(fpml_data)

    swaption_trade = {
        "buySell": fpml_data.get("buySell", ""),
        "premium": premium,
        "exercise": exercise,
        "underlyingSwap": underlying_swap,
    }

    return [{
        "commoditySwaption": swaption_trade,
        "metadata": {
            "type": "commoditySwaption",
            "version": "1.0"
        }
    }]


def _build_underlying_swap(fpml_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the underlying commodity swap structure for the swaption.

    :param fpml_data: Dictionary containing mapped FpML trade data.
    :return: Dictionary representing the underlying swap.
    """
    swap = {
        "effectiveDate": fpml_data.get("swapEffectiveDate", ""),
        "terminationDate": fpml_data.get("swapTerminationDate", ""),
        "settlementCurrency": fpml_data.get("settlementCurrency", ""),
    }

    # Fixed leg
    swap["fixedLeg"] = {
        "payerPartyReference": {"href": fpml_data.get("fixedLeg.payerPartyReference", "")},
        "receiverPartyReference": {"href": fpml_data.get("fixedLeg.receiverPartyReference", "")},
        "fixedPrice": {
            "price": fpml_data.get("fixedLeg.price", ""),
            "priceCurrency": fpml_data.get("fixedLeg.priceCurrency", ""),
            "priceUnit": fpml_data.get("fixedLeg.priceUnit", ""),
        },
        "notionalQuantity": {
            "quantityUnit": fpml_data.get("fixedLeg.quantityUnit", ""),
            "quantityFrequency": fpml_data.get("fixedLeg.quantityFrequency", ""),
            "quantity": fpml_data.get("fixedLeg.quantity", ""),
        },
    }

    # Floating leg
    swap["floatingLeg"] = {
        "payerPartyReference": {"href": fpml_data.get("floatingLeg.payerPartyReference", "")},
        "receiverPartyReference": {"href": fpml_data.get("floatingLeg.receiverPartyReference", "")},
        "commodity": {
            "instrumentId": fpml_data.get("floatingLeg.instrumentId", ""),
            "specifiedPrice": fpml_data.get("floatingLeg.specifiedPrice", ""),
        },
        "notionalQuantity": {
            "quantityUnit": fpml_data.get("floatingLeg.quantityUnit", ""),
            "quantityFrequency": fpml_data.get("floatingLeg.quantityFrequency", ""),
            "quantity": fpml_data.get("floatingLeg.quantity", ""),
        },
    }

    return swap
