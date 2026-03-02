"""
physical.py

This module provides functionality to create the commodity physical trade section
of a trade message. It uses global and instrument-specific mappings to translate hgraph
trade data into FpML-like keys. Depending on the sub-instrument type (e.g., gasPhysical,
oilPhysical, electricityPhysical, coalPhysical, bullionPhysical), it constructs a structured
dictionary representing the physical leg(s).

This code uses a forward-propagation graph (FPG) style:
1. Map raw data to fpml_data.
2. Construct top-level adjustableDates, settlementCurrency, etc.
3. Build the specific physical leg structure based on sub-instrument.
4. Add fixed/floating legs if present.
5. Return the assembled commodityPhysical dictionary.

Supports:
- Simple and shaped delivery quantities
- Delivery period scheduling
- Commodity-specific product details (gas, oil, electricity, coal, bullion)
- Delivery conditions per commodity type
- Fixed and floating pricing legs
"""

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
)

__all__ = (
    "PHYSICAL_SUB_INSTRUMENTS",
    "SUB_INSTRUMENT_TO_LEG_KEY",
    "create_commodity_physical",
)

# Valid physical sub-instrument types
PHYSICAL_SUB_INSTRUMENTS = [
    "gasPhysical",
    "oilPhysical",
    "electricityPhysical",
    "coalPhysical",
    "bullionPhysical",
]

# Mapping from sub-instrument type to the FpML leg key
SUB_INSTRUMENT_TO_LEG_KEY = {
    "gasPhysical": "gasPhysicalLeg",
    "oilPhysical": "oilPhysicalLeg",
    "electricityPhysical": "electricityPhysicalLeg",
    "coalPhysical": "coalPhysicalLeg",
    "bullionPhysical": "bullionPhysicalLeg",
}


def create_commodity_physical(trade_data: Dict[str, Any]) -> list:
    """
    Create a commodity physical trade structure from raw hgraph trade data.

    :param trade_data: Dictionary containing raw hgraph trade data.
    :return: A list containing a single dictionary with the commodity physical structure.
    """
    global_mapping = get_global_mapping()
    physical_mapping = get_instrument_mapping("physical")
    combined_mapping = {**global_mapping, **physical_mapping}
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    sub_instrument_type = fpml_data.get("sub_instrument_type", "")

    if sub_instrument_type and sub_instrument_type not in PHYSICAL_SUB_INSTRUMENTS:
        raise ValueError(
            f"Invalid physical sub-instrument type: {sub_instrument_type}. "
            f"Expected one of {PHYSICAL_SUB_INSTRUMENTS}."
        )

    effective_date = _build_adjustable_date(fpml_data, "effectiveDate")
    termination_date = _build_adjustable_date(fpml_data, "terminationDate")
    value_date = _build_adjustable_date(fpml_data, "valueDate")

    settlement_currency = fpml_data.get("settlementCurrency", "")

    # Bullion physicals are structured as forwards; all others as swaps
    product_type = "commodityForward" if sub_instrument_type == "bullionPhysical" else "commoditySwap"

    top_level = {}
    if product_type == "commoditySwap":
        top_level["effectiveDate"] = effective_date
        top_level["terminationDate"] = termination_date
        top_level["settlementCurrency"] = settlement_currency
    else:
        if value_date:
            top_level["valueDate"] = value_date

    # Build the physical leg
    physical_leg = _build_physical_leg(fpml_data, sub_instrument_type)
    top_level.update(physical_leg)

    # Add optional fixed leg
    fixed_leg = _build_fixed_leg(fpml_data)
    if fixed_leg:
        top_level.update(fixed_leg)

    # Add optional floating leg
    floating_leg = _build_floating_leg(fpml_data)
    if floating_leg:
        top_level.update(floating_leg)

    commodity_structure = {product_type: top_level}

    return [commodity_structure]


def _build_adjustable_date(fpml_data: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """
    Build an adjustable date structure from mapped FpML data.

    :param fpml_data: Mapped trade data dictionary.
    :param prefix: The date field prefix (e.g., "effectiveDate").
    :return: Dictionary with adjustableDate structure, or empty dict if no date found.
    """
    unadjusted_date = fpml_data.get(f"{prefix}.unadjustedDate") or fpml_data.get(prefix, "")
    if unadjusted_date:
        return {
            "adjustableDate": {
                "unadjustedDate": unadjusted_date,
                "dateAdjustments": {
                    "businessDayConvention": fpml_data.get(f"{prefix}.businessDayConvention", "NotApplicable")
                },
            }
        }
    return {}


def _build_delivery_quantity(fpml_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the delivery quantity structure, supporting both simple and shaped quantities.

    :param fpml_data: Mapped trade data dictionary.
    :return: Dictionary with deliveryQuantity structure.
    """
    if fpml_data.get("hasShapedQuantity"):
        return {
            "deliveryQuantity": {
                "physicalQuantitySchedule": [
                    {
                        "quantityStep": [
                            {
                                "quantityUnit": step.get("quantityUnit", ""),
                                "quantityFrequency": step.get("quantityFrequency", ""),
                                "quantity": step.get("quantity", ""),
                            }
                            for step in fpml_data.get("quantitySteps", [])
                        ],
                        "deliveryPeriodsScheduleReference": {
                            "href": fpml_data.get("deliveryPeriodsScheduleReference", "")
                        },
                        "settlementPeriodsReference": [
                            {"href": ref} for ref in fpml_data.get("settlementPeriodsRefs", [])
                        ],
                    }
                ]
            }
        }
    else:
        delivery_quantity = {
            "physicalQuantity": {
                "quantityUnit": fpml_data.get("quantityUnit", ""),
                "quantityFrequency": fpml_data.get("quantityFrequency", ""),
                "quantity": fpml_data.get("quantity", ""),
            }
        }
        if fpml_data.get("totalQuantity"):
            delivery_quantity["totalPhysicalQuantity"] = {
                "quantityUnit": fpml_data.get("totalQuantityUnit", ""),
                "quantity": fpml_data.get("totalQuantity", ""),
            }
        return {"deliveryQuantity": delivery_quantity}


def _build_delivery_conditions(fpml_data: Dict[str, Any], sub_instrument_type: str) -> Dict[str, Any]:
    """
    Build delivery conditions based on the physical commodity sub-instrument type.

    :param fpml_data: Mapped trade data dictionary.
    :param sub_instrument_type: The physical sub-instrument type.
    :return: Dictionary with delivery condition fields.
    """
    conditions = {}

    if sub_instrument_type == "gasPhysical":
        conditions["deliveryPoint"] = fpml_data.get("deliveryPoint", "")
        conditions["interconnectionPoint"] = fpml_data.get("interconnectionPoint", "")
        if fpml_data.get("deliveryType"):
            conditions["deliveryType"] = fpml_data.get("deliveryType", "")

    elif sub_instrument_type == "oilPhysical":
        conditions["deliveryLocation"] = fpml_data.get("deliveryLocation", "")
        if fpml_data.get("pipeline"):
            conditions["pipeline"] = fpml_data.get("pipeline", "")
        if fpml_data.get("deliveryTerms"):
            conditions["deliveryTerms"] = fpml_data.get("deliveryTerms", "")

    elif sub_instrument_type == "electricityPhysical":
        conditions["deliveryPoint"] = fpml_data.get("deliveryPoint", "")
        conditions["loadType"] = fpml_data.get("loadType", "")
        if fpml_data.get("transmissionContingency"):
            conditions["transmissionContingency"] = fpml_data.get("transmissionContingency", "")
        if fpml_data.get("interconnectionPoint"):
            conditions["interconnectionPoint"] = fpml_data.get("interconnectionPoint", "")

    elif sub_instrument_type == "coalPhysical":
        conditions["deliveryLocation"] = fpml_data.get("deliveryLocation", "")
        if fpml_data.get("coalQualityAdjustments"):
            conditions["coalQualityAdjustments"] = fpml_data.get("coalQualityAdjustments", "")
        if fpml_data.get("transportationTerms"):
            conditions["transportationTerms"] = fpml_data.get("transportationTerms", "")

    elif sub_instrument_type == "bullionPhysical":
        conditions["deliveryLocation"] = fpml_data.get("deliveryLocation", "")
        if fpml_data.get("bullionType"):
            conditions["bullionType"] = fpml_data.get("bullionType", "")
        if fpml_data.get("fineness"):
            conditions["fineness"] = fpml_data.get("fineness", "")

    return conditions


def _build_product_details(fpml_data: Dict[str, Any], sub_instrument_type: str) -> Dict[str, Any]:
    """
    Build commodity-specific product detail fields.

    :param fpml_data: Mapped trade data dictionary.
    :param sub_instrument_type: The physical sub-instrument type.
    :return: Dictionary with product-specific detail fields.
    """
    details = {}

    if sub_instrument_type == "gasPhysical":
        if fpml_data.get("gasType"):
            details["gasType"] = fpml_data.get("gasType", "")
        if fpml_data.get("calorificValue"):
            details["calorificValue"] = fpml_data.get("calorificValue", "")

    elif sub_instrument_type == "oilPhysical":
        if fpml_data.get("oilType"):
            details["oilType"] = fpml_data.get("oilType", "")
        if fpml_data.get("oilGrade"):
            details["oilGrade"] = fpml_data.get("oilGrade", "")
        if fpml_data.get("apiGravity"):
            details["apiGravity"] = fpml_data.get("apiGravity", "")
        if fpml_data.get("sulfurContent"):
            details["sulfurContent"] = fpml_data.get("sulfurContent", "")

    elif sub_instrument_type == "electricityPhysical":
        if fpml_data.get("electricityType"):
            details["electricityType"] = fpml_data.get("electricityType", "")
        if fpml_data.get("voltage"):
            details["voltage"] = fpml_data.get("voltage", "")

    elif sub_instrument_type == "coalPhysical":
        if fpml_data.get("coalType"):
            details["coalType"] = fpml_data.get("coalType", "")
        if fpml_data.get("btuPerPound"):
            details["btuPerPound"] = fpml_data.get("btuPerPound", "")
        if fpml_data.get("ashContent"):
            details["ashContent"] = fpml_data.get("ashContent", "")
        if fpml_data.get("sulfurContent"):
            details["sulfurContent"] = fpml_data.get("sulfurContent", "")
        if fpml_data.get("moistureContent"):
            details["moistureContent"] = fpml_data.get("moistureContent", "")

    elif sub_instrument_type == "bullionPhysical":
        if fpml_data.get("bullionType"):
            details["bullionType"] = fpml_data.get("bullionType", "")
        if fpml_data.get("fineness"):
            details["fineness"] = fpml_data.get("fineness", "")
        if fpml_data.get("weight"):
            details["weight"] = fpml_data.get("weight", "")
        if fpml_data.get("weightUnit"):
            details["weightUnit"] = fpml_data.get("weightUnit", "")

    return details


def _build_physical_leg(fpml_data: Dict[str, Any], sub_instrument_type: str) -> Dict[str, Any]:
    """
    Build the physical leg structure based on the commodity sub-instrument type.

    :param fpml_data: Mapped trade data dictionary.
    :param sub_instrument_type: The physical sub-instrument type.
    :return: Dictionary with the physical leg keyed by the appropriate FpML leg name.
    """
    payer = {"href": fpml_data.get("payerPartyReference", "")}
    receiver = {"href": fpml_data.get("receiverPartyReference", "")}

    leg_key = SUB_INSTRUMENT_TO_LEG_KEY.get(sub_instrument_type, "physicalLeg")

    leg = {
        "payerPartyReference": payer,
        "receiverPartyReference": receiver,
    }

    # Add delivery periods if present
    if fpml_data.get("hasDeliveryPeriods"):
        leg["deliveryPeriods"] = {
            "periodsSchedule": {
                "id": "deliveryPeriods",
                "periodMultiplier": fpml_data.get("periodMultiplier", "1"),
                "period": fpml_data.get("period", "T"),
                "balanceOfFirstPeriod": fpml_data.get("balanceOfFirstPeriod", "false"),
            }
        }

    # Add product-specific details
    product_details = _build_product_details(fpml_data, sub_instrument_type)
    leg.update(product_details)

    # Add delivery conditions
    conditions = _build_delivery_conditions(fpml_data, sub_instrument_type)
    leg.update(conditions)

    # Add delivery quantity
    quantity_struct = _build_delivery_quantity(fpml_data)
    leg.update(quantity_struct)

    return {leg_key: leg}


def _build_fixed_leg(fpml_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the optional fixed pricing leg for a physical trade.

    :param fpml_data: Mapped trade data dictionary.
    :return: Dictionary with fixedLeg structure, or empty dict if not applicable.
    """
    if fpml_data.get("hasFixedLeg"):
        return {
            "fixedLeg": {
                "payerPartyReference": {"href": fpml_data.get("fixedLeg.payerPartyReference", "")},
                "receiverPartyReference": {"href": fpml_data.get("fixedLeg.receiverPartyReference", "")},
                "fixedPrice": {
                    "price": fpml_data.get("fixedLeg.price", ""),
                    "priceCurrency": fpml_data.get("fixedLeg.priceCurrency", ""),
                    "priceUnit": fpml_data.get("fixedLeg.priceUnit", ""),
                },
                "quantityReference": {"href": fpml_data.get("fixedLeg.quantityReference", "")},
                "masterAgreementPaymentDates": fpml_data.get("fixedLeg.masterAgreementPaymentDates", "true") == "true",
            }
        }
    return {}


def _build_floating_leg(fpml_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the optional floating pricing leg for a physical trade.

    :param fpml_data: Mapped trade data dictionary.
    :return: Dictionary with floatingLeg structure, or empty dict if not applicable.
    """
    if fpml_data.get("hasFloatingLeg"):
        return {
            "floatingLeg": {
                "payerPartyReference": {"href": fpml_data.get("floatingLeg.payerPartyReference", "")},
                "receiverPartyReference": {"href": fpml_data.get("floatingLeg.receiverPartyReference", "")},
                "commodity": {
                    "instrumentId": fpml_data.get("floatingLeg.instrumentId", ""),
                    "specifiedPrice": fpml_data.get("floatingLeg.specifiedPrice", ""),
                },
                "quantityReference": {"href": fpml_data.get("floatingLeg.quantityReference", "")},
                "calculation": {
                    "pricingDates": {
                        "calculationPeriodsScheduleReference": {
                            "href": fpml_data.get("floatingLeg.calculationPeriodsScheduleReference", "")
                        },
                        "dayType": fpml_data.get("floatingLeg.dayType", ""),
                        "dayDistribution": fpml_data.get("floatingLeg.dayDistribution", ""),
                    },
                    "spread": {
                        "currency": fpml_data.get("floatingLeg.spreadCurrency", ""),
                        "amount": fpml_data.get("floatingLeg.spreadAmount", ""),
                    },
                },
                "masterAgreementPaymentDates": fpml_data.get("floatingLeg.masterAgreementPaymentDates", "true")
                == "true",
            }
        }
    return {}
