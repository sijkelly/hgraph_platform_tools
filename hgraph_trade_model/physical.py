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

Shaped pricing/volume and multiple pricing periods are acknowledged but not fully implemented yet.
We will address that in a subsequent update as requested.
"""

import json
from typing import Dict, Any, List
from hgraph_trade_model.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml
)


def create_commodity_physical(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the commodity physical trade section based on sub-instrument type.

    :param trade_data: Dictionary containing hgraph trade data, must include `sub_instrument_type`.
    :return: A dictionary representing the commodityPhysical (or commodityForward/Swap) section.
    """
    # Forward propagation step 1: Map raw data
    global_mapping = get_global_mapping()
    physical_mapping = get_instrument_mapping("comm_physical")
    combined_mapping = {**global_mapping, **physical_mapping}
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    sub_instrument_type = fpml_data.get("sub_instrument_type", "")

    # Helper for adjustable date
    def build_adjustable_date(prefix: str) -> Dict[str, Any]:
        if fpml_data.get(f"{prefix}.unadjustedDate"):
            return {
                "adjustableDate": {
                    "unadjustedDate": fpml_data.get(f"{prefix}.unadjustedDate", ""),
                    "dateAdjustments": {
                        "businessDayConvention": fpml_data.get(f"{prefix}.businessDayConvention", "NotApplicable")
                    }
                }
            }
        return {}

    # Build effective/termination/value date depending on product type
    effective_date = build_adjustable_date("effectiveDate")
    termination_date = build_adjustable_date("terminationDate")
    value_date = build_adjustable_date("valueDate")

    # Settlement currency
    settlement_currency = fpml_data.get("settlementCurrency", "")

    # Forward propagation step 2: Identify product type
    # For physical trades, we may have commoditySwap or commodityForward as per the examples
    # bullionPhysical is often under commodityForward; others often under commoditySwap.
    product_type = "commoditySwap"
    if sub_instrument_type == "bullionPhysical":
        product_type = "commodityForward"

    # Common top-level structure
    top_level = {}
    if product_type == "commoditySwap":
        top_level["effectiveDate"] = effective_date
        top_level["terminationDate"] = termination_date
        top_level["settlementCurrency"] = settlement_currency
    else:
        # commodityForward might use valueDate instead of effective/termination date
        if value_date:
            top_level["valueDate"] = value_date

    # Forward propagation step 3: Build the physical leg
    # We'll define helper functions for each physical sub-instrument type.

    def build_delivery_quantity():
        # For physical trades, quantity can be fixed or scheduled (shaped)
        # We'll just show basic structure for now.
        # For shaped volumes or schedules, we would iterate through fpml_data arrays.
        if fpml_data.get("hasShapedQuantity"):
            # Example structure if shaped:
            return {
                "deliveryQuantity": {
                    # structured schedules from fpml_data...
                    "physicalQuantitySchedule": [
                        {
                            "quantityStep": [
                                {
                                    "quantityUnit": step.get("quantityUnit", ""),
                                    "quantityFrequency": step.get("quantityFrequency", ""),
                                    "quantity": step.get("quantity", "")
                                } for step in fpml_data.get("quantitySteps", [])
                            ],
                            "deliveryPeriodsScheduleReference": {
                                "href": fpml_data.get("deliveryPeriodsScheduleReference", "")
                            },
                            # Potentially multiple settlementPeriods references
                            "settlementPeriodsReference": [
                                {"href": ref} for ref in fpml_data.get("settlementPeriodsRefs", [])
                            ]
                        }
                    ]
                }
            }
        else:
            # Simple quantity
            return {
                "deliveryQuantity": {
                    "physicalQuantity": {
                        "quantityUnit": fpml_data.get("quantityUnit", ""),
                        "quantityFrequency": fpml_data.get("quantityFrequency", ""),
                        "quantity": fpml_data.get("quantity", "")
                    },
                    "totalPhysicalQuantity": {
                        "quantityUnit": fpml_data.get("totalQuantityUnit", ""),
                        "quantity": fpml_data.get("totalQuantity", "")
                    } if fpml_data.get("totalQuantity") else {}
                }
            }

    def build_delivery_conditions(prefix: str) -> Dict[str, Any]:
        # Delivery conditions differ by type
        # We'll just return fields if present.
        conditions = {}
        if sub_instrument_type == "gasPhysical":
            conditions["deliveryConditions"] = {
                "deliveryPoint": {
                    "@deliveryPointScheme": fpml_data.get("deliveryPointScheme", ""),
                    "#value": fpml_data.get("deliveryPoint", "")
                } if fpml_data.get("deliveryPoint") else {},
                "deliveryType": fpml_data.get("deliveryType", "")
            }
        elif sub_instrument_type == "oilPhysical":
            # Pipeline or transfer
            pipeline = {}
            if fpml_data.get("pipelineName"):
                pipeline = {
                    "pipelineName": {
                        "@pipelineScheme": fpml_data.get("pipelineScheme", ""),
                        "#value": fpml_data.get("pipelineName", "")
                    },
                    "withdrawalPoint": {
                        "@deliveryPointScheme": fpml_data.get("withdrawalPointScheme", ""),
                        "#value": fpml_data.get("withdrawalPoint", "")
                    },
                    "deliverableByBarge": fpml_data.get("deliverableByBarge", "false"),
                    "risk": fpml_data.get("risk", "")
                }
            conditions["deliveryConditions"] = {
                "pipeline": pipeline
            }
        elif sub_instrument_type == "electricityPhysical":
            delivery_data = {}
            if fpml_data.get("deliveryPoint"):
                delivery_data["deliveryPoint"] = {
                    "@deliveryPointScheme": fpml_data.get("deliveryPointScheme", ""),
                    "#value": fpml_data.get("deliveryPoint", "")
                }
            # Could handle firm/non-firm
            if fpml_data.get("deliveryType"):
                delivery_data["deliveryType"] = {"firm": {"forceMajeure": True}} if fpml_data.get("forceMajeure") else {"nonFirm": True}

            # Optional fields like deliveryZone
            if fpml_data.get("deliveryZone"):
                delivery_data["deliveryZone"] = {
                    "@deliveryPointScheme": fpml_data.get("deliveryZoneScheme", ""),
                    "#value": fpml_data.get("deliveryZone", "")
                }

            # electing party
            if fpml_data.get("electingPartyReference"):
                delivery_data["electingPartyReference"] = {"href": fpml_data.get("electingPartyReference", "")}

            conditions["deliveryConditions"] = delivery_data
        elif sub_instrument_type == "coalPhysical":
            # For coal:
            conditions["deliveryConditions"] = {}
            if fpml_data.get("deliveryAtSource") == "true":
                conditions["deliveryConditions"]["deliveryAtSource"] = True
            else:
                conditions["deliveryConditions"]["deliveryPoint"] = {
                    "@deliveryPointScheme": fpml_data.get("deliveryPointScheme", ""),
                    "#value": fpml_data.get("deliveryPoint", "")
                }

            # Additional coal fields
            conditions["deliveryConditions"]["quantityVariationAdjustment"] = fpml_data.get("quantityVariationAdjustment", "false")
            conditions["deliveryConditions"]["transportationEquipment"] = fpml_data.get("transportationEquipment", "")
            conditions["deliveryConditions"]["risk"] = fpml_data.get("risk", "")
        elif sub_instrument_type == "bullionPhysical":
            conditions["deliveryLocation"] = fpml_data.get("deliveryLocation", "")
        return conditions

    def build_product_details():
        # Details differ by type
        if sub_instrument_type == "gasPhysical":
            return {"gas": {"type": fpml_data.get("gasType", "NaturalGas"), "quality": fpml_data.get("quality", "")}}
        elif sub_instrument_type == "oilPhysical":
            return {
                "oil": {
                    "type": fpml_data.get("oilType", "Oil"),
                    "grade": fpml_data.get("oilGrade", "")
                }
            }
        elif sub_instrument_type == "electricityPhysical":
            # Could include settlement periods, loadType, electricity type
            elec_dict = {
                "electricity": {
                    "type": fpml_data.get("electricityType", "Electricity")
                }
            }
            return elec_dict
        elif sub_instrument_type == "coalPhysical":
            return {
                "coal": {
                    "type": fpml_data.get("coalType", ""),
                    "source": fpml_data.get("coalSource", ""),
                    "btuQualityAdjustment": fpml_data.get("btuQualityAdjustment", "")
                }
            }
        elif sub_instrument_type == "bullionPhysical":
            return {
                "bullionType": fpml_data.get("bullionType", ""),
                "deliveryLocation": fpml_data.get("deliveryLocation", "")
            }
        return {}

    def build_physical_leg():
        # Build leg depending on sub_instrument_type
        payer = {"href": fpml_data.get("payerPartyReference", "")}
        receiver = {"href": fpml_data.get("receiverPartyReference", "")}

        leg_key = ""
        if sub_instrument_type == "gasPhysical":
            leg_key = "gasPhysicalLeg"
        elif sub_instrument_type == "oilPhysical":
            leg_key = "oilPhysicalLeg"
        elif sub_instrument_type == "electricityPhysical":
            leg_key = "electricityPhysicalLeg"
        elif sub_instrument_type == "coalPhysical":
            leg_key = "coalPhysicalLeg"
        elif sub_instrument_type == "bullionPhysical":
            # Bullion might appear under commodityForward as bullionPhysicalLeg
            leg_key = "bullionPhysicalLeg"

        leg = {
            leg_key: {
                "payerPartyReference": payer,
                "receiverPartyReference": receiver
            }
        }

        # DeliveryPeriods or valueDates as required
        # For physical swaps typically have deliveryPeriods
        if fpml_data.get("hasDeliveryPeriods"):
            leg[leg_key]["deliveryPeriods"] = {
                "periodsSchedule": {
                    "id": "deliveryPeriods",  # Example, adjust as needed
                    "periodMultiplier": fpml_data.get("periodMultiplier", "1"),
                    "period": fpml_data.get("period", "T"),
                    "balanceOfFirstPeriod": fpml_data.get("balanceOfFirstPeriod", "false")
                }
            }

        # Add product details
        product_details = build_product_details()
        leg[leg_key].update(product_details)

        # Add delivery conditions
        conditions = build_delivery_conditions(leg_key)
        leg[leg_key].update(conditions)

        # Add quantity
        quantity_struct = build_delivery_quantity()
        leg[leg_key].update(quantity_struct)

        return leg

    # Forward propagation step 4: possibly add fixedLeg/floatingLeg if needed
    # In physical trades, we often have a physical leg plus a financial leg (fixed or floating)
    def build_fixed_leg():
        if fpml_data.get("hasFixedLeg"):
            fixed_leg = {
                "fixedLeg": {
                    "payerPartyReference": {"href": fpml_data.get("fixedLeg.payerPartyReference", "")},
                    "receiverPartyReference": {"href": fpml_data.get("fixedLeg.receiverPartyReference", "")},
                    # Price could be a single fixed price or a schedule
                    "fixedPrice": {
                        "price": fpml_data.get("fixedLeg.price", ""),
                        "priceCurrency": fpml_data.get("fixedLeg.priceCurrency", ""),
                        "priceUnit": fpml_data.get("fixedLeg.priceUnit", "")
                    },
                    "quantityReference": {"href": fpml_data.get("fixedLeg.quantityReference", "")},
                    # Payment dates could be masterAgreementPaymentDates or relativePaymentDates
                    "masterAgreementPaymentDates": fpml_data.get("fixedLeg.masterAgreementPaymentDates", "true") == "true"
                }
            }
            return fixed_leg
        return {}

    def build_floating_leg():
        if fpml_data.get("hasFloatingLeg"):
            floating_leg = {
                "floatingLeg": {
                    "payerPartyReference": {"href": fpml_data.get("floatingLeg.payerPartyReference", "")},
                    "receiverPartyReference": {"href": fpml_data.get("floatingLeg.receiverPartyReference", "")},
                    "commodity": {
                        "instrumentId": fpml_data.get("floatingLeg.instrumentId", ""),
                        "specifiedPrice": fpml_data.get("floatingLeg.specifiedPrice", "")
                    },
                    "quantityReference": {"href": fpml_data.get("floatingLeg.quantityReference", "")},
                    # Calculation details if needed
                    "calculation": {
                        "pricingDates": {
                            "calculationPeriodsScheduleReference": {"href": fpml_data.get("floatingLeg.calculationPeriodsScheduleReference", "")},
                            "dayType": fpml_data.get("floatingLeg.dayType", ""),
                            "dayDistribution": fpml_data.get("floatingLeg.dayDistribution", "")
                        },
                        "spread": {
                            "currency": fpml_data.get("floatingLeg.spreadCurrency", ""),
                            "amount": fpml_data.get("floatingLeg.spreadAmount", "")
                        }
                    },
                    "masterAgreementPaymentDates": fpml_data.get("floatingLeg.masterAgreementPaymentDates", "true") == "true"
                }
            }
            return floating_leg
        return {}

    # Build the top-level product dictionary
    if product_type == "commoditySwap":
        commodity_structure = {"commoditySwap": top_level}
    else:
        commodity_structure = {"commodityForward": top_level}

    # Add physical leg
    physical_leg = build_physical_leg()
    commodity_structure[list(commodity_structure.keys())[0]].update(physical_leg)

    # Add fixed/floating legs if present
    fixed_leg = build_fixed_leg()
    if fixed_leg:
        commodity_structure[list(commodity_structure.keys())[0]].update(fixed_leg)

    floating_leg = build_floating_leg()
    if floating_leg:
        commodity_structure[list(commodity_structure.keys())[0]].update(floating_leg)

    # Return the assembled structure
    return commodity_structure

# Example usage:
# if __name__ == "__main__":
#     sample_trade_data = {
#         "sub_instrument_type": "oilPhysical",
#         "oilType": "HeatingOil",
#         "oilGrade": "WTI",
#         "hasFixedLeg": True,
#         "fixedLeg.price": 1.45,
#         "fixedLeg.priceCurrency": "USD",
#         "fixedLeg.priceUnit": "GAL",
#         "fixedLeg.quantityReference": "deliveryQuantity",
#         "fixedLeg.masterAgreementPaymentDates": "true",
#         "hasDeliveryPeriods": True
#     }
#
#     commodity_physical = create_commodity_physical(sample_trade_data)
#     print(json.dumps(commodity_physical, indent=4))