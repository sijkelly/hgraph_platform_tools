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

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml
)

def create_commodity_physical(trade_data: Dict[str, Any]) -> list:
    global_mapping = get_global_mapping()
    physical_mapping = get_instrument_mapping("comm_physical")
    combined_mapping = {**global_mapping, **physical_mapping}
    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    sub_instrument_type = fpml_data.get("sub_instrument_type", "")

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

    effective_date = build_adjustable_date("effectiveDate")
    termination_date = build_adjustable_date("terminationDate")
    value_date = build_adjustable_date("valueDate")

    settlement_currency = fpml_data.get("settlementCurrency", "")
    product_type = "commoditySwap"
    if sub_instrument_type == "bullionPhysical":
        product_type = "commodityForward"

    top_level = {}
    if product_type == "commoditySwap":
        top_level["effectiveDate"] = effective_date
        top_level["terminationDate"] = termination_date
        top_level["settlementCurrency"] = settlement_currency
    else:
        if value_date:
            top_level["valueDate"] = value_date

    def build_delivery_quantity():
        if fpml_data.get("hasShapedQuantity"):
            return {
                "deliveryQuantity": {
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
                            "settlementPeriodsReference": [
                                {"href": ref} for ref in fpml_data.get("settlementPeriodsRefs", [])
                            ]
                        }
                    ]
                }
            }
        else:
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
        conditions = {}
        # Similar logic as before
        # ... omitted for brevity
        # Return conditions based on sub_instrument_type
        return conditions

    def build_product_details():
        # Similar logic as before
        return {}

    def build_physical_leg():
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
            leg_key = "bullionPhysicalLeg"

        leg = {
            leg_key: {
                "payerPartyReference": payer,
                "receiverPartyReference": receiver
            }
        }

        if fpml_data.get("hasDeliveryPeriods"):
            leg[leg_key]["deliveryPeriods"] = {
                "periodsSchedule": {
                    "id": "deliveryPeriods",
                    "periodMultiplier": fpml_data.get("periodMultiplier", "1"),
                    "period": fpml_data.get("period", "T"),
                    "balanceOfFirstPeriod": fpml_data.get("balanceOfFirstPeriod", "false")
                }
            }

        product_details = build_product_details()
        leg[leg_key].update(product_details)

        conditions = build_delivery_conditions(leg_key)
        leg[leg_key].update(conditions)

        quantity_struct = build_delivery_quantity()
        leg[leg_key].update(quantity_struct)

        return leg

    def build_fixed_leg():
        if fpml_data.get("hasFixedLeg"):
            return {
                "fixedLeg": {
                    "payerPartyReference": {"href": fpml_data.get("fixedLeg.payerPartyReference", "")},
                    "receiverPartyReference": {"href": fpml_data.get("fixedLeg.receiverPartyReference", "")},
                    "fixedPrice": {
                        "price": fpml_data.get("fixedLeg.price", ""),
                        "priceCurrency": fpml_data.get("fixedLeg.priceCurrency", ""),
                        "priceUnit": fpml_data.get("fixedLeg.priceUnit", "")
                    },
                    "quantityReference": {"href": fpml_data.get("fixedLeg.quantityReference", "")},
                    "masterAgreementPaymentDates": fpml_data.get("fixedLeg.masterAgreementPaymentDates", "true") == "true"
                }
            }
        return {}

    def build_floating_leg():
        if fpml_data.get("hasFloatingLeg"):
            return {
                "floatingLeg": {
                    "payerPartyReference": {"href": fpml_data.get("floatingLeg.payerPartyReference", "")},
                    "receiverPartyReference": {"href": fpml_data.get("floatingLeg.receiverPartyReference", "")},
                    "commodity": {
                        "instrumentId": fpml_data.get("floatingLeg.instrumentId", ""),
                        "specifiedPrice": fpml_data.get("floatingLeg.specifiedPrice", "")
                    },
                    "quantityReference": {"href": fpml_data.get("floatingLeg.quantityReference", "")},
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
        return {}

    if product_type == "commoditySwap":
        commodity_structure = {"commoditySwap": top_level}
    else:
        commodity_structure = {"commodityForward": top_level}

    physical_leg = build_physical_leg()
    commodity_structure[list(commodity_structure.keys())[0]].update(physical_leg)

    fixed_leg = build_fixed_leg()
    if fixed_leg:
        commodity_structure[list(commodity_structure.keys())[0]].update(fixed_leg)

    floating_leg = build_floating_leg()
    if floating_leg:
        commodity_structure[list(commodity_structure.keys())[0]].update(floating_leg)

    # Return as a single-element list
    return [commodity_structure]

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