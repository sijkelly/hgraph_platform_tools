"""
swap.py

Enhances the commodity swap creation to include detailed fields aligned with FpML specifications
for fixedFloat and floatFloat sub-instruments. Fields such as effectiveDate, terminationDate,
settlementCurrency, payment schedules, and detailed leg structures are included.

Event-driven forward propagation graph (FPG) concept:
1. Build a graph of transformations from raw hgraph trade data to final commoditySwap structure.
2. Forward propagate data through these transformation nodes.
3. Each node event triggers the next stage, assembling the final dictionary.

We assume ignoring physical legs as requested, focusing only on financial legs (fixed, floating).
"""

import json
import sys
from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
    map_sub_instrument_type
)

def create_commodity_swap(trade_data: Dict[str, Any], sub_instrument_type: str) -> list:
    global_mapping = get_global_mapping()
    swap_mapping = get_instrument_mapping("swap")
    combined_mapping = {**global_mapping, **swap_mapping}

    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

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

    swap = {
        "effectiveDate": effective_date,
        "terminationDate": termination_date,
        "settlementCurrency": fpml_data.get("settlementCurrency", "")
    }

    def build_relative_payment_dates(leg_prefix: str) -> Dict[str, Any]:
        if fpml_data.get(f"{leg_prefix}.payRelativeTo"):
            return {
                "relativePaymentDates": {
                    "payRelativeTo": fpml_data.get(f"{leg_prefix}.payRelativeTo", "CalculationPeriodEndDate"),
                    "calculationPeriodsScheduleReference": {
                        "href": fpml_data.get(f"{leg_prefix}.calculationPeriodsScheduleReference", "")
                    },
                    "paymentDaysOffset": {
                        "periodMultiplier": fpml_data.get(f"{leg_prefix}.paymentDaysOffset.periodMultiplier", ""),
                        "period": fpml_data.get(f"{leg_prefix}.paymentDaysOffset.period", ""),
                        "dayType": fpml_data.get(f"{leg_prefix}.paymentDaysOffset.dayType", ""),
                        "businessDayConvention": fpml_data.get(f"{leg_prefix}.paymentDaysOffset.businessDayConvention", "NONE")
                    },
                    "businessCenters": {
                        "businessCenter": fpml_data.get(f"{leg_prefix}.businessCenters", [])
                    }
                }
            }
        return {}

    if sub_instrument_type == "fixedFloat":
        fixed_leg = {
            "fixedLeg": {
                "payerPartyReference": {"href": fpml_data.get("fixedLeg.payerPartyReference", "")},
                "receiverPartyReference": {"href": fpml_data.get("fixedLeg.receiverPartyReference", "")},
                "calculationPeriodsScheduleReference": {"href": fpml_data.get("fixedLeg.calculationPeriodsScheduleReference", "")},
                "fixedPrice": {
                    "price": fpml_data.get("fixedLeg.price", ""),
                    "priceCurrency": fpml_data.get("fixedLeg.priceCurrency", ""),
                    "priceUnit": fpml_data.get("fixedLeg.priceUnit", "")
                },
                "notionalQuantity": {
                    "quantityUnit": fpml_data.get("fixedLeg.quantityUnit", ""),
                    "quantityFrequency": fpml_data.get("fixedLeg.quantityFrequency", ""),
                    "quantity": fpml_data.get("fixedLeg.quantity", "")
                },
                "totalNotionalQuantity": fpml_data.get("fixedLeg.totalNotionalQuantity", "")
            }
        }
        fixed_leg.update(build_relative_payment_dates("fixedLeg"))

        float_leg = {
            "floatingLeg": {
                "payerPartyReference": {"href": fpml_data.get("floatingLeg.payerPartyReference", "")},
                "receiverPartyReference": {"href": fpml_data.get("floatingLeg.receiverPartyReference", "")},
                "calculationPeriodsSchedule": {
                    "id": fpml_data.get("floatingLeg.calculationPeriodsSchedule.id", ""),
                    "periodMultiplier": fpml_data.get("floatingLeg.periodMultiplier", ""),
                    "period": fpml_data.get("floatingLeg.period", ""),
                    "balanceOfFirstPeriod": fpml_data.get("floatingLeg.balanceOfFirstPeriod", "false")
                },
                "commodity": {
                    "instrumentId": fpml_data.get("floatingLeg.instrumentId", ""),
                    "specifiedPrice": fpml_data.get("floatingLeg.specifiedPrice", ""),
                    "deliveryDates": fpml_data.get("floatingLeg.deliveryDates", "")
                },
                "notionalQuantity": {
                    "quantityUnit": fpml_data.get("floatingLeg.quantityUnit", ""),
                    "quantityFrequency": fpml_data.get("floatingLeg.quantityFrequency", ""),
                    "quantity": fpml_data.get("floatingLeg.quantity", "")
                },
                "totalNotionalQuantity": fpml_data.get("floatingLeg.totalNotionalQuantity", "")
            }
        }
        float_leg["floatingLeg"].update(build_relative_payment_dates("floatingLeg"))

        swap.update(fixed_leg)
        swap.update(float_leg)

    elif sub_instrument_type == "floatFloat":
        float_leg1 = {
            "floatingLeg": {
                "id": "floatLeg1",
                "payerPartyReference": {"href": fpml_data.get("floatLeg1.payerPartyReference", "")},
                "receiverPartyReference": {"href": fpml_data.get("floatLeg1.receiverPartyReference", "")},
                "calculationPeriodsSchedule": {
                    "id": fpml_data.get("floatLeg1.calculationPeriodsSchedule.id", ""),
                    "periodMultiplier": fpml_data.get("floatLeg1.periodMultiplier", ""),
                    "period": fpml_data.get("floatLeg1.period", ""),
                    "balanceOfFirstPeriod": fpml_data.get("floatLeg1.balanceOfFirstPeriod", "false")
                },
                "commodity": {
                    "instrumentId": fpml_data.get("floatLeg1.instrumentId", ""),
                    "specifiedPrice": fpml_data.get("floatLeg1.specifiedPrice", ""),
                    "deliveryDates": fpml_data.get("floatLeg1.deliveryDates", "")
                },
                "notionalQuantity": {
                    "quantityUnit": fpml_data.get("floatLeg1.quantityUnit", ""),
                    "quantityFrequency": fpml_data.get("floatLeg1.quantityFrequency", ""),
                    "quantity": fpml_data.get("floatLeg1.quantity", "")
                },
                "totalNotionalQuantity": fpml_data.get("floatLeg1.totalNotionalQuantity", "")
            }
        }
        float_leg1["floatingLeg"].update(build_relative_payment_dates("floatLeg1"))

        float_leg2 = {
            "floatingLeg": {
                "id": "floatLeg2",
                "payerPartyReference": {"href": fpml_data.get("floatLeg2.payerPartyReference", "")},
                "receiverPartyReference": {"href": fpml_data.get("floatLeg2.receiverPartyReference", "")},
                "calculationPeriodsScheduleReference": {
                    "href": fpml_data.get("floatLeg2.calculationPeriodsScheduleReference", "")
                },
                "commodity": {
                    "instrumentId": fpml_data.get("floatLeg2.instrumentId", ""),
                    "specifiedPrice": fpml_data.get("floatLeg2.specifiedPrice", "")
                },
                "notionalQuantity": {
                    "quantityUnit": fpml_data.get("floatLeg2.quantityUnit", ""),
                    "quantityFrequency": fpml_data.get("floatLeg2.quantityFrequency", ""),
                    "quantity": fpml_data.get("floatLeg2.quantity", "")
                },
                "totalNotionalQuantity": fpml_data.get("floatLeg2.totalNotionalQuantity", ""),
                "calculation": {
                    "pricingDates": {
                        "calculationPeriodsScheduleReference": {
                            "href": fpml_data.get("floatLeg2.calculationPeriodsScheduleReference", "")
                        },
                        "dayType": fpml_data.get("floatLeg2.dayType", ""),
                        "dayDistribution": fpml_data.get("floatLeg2.dayDistribution", ""),
                        "businessCalendar": fpml_data.get("floatLeg2.businessCalendar", "")
                    },
                    "spread": {
                        "currency": fpml_data.get("floatLeg2.spreadCurrency", ""),
                        "amount": fpml_data.get("floatLeg2.spreadAmount", "")
                    }
                }
            }
        }
        float_leg2["floatingLeg"].update(build_relative_payment_dates("floatLeg2"))

        swap["floatLeg1"] = float_leg1["floatingLeg"]
        swap["floatLeg2"] = float_leg2["floatingLeg"]
    else:
        raise ValueError(
            f"Unsupported sub-instrument type: {sub_instrument_type}. "
            f"Ensure `sub_instrument` is one of: ['fixedFloat', 'floatFloat']"
        )

    return [{"commoditySwap": swap}]

def main() -> None:
    """
    Command line usage example:
    python swap.py '{"tradeType":"newTrade","sub_instrument":"fixed_float","quantity":10000,"quantityUnit":"MMBTU","floatingLeg.priceCurrency":"USD"}'
    """
    if len(sys.argv) < 2:
        print("Please provide a JSON string with trade data.")
        sys.exit(1)

    trade_data = json.loads(sys.argv[1])
    mapped_sub_instrument = map_sub_instrument_type(trade_data.get("sub_instrument", ""))
    commodity_swap = create_commodity_swap(trade_data, mapped_sub_instrument)
    print(json.dumps(commodity_swap, indent=4))


if __name__ == "__main__":
    main()