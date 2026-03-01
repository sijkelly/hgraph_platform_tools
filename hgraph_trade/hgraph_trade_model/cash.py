"""
cash.py

This module provides functionality to create the cash trade section of a trade message.
Cash trades represent simple cash flow transactions (e.g., fees, settlements, margin payments)
that do not involve a derivative instrument but still need to be booked and tracked.

It uses global and instrument-specific mappings to convert hgraph keys to FpML-like fields,
and constructs a standardized cashTrade dictionary.
"""

from typing import Dict, Any
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
)


def create_cash_trade(trade_data: Dict[str, Any]) -> list:
    """
    Create a cash trade structure from raw hgraph trade data.

    Cash trades represent non-derivative cash flows such as:
    - Fee payments
    - Margin calls / returns
    - Settlement payments
    - Collateral transfers

    :param trade_data: Dictionary containing raw hgraph trade data.
    :return: A list containing a single dictionary with the cashTrade structure.
    """
    global_mapping = get_global_mapping()
    cash_mapping = get_instrument_mapping("cash")
    combined_mapping = {**global_mapping, **cash_mapping}

    fpml_data = map_hgraph_to_fpml(trade_data, combined_mapping)

    cash_trade = {
        "buySell": fpml_data.get("buySell", ""),
        "tradeDate": fpml_data.get("tradeDate", ""),
        "paymentDate": fpml_data.get("paymentDate", ""),
        "paymentAmount": {
            "amount": fpml_data.get("amount", ""),
            "currency": fpml_data.get("paymentCurrency", ""),
        },
        "payerPartyReference": {"href": fpml_data.get("payerPartyReference", "")},
        "receiverPartyReference": {"href": fpml_data.get("receiverPartyReference", "")},
        "cashFlowType": fpml_data.get("cashFlowType", ""),
    }

    # Add optional description / reference fields
    if fpml_data.get("description"):
        cash_trade["description"] = fpml_data.get("description", "")

    if fpml_data.get("relatedTradeId"):
        cash_trade["relatedTradeId"] = fpml_data.get("relatedTradeId", "")

    return [{
        "cashTrade": cash_trade,
        "metadata": {
            "type": "cashTrade",
            "version": "1.0"
        }
    }]
