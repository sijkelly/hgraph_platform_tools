"""
decomposition.py

This module handles the decomposition of certain pricing instruments that need to be split
into multiple bookable trades.
"""

from typing import Dict, Any, List

def decompose_instrument(trade_data: Dict[str, Any], instrument: str, sub_instrument: str) -> List[Dict[str, Any]]:
    """
    Decompose a single hgraph pricing instrument into one or more bookable trade data sets.
    For example, a "calender_spread" might need to be turned into two separate trades.

    :param trade_data: Original hgraph trade data dictionary.
    :param instrument: Identified instrument (e.g. "swap")
    :param sub_instrument: Identified sub-instrument (e.g. "fixedFloat")
    :return: A list of trade_data subsets, each representing a separate bookable trade.
    """
    pricing_instrument = trade_data.get("pricing_instrument", "").strip().lower()

    if instrument == "swap" and sub_instrument == "fixedFloat":
        if pricing_instrument in ["calender_spread", "calender_spread_strip"]:
            # Example: split into two trades with different dates
            trade_data_1 = dict(trade_data)
            trade_data_2 = dict(trade_data)

            trade_data_1["effective_date"] = "2024-01-01"
            trade_data_1["termination_date"] = "2024-02-01"

            trade_data_2["effective_date"] = "2024-02-01"
            trade_data_2["termination_date"] = "2024-03-01"

            return [trade_data_1, trade_data_2]

        # Otherwise, no decomposition needed
        return [trade_data]

    # No decomposition for other instruments
    return [trade_data]