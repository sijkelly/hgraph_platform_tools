"""
instrument_mappings.py

This module defines mappings and utility functions to translate hgraph pricing instruments
to hgraph_trade_model instruments and sub-instruments. It also provides instrument and
sub-instrument mapping logic previously contained in fpml_mappings.py.
"""

from typing import Tuple, Optional

# Mapping from hgraph_pricing_instrument to (instrument, sub_instrument).
# If sub_instrument is N/A, we represent it as None.
PRICING_TO_BOOKABLE = {
    "outright": ("swap", "fixedFloat"),
    "outright_strip": ("swap", "fixedFloat"),
    "calender_spread": ("swap", "fixedFloat"),
    "calender_spread_strip": ("swap", "fixedFloat"),
    "calender_spread_flfl": ("swap", "floatFloat"),
    "crack_spread": ("swap", "floatFloat"),
    "crack_spread_strip": ("swap", "floatFloat"),
    "outright_box": ("swap", "fixedFloat"),
    "crack_spread_box": ("swap", "floatFloat"),

    "future": ("future", None),
    "future_spread": ("future", None),
    "forward": ("forward", None),
    "forward_spread": ("forward", None),
    "fx": ("forward", None),
    "fx_spread": ("forward", None),
    "cash": ("cash", None),

    "physical": ("physical", "fixedPhysical"),
    "physical_cal_spread": ("physical", "fixedPhysical"),
    "physical_index_spread": ("physical", "indexPhysical"),
    "physical_index_spread_box": ("physical", "indexPhysical"),
    "physical_strip": ("physical", "fixedPhysical"),
    "physical_cal_spread_strip": ("physical", "fixedPhysical"),
    "physical_index_spread_strip": ("physical", "indexPhysical"),
    "physical_index_spread_box_strip": ("physical", "indexPhysical"),

    "option": ("option", "vanilla"),
    "option_spread": ("option", "vanilla"),
    "option_strip": ("option", "vanilla"),
    "option_spread_strip": ("option", "vanilla"),
    "option_collar": ("option", "vanilla"),
    "option_collar_strip": ("option", "vanilla"),

    "swaption": ("swaption", "vanilla"),
    "swaption_spread": ("swaption", "vanilla"),
    "swaption_strip": ("swaption", "vanilla"),
    "swaption_spread_strip": ("swaption", "vanilla"),
    "swaption_collar": ("swaption", "vanilla"),
    "swaption_collar_strip": ("swaption", "vanilla"),
}


def map_pricing_instrument(hgraph_pricing_instrument: str) -> Tuple[str, Optional[str]]:
    """
    Given an hgraph_pricing_instrument type, return the corresponding instrument and sub-instrument
    as per the defined mappings. If not found, return the original and None.

    :param hgraph_pricing_instrument: The pricing instrument from hgraph.
    :return: A tuple (instrument, sub_instrument) where sub_instrument can be None.
    """
    normalized = hgraph_pricing_instrument.strip().lower()
    return PRICING_TO_BOOKABLE.get(normalized, (normalized, None))