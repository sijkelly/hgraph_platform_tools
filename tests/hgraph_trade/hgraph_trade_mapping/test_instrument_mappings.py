"""Tests for instrument_mappings — pricing instrument classification."""

import pytest
from hgraph_trade.hgraph_trade_mapping.instrument_mappings import map_pricing_instrument


@pytest.mark.parametrize(
    "pricing_instrument,expected_instrument,expected_sub",
    [
        # Swaps
        ("outright", "swap", "fixedFloat"),
        ("outright_strip", "swap", "fixedFloat"),
        ("calender_spread", "swap", "fixedFloat"),
        ("crack_spread", "swap", "floatFloat"),
        ("crack_spread_strip", "swap", "floatFloat"),
        # Futures
        ("future", "future", None),
        ("future_spread", "future", None),
        # Forwards
        ("forward", "forward", None),
        ("fx", "forward", None),
        # Physical
        ("physical", "physical", "fixedPhysical"),
        ("physical_index_spread", "physical", "indexPhysical"),
        # Options
        ("option", "option", "vanilla"),
        ("option_collar", "option", "vanilla"),
        # Swaptions
        ("swaption", "swaption", "vanilla"),
        # Cash
        ("cash", "cash", None),
    ],
)
def test_map_pricing_instrument(pricing_instrument, expected_instrument, expected_sub):
    instrument, sub = map_pricing_instrument(pricing_instrument)
    assert instrument == expected_instrument
    assert sub == expected_sub


def test_unknown_instrument_returns_original():
    instrument, sub = map_pricing_instrument("unknown_type")
    assert instrument == "unknown_type"
    assert sub is None


def test_whitespace_is_stripped():
    instrument, sub = map_pricing_instrument("  outright  ")
    assert instrument == "swap"
    assert sub == "fixedFloat"


def test_case_insensitive():
    instrument, sub = map_pricing_instrument("OUTRIGHT")
    assert instrument == "swap"
    assert sub == "fixedFloat"


def test_empty_string_returns_original():
    instrument, sub = map_pricing_instrument("")
    assert instrument == ""
    assert sub is None
