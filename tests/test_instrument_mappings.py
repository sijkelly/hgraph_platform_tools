"""
Tests for hgraph_trade.hgraph_trade_mapping.instrument_mappings
"""

import pytest
from hgraph_trade.hgraph_trade_mapping.instrument_mappings import map_pricing_instrument


class TestMapPricingInstrument:
    """Tests for the map_pricing_instrument function."""

    # --- Swap instruments ---

    def test_outright_maps_to_swap_fixed_float(self):
        instrument, sub = map_pricing_instrument("outright")
        assert instrument == "swap"
        assert sub == "fixedFloat"

    def test_outright_strip_maps_to_swap_fixed_float(self):
        instrument, sub = map_pricing_instrument("outright_strip")
        assert instrument == "swap"
        assert sub == "fixedFloat"

    def test_calender_spread_maps_to_swap_fixed_float(self):
        instrument, sub = map_pricing_instrument("calender_spread")
        assert instrument == "swap"
        assert sub == "fixedFloat"

    def test_crack_spread_maps_to_swap_float_float(self):
        instrument, sub = map_pricing_instrument("crack_spread")
        assert instrument == "swap"
        assert sub == "floatFloat"

    def test_crack_spread_strip_maps_to_swap_float_float(self):
        instrument, sub = map_pricing_instrument("crack_spread_strip")
        assert instrument == "swap"
        assert sub == "floatFloat"

    # --- Future instruments ---

    def test_future_maps_to_future(self):
        instrument, sub = map_pricing_instrument("future")
        assert instrument == "future"
        assert sub is None

    def test_future_spread_maps_to_future(self):
        instrument, sub = map_pricing_instrument("future_spread")
        assert instrument == "future"
        assert sub is None

    # --- Forward instruments ---

    def test_forward_maps_to_forward(self):
        instrument, sub = map_pricing_instrument("forward")
        assert instrument == "forward"
        assert sub is None

    def test_fx_maps_to_forward(self):
        instrument, sub = map_pricing_instrument("fx")
        assert instrument == "forward"
        assert sub is None

    # --- Physical instruments ---

    def test_physical_maps_to_physical_fixed(self):
        instrument, sub = map_pricing_instrument("physical")
        assert instrument == "physical"
        assert sub == "fixedPhysical"

    def test_physical_index_spread_maps_to_physical_index(self):
        instrument, sub = map_pricing_instrument("physical_index_spread")
        assert instrument == "physical"
        assert sub == "indexPhysical"

    # --- Option instruments ---

    def test_option_maps_to_option_vanilla(self):
        instrument, sub = map_pricing_instrument("option")
        assert instrument == "option"
        assert sub == "vanilla"

    def test_option_collar_maps_to_option_vanilla(self):
        instrument, sub = map_pricing_instrument("option_collar")
        assert instrument == "option"
        assert sub == "vanilla"

    # --- Swaption instruments ---

    def test_swaption_maps_to_swaption_vanilla(self):
        instrument, sub = map_pricing_instrument("swaption")
        assert instrument == "swaption"
        assert sub == "vanilla"

    # --- Cash instruments ---

    def test_cash_maps_to_cash(self):
        instrument, sub = map_pricing_instrument("cash")
        assert instrument == "cash"
        assert sub is None

    # --- Edge cases ---

    def test_unknown_instrument_returns_original(self):
        instrument, sub = map_pricing_instrument("unknown_type")
        assert instrument == "unknown_type"
        assert sub is None

    def test_whitespace_is_stripped(self):
        instrument, sub = map_pricing_instrument("  outright  ")
        assert instrument == "swap"
        assert sub == "fixedFloat"

    def test_case_insensitive(self):
        instrument, sub = map_pricing_instrument("OUTRIGHT")
        assert instrument == "swap"
        assert sub == "fixedFloat"

    def test_empty_string_returns_original(self):
        instrument, sub = map_pricing_instrument("")
        assert instrument == ""
        assert sub is None
