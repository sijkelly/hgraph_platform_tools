"""
Tests for hgraph_trade.hgraph_trade_mapping.fpml_mappings
"""

import pytest
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import (
    map_hgraph_to_fpml,
    get_global_mapping,
    get_instrument_mapping,
)


class TestGetGlobalMapping:
    """Tests for the get_global_mapping function."""

    def test_returns_dict(self):
        mapping = get_global_mapping()
        assert isinstance(mapping, dict)

    def test_contains_core_fields(self):
        mapping = get_global_mapping()
        assert "buy_sell" in mapping
        assert "trade_date" in mapping
        assert "trade_id" in mapping
        assert "effective_date" in mapping
        assert "termination_date" in mapping

    def test_maps_buy_sell_to_fpml(self):
        mapping = get_global_mapping()
        assert mapping["buy_sell"] == "buySell"

    def test_maps_trade_date_to_fpml(self):
        mapping = get_global_mapping()
        assert mapping["trade_date"] == "tradeDate"


class TestGetInstrumentMapping:
    """Tests for the get_instrument_mapping function."""

    def test_swap_mapping_exists(self):
        mapping = get_instrument_mapping("swap")
        assert isinstance(mapping, dict)
        assert len(mapping) > 0

    def test_option_mapping_exists(self):
        mapping = get_instrument_mapping("option")
        assert "strike_price" in mapping

    def test_forward_mapping_exists(self):
        mapping = get_instrument_mapping("forward")
        assert "fixed_price" in mapping

    def test_future_mapping_exists(self):
        mapping = get_instrument_mapping("future")
        assert "contract_price" in mapping

    def test_physical_mapping_has_real_fields(self):
        mapping = get_instrument_mapping("physical")
        assert "delivery_point" in mapping
        assert "delivery_location" in mapping

    def test_swaption_mapping_has_real_fields(self):
        mapping = get_instrument_mapping("swaption")
        assert "strike_price" in mapping
        assert "expiration_date" in mapping

    def test_cash_mapping_exists(self):
        mapping = get_instrument_mapping("cash")
        assert "payment_date" in mapping
        assert "cash_flow_type" in mapping

    def test_fx_mapping_exists(self):
        mapping = get_instrument_mapping("fx")
        assert "currency_pair" in mapping

    def test_unknown_instrument_returns_empty_dict(self):
        mapping = get_instrument_mapping("nonexistent")
        assert mapping == {}


class TestMapHgraphToFpml:
    """Tests for the map_hgraph_to_fpml function."""

    def test_maps_simple_fields(self):
        trade_data = {"buy_sell": "Buy", "trade_date": "2024-01-01"}
        mapping = {"buy_sell": "buySell", "trade_date": "tradeDate"}
        result = map_hgraph_to_fpml(trade_data, mapping)
        assert result["buySell"] == "Buy"
        assert result["tradeDate"] == "2024-01-01"

    def test_unmapped_keys_pass_through(self):
        trade_data = {"unknown_field": "value123"}
        mapping = {"buy_sell": "buySell"}
        result = map_hgraph_to_fpml(trade_data, mapping)
        assert result["unknown_field"] == "value123"

    def test_nested_dicts_mapped_recursively(self):
        trade_data = {
            "counterparty": {
                "internal_party": "InternalCo",
                "external_party": "ExternalCo"
            }
        }
        mapping = {
            "internal_party": "internalParty",
            "external_party": "externalParty"
        }
        result = map_hgraph_to_fpml(trade_data, mapping)
        assert result["counterparty"]["internalParty"] == "InternalCo"
        assert result["counterparty"]["externalParty"] == "ExternalCo"

    def test_empty_trade_data_returns_empty(self):
        result = map_hgraph_to_fpml({}, {"buy_sell": "buySell"})
        assert result == {}

    def test_empty_mapping_passes_through(self):
        trade_data = {"buy_sell": "Buy"}
        result = map_hgraph_to_fpml(trade_data, {})
        assert result["buy_sell"] == "Buy"

    def test_preserves_numeric_values(self):
        trade_data = {"qty": 10000, "strike_price": 4.50}
        mapping = {"qty": "quantity", "strike_price": "strikePrice"}
        result = map_hgraph_to_fpml(trade_data, mapping)
        assert result["quantity"] == 10000
        assert result["strikePrice"] == 4.50

    def test_preserves_list_values(self):
        trade_data = {"exercise_dates": ["2025-06-01", "2025-09-01"]}
        mapping = {"exercise_dates": "exerciseDates"}
        result = map_hgraph_to_fpml(trade_data, mapping)
        assert result["exerciseDates"] == ["2025-06-01", "2025-09-01"]
