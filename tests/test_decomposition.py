"""
Tests for hgraph_trade.hgraph_trade_booker.decomposition
"""

import pytest
from hgraph_trade.hgraph_trade_booker.decomposition import decompose_instrument


class TestDecomposeInstrument:
    """Tests for the decompose_instrument function."""

    def test_non_decomposable_returns_single_item(self, base_trade_data):
        result = decompose_instrument(base_trade_data, "forward", None)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_non_decomposable_preserves_data(self, base_trade_data):
        result = decompose_instrument(base_trade_data, "forward", None)
        assert result[0]["trade_id"] == "TEST-001"

    def test_option_not_decomposed(self, option_data):
        result = decompose_instrument(option_data, "option", "vanilla")
        assert len(result) == 1

    def test_future_not_decomposed(self, future_data):
        result = decompose_instrument(future_data, "future", None)
        assert len(result) == 1

    def test_fx_not_decomposed(self, fx_data):
        result = decompose_instrument(fx_data, "forward", None)
        assert len(result) == 1

    def test_cash_not_decomposed(self, cash_data):
        result = decompose_instrument(cash_data, "cash", None)
        assert len(result) == 1
