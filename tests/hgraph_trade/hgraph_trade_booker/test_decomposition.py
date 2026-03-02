"""Tests for decomposition — splitting complex trades."""

import pytest
from hgraph_trade.hgraph_trade_booker.decomposition import decompose_instrument


@pytest.mark.parametrize(
    "fixture_name,instrument,sub",
    [
        ("base_trade_data", "forward", None),
        ("option_data", "option", "vanilla"),
        ("future_data", "future", None),
        ("fx_data", "forward", None),
        ("cash_data", "cash", None),
    ],
)
def test_non_decomposable_returns_single_item(fixture_name, instrument, sub, request):
    data = request.getfixturevalue(fixture_name)
    result = decompose_instrument(data, instrument, sub)
    assert isinstance(result, list)
    assert len(result) == 1


def test_non_decomposable_preserves_data(base_trade_data):
    result = decompose_instrument(base_trade_data, "forward", None)
    assert result[0]["trade_id"] == "TEST-001"
