"""Tests for fpml_mappings — field name translation."""

import pytest
from hgraph_trade.hgraph_trade_mapping.fpml_mappings import (
    get_global_mapping,
    get_instrument_mapping,
    map_hgraph_to_fpml,
)

# ---------- get_global_mapping ----------


def test_global_mapping_returns_dict():
    assert isinstance(get_global_mapping(), dict)


@pytest.mark.parametrize(
    "field",
    ["buy_sell", "trade_date", "trade_id", "effective_date", "termination_date"],
)
def test_global_mapping_contains_core_fields(field):
    assert field in get_global_mapping()


@pytest.mark.parametrize(
    "hgraph_key,fpml_key",
    [
        ("buy_sell", "buySell"),
        ("trade_date", "tradeDate"),
    ],
)
def test_global_mapping_values(hgraph_key, fpml_key):
    assert get_global_mapping()[hgraph_key] == fpml_key


# ---------- get_instrument_mapping ----------


@pytest.mark.parametrize(
    "instrument,expected_key",
    [
        ("swap", None),
        ("option", "strike_price"),
        ("forward", "fixed_price"),
        ("future", "contract_price"),
        ("physical", "delivery_point"),
        ("swaption", "strike_price"),
        ("cash", "payment_date"),
        ("fx", "currency_pair"),
    ],
)
def test_instrument_mapping_exists(instrument, expected_key):
    mapping = get_instrument_mapping(instrument)
    assert isinstance(mapping, dict)
    if expected_key is not None:
        assert expected_key in mapping


def test_physical_mapping_has_delivery_fields():
    mapping = get_instrument_mapping("physical")
    assert "delivery_point" in mapping
    assert "delivery_location" in mapping


def test_swaption_mapping_has_expiration():
    mapping = get_instrument_mapping("swaption")
    assert "expiration_date" in mapping


def test_unknown_instrument_returns_empty():
    assert get_instrument_mapping("nonexistent") == {}


# ---------- map_hgraph_to_fpml ----------


def test_maps_simple_fields():
    trade_data = {"buy_sell": "Buy", "trade_date": "2024-01-01"}
    mapping = {"buy_sell": "buySell", "trade_date": "tradeDate"}
    result = map_hgraph_to_fpml(trade_data, mapping)
    assert result["buySell"] == "Buy"
    assert result["tradeDate"] == "2024-01-01"


def test_unmapped_keys_pass_through():
    result = map_hgraph_to_fpml({"unknown_field": "value123"}, {"buy_sell": "buySell"})
    assert result["unknown_field"] == "value123"


def test_nested_dicts_mapped_recursively():
    trade_data = {"counterparty": {"internal_party": "InternalCo", "external_party": "ExternalCo"}}
    mapping = {"internal_party": "internalParty", "external_party": "externalParty"}
    result = map_hgraph_to_fpml(trade_data, mapping)
    assert result["counterparty"]["internalParty"] == "InternalCo"
    assert result["counterparty"]["externalParty"] == "ExternalCo"


def test_empty_trade_data_returns_empty():
    assert map_hgraph_to_fpml({}, {"buy_sell": "buySell"}) == {}


def test_empty_mapping_passes_through():
    assert map_hgraph_to_fpml({"buy_sell": "Buy"}, {})["buy_sell"] == "Buy"


@pytest.mark.parametrize(
    "key,value,mapped_key",
    [
        ("qty", 10000, "quantity"),
        ("strike_price", 4.50, "strikePrice"),
    ],
)
def test_preserves_numeric_values(key, value, mapped_key):
    result = map_hgraph_to_fpml({key: value}, {key: mapped_key})
    assert result[mapped_key] == value


def test_preserves_list_values():
    trade_data = {"exercise_dates": ["2025-06-01", "2025-09-01"]}
    result = map_hgraph_to_fpml(trade_data, {"exercise_dates": "exerciseDates"})
    assert result["exerciseDates"] == ["2025-06-01", "2025-09-01"]
