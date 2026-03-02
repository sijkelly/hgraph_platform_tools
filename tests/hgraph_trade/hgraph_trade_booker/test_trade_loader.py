"""Tests for trade_loader — loading and validating trade files."""

import json
import os
import tempfile

import pytest
from hgraph_trade.hgraph_trade_booker.trade_loader import (
    load_trade_from_file,
    validate_nested_fields,
    validate_required_keys,
)

# ---------- validate_required_keys ----------


def test_validate_required_keys_valid():
    content = json.dumps({"tradeType": "newTrade", "instrument": "outright", "trade_id": "T001"})
    required = ["tradeType", "instrument", "trade_id"]
    result = validate_required_keys(content, required)
    assert result is True or result is None


def test_validate_required_keys_missing():
    content = json.dumps({"tradeType": "newTrade"})
    with pytest.raises(Exception):
        validate_required_keys(content, ["tradeType", "instrument"])


# ---------- validate_nested_fields ----------


def test_validate_nested_fields_valid():
    content = json.dumps(
        {
            "traders": {"internal": "TraderX", "external": "TraderY"},
            "counterparty": {"internal": "CoA", "external": "CoB"},
            "portfolio": {"internal": "PortA", "external": "PortB"},
        }
    )
    nested = {
        "traders": ["internal", "external"],
        "counterparty": ["internal", "external"],
        "portfolio": ["internal", "external"],
    }
    result = validate_nested_fields(content, nested)
    assert result is True or result is None


# ---------- load_trade_from_file ----------


def test_load_valid_trade_file():
    trade = {
        "tradeType": "newTrade",
        "instrument": "outright",
        "trade_id": "TEST-LOAD-001",
        "trade_date": "2024-11-20",
        "buy_sell": "Buy",
        "counterparty": {"internal": "CoA", "external": "CoB"},
        "portfolio": {"internal": "PortA", "external": "PortB"},
        "traders": {"internal": "TraderX", "external": "TraderY"},
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(trade, f)
        temp_path = f.name

    try:
        result = load_trade_from_file(temp_path)
        assert result["trade_id"] == "TEST-LOAD-001"
        assert result["instrument"] == "outright"
    finally:
        os.unlink(temp_path)


def test_load_nonexistent_file_raises():
    with pytest.raises(Exception):
        load_trade_from_file("/nonexistent/path/trade.json")


def test_load_invalid_json_raises():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("this is not valid json {{{")
        temp_path = f.name

    try:
        with pytest.raises(Exception):
            load_trade_from_file(temp_path)
    finally:
        os.unlink(temp_path)
