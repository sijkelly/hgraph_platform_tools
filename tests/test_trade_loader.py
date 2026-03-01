"""
Tests for hgraph_trade.hgraph_trade_booker.trade_loader
"""

import json
import os
import tempfile
import pytest
from hgraph_trade.hgraph_trade_booker.trade_loader import (
    load_trade_from_file,
    validate_required_keys,
    validate_nested_fields,
)


class TestValidateRequiredKeys:
    """Tests for the validate_required_keys function."""

    def test_valid_content_passes(self):
        content = json.dumps({
            "tradeType": "newTrade",
            "instrument": "outright",
            "trade_id": "T001",
        })
        required = ["tradeType", "instrument", "trade_id"]
        # Should not raise
        result = validate_required_keys(content, required)
        assert result is True or result is None  # Implementation may vary

    def test_missing_key_fails(self):
        content = json.dumps({
            "tradeType": "newTrade",
        })
        required = ["tradeType", "instrument"]
        with pytest.raises(Exception):
            validate_required_keys(content, required)


class TestValidateNestedFields:
    """Tests for the validate_nested_fields function."""

    def test_valid_nested_content_passes(self):
        content = json.dumps({
            "traders": {"internal": "TraderX", "external": "TraderY"},
            "counterparty": {"internal": "CoA", "external": "CoB"},
            "portfolio": {"internal": "PortA", "external": "PortB"},
        })
        nested = {
            "traders": ["internal", "external"],
            "counterparty": ["internal", "external"],
            "portfolio": ["internal", "external"],
        }
        result = validate_nested_fields(content, nested)
        assert result is True or result is None


class TestLoadTradeFromFile:
    """Tests for the load_trade_from_file function."""

    def test_loads_valid_trade_file(self):
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
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(trade, f)
            temp_path = f.name

        try:
            result = load_trade_from_file(temp_path)
            assert result["trade_id"] == "TEST-LOAD-001"
            assert result["instrument"] == "outright"
        finally:
            os.unlink(temp_path)

    def test_nonexistent_file_raises(self):
        with pytest.raises(Exception):
            load_trade_from_file("/nonexistent/path/trade.json")

    def test_invalid_json_raises(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            f.write("this is not valid json {{{")
            temp_path = f.name

        try:
            with pytest.raises(Exception):
                load_trade_from_file(temp_path)
        finally:
            os.unlink(temp_path)
