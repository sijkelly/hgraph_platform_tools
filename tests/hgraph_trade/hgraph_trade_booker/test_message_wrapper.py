"""Tests for message_wrapper — header, footer, checksum, and wrapping."""

import pytest
from hgraph_trade.hgraph_trade_booker.message_wrapper import (
    calculate_checksum,
    create_message_footer,
    create_message_header,
    wrap_message_with_headers_and_footers,
)

# ---------- create_message_header ----------


def test_header_returns_dict():
    result = create_message_header("newTrade", "SenderA", "TargetB")
    assert isinstance(result, dict)


@pytest.mark.parametrize(
    "key,expected",
    [
        ("messageType", "newTrade"),
        ("senderCompID", "SenderA"),
        ("targetCompID", "TargetB"),
        ("messageVersion", "1.0"),
    ],
)
def test_header_fields(key, expected):
    result = create_message_header("newTrade", "SenderA", "TargetB")
    assert result[key] == expected


def test_header_has_sending_time():
    result = create_message_header("newTrade", "SenderA", "TargetB")
    assert "sendingTime" in result


# ---------- create_message_footer ----------


def test_footer_returns_dict():
    result = create_message_footer()
    assert isinstance(result, dict)
    assert "checksum" in result


# ---------- calculate_checksum ----------


def test_checksum_returns_string():
    assert isinstance(calculate_checksum('{"key": "value"}'), str)


def test_checksum_deterministic():
    message = '{"key": "value", "number": 42}'
    assert calculate_checksum(message) == calculate_checksum(message)


def test_checksum_different_messages():
    assert calculate_checksum('{"key": "value1"}') != calculate_checksum('{"key": "value2"}')


# ---------- wrap_message_with_headers_and_footers ----------


def test_wrap_returns_dict():
    trade_data = {"tradeHeader": {}, "tradeEconomics": {}, "tradeFooter": {}}
    result = wrap_message_with_headers_and_footers(trade_data, "newTrade", "Sender", "Target")
    assert isinstance(result, dict)


@pytest.mark.parametrize("key", ["messageHeader", "messageFooter"])
def test_wrap_contains_envelope(key):
    trade_data = {"tradeHeader": {}, "tradeEconomics": {}, "tradeFooter": {}}
    result = wrap_message_with_headers_and_footers(trade_data, "newTrade", "Sender", "Target")
    assert key in result
