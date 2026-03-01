"""
Tests for hgraph_trade.hgraph_trade_booker.message_wrapper
"""

import pytest
from hgraph_trade.hgraph_trade_booker.message_wrapper import (
    create_message_header,
    create_message_footer,
    calculate_checksum,
    wrap_message_with_headers_and_footers,
)


class TestCreateMessageHeader:
    """Tests for the create_message_header function."""

    def test_returns_dict(self):
        result = create_message_header("newTrade", "SenderA", "TargetB")
        assert isinstance(result, dict)

    def test_has_message_type(self):
        result = create_message_header("newTrade", "SenderA", "TargetB")
        assert result["messageType"] == "newTrade"

    def test_has_sender(self):
        result = create_message_header("newTrade", "SenderA", "TargetB")
        assert result["senderCompID"] == "SenderA"

    def test_has_target(self):
        result = create_message_header("newTrade", "SenderA", "TargetB")
        assert result["targetCompID"] == "TargetB"

    def test_has_sending_time(self):
        result = create_message_header("newTrade", "SenderA", "TargetB")
        assert "sendingTime" in result

    def test_has_version(self):
        result = create_message_header("newTrade", "SenderA", "TargetB")
        assert result["messageVersion"] == "1.0"


class TestCreateMessageFooter:
    """Tests for the create_message_footer function."""

    def test_returns_dict(self):
        result = create_message_footer()
        assert isinstance(result, dict)

    def test_has_checksum_field(self):
        result = create_message_footer()
        assert "checksum" in result


class TestCalculateChecksum:
    """Tests for the calculate_checksum function."""

    def test_returns_string(self):
        result = calculate_checksum('{"key": "value"}')
        assert isinstance(result, str)

    def test_deterministic(self):
        message = '{"key": "value", "number": 42}'
        result1 = calculate_checksum(message)
        result2 = calculate_checksum(message)
        assert result1 == result2

    def test_different_messages_different_checksums(self):
        checksum1 = calculate_checksum('{"key": "value1"}')
        checksum2 = calculate_checksum('{"key": "value2"}')
        assert checksum1 != checksum2


class TestWrapMessageWithHeadersAndFooters:
    """Tests for the wrap_message_with_headers_and_footers function."""

    def test_returns_dict(self):
        trade_data = {"tradeHeader": {}, "tradeEconomics": {}, "tradeFooter": {}}
        result = wrap_message_with_headers_and_footers(trade_data, "newTrade", "Sender", "Target")
        assert isinstance(result, dict)

    def test_has_message_header(self):
        trade_data = {"tradeHeader": {}, "tradeEconomics": {}, "tradeFooter": {}}
        result = wrap_message_with_headers_and_footers(trade_data, "newTrade", "Sender", "Target")
        assert "messageHeader" in result

    def test_has_message_footer(self):
        trade_data = {"tradeHeader": {}, "tradeEconomics": {}, "tradeFooter": {}}
        result = wrap_message_with_headers_and_footers(trade_data, "newTrade", "Sender", "Target")
        assert "messageFooter" in result
