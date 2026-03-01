"""
Tests for error recovery behaviour across the pipeline.

Covers:
- trade_mapper graceful degradation (per-trade isolation)
- trade_booker batch booking and quarantine
"""

import json
import os
import pytest

from hgraph_trade.hgraph_trade_booker.trade_booker import (
    book_trade,
    book_trades_batch,
)


class TestBookTrade:
    """Tests for the single-trade book_trade function."""

    def test_writes_json_file(self, tmp_path):
        data = {"tradeHeader": {"tradeId": "T1"}, "tradeEconomics": {}}
        book_trade(data, "trade.json", str(tmp_path))
        output = tmp_path / "trade.json"
        assert output.exists()
        loaded = json.loads(output.read_text())
        assert loaded["tradeHeader"]["tradeId"] == "T1"

    def test_creates_output_dir(self, tmp_path):
        out_dir = str(tmp_path / "nested" / "dir")
        data = {"tradeHeader": {"tradeId": "T2"}}
        book_trade(data, "trade.json", out_dir)
        assert os.path.exists(os.path.join(out_dir, "trade.json"))


class TestBookTradesBatch:
    """Tests for batch booking with quarantine."""

    def test_batch_books_all_successfully(self, tmp_path):
        messages = [
            {"tradeHeader": {"partyTradeIdentifier": {"tradeId": f"T{i}"}}}
            for i in range(3)
        ]
        result = book_trades_batch(messages, str(tmp_path))
        assert len(result["booked"]) == 3
        assert len(result["quarantined"]) == 0

    def test_batch_uses_trade_id_as_filename(self, tmp_path):
        messages = [
            {"tradeHeader": {"partyTradeIdentifier": {"tradeId": "SWAP-001"}}}
        ]
        book_trades_batch(messages, str(tmp_path))
        assert (tmp_path / "SWAP-001.json").exists()

    def test_batch_fallback_filename_when_no_trade_id(self, tmp_path):
        messages = [{"tradeHeader": {}}]
        book_trades_batch(messages, str(tmp_path))
        assert (tmp_path / "trade_0.json").exists()

    def test_batch_quarantines_on_write_failure(self, tmp_path, monkeypatch):
        """Simulate a write failure for the first trade."""
        messages = [
            {"tradeHeader": {"partyTradeIdentifier": {"tradeId": "FAIL"}}},
            {"tradeHeader": {"partyTradeIdentifier": {"tradeId": "OK"}}},
        ]
        call_count = {"n": 0}
        original_book = book_trade

        def failing_book(trade_data, output_file, output_dir):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise IOError("Disk full")
            return original_book(trade_data, output_file, output_dir)

        monkeypatch.setattr(
            "hgraph_trade.hgraph_trade_booker.trade_booker.book_trade",
            failing_book,
        )
        result = book_trades_batch(messages, str(tmp_path))
        assert len(result["booked"]) == 1
        assert len(result["quarantined"]) == 1
        # Quarantine file should contain both the message and the error
        q_path = result["quarantined"][0]
        q_data = json.loads(open(q_path).read())
        assert "Disk full" in q_data["error"]
        assert q_data["original_message"]["tradeHeader"]["partyTradeIdentifier"]["tradeId"] == "FAIL"
