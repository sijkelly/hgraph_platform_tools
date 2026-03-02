"""Tests for error recovery — trade booking and quarantine."""

import json

import pytest
from hgraph_trade.hgraph_trade_booker.trade_booker import book_trade, book_trades_batch

# ---------- book_trade ----------


def test_book_trade_writes_json_file(tmp_path):
    data = {"tradeHeader": {"tradeId": "T1"}, "tradeEconomics": {}}
    book_trade(data, "trade.json", str(tmp_path))
    output = tmp_path / "trade.json"
    assert output.exists()
    loaded = json.loads(output.read_text())
    assert loaded["tradeHeader"]["tradeId"] == "T1"


def test_book_trade_creates_output_dir(tmp_path):
    out_dir = str(tmp_path / "nested" / "dir")
    data = {"tradeHeader": {"tradeId": "T2"}}
    book_trade(data, "trade.json", out_dir)
    assert (tmp_path / "nested" / "dir" / "trade.json").exists()


# ---------- book_trades_batch ----------


def test_batch_books_all_successfully(tmp_path):
    messages = [{"tradeHeader": {"partyTradeIdentifier": {"tradeId": f"T{i}"}}} for i in range(3)]
    result = book_trades_batch(messages, str(tmp_path))
    assert len(result["booked"]) == 3
    assert len(result["quarantined"]) == 0


def test_batch_uses_trade_id_as_filename(tmp_path):
    messages = [{"tradeHeader": {"partyTradeIdentifier": {"tradeId": "SWAP-001"}}}]
    book_trades_batch(messages, str(tmp_path))
    assert (tmp_path / "SWAP-001.json").exists()


def test_batch_fallback_filename_when_no_trade_id(tmp_path):
    messages = [{"tradeHeader": {}}]
    book_trades_batch(messages, str(tmp_path))
    assert (tmp_path / "trade_0.json").exists()


def test_batch_quarantines_on_write_failure(tmp_path, monkeypatch):
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

    monkeypatch.setattr("hgraph_trade.hgraph_trade_booker.trade_booker.book_trade", failing_book)
    result = book_trades_batch(messages, str(tmp_path))
    assert len(result["booked"]) == 1
    assert len(result["quarantined"]) == 1

    q_data = json.loads(open(result["quarantined"][0]).read())
    assert "Disk full" in q_data["error"]
    assert q_data["original_message"]["tradeHeader"]["partyTradeIdentifier"]["tradeId"] == "FAIL"
