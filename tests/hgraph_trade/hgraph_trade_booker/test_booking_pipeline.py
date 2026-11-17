"""End-to-end tests for the booking pipeline: load -> map -> book, and the ``book`` CLI command."""

import json
from pathlib import Path

import pytest

import cli
from hgraph_trade.hgraph_trade_booker.trade_booker import book_trades_batch
from hgraph_trade.hgraph_trade_booker.trade_loader import load_trade_from_file
from hgraph_trade.hgraph_trade_booker.trade_mapper import map_trade_to_model

SAMPLE_TRADE = Path(__file__).parents[3] / "hgraph_trade" / "test_trades" / "fixed_float_BM_swap_001.txt"

MESSAGE_SECTIONS = ("messageHeader", "tradeHeader", "tradeEconomics", "tradeFooter", "messageFooter")


def test_map_trade_builds_complete_message(swap_fixed_float_data):
    messages = map_trade_to_model(swap_fixed_float_data, fail_fast=True)
    assert len(messages) == 1
    assert tuple(messages[0]) == MESSAGE_SECTIONS


def test_map_trade_decomposes_calendar_spread(swap_fixed_float_data):
    trade = {**swap_fixed_float_data, "pricing_instrument": "calender_spread"}
    messages = map_trade_to_model(trade, fail_fast=True)
    assert len(messages) == 2


def test_sample_trade_books_under_its_trade_id(tmp_path):
    trade_data = load_trade_from_file(str(SAMPLE_TRADE))
    messages = map_trade_to_model(trade_data, fail_fast=True)
    result = book_trades_batch(messages, str(tmp_path))

    assert result["quarantined"] == []
    booked = tmp_path / "hgraph-SWAP-011.json"
    assert result["booked"] == [str(booked)]
    assert tuple(json.loads(booked.read_text())) == MESSAGE_SECTIONS


def test_decomposed_trades_do_not_overwrite_each_other(swap_fixed_float_data, tmp_path):
    trade = {**swap_fixed_float_data, "pricing_instrument": "calender_spread"}
    messages = map_trade_to_model(trade, fail_fast=True)
    result = book_trades_batch(messages, str(tmp_path))

    assert sorted(Path(p).name for p in result["booked"]) == ["TEST-001.json", "TEST-001_2.json"]


@pytest.mark.parametrize("input_flag", ["--input_file", "--input_dir"])
def test_cli_book_command(input_flag, tmp_path, monkeypatch):
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    trade_file = input_dir / "trade.json"
    trade_file.write_text(SAMPLE_TRADE.read_text())
    source = trade_file if input_flag == "--input_file" else input_dir
    output_dir = tmp_path / "out"

    monkeypatch.setattr("sys.argv", ["hgraph-tools", "book", input_flag, str(source), "--output_dir", str(output_dir)])
    with pytest.raises(SystemExit) as exit_info:
        cli.main()

    assert exit_info.value.code == 0
    assert (output_dir / "hgraph-SWAP-011.json").exists()
