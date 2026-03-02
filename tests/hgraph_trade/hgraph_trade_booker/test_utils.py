"""Tests for utils — directory and file helpers."""

import json
import os
import tempfile

import pytest
from hgraph_trade.hgraph_trade_booker.utils import ensure_directory_exists, save_trade_to_file

# ---------- ensure_directory_exists ----------


def test_creates_new_directory():
    with tempfile.TemporaryDirectory() as tmp:
        new_dir = os.path.join(tmp, "new_subdir")
        assert not os.path.exists(new_dir)
        ensure_directory_exists(new_dir)
        assert os.path.isdir(new_dir)


def test_existing_directory_no_error():
    with tempfile.TemporaryDirectory() as tmp:
        ensure_directory_exists(tmp)
        assert os.path.isdir(tmp)


# ---------- save_trade_to_file ----------


def test_saves_json_file():
    trade_data = {"trade_id": "SAVE-001", "buySell": "Buy"}
    with tempfile.TemporaryDirectory() as tmp:
        save_trade_to_file(trade_data, "test_output.json", tmp)
        output_path = os.path.join(tmp, "test_output.json")
        assert os.path.exists(output_path)
        with open(output_path) as f:
            loaded = json.load(f)
        assert loaded["trade_id"] == "SAVE-001"


def test_creates_directory_if_missing():
    trade_data = {"trade_id": "SAVE-002"}
    with tempfile.TemporaryDirectory() as tmp:
        output_dir = os.path.join(tmp, "nested", "dir")
        save_trade_to_file(trade_data, "output.json", output_dir)
        assert os.path.exists(os.path.join(output_dir, "output.json"))
