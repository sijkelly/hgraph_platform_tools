"""
Tests for pipeline_result.py — structured error reporting.
"""

import pytest
from hgraph_trade.hgraph_trade_booker.pipeline_result import (
    PipelineResult,
    TradeResult,
    TradeStatus,
)


class TestTradeResult:
    """Tests for the TradeResult dataclass."""

    def test_success_result(self):
        r = TradeResult(trade_id="T1", status=TradeStatus.SUCCESS, message="OK")
        assert r.succeeded is True

    def test_failure_result(self):
        r = TradeResult(trade_id="T2", status=TradeStatus.VALIDATION_FAILED, message="bad")
        assert r.succeeded is False

    def test_to_dict_has_required_keys(self):
        r = TradeResult(trade_id="T3", status=TradeStatus.MAPPING_FAILED, stage="mapping")
        d = r.to_dict()
        assert d["trade_id"] == "T3"
        assert d["status"] == "mapping_failed"
        assert d["stage"] == "mapping"
        assert "timestamp" in d

    def test_error_serialised(self):
        exc = ValueError("something went wrong")
        r = TradeResult(trade_id="T4", status=TradeStatus.BOOKING_FAILED, error=exc)
        d = r.to_dict()
        assert "something went wrong" in d["error"]

    def test_no_error_is_none(self):
        r = TradeResult(trade_id="T5", status=TradeStatus.SUCCESS)
        d = r.to_dict()
        assert d["error"] is None


class TestPipelineResult:
    """Tests for the PipelineResult aggregator."""

    def test_empty_pipeline(self):
        p = PipelineResult()
        assert p.total == 0
        assert p.success_count == 0
        assert p.failure_count == 0

    def test_add_results(self):
        p = PipelineResult()
        p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
        p.add(TradeResult(trade_id="T2", status=TradeStatus.VALIDATION_FAILED))
        p.add(TradeResult(trade_id="T3", status=TradeStatus.SUCCESS))
        assert p.total == 3
        assert p.success_count == 2
        assert p.failure_count == 1

    def test_succeeded_list(self):
        p = PipelineResult()
        p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
        p.add(TradeResult(trade_id="T2", status=TradeStatus.MAPPING_FAILED))
        assert len(p.succeeded) == 1
        assert p.succeeded[0].trade_id == "T1"

    def test_failed_list(self):
        p = PipelineResult()
        p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
        p.add(TradeResult(trade_id="T2", status=TradeStatus.SEND_FAILED))
        assert len(p.failed) == 1
        assert p.failed[0].trade_id == "T2"

    def test_finalise_sets_finished_at(self):
        p = PipelineResult()
        assert p.finished_at is None
        p.finalise()
        assert p.finished_at is not None

    def test_summary_contains_counts(self):
        p = PipelineResult()
        p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
        p.add(TradeResult(trade_id="T2", status=TradeStatus.BOOKING_FAILED, message="disk full"))
        p.finalise()
        summary = p.summary()
        assert "Succeeded: 1" in summary
        assert "Failed:    1" in summary
        assert "T2" in summary
        assert "disk full" in summary

    def test_to_dict_structure(self):
        p = PipelineResult()
        p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
        p.finalise()
        d = p.to_dict()
        assert d["total"] == 1
        assert d["success_count"] == 1
        assert d["failure_count"] == 0
        assert len(d["results"]) == 1


class TestTradeStatus:
    """Tests for the TradeStatus enum."""

    def test_all_statuses(self):
        assert TradeStatus.SUCCESS.value == "success"
        assert TradeStatus.VALIDATION_FAILED.value == "validation_failed"
        assert TradeStatus.MAPPING_FAILED.value == "mapping_failed"
        assert TradeStatus.BOOKING_FAILED.value == "booking_failed"
        assert TradeStatus.SEND_FAILED.value == "send_failed"
