"""Tests for pipeline_result — structured error reporting."""

import pytest
from hgraph_trade.hgraph_trade_booker.pipeline_result import PipelineResult, TradeResult, TradeStatus

# ---------- TradeStatus ----------


@pytest.mark.parametrize(
    "member,value",
    [
        (TradeStatus.SUCCESS, "success"),
        (TradeStatus.VALIDATION_FAILED, "validation_failed"),
        (TradeStatus.MAPPING_FAILED, "mapping_failed"),
        (TradeStatus.BOOKING_FAILED, "booking_failed"),
        (TradeStatus.SEND_FAILED, "send_failed"),
    ],
)
def test_trade_status_values(member, value):
    assert member.value == value


# ---------- TradeResult ----------


@pytest.mark.parametrize(
    "status,expected",
    [
        (TradeStatus.SUCCESS, True),
        (TradeStatus.VALIDATION_FAILED, False),
        (TradeStatus.MAPPING_FAILED, False),
        (TradeStatus.BOOKING_FAILED, False),
        (TradeStatus.SEND_FAILED, False),
    ],
)
def test_trade_result_succeeded(status, expected):
    r = TradeResult(trade_id="T1", status=status)
    assert r.succeeded is expected


def test_trade_result_to_dict():
    r = TradeResult(trade_id="T3", status=TradeStatus.MAPPING_FAILED, stage="mapping")
    d = r.to_dict()
    assert d["trade_id"] == "T3"
    assert d["status"] == "mapping_failed"
    assert d["stage"] == "mapping"
    assert "timestamp" in d


def test_trade_result_error_serialised():
    exc = ValueError("something went wrong")
    r = TradeResult(trade_id="T4", status=TradeStatus.BOOKING_FAILED, error=exc)
    d = r.to_dict()
    assert "something went wrong" in d["error"]


def test_trade_result_no_error_is_none():
    r = TradeResult(trade_id="T5", status=TradeStatus.SUCCESS)
    assert r.to_dict()["error"] is None


# ---------- PipelineResult ----------


def test_empty_pipeline():
    p = PipelineResult()
    assert p.total == 0
    assert p.success_count == 0
    assert p.failure_count == 0


def test_pipeline_add_results():
    p = PipelineResult()
    p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
    p.add(TradeResult(trade_id="T2", status=TradeStatus.VALIDATION_FAILED))
    p.add(TradeResult(trade_id="T3", status=TradeStatus.SUCCESS))
    assert p.total == 3
    assert p.success_count == 2
    assert p.failure_count == 1


def test_pipeline_succeeded_list():
    p = PipelineResult()
    p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
    p.add(TradeResult(trade_id="T2", status=TradeStatus.MAPPING_FAILED))
    assert len(p.succeeded) == 1
    assert p.succeeded[0].trade_id == "T1"


def test_pipeline_failed_list():
    p = PipelineResult()
    p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
    p.add(TradeResult(trade_id="T2", status=TradeStatus.SEND_FAILED))
    assert len(p.failed) == 1
    assert p.failed[0].trade_id == "T2"


def test_pipeline_finalise_sets_finished_at():
    p = PipelineResult()
    assert p.finished_at is None
    p.finalise()
    assert p.finished_at is not None


def test_pipeline_summary_contains_counts():
    p = PipelineResult()
    p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
    p.add(TradeResult(trade_id="T2", status=TradeStatus.BOOKING_FAILED, message="disk full"))
    p.finalise()
    summary = p.summary()
    assert "Succeeded: 1" in summary
    assert "Failed:    1" in summary
    assert "T2" in summary
    assert "disk full" in summary


def test_pipeline_to_dict_structure():
    p = PipelineResult()
    p.add(TradeResult(trade_id="T1", status=TradeStatus.SUCCESS))
    p.finalise()
    d = p.to_dict()
    assert d["total"] == 1
    assert d["success_count"] == 1
    assert d["failure_count"] == 0
    assert len(d["results"]) == 1
