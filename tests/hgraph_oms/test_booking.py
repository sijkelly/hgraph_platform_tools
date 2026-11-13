"""Tests for booking trade tickets, and for the engine handing its fills to the booker."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from hgraph_oms.booking import BOOKED, FAILED, TradeBooker
from hgraph_oms.engine import OrderEngine
from hgraph_oms.model import AcceptOrder, FillOrder, NewOrder, OrderDetails
from tests.hgraph_oms.test_trades import DETAILS, TICKET


@pytest.fixture
def booker(tmp_path) -> TradeBooker:
    return TradeBooker(str(tmp_path / "trade_bookings.db"), str(tmp_path / "booked"), internal_party="OURCO")


def _engine(tmp_path, booker=None) -> OrderEngine:
    return OrderEngine(str(tmp_path / "order_events.db"), trade_sink=booker.book if booker else None)


def _working_order(engine, order_id="ORD-1", details: OrderDetails = DETAILS) -> None:
    assert engine.submit(NewOrder(order_id=order_id, user_id="trader1", details=details)).accepted
    assert engine.submit(AcceptOrder(order_id=order_id, user_id="desk")).accepted


def _fill(order_id="ORD-1", fill_id="F1", quantity=20_000.0, price=3.40) -> FillOrder:
    return FillOrder(order_id=order_id, user_id="desk", fill_id=fill_id, quantity=quantity, price=price)


# ---------- booker ----------


def test_booked_trade_is_written_and_recorded(booker, tmp_path):
    (outcome,) = booker.book([TICKET])

    booked_file = tmp_path / "booked" / "ORD-1-F1.json"
    assert (outcome.status, outcome.detail) == (BOOKED, str(booked_file))
    assert (
        json.loads(booked_file.read_text())["tradeEconomics"]["commoditySwap"]["fixedLeg"]["fixedPrice"]["price"]
        == 3.40
    )
    assert booker.get("ORD-1-F1") == outcome
    assert booker.bookings(order_id="ORD-1") == [outcome]
    assert booker.bookings(order_id="ORD-2") == []


def test_unbookable_trade_is_recorded_as_failed_with_the_reason(booker, tmp_path):
    (outcome,) = booker.book([replace(TICKET, instrument="Unobtainium")])

    assert outcome.status == FAILED
    assert "not in the product reference data" in outcome.detail
    assert not (tmp_path / "booked").exists()
    assert booker.bookings(status=FAILED) == [outcome]


def test_booking_is_idempotent_and_retries_failures(booker, tmp_path):
    bad = replace(TICKET, delivery_period="soon")
    assert booker.book([bad])[0].status == FAILED

    # The same trade, now bookable, is retried; once booked it is never booked again
    assert booker.book([TICKET])[0].status == BOOKED
    booked_file = Path(booker.get(TICKET.trade_id).detail)
    booked_file.write_text("sent downstream")
    assert booker.book([TICKET, bad]) == []
    assert booked_file.read_text() == "sent downstream"


# ---------- engine -> booker ----------


def test_each_fill_is_booked_before_the_caller_is_answered(tmp_path, booker):
    engine = _engine(tmp_path, booker)
    engine.start()
    try:
        _working_order(engine)
        assert booker.bookings() == []  # nothing is booked until something executes

        engine.submit(_fill(fill_id="F1", quantity=20_000.0, price=3.40))
        assert [(b.trade_id, b.status, b.quantity) for b in booker.bookings()] == [("ORD-1-F1", BOOKED, 20_000.0)]

        engine.submit(_fill(fill_id="F2", quantity=30_000.0, price=3.50))
        assert not engine.submit(_fill(fill_id="F3", quantity=1.0)).accepted  # order is filled: no trade
    finally:
        engine.stop()

    assert [(b.trade_id, b.price) for b in booker.bookings()] == [("ORD-1-F1", 3.40), ("ORD-1-F2", 3.50)]
    assert sorted(p.name for p in (tmp_path / "booked").iterdir()) == ["ORD-1-F1.json", "ORD-1-F2.json"]


def test_fills_missed_by_the_booker_are_booked_on_restart(tmp_path, booker):
    # First run has no booker attached, as if the process died between logging and booking
    first = _engine(tmp_path)
    first.start()
    _working_order(first)
    first.submit(_fill())
    first.stop()
    assert booker.bookings() == []

    second = _engine(tmp_path, booker)
    second.start()
    second.stop()
    assert [(b.trade_id, b.status) for b in booker.bookings()] == [("ORD-1-F1", BOOKED)]

    # A further restart re-offers the fill; it is not booked twice
    updated_at = booker.get("ORD-1-F1").updated_at
    third = _engine(tmp_path, booker)
    third.start()
    third.stop()
    assert booker.get("ORD-1-F1").updated_at == updated_at


def test_failing_trade_sink_does_not_stop_the_engine(tmp_path):
    def broken_sink(tickets):
        raise RuntimeError("ETRM is down")

    engine = OrderEngine(str(tmp_path / "order_events.db"), trade_sink=broken_sink)
    engine.start()
    try:
        _working_order(engine)
        assert engine.submit(_fill()).accepted
        assert engine.submit(_fill(fill_id="F2", quantity=10.0)).accepted
    finally:
        engine.stop()
