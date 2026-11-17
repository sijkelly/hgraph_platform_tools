"""Tests for the real-time order engine and its event log recovery."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from hgraph_oms.engine import OrderEngine
from hgraph_oms.event_log import read_events
from hgraph_oms.model import (
    AcceptOrder,
    CancelOrder,
    CreditPosition,
    FillOrder,
    NewOrder,
    OrderDetails,
    OrderStatus,
)


def _new(order_id, user_id="trader1", quantity=100.0, price=10.0) -> NewOrder:
    details = OrderDetails(instrument="Henry Hub", side="Buy", quantity=quantity, price=price, counterparty="ACME")
    return NewOrder(order_id=order_id, user_id=user_id, details=details)


@pytest.fixture
def db_path(oms_db):
    return oms_db


@pytest.fixture
def engine(db_path):
    engine = OrderEngine(db_path)
    engine.start()
    yield engine
    engine.stop()


def test_submit_returns_response_and_updates_orders(engine):
    response = engine.submit(_new("O1"))

    assert response.accepted
    assert response.snapshot.status is OrderStatus.PENDING
    assert engine.get_order("O1") == response.snapshot
    assert [o.order_id for o in engine.orders()] == ["O1"]


def test_lifecycle_through_engine(engine):
    engine.submit(_new("O1"))
    engine.submit(AcceptOrder(order_id="O1", user_id="desk"))
    engine.submit(FillOrder(order_id="O1", user_id="desk", fill_id="F1", quantity=100.0, price=9.9))

    order = engine.get_order("O1")
    assert order.status is OrderStatus.FILLED
    assert order.average_price == 9.9
    assert order.updated_by == "desk"


def test_refused_message_is_answered_and_logged(engine, db_path):
    response = engine.submit(CancelOrder(order_id="MISSING", user_id="trader1"))

    assert not response.accepted
    assert response.reason == "Order MISSING not found"
    assert [(type(e.message).__name__, e.accepted) for e in read_events(db_path)] == [("CancelOrder", False)]


def test_credit_limit_applies_to_submitted_orders(engine):
    engine.set_credit(CreditPosition(counterparty="ACME", limit_amount=1_500.0))

    assert engine.submit(_new("O1")).accepted
    rejected = engine.submit(_new("O2"))
    assert not rejected.accepted
    assert engine.get_order("O2").status is OrderStatus.REJECTED


def test_entitlements_enforced_by_engine(db_path):
    engine = OrderEngine(db_path, enforce_entitlements=True)
    engine.start()
    try:
        engine.set_permissions("trader1", frozenset({"execute_trade"}))
        assert engine.submit(_new("O1", user_id="trader1")).accepted
        assert not engine.submit(_new("O2", user_id="intruder")).accepted
    finally:
        engine.stop()


def test_concurrent_submissions_all_answered(engine):
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda i: engine.submit(_new(f"O{i}")), range(50)))

    assert all(r.accepted for r in responses)
    assert sorted(r.message.order_id for r in responses) == sorted(f"O{i}" for i in range(50))
    assert len(engine.orders()) == 50


def test_restart_recovers_orders_from_event_log(db_path):
    first = OrderEngine(db_path)
    first.start()
    first.set_credit(CreditPosition(counterparty="ACME", limit_amount=1_500.0))
    first.submit(_new("O1"))
    first.submit(_new("O2"))  # rejected on credit
    first.submit(AcceptOrder(order_id="O1"))
    first.submit(FillOrder(order_id="O1", fill_id="F1", quantity=40.0, price=10.0))
    before = {o.order_id: o for o in first.orders()}
    first.stop()

    second = OrderEngine(db_path)
    second.start()
    try:
        assert {o.order_id: o for o in second.orders()} == before
        # Recovered state is live: the duplicate fill is refused, a new fill is applied
        assert not second.submit(FillOrder(order_id="O1", fill_id="F1", quantity=10.0, price=10.0)).accepted
        assert second.submit(FillOrder(order_id="O1", fill_id="F2", quantity=60.0, price=10.0)).accepted
        assert second.get_order("O1").status is OrderStatus.FILLED
    finally:
        second.stop()


def test_submit_after_stop_raises(db_path):
    engine = OrderEngine(db_path)
    engine.start()
    engine.stop()

    with pytest.raises(RuntimeError, match="not running"):
        engine.submit(_new("O1"))


def test_credit_can_be_cleared(engine):
    engine.set_credit(CreditPosition(counterparty="ACME", limit_amount=500.0))
    assert not engine.submit(_new("O1")).accepted

    engine.clear_credit("ACME")
    assert engine.submit(_new("O2")).accepted


def test_listener_receives_changed_orders_until_removed(engine):
    seen = []
    remove = engine.add_listener(lambda changed: seen.append([o.order_id for o in changed]))

    engine.submit(_new("O1"))
    engine.submit(CancelOrder(order_id="MISSING"))  # refused: nothing changed, no callback
    remove()
    engine.submit(_new("O2"))

    assert seen == [["O1"]]


def test_orders_are_stamped_with_engine_time(engine):
    created = engine.submit(_new("O1")).snapshot
    accepted = engine.submit(AcceptOrder(order_id="O1")).snapshot

    assert created.created_at is not None and created.created_at.tzinfo is not None
    assert accepted.created_at == created.created_at
    assert accepted.updated_at > created.updated_at
