"""Tests for the pure order lifecycle state machine, pre-trade checks and event log replay."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from hgraph_oms.event_log import append_events, init_event_log, message_from_dict, message_to_dict, read_events, replay
from hgraph_oms.model import (
    AcceptOrder,
    AmendOrder,
    CancelOrder,
    CreditPosition,
    FillOrder,
    NewOrder,
    OrderDetails,
    OrderResponse,
    OrderSnapshot,
    OrderStatus,
    RejectOrder,
)
from hgraph_oms.pre_trade import check_credit, check_entitlement
from hgraph_oms.state_machine import apply_message, validate_details

DETAILS = OrderDetails(instrument="Henry Hub", side="Buy", quantity=100.0, price=10.0, counterparty="ACME")


def _order(status: OrderStatus, **kwargs) -> OrderSnapshot:
    return OrderSnapshot(order_id="O1", details=DETAILS, status=status, **kwargs)


def _fill(quantity=10.0, price=10.0, fill_id="F1") -> FillOrder:
    return FillOrder(order_id="O1", fill_id=fill_id, quantity=quantity, price=price)


# ---------- validation ----------


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({}, ""),
        ({"price": None}, ""),
        ({"instrument": "  "}, "Instrument must not be empty"),
        ({"side": "Hold"}, "Side must be 'Buy' or 'Sell'"),
        ({"quantity": 0.0}, "Quantity must be greater than zero"),
        ({"quantity": float("nan")}, "Quantity must be greater than zero"),
        ({"price": -1.0}, "Price must be greater than zero"),
    ],
)
def test_validate_details(changes, expected):
    assert validate_details(replace(DETAILS, **changes)) == expected


# ---------- new orders ----------


def test_new_order_starts_pending():
    order, reason = apply_message(None, NewOrder(order_id="O1", user_id="trader1", details=DETAILS))

    assert reason == ""
    assert order == OrderSnapshot(
        order_id="O1", details=DETAILS, status=OrderStatus.PENDING, created_by="trader1", updated_by="trader1"
    )


@pytest.mark.parametrize(
    "details,check_failure,expected_reason",
    [
        (replace(DETAILS, quantity=-5.0), "", "Quantity must be greater than zero"),
        (DETAILS, "Credit limit exceeded", "Credit limit exceeded"),
    ],
)
def test_refused_new_order_is_recorded_as_rejected(details, check_failure, expected_reason):
    order, reason = apply_message(None, NewOrder(order_id="O1", details=details), check_failure=check_failure)

    assert reason == expected_reason
    assert order.status is OrderStatus.REJECTED
    assert order.reason == expected_reason


def test_duplicate_order_id_is_refused_and_leaves_order_unchanged():
    existing = _order(OrderStatus.WORKING)
    order, reason = apply_message(existing, NewOrder(order_id="O1", details=DETAILS))

    assert order is existing
    assert reason == "Order O1 already exists"


# ---------- transitions ----------

_ALL = set(OrderStatus)
_OPEN = {OrderStatus.PENDING, OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED}

_TRANSITIONS = [
    (AcceptOrder(order_id="O1"), {OrderStatus.PENDING}, OrderStatus.WORKING),
    (RejectOrder(order_id="O1", reason="no"), {OrderStatus.PENDING}, OrderStatus.REJECTED),
    (CancelOrder(order_id="O1", reason="client"), _OPEN, OrderStatus.CANCELLED),
    (_fill(), {OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED}, OrderStatus.PARTIALLY_FILLED),
]


@pytest.mark.parametrize(
    "message,status,allowed,target",
    [(message, status, status in allowed, target) for message, allowed, target in _TRANSITIONS for status in _ALL],
)
def test_transition_table(message, status, allowed, target):
    current = _order(status)
    order, reason = apply_message(current, message)

    if allowed:
        assert reason == ""
        assert order.status is target
        assert order.version == current.version + 1
    else:
        assert order is current
        assert f"'{status.value}' state" in reason


def test_message_for_unknown_order_is_refused():
    assert apply_message(None, CancelOrder(order_id="O9")) == (None, "Order O9 not found")


def test_cancel_and_reject_record_reason():
    cancelled, _ = apply_message(_order(OrderStatus.WORKING), CancelOrder(order_id="O1", reason="client request"))
    rejected, _ = apply_message(_order(OrderStatus.PENDING), RejectOrder(order_id="O1", reason="off market"))

    assert (cancelled.reason, rejected.reason) == ("client request", "off market")


# ---------- fills ----------


def test_fills_accumulate_to_filled():
    order = _order(OrderStatus.WORKING)
    order, _ = apply_message(order, _fill(quantity=40.0, price=9.0, fill_id="F1"))
    assert (order.status, order.remaining_quantity) == (OrderStatus.PARTIALLY_FILLED, 60.0)

    order, _ = apply_message(order, _fill(quantity=60.0, price=10.0, fill_id="F2"))
    assert order.status is OrderStatus.FILLED
    assert order.filled_quantity == 100.0
    assert order.average_price == pytest.approx(9.6)
    assert order.fill_ids == ("F1", "F2")
    assert order.open_exposure == 0.0


@pytest.mark.parametrize(
    "fill,expected",
    [
        (_fill(quantity=100.5), "Fill of 100.5 exceeds remaining quantity 100"),
        (_fill(quantity=0.0), "Fill quantity and price must be greater than zero"),
        (_fill(price=0.0), "Fill quantity and price must be greater than zero"),
        (_fill(fill_id=""), "Fill ID must not be empty"),
        (_fill(fill_id="F0"), "Fill F0 already applied"),
    ],
)
def test_invalid_fill_is_refused(fill, expected):
    current = _order(OrderStatus.WORKING, fill_ids=("F0",))
    assert apply_message(current, fill) == (current, expected)


# ---------- amendments ----------


def test_amend_changes_terms_and_keeps_status():
    order, reason = apply_message(_order(OrderStatus.WORKING), AmendOrder(order_id="O1", quantity=150.0, price=11.0))

    assert reason == ""
    assert (order.details.quantity, order.details.price, order.status) == (150.0, 11.0, OrderStatus.WORKING)


@pytest.mark.parametrize(
    "amend,check_failure,expected",
    [
        (AmendOrder(order_id="O1"), "", "Amendment changes nothing"),
        (AmendOrder(order_id="O1", quantity=-1.0), "", "Quantity must be greater than zero"),
        (AmendOrder(order_id="O1", quantity=40.0), "", "Amended quantity must exceed the 40 already filled"),
        (AmendOrder(order_id="O1", quantity=500.0), "Credit limit exceeded", "Credit limit exceeded"),
    ],
)
def test_invalid_amend_is_refused(amend, check_failure, expected):
    current = _order(OrderStatus.PARTIALLY_FILLED, filled_quantity=40.0, filled_notional=400.0)
    assert apply_message(current, amend, check_failure=check_failure) == (current, expected)


# ---------- pre-trade checks ----------


@pytest.mark.parametrize(
    "limit,utilized,open_exposure,expect_pass",
    [
        (2_000.0, 0.0, 0.0, True),
        (1_000.0, 0.0, 0.0, True),  # exactly at the limit is allowed
        (999.0, 0.0, 0.0, False),
        (2_000.0, 1_500.0, 0.0, False),
        (2_000.0, 500.0, 600.0, False),  # other open orders count
        (2_000.0, 2_000.0, 0.0, False),
    ],
)
def test_check_credit(limit, utilized, open_exposure, expect_pass):
    position = CreditPosition(counterparty="ACME", limit_amount=limit, utilized_amount=utilized)
    assert (check_credit(DETAILS, position, open_exposure=open_exposure) == "") is expect_pass


def test_check_credit_allows_when_no_limit_or_no_counterparty():
    position = CreditPosition(counterparty="ACME", limit_amount=1.0)
    assert check_credit(DETAILS, None) == ""
    assert check_credit(replace(DETAILS, counterparty=""), position) == ""


def test_market_order_passes_credit_unless_limit_already_used():
    market = replace(DETAILS, price=None)
    assert check_credit(market, CreditPosition(counterparty="ACME", limit_amount=100.0, utilized_amount=99.0)) == ""
    assert check_credit(market, CreditPosition(counterparty="ACME", limit_amount=100.0, utilized_amount=100.0)) != ""


@pytest.mark.parametrize(
    "user_id,permissions,expected",
    [
        ("anyone", None, ""),
        ("trader1", {"trader1": frozenset({"execute_trade"})}, ""),
        ("viewer1", {"viewer1": frozenset({"view_trades"})}, "User 'viewer1' lacks permission 'execute_trade'"),
        ("stranger", {}, "User 'stranger' lacks permission 'execute_trade'"),
        ("", {}, "User ID is required"),
    ],
)
def test_check_entitlement(user_id, permissions, expected):
    assert check_entitlement(user_id, "execute_trade", permissions) == expected


# ---------- event log ----------

_MESSAGES = [
    NewOrder(order_id="O1", user_id="trader1", request_id="r1", details=DETAILS),
    AcceptOrder(order_id="O1", user_id="desk"),
    AmendOrder(order_id="O1", price=10.5),
    _fill(quantity=40.0),
    CancelOrder(order_id="O1", reason="client"),
    RejectOrder(order_id="O2", reason="no"),
]


@pytest.mark.parametrize("message", _MESSAGES)
def test_message_round_trips_through_dict(message):
    assert message_from_dict(message_to_dict(message)) == message


def test_message_from_dict_rejects_unknown_type():
    with pytest.raises(ValueError, match="Unknown message type"):
        message_from_dict({"type": "DeleteEverything", "order_id": "O1"})


def test_replay_rebuilds_orders_from_log(oms_db):
    db_path = oms_db
    init_event_log(db_path)

    orders: dict[str, OrderSnapshot] = {}
    responses = []
    rejected_new = NewOrder(order_id="O2", details=DETAILS)
    start = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)
    inputs = [(m, "") for m in _MESSAGES[:5]] + [(rejected_new, "Credit limit exceeded")]
    for i, (message, check_failure) in enumerate(inputs):
        at = start + timedelta(minutes=i)
        order, reason = apply_message(orders.get(message.order_id), message, check_failure=check_failure, at=at)
        if order is not None:
            orders[order.order_id] = order
        responses.append(
            OrderResponse(message=message, accepted=not reason, reason=reason, snapshot=order, processed_at=at)
        )
    append_events(db_path, responses)

    assert replay(read_events(db_path)) == orders
    assert (orders["O1"].created_at, orders["O1"].updated_at) == (start, start + timedelta(minutes=4))
    assert orders["O1"].status is OrderStatus.CANCELLED
    assert orders["O2"].status is OrderStatus.REJECTED
    assert len(read_events(db_path, order_id="O2")) == 1


def test_replay_fails_loudly_when_log_and_state_machine_disagree():
    with pytest.raises(ValueError, match="cannot be replayed"):
        replay([(AcceptOrder(order_id="O1"), True, "")])
