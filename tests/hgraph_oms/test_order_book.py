"""Tests for the order book graph node, run in simulation with ``eval_node``."""

from hgraph import TS, TSB, TSD, graph
from hgraph.test import eval_node

from hgraph_oms.model import (
    AcceptOrder,
    AmendOrder,
    CancelOrder,
    CreditPosition,
    FillOrder,
    NewOrder,
    OrderDetails,
    OrderMessage,
    OrderSnapshot,
    OrderStatus,
)
from hgraph_oms.order_book import OrderBookOutput, order_book


@graph
def _book(
    messages: TS[tuple[OrderMessage, ...]],
    credit: TSD[str, TS[CreditPosition]],
    permissions: TSD[str, TS[frozenset[str]]],
) -> TSB[OrderBookOutput]:
    return order_book(messages, credit, permissions)


@graph
def _enforced_book(
    messages: TS[tuple[OrderMessage, ...]],
    credit: TSD[str, TS[CreditPosition]],
    permissions: TSD[str, TS[frozenset[str]]],
) -> TSB[OrderBookOutput]:
    return order_book(messages, credit, permissions, enforce_entitlements=True)


def _details(quantity=100.0, price=10.0, counterparty="ACME") -> OrderDetails:
    return OrderDetails(instrument="Henry Hub", side="Buy", quantity=quantity, price=price, counterparty=counterparty)


def _new(order_id, user_id="trader1", **kwargs) -> NewOrder:
    return NewOrder(order_id=order_id, user_id=user_id, details=_details(**kwargs))


def _run(book, messages, credit=(), permissions=()):
    """Run ``book`` and return the output of every cycle that ticked."""
    return [tick for tick in eval_node(book, list(messages), list(credit), list(permissions)) if tick is not None]


def _statuses(ticks) -> dict[str, OrderStatus]:
    latest: dict[str, OrderStatus] = {}
    for tick in ticks:
        latest.update({order_id: snapshot.status for order_id, snapshot in tick.get("orders", {}).items()})
    return latest


def test_full_lifecycle_new_accept_partial_fill_fill():
    ticks = _run(
        _book,
        [
            (_new("O1"),),
            (AcceptOrder(order_id="O1", user_id="desk"),),
            (FillOrder(order_id="O1", fill_id="F1", quantity=40.0, price=9.5),),
            (FillOrder(order_id="O1", fill_id="F2", quantity=60.0, price=10.0),),
        ],
    )

    assert [tick["orders"]["O1"].status for tick in ticks] == [
        OrderStatus.PENDING,
        OrderStatus.WORKING,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
    ]
    final: OrderSnapshot = ticks[-1]["orders"]["O1"]
    assert final.version == 3
    assert final.filled_quantity == 100.0
    assert final.average_price == 9.8
    assert all(response.accepted for tick in ticks for response in tick["responses"])


def test_refused_message_does_not_tick_orders():
    ticks = _run(_book, [(_new("O1"),), (FillOrder(order_id="O1", fill_id="F1", quantity=1.0, price=1.0),)])

    assert "orders" not in ticks[1]
    (response,) = ticks[1]["responses"]
    assert not response.accepted
    assert response.reason == "Cannot fill order in 'Pending' state"


def test_credit_breach_records_rejected_order():
    ticks = _run(
        _book,
        [None, (_new("O1", quantity=100.0, price=10.0),)],
        credit=[{"ACME": CreditPosition(counterparty="ACME", limit_amount=5_000.0, utilized_amount=4_500.0)}],
    )

    snapshot = ticks[0]["orders"]["O1"]
    assert snapshot.status is OrderStatus.REJECTED
    assert "would breach credit limit for ACME" in snapshot.reason
    assert not ticks[0]["responses"][0].accepted


def test_open_orders_reserve_credit_within_one_cycle():
    limit = {"ACME": CreditPosition(counterparty="ACME", limit_amount=1_500.0)}
    ticks = _run(_book, [None, (_new("O1"), _new("O2"))], credit=[limit])

    assert _statuses(ticks) == {"O1": OrderStatus.PENDING, "O2": OrderStatus.REJECTED}


def test_cancel_releases_reserved_credit():
    limit = {"ACME": CreditPosition(counterparty="ACME", limit_amount=1_500.0)}
    ticks = _run(_book, [None, (_new("O1"),), (CancelOrder(order_id="O1"),), (_new("O2"),)], credit=[limit])

    assert _statuses(ticks) == {"O1": OrderStatus.CANCELLED, "O2": OrderStatus.PENDING}


def test_amend_is_credit_checked_against_other_orders():
    limit = {"ACME": CreditPosition(counterparty="ACME", limit_amount=2_500.0)}
    ticks = _run(
        _book,
        [None, (_new("O1"), _new("O2")), (AmendOrder(order_id="O1", quantity=200.0),)],
        credit=[limit],
    )

    (response,) = ticks[-1]["responses"]
    assert not response.accepted
    assert "would breach credit limit" in response.reason
    assert response.snapshot.details.quantity == 100.0


def test_credit_update_applies_to_later_orders():
    ticks = _run(
        _book,
        [None, (_new("O1"),), None, (_new("O2"),)],
        credit=[
            {"ACME": CreditPosition(counterparty="ACME", limit_amount=500.0)},
            None,
            {"ACME": CreditPosition(counterparty="ACME", limit_amount=5_000.0)},
        ],
    )

    assert _statuses(ticks) == {"O1": OrderStatus.REJECTED, "O2": OrderStatus.PENDING}


def test_entitlements_enforced():
    ticks = _run(
        _enforced_book,
        [None, (_new("O1", user_id="trader1"), _new("O2", user_id="viewer1"))],
        permissions=[{"trader1": frozenset({"execute_trade"}), "viewer1": frozenset({"view_trades"})}],
    )

    assert _statuses(ticks) == {"O1": OrderStatus.PENDING, "O2": OrderStatus.REJECTED}
    assert ticks[0]["orders"]["O2"].reason == "User 'viewer1' lacks permission 'execute_trade'"


def test_unentitled_user_cannot_cancel():
    ticks = _run(
        _enforced_book,
        [None, (_new("O1"),), (CancelOrder(order_id="O1", user_id="viewer1"),)],
        permissions=[{"trader1": frozenset({"execute_trade"})}],
    )

    assert "orders" not in ticks[-1]
    assert not ticks[-1]["responses"][0].accepted


def test_same_inputs_give_same_outputs():
    messages = [(_new("O1"), _new("O2")), (AcceptOrder(order_id="O1"),), (CancelOrder(order_id="O2"),)]
    limit = [{"ACME": CreditPosition(counterparty="ACME", limit_amount=1_500.0)}]

    assert _run(_book, messages, credit=limit) == _run(_book, messages, credit=limit)
