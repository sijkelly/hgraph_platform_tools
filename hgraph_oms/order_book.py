"""
The order book: pre-trade checks plus the lifecycle state machine, as an hgraph node.

:func:`process_messages` is the pure core.  :func:`order_book` wraps it as a
stateful ``compute_node`` so it can be wired into a graph and run either live
or in simulation against recorded inputs with identical results.

Checking and applying happen in one step on purpose: an order that passes the
credit check reserves its notional immediately, so the next order in the same
engine cycle sees the reduced headroom.
"""

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Mapping

from hgraph import STATE, TS, TSB, TSD, TimeSeriesSchema, compute_node

from hgraph_oms.model import (
    AmendOrder,
    CreditPosition,
    NewOrder,
    OrderDetails,
    OrderMessage,
    OrderResponse,
    OrderSnapshot,
)
from hgraph_oms.pre_trade import EXECUTE_TRADE, check_credit, check_entitlement
from hgraph_oms.state_machine import amended_details, apply_message

__all__ = ("OrderBookOutput", "process_messages", "order_book")


def _open_exposure(orders: Mapping[str, OrderSnapshot], counterparty: str, *, exclude: str = "") -> float:
    """Notional of the counterparty's open orders, optionally leaving one order out."""
    return sum(
        o.open_exposure for o in orders.values() if o.details.counterparty == counterparty and o.order_id != exclude
    )


def _credit_failure(
    details: OrderDetails,
    orders: Mapping[str, OrderSnapshot],
    credit: Mapping[str, CreditPosition],
    *,
    exclude: str = "",
) -> str:
    if not details.counterparty:
        return ""
    return check_credit(
        details,
        credit.get(details.counterparty),
        open_exposure=_open_exposure(orders, details.counterparty, exclude=exclude),
    )


def process_messages(
    orders: dict[str, OrderSnapshot],
    messages: tuple[OrderMessage, ...],
    credit: Mapping[str, CreditPosition],
    permissions: Mapping[str, frozenset[str]] | None = None,
    *,
    at: datetime | None = None,
) -> tuple[dict[str, OrderSnapshot], tuple[OrderResponse, ...]]:
    """Check and apply a batch of messages in order.

    :param orders: All known orders by ID.  Updated in place.
    :param messages: The messages to process, in arrival order.
    :param credit: Credit position per counterparty symbol.
    :param permissions: Allowed actions per user, or ``None`` when entitlements are not enforced.
    :param at: Processing time stamped on the orders and responses.
    :returns: ``(changed, responses)`` — the orders that changed, and one response per message.
    """
    changed: dict[str, OrderSnapshot] = {}
    responses: list[OrderResponse] = []

    for message in messages:
        current = orders.get(message.order_id)
        failure = check_entitlement(message.user_id, EXECUTE_TRADE, permissions)

        if isinstance(message, NewOrder):
            failure = failure or _credit_failure(message.details, orders, credit)
        elif isinstance(message, AmendOrder) and current is not None and current.is_open and not failure:
            details = amended_details(current.details, message)
            still_to_fill = replace(details, quantity=max(details.quantity - current.filled_quantity, 0.0))
            failure = _credit_failure(still_to_fill, orders, credit, exclude=current.order_id)
        elif failure:
            # Not entitled: refuse without touching the order
            responses.append(
                OrderResponse(message=message, accepted=False, reason=failure, snapshot=current, processed_at=at)
            )
            continue

        updated, reason = apply_message(current, message, check_failure=failure, at=at)
        if updated is not None and updated is not current:
            orders[updated.order_id] = updated
            changed[updated.order_id] = updated
        responses.append(
            OrderResponse(message=message, accepted=not reason, reason=reason, snapshot=updated, processed_at=at)
        )

    return changed, tuple(responses)


@dataclass
class _OrderBookState:
    orders: dict[str, OrderSnapshot] = field(default_factory=dict)
    seeded: bool = False


@dataclass
class OrderBookOutput(TimeSeriesSchema):
    """Output of :func:`order_book`.

    ``orders`` holds the latest snapshot of every order and ticks only for orders that changed.
    ``responses`` ticks once per batch with one response per message, in order.
    """

    orders: TSD[str, TS[OrderSnapshot]]
    responses: TS[tuple[OrderResponse, ...]]


@compute_node(active=("messages",), valid=("messages",))
def order_book(
    messages: TS[tuple[OrderMessage, ...]],
    credit: TSD[str, TS[CreditPosition]],
    permissions: TSD[str, TS[frozenset[str]]],
    enforce_entitlements: bool = False,
    initial_orders: tuple[OrderSnapshot, ...] = (),
    _state: STATE[_OrderBookState] = None,
) -> TSB[OrderBookOutput]:
    """Check and apply order messages, publishing order snapshots and responses.

    Only ``messages`` triggers evaluation; ``credit`` and ``permissions`` are read at
    their latest values when a message arrives.  Orders and responses are stamped with
    the engine time of the cycle, so a simulation run reproduces the same timestamps.

    :param messages: Batches of order messages.
    :param credit: Credit position per counterparty symbol.  A counterparty with no entry has no limit.
    :param permissions: Allowed entitlement actions per user.
    :param enforce_entitlements: When ``True``, every message's user must hold ``execute_trade``.
    :param initial_orders: Orders to start with, e.g. rebuilt from the event log after a restart.
    """
    if not _state.seeded:
        _state.seeded = True
        _state.orders.update({o.order_id: o for o in initial_orders})

    credit_now = dict(credit.value) if credit.valid else {}
    permissions_now = (dict(permissions.value) if permissions.valid else {}) if enforce_entitlements else None

    at = messages.last_modified_time.replace(tzinfo=timezone.utc)
    changed, responses = process_messages(_state.orders, messages.value, credit_now, permissions_now, at=at)
    return {"orders": changed, "responses": responses}
