"""
Order lifecycle state machine.

Pure functions that decide whether a message may be applied to an order and
what the order looks like afterwards.  No I/O and no clock (the caller passes
the processing time), so the same inputs
always give the same result — which is what makes replay and audit possible.

Allowed transitions::

    (none)            --NewOrder-->    Pending   (or Rejected if a pre-trade check fails)
    Pending           --AcceptOrder--> Working
    Pending           --RejectOrder--> Rejected
    Working / Partial --FillOrder-->   Partially Filled / Filled
    any open status   --AmendOrder-->  same status
    any open status   --CancelOrder--> Cancelled
"""

from dataclasses import replace
from datetime import datetime

from hgraph_oms.model import (
    AcceptOrder,
    AmendOrder,
    CancelOrder,
    FillOrder,
    NewOrder,
    OrderDetails,
    OrderMessage,
    OrderSnapshot,
    OrderStatus,
    RejectOrder,
)

__all__ = ("validate_details", "amended_details", "apply_message")

_VALID_SIDES = ("Buy", "Sell")
_FILLABLE = frozenset({OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED})

# Tolerance when comparing a fill against the remaining quantity
_QTY_EPSILON = 1e-9


def validate_details(details: OrderDetails) -> str:
    """Return why the order terms are invalid, or an empty string if they are fine."""
    if not details.instrument.strip():
        return "Instrument must not be empty"
    if details.side not in _VALID_SIDES:
        return "Side must be 'Buy' or 'Sell'"
    if not details.quantity > 0:
        return "Quantity must be greater than zero"
    if details.price is not None and not details.price > 0:
        return "Price must be greater than zero"
    return ""


def amended_details(details: OrderDetails, message: AmendOrder) -> OrderDetails:
    """Return ``details`` with the amendment's quantity and price applied."""
    return replace(
        details,
        quantity=details.quantity if message.quantity is None else message.quantity,
        price=details.price if message.price is None else message.price,
    )


def apply_message(
    current: OrderSnapshot | None, message: OrderMessage, *, check_failure: str = "", at: datetime | None = None
) -> tuple[OrderSnapshot | None, str]:
    """Apply one message to an order.

    :param current: The order as it stands, or ``None`` if it does not exist yet.
    :param message: The message to apply.
    :param check_failure: A pre-trade check failure for this message, if any.  A failed
        :class:`NewOrder` is recorded as a ``Rejected`` order; a failed amendment is refused.
    :param at: When the message is being processed; stamped on the order as ``updated_at``
        (and ``created_at`` for a new order).
    :returns: ``(order, reason)``.  An empty ``reason`` means the message was applied.
        Otherwise the message was refused and ``order`` is unchanged — except for a refused
        :class:`NewOrder` with valid identity, which yields a new ``Rejected`` order.
    """
    if isinstance(message, NewOrder):
        return _new_order(current, message, check_failure, at)

    if current is None:
        return None, f"Order {message.order_id} not found"

    if isinstance(message, AcceptOrder):
        if current.status is not OrderStatus.PENDING:
            return current, _wrong_state("accept", current)
        return _next(current, message, at, status=OrderStatus.WORKING), ""

    if isinstance(message, RejectOrder):
        if current.status is not OrderStatus.PENDING:
            return current, _wrong_state("reject", current)
        return _next(current, message, at, status=OrderStatus.REJECTED, reason=message.reason), ""

    if isinstance(message, CancelOrder):
        if not current.is_open:
            return current, _wrong_state("cancel", current)
        return _next(current, message, at, status=OrderStatus.CANCELLED, reason=message.reason), ""

    if isinstance(message, AmendOrder):
        return _amend(current, message, check_failure, at)

    if isinstance(message, FillOrder):
        return _fill(current, message, at)

    return current, f"Unsupported message type: {type(message).__name__}"


def _wrong_state(action: str, current: OrderSnapshot) -> str:
    return f"Cannot {action} order in '{current.status.value}' state"


def _next(current: OrderSnapshot, message: OrderMessage, at: datetime | None, **changes) -> OrderSnapshot:
    return replace(
        current,
        version=current.version + 1,
        updated_by=message.user_id,
        updated_at=at or current.updated_at,
        **changes,
    )


def _new_order(
    current: OrderSnapshot | None, message: NewOrder, check_failure: str, at: datetime | None
) -> tuple[OrderSnapshot | None, str]:
    if current is not None:
        return current, f"Order {message.order_id} already exists"
    if not message.order_id:
        return None, "Order ID must not be empty"

    reason = validate_details(message.details) or check_failure
    snapshot = OrderSnapshot(
        order_id=message.order_id,
        details=message.details,
        status=OrderStatus.REJECTED if reason else OrderStatus.PENDING,
        reason=reason,
        created_by=message.user_id,
        updated_by=message.user_id,
        created_at=at,
        updated_at=at,
    )
    return snapshot, reason


def _amend(
    current: OrderSnapshot, message: AmendOrder, check_failure: str, at: datetime | None
) -> tuple[OrderSnapshot, str]:
    if not current.is_open:
        return current, _wrong_state("amend", current)
    if message.quantity is None and message.price is None:
        return current, "Amendment changes nothing"

    details = amended_details(current.details, message)
    reason = validate_details(details)
    if not reason and details.quantity <= current.filled_quantity + _QTY_EPSILON:
        reason = f"Amended quantity must exceed the {current.filled_quantity:g} already filled"
    reason = reason or check_failure
    if reason:
        return current, reason
    return _next(current, message, at, details=details), ""


def _fill(current: OrderSnapshot, message: FillOrder, at: datetime | None) -> tuple[OrderSnapshot, str]:
    if current.status not in _FILLABLE:
        return current, _wrong_state("fill", current)
    if not message.fill_id:
        return current, "Fill ID must not be empty"
    if message.fill_id in current.fill_ids:
        return current, f"Fill {message.fill_id} already applied"
    if not message.quantity > 0 or not message.price > 0:
        return current, "Fill quantity and price must be greater than zero"
    if message.quantity > current.remaining_quantity + _QTY_EPSILON:
        return current, f"Fill of {message.quantity:g} exceeds remaining quantity {current.remaining_quantity:g}"

    filled = current.filled_quantity + message.quantity
    done = filled >= current.details.quantity - _QTY_EPSILON
    return (
        _next(
            current,
            message,
            at,
            status=OrderStatus.FILLED if done else OrderStatus.PARTIALLY_FILLED,
            filled_quantity=current.details.quantity if done else filled,
            filled_notional=current.filled_notional + message.quantity * message.price,
            fill_ids=current.fill_ids + (message.fill_id,),
        ),
        "",
    )
