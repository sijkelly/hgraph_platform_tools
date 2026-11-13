"""
Order model for the OMS core.

Everything here is an immutable value: the messages that ask for an order to
change, the snapshot describing an order after a change, and the response that
says whether a message was applied.  The same values flow through the pure
state machine, the hgraph order book graph and the event log.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

__all__ = (
    "OPEN_STATUSES",
    "OrderStatus",
    "OrderDetails",
    "OmsInput",
    "OrderMessage",
    "NewOrder",
    "AcceptOrder",
    "AmendOrder",
    "CancelOrder",
    "RejectOrder",
    "FillOrder",
    "CreditPosition",
    "CreditRemoved",
    "OrderSnapshot",
    "OrderResponse",
)


class OrderStatus(Enum):
    """Lifecycle states of an order. Values match the strings used by the REST API and UI."""

    PENDING = "Pending"
    WORKING = "Working"
    PARTIALLY_FILLED = "Partially Filled"
    FILLED = "Filled"
    REJECTED = "Rejected"
    CANCELLED = "Cancelled"


OPEN_STATUSES = frozenset({OrderStatus.PENDING, OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED})


@dataclass(frozen=True, kw_only=True)
class OrderDetails:
    """The economic terms of an order.

    :param price: Limit price per unit, or ``None`` for a market order.
    """

    instrument: str
    side: str
    quantity: float
    unit: str = ""
    price: float | None = None
    counterparty: str = ""
    delivery_period: str = ""
    portfolio: str = ""
    book: str = ""

    @property
    def order_type(self) -> str:
        return "Market" if self.price is None else "Limit"


@dataclass(frozen=True)
class OmsInput:
    """Marker base for everything that can be pushed into the order book graph."""


@dataclass(frozen=True, kw_only=True)
class OrderMessage(OmsInput):
    """A request or event addressed to one order.

    :param order_id: The order the message applies to.
    :param user_id: Who sent it; recorded on the order for audit.
    :param request_id: Caller-supplied correlation ID, echoed back on the response.
    """

    order_id: str
    user_id: str = ""
    request_id: str = ""


@dataclass(frozen=True, kw_only=True)
class NewOrder(OrderMessage):
    """Create an order. It is pre-trade checked and, if it passes, starts as ``Pending``."""

    details: OrderDetails


@dataclass(frozen=True, kw_only=True)
class AcceptOrder(OrderMessage):
    """The desk takes a ``Pending`` order on; it becomes ``Working``."""


@dataclass(frozen=True, kw_only=True)
class AmendOrder(OrderMessage):
    """Change the quantity and/or limit price of an open order. ``None`` leaves a field unchanged."""

    quantity: float | None = None
    price: float | None = None


@dataclass(frozen=True, kw_only=True)
class CancelOrder(OrderMessage):
    """Cancel an open order. Any quantity already filled stays filled."""

    reason: str = ""


@dataclass(frozen=True, kw_only=True)
class RejectOrder(OrderMessage):
    """The desk declines a ``Pending`` order."""

    reason: str = ""


@dataclass(frozen=True, kw_only=True)
class FillOrder(OrderMessage):
    """An execution against a ``Working`` or ``Partially Filled`` order.

    :param fill_id: Unique per order; a repeated ``fill_id`` is rejected so replays cannot double-count.
    """

    fill_id: str
    quantity: float
    price: float


@dataclass(frozen=True, kw_only=True)
class CreditPosition(OmsInput):
    """A counterparty's credit limit and the amount already utilised by booked trades."""

    counterparty: str
    limit_amount: float
    utilized_amount: float = 0.0


@dataclass(frozen=True, kw_only=True)
class CreditRemoved(OmsInput):
    """The counterparty no longer has a credit limit configured."""

    counterparty: str


@dataclass(frozen=True, kw_only=True)
class OrderSnapshot:
    """The full state of an order after its latest change.

    :param version: Starts at 0 and increases by one with every applied message.
    :param reason: Why the order was rejected or cancelled; empty otherwise.
    :param created_at: When the order was first processed (UTC), if the caller supplied a time.
    :param updated_at: When the latest applied message was processed (UTC).
    """

    order_id: str
    details: OrderDetails
    status: OrderStatus
    version: int = 0
    filled_quantity: float = 0.0
    filled_notional: float = 0.0
    fill_ids: tuple[str, ...] = ()
    reason: str = ""
    created_by: str = ""
    updated_by: str = ""
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def is_open(self) -> bool:
        return self.status in OPEN_STATUSES

    @property
    def remaining_quantity(self) -> float:
        return max(self.details.quantity - self.filled_quantity, 0.0)

    @property
    def average_price(self) -> float | None:
        return self.filled_notional / self.filled_quantity if self.filled_quantity else None

    @property
    def open_exposure(self) -> float:
        """Notional still to be executed; zero for closed orders and market orders."""
        if not self.is_open or self.details.price is None:
            return 0.0
        return self.remaining_quantity * self.details.price


@dataclass(frozen=True, kw_only=True)
class OrderResponse:
    """The outcome of one message.

    :param accepted: ``False`` means the message was refused; ``reason`` says why.
    :param snapshot: The order after the message, or ``None`` if the order does not exist.
        A refused :class:`NewOrder` still produces a ``Rejected`` snapshot so it is on record.
    :param processed_at: When the order book processed the message (UTC).
    """

    message: OrderMessage
    accepted: bool
    reason: str = ""
    snapshot: OrderSnapshot | None = None
    processed_at: datetime | None = None
