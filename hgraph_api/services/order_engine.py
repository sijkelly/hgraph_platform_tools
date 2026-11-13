"""
The API's connection to the order engine.

Owns the single :class:`~hgraph_oms.engine.OrderEngine` for an app, converts
between the API's string fields and the engine's order model, and refreshes
the engine's credit and entitlement inputs from their stores before a message
that depends on them is submitted.
"""

import logging
import math
import os
import threading
import uuid
from typing import Any

from fastapi import FastAPI

from hgraph_api.config import api_config
from hgraph_oms.booking import BOOKED, PENDING, TradeBooker, TradeBooking
from hgraph_oms.delivery import TradePublisher, TradeSender, kafka_trade_sender
from hgraph_oms.engine import OrderEngine
from hgraph_oms.settings import database_target
from hgraph_oms.model import CreditPosition, OrderDetails, OrderSnapshot

__all__ = (
    "ORDER_EVENTS_DB",
    "TRADE_BOOKINGS_DB",
    "order_db_target",
    "get_order_engine",
    "get_trade_booker",
    "stop_order_engine",
    "new_order_id",
    "parse_number",
    "format_number",
    "details_from_fields",
    "order_to_dict",
    "booking_to_dict",
    "orders_to_dicts",
    "sync_credit",
    "sync_permissions",
)

logger = logging.getLogger(__name__)

ORDER_EVENTS_DB = "order_events.db"
TRADE_BOOKINGS_DB = "trade_bookings.db"

_engine_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Engine lifecycle
# ---------------------------------------------------------------------------


def _setting(name: str, default: str = "") -> str:
    """Read a setting from the environment now, falling back to the value loaded at import."""
    return os.getenv(name) or api_config.get(name) or default


def order_db_target(app: FastAPI | None, db_dir: str, filename: str) -> str:
    """Return where one of the API's stores keeps its data.

    With ``DATABASE_URL`` set (or ``app.state.database_url``) every store uses that PostgreSQL
    database, in the tenant's own schema if there is one; otherwise each has its own SQLite
    file in ``db_dir``.
    """
    state = app.state if app is not None else None
    return database_target(
        os.path.join(db_dir, filename),
        database_url=getattr(state, "database_url", None),
        schema=getattr(state, "database_schema", None),
    )


def _trade_publisher(app: FastAPI, booker: TradeBooker) -> TradePublisher | None:
    """Build the publisher that delivers booked trades, or ``None`` if no topic is configured.

    ``app.state.trade_sender`` replaces the Kafka sender, e.g. in tests.
    """
    topic = _setting("TRADE_KAFKA_TOPIC")
    if not topic:
        return None
    sender: TradeSender = getattr(app.state, "trade_sender", None) or kafka_trade_sender(
        _setting("KAFKA_BOOTSTRAP_SERVERS") or None
    )
    return TradePublisher(booker, topic, sender)


def get_order_engine(app: FastAPI) -> OrderEngine:
    """Return the app's running order engine, starting it on first use.

    The engine is started lazily so that ``app.state.db_dir`` and
    ``app.state.enforce_entitlements`` can be set after the app is created.
    """
    with _engine_lock:
        engine: OrderEngine | None = getattr(app.state, "order_engine", None)
        if engine is not None and engine.is_running:
            return engine
        if engine is not None:
            logger.error("Order engine was not running; restarting it")
            engine.stop()
            old_publisher: TradePublisher | None = getattr(app.state, "trade_publisher", None)
            if old_publisher is not None:
                old_publisher.stop()

        db_dir = getattr(app.state, "db_dir", ".")
        # Every fill is booked as a trade; TRADE_OUTPUT_DIR overrides where the trade files go
        booker = TradeBooker(
            order_db_target(app, db_dir, TRADE_BOOKINGS_DB),
            _setting("TRADE_OUTPUT_DIR", os.path.join(db_dir, "booked_trades")),
            internal_party=_setting("INTERNAL_PARTY_ID", "INTERNAL"),
        )
        # ...and, if a topic is configured, delivered to it from a background thread
        publisher = _trade_publisher(app, booker)

        def trade_sink(tickets) -> None:
            booker.book(tickets)
            if publisher is not None:
                publisher.notify()

        engine = OrderEngine(
            order_db_target(app, db_dir, ORDER_EVENTS_DB),
            enforce_entitlements=bool(getattr(app.state, "enforce_entitlements", False)),
            trade_sink=trade_sink,
        )
        engine.start()
        if publisher is not None:
            publisher.start()
        app.state.order_engine = engine
        app.state.trade_booker = booker
        app.state.trade_publisher = publisher
        return engine


def get_trade_booker(app: FastAPI) -> TradeBooker:
    """Return the booker attached to the app's order engine, starting the engine if needed."""
    get_order_engine(app)
    return app.state.trade_booker


def stop_order_engine(app: FastAPI) -> None:
    """Stop the app's order engine if it was started."""
    with _engine_lock:
        engine: OrderEngine | None = getattr(app.state, "order_engine", None)
        publisher: TradePublisher | None = getattr(app.state, "trade_publisher", None)
        app.state.order_engine = None
        app.state.trade_publisher = None
    if engine is not None:
        engine.stop()
    if publisher is not None:
        publisher.stop()


# ---------------------------------------------------------------------------
# Conversion between API fields and the order model
# ---------------------------------------------------------------------------


def new_order_id() -> str:
    return f"ORD-{uuid.uuid4().hex[:8].upper()}"


def parse_number(text: str, field: str) -> float:
    """Parse a numeric API field such as ``"50,000"``.

    :raises ValueError: If the text is empty or not a finite number.
    """
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        raise ValueError(f"{field} must not be empty")
    try:
        value = float(cleaned)
    except ValueError:
        raise ValueError(f"{field} must be a number") from None
    if not math.isfinite(value):
        raise ValueError(f"{field} must be a number")
    return value


def format_number(value: float | None) -> str:
    """Format a number for the API: ``50000.0`` becomes ``"50000"``; ``None`` becomes ``""``."""
    return "" if value is None else format(value, ".15g")


def details_from_fields(
    *,
    instrument: str,
    side: str,
    quantity: str,
    unit: str = "",
    price: str = "",
    counterparty: str = "",
    delivery_period: str = "",
    order_type: str = "Limit",
    portfolio: str = "",
    book: str = "",
) -> OrderDetails:
    """Build :class:`OrderDetails` from the API's string fields.

    A limit order must carry a price and a market order must not.

    :raises ValueError: If a number cannot be parsed or the price does not fit the order type.
    """
    has_price = bool(price.strip())
    if order_type == "Market":
        if has_price:
            raise ValueError("A Market order must not have a price")
    elif order_type == "Limit":
        if not has_price:
            raise ValueError("A Limit order needs a price")
    else:
        raise ValueError("Order type must be 'Limit' or 'Market'")

    return OrderDetails(
        instrument=instrument,
        side=side,
        quantity=parse_number(quantity, "Quantity"),
        unit=unit,
        price=parse_number(price, "Price") if has_price else None,
        counterparty=counterparty,
        delivery_period=delivery_period,
        portfolio=portfolio,
        book=book,
    )


def order_to_dict(order: OrderSnapshot) -> dict[str, Any]:
    """Convert an order snapshot to the API's order representation."""
    details = order.details
    return {
        "order_id": order.order_id,
        "instrument": details.instrument,
        "side": details.side,
        "quantity": format_number(details.quantity),
        "unit": details.unit,
        "price": format_number(details.price),
        "counterparty": details.counterparty,
        "delivery_period": details.delivery_period,
        "order_type": details.order_type,
        "portfolio": details.portfolio,
        "book": details.book,
        "status": order.status.value,
        "filled_quantity": format_number(order.filled_quantity) if order.filled_quantity else "",
        "average_price": format_number(order.average_price),
        "reason": order.reason,
        "version": order.version,
        "created_by": order.created_by,
        "updated_by": order.updated_by,
        "created_at": order.created_at.isoformat() if order.created_at else "",
        "updated_at": order.updated_at.isoformat() if order.updated_at else "",
    }


def booking_to_dict(booking: TradeBooking, *, delivery_enabled: bool = False) -> dict[str, Any]:
    """Convert a trade booking record to the API's representation.

    :param delivery_enabled: Whether booked trades are being delivered downstream; a booked trade
        not yet attempted then shows as ``Pending`` rather than blank.
    """
    delivery_status = booking.delivery_status
    if not delivery_status and delivery_enabled and booking.status == BOOKED:
        delivery_status = PENDING
    return {
        "trade_id": booking.trade_id,
        "order_id": booking.order_id,
        "fill_id": booking.fill_id,
        "quantity": format_number(booking.quantity),
        "price": format_number(booking.price),
        "executed_at": booking.executed_at,
        "status": booking.status,
        "detail": booking.detail,
        "updated_at": booking.updated_at,
        "delivery_status": delivery_status,
        "delivery_attempts": booking.delivery_attempts,
        "delivery_detail": booking.delivery_detail,
        "delivery_updated_at": booking.delivery_updated_at,
    }


def orders_to_dicts(engine: OrderEngine, *, limit: int = 500) -> list[dict[str, Any]]:
    """Return the engine's orders in API form, most recent first."""
    orders = sorted(engine.orders(), key=lambda o: (o.created_at is not None, o.created_at), reverse=True)
    return [order_to_dict(o) for o in orders[:limit]]


# ---------------------------------------------------------------------------
# Engine inputs from the stores
# ---------------------------------------------------------------------------


def sync_credit(engine: OrderEngine, credit_db: str, counterparty: str) -> None:
    """Publish the counterparty's current credit position from the credit store to the engine.

    Call this before submitting a message that is credit checked.  The position and the
    message travel through the same queue in order, so the check sees this position.

    :raises Exception: Whatever the credit store raises; the caller must fail closed.
    """
    from hgraph_static_admin import credit_store

    credit_store.init_credit_db(credit_db)
    utilization = credit_store.get_credit_utilization(credit_db, counterparty)
    if utilization is None:
        engine.clear_credit(counterparty)  # no limit configured
        return

    if utilization.utilization_percentage >= 90:
        logger.warning("High credit utilization for %s: %.1f%%", counterparty, utilization.utilization_percentage)
    engine.set_credit(
        CreditPosition(
            counterparty=counterparty,
            limit_amount=utilization.limit_amount,
            utilized_amount=utilization.utilized_amount,
        )
    )


def sync_permissions(engine: OrderEngine, user_id: str) -> None:
    """Publish the user's current entitlement actions to the engine, if it enforces them.

    :raises Exception: Whatever the entitlements store raises; the caller must fail closed.
    """
    if not engine.enforce_entitlements:
        return

    from hgraph_entitlements.checker import get_allowed_actions
    from hgraph_entitlements.example_code import get_db_connection, get_user_role, initialize_db

    conn = get_db_connection()
    try:
        initialize_db(conn)
        role = get_user_role(conn, user_id)
    finally:
        conn.close()
    engine.set_permissions(user_id, frozenset(get_allowed_actions(role)) if role else frozenset())
