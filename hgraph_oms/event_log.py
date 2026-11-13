"""
Append-only SQLite log of order messages and their outcomes.

Every message the order book processes is written here with whether it was
accepted and why not.  The log is the system of record: :func:`replay` rebuilds
every order from it, which is how the engine recovers after a restart and how
an order's history can be audited.
"""

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Iterable, NamedTuple

from hgraph_oms.model import (
    AcceptOrder,
    AmendOrder,
    CancelOrder,
    FillOrder,
    NewOrder,
    OrderDetails,
    OrderMessage,
    OrderResponse,
    OrderSnapshot,
    RejectOrder,
)
from hgraph_oms.state_machine import apply_message

__all__ = (
    "LoggedEvent",
    "init_event_log",
    "message_to_dict",
    "message_from_dict",
    "append_events",
    "read_events",
    "replay",
)


class LoggedEvent(NamedTuple):
    """One row of the log: a message, whether it was applied, why not, and when it was processed."""

    message: OrderMessage
    accepted: bool
    reason: str = ""
    recorded_at: datetime | None = None


_MESSAGE_TYPES: dict[str, type[OrderMessage]] = {
    cls.__name__: cls for cls in (NewOrder, AcceptOrder, AmendOrder, CancelOrder, RejectOrder, FillOrder)
}

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS order_events (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at   TEXT    NOT NULL,
    order_id      TEXT    NOT NULL,
    user_id       TEXT    NOT NULL DEFAULT '',
    message_type  TEXT    NOT NULL,
    payload       TEXT    NOT NULL,
    accepted      INTEGER NOT NULL,
    reason        TEXT    NOT NULL DEFAULT ''
)
"""
_CREATE_IDX_ORDER_ID = "CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events (order_id)"


def init_event_log(db_path: str) -> None:
    """Create the ``order_events`` table if it does not exist."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(_CREATE_TABLE_SQL)
        conn.execute(_CREATE_IDX_ORDER_ID)
        conn.commit()
    finally:
        conn.close()


def message_to_dict(message: OrderMessage) -> dict[str, Any]:
    """Convert a message to a JSON-serialisable dict tagged with its type."""
    type_name = type(message).__name__
    if type_name not in _MESSAGE_TYPES:
        raise ValueError(f"Cannot log message of type {type_name}")
    return {"type": type_name, **asdict(message)}


def message_from_dict(data: dict[str, Any]) -> OrderMessage:
    """Rebuild a message from :func:`message_to_dict` output.

    :raises ValueError: If the message type is not recognised.
    """
    fields = dict(data)
    type_name = fields.pop("type", None)
    cls = _MESSAGE_TYPES.get(type_name)
    if cls is None:
        raise ValueError(f"Unknown message type: {type_name!r}")
    if "details" in fields:
        fields["details"] = OrderDetails(**fields["details"])
    return cls(**fields)


def append_events(db_path: str, responses: Iterable[OrderResponse], *, recorded_at: datetime | None = None) -> None:
    """Append one row per response, in order, in a single transaction.

    Each row is stamped with the response's ``processed_at``, falling back to ``recorded_at`` or now.
    """
    fallback = recorded_at or datetime.now(timezone.utc)
    rows = [
        (
            (r.processed_at or fallback).isoformat(),
            r.message.order_id,
            r.message.user_id,
            type(r.message).__name__,
            json.dumps(message_to_dict(r.message)),
            int(r.accepted),
            r.reason,
        )
        for r in responses
    ]
    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(
            "INSERT INTO order_events (recorded_at, order_id, user_id, message_type, payload, accepted, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def read_events(db_path: str, *, order_id: str | None = None) -> list[LoggedEvent]:
    """Return logged events in the order they were processed, optionally for one order."""
    query = "SELECT payload, accepted, reason, recorded_at FROM order_events"
    params: tuple[str, ...] = ()
    if order_id is not None:
        query += " WHERE order_id = ?"
        params = (order_id,)
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(query + " ORDER BY seq", params).fetchall()
    finally:
        conn.close()
    return [
        LoggedEvent(message_from_dict(json.loads(payload)), bool(accepted), reason, datetime.fromisoformat(recorded_at))
        for payload, accepted, reason, recorded_at in rows
    ]


def replay(events: Iterable[LoggedEvent | tuple]) -> dict[str, OrderSnapshot]:
    """Rebuild every order from logged events.

    Pre-trade checks are not re-run: the logged outcome is authoritative, because
    credit and entitlements may have changed since.  Accepted messages are re-applied
    through the state machine; a refused :class:`NewOrder` is restored as a ``Rejected`` order.

    :raises ValueError: If an accepted message can no longer be applied, which means the log
        and the state machine disagree.
    """
    orders: dict[str, OrderSnapshot] = {}
    for event in events:
        message, accepted, reason, at = LoggedEvent(*event)
        current = orders.get(message.order_id)
        if accepted:
            updated, failure = apply_message(current, message, at=at)
            if failure:
                raise ValueError(f"Logged event for order {message.order_id} cannot be replayed: {failure}")
        elif isinstance(message, NewOrder):
            updated, _ = apply_message(current, message, check_failure=reason, at=at)
        else:
            continue
        if updated is not None:
            orders[updated.order_id] = updated
    return orders
