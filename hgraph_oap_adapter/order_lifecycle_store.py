"""
SQLite-backed event store for order lifecycle events.

Persists the flat dictionaries produced by
:func:`~hgraph_oap_adapter.order_analytics_adapter.order_to_lifecycle_dict`
so they can be queried offline without a live Kafka/hgraph connection.

Events are *immutable* once written — each row captures the order state at a
specific point in time.  Duplicate event IDs are silently ignored (idempotent
inserts), making it safe to replay Kafka topics.

Schema
------
``order_lifecycle_events`` table:

.. code-block:: sql

    event_id             TEXT  UNIQUE     -- natural key (EVT-…)
    order_id             TEXT  NOT NULL   -- parent order identifier
    timestamp            TEXT  NOT NULL   -- ISO-8601 UTC timestamp
    state                TEXT  NOT NULL   -- OrderLifecycleState.value
    salesperson_id       TEXT
    desk                 TEXT
    instrument_category  TEXT
    underlying_asset     TEXT
    counterparty         TEXT
    notional_qty         REAL
    notional_unit        TEXT
    price                REAL
    price_currency       TEXT
    ev_amount            REAL
    ev_currency          TEXT
    missed_reason        TEXT
    missed_detail        TEXT
    notes                TEXT

Usage::

    from hgraph_oap_adapter.order_lifecycle_store import (
        init_order_lifecycle_db,
        insert_lifecycle_event,
        get_events_for_order,
        get_latest_event_for_order,
    )

    init_order_lifecycle_db("order_lifecycle.db")
    inserted = insert_lifecycle_event("order_lifecycle.db", event_dict)
"""

import logging
import sqlite3
from typing import Any, Dict, List

__all__ = (
    "init_order_lifecycle_db",
    "insert_lifecycle_event",
    "get_events_for_order",
    "get_latest_event_for_order",
    "get_events_by_state",
    "get_all_events",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS order_lifecycle_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id         TEXT    NOT NULL UNIQUE,
    order_id         TEXT    NOT NULL,
    timestamp        TEXT    NOT NULL,
    state            TEXT    NOT NULL,
    salesperson_id   TEXT    NOT NULL DEFAULT '',
    desk             TEXT    NOT NULL DEFAULT '',
    instrument_category TEXT NOT NULL DEFAULT '',
    underlying_asset TEXT    NOT NULL DEFAULT '',
    counterparty     TEXT    NOT NULL DEFAULT '',
    notional_qty     REAL    NOT NULL DEFAULT 0.0,
    notional_unit    TEXT    NOT NULL DEFAULT '',
    price            REAL    NOT NULL DEFAULT 0.0,
    price_currency   TEXT    NOT NULL DEFAULT '',
    ev_amount        REAL    NOT NULL DEFAULT 0.0,
    ev_currency      TEXT    NOT NULL DEFAULT '',
    missed_reason    TEXT    NOT NULL DEFAULT '',
    missed_detail    TEXT    NOT NULL DEFAULT '',
    notes            TEXT    NOT NULL DEFAULT ''
)
"""

_CREATE_IDX_ORDER_ID = (
    "CREATE INDEX IF NOT EXISTS idx_order_lifecycle_order_id ON order_lifecycle_events (order_id)"
)
_CREATE_IDX_STATE = (
    "CREATE INDEX IF NOT EXISTS idx_order_lifecycle_state ON order_lifecycle_events (state)"
)
_CREATE_IDX_TIMESTAMP = (
    "CREATE INDEX IF NOT EXISTS idx_order_lifecycle_timestamp ON order_lifecycle_events (timestamp)"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    """Convert a sqlite3.Row to a plain dict, dropping the auto-increment id."""
    d = dict(row)
    d.pop("id", None)
    return d


def _validate_event_dict(event: Dict[str, Any]) -> None:
    """Raise ValueError if mandatory fields are absent or empty."""
    for field in ("event_id", "order_id", "timestamp", "state"):
        if not event.get(field):
            raise ValueError(f"Order lifecycle event missing required field: '{field}'")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_order_lifecycle_db(db_path: str) -> None:
    """Create the ``order_lifecycle_events`` table and indexes if they do not exist.

    Safe to call multiple times (idempotent).

    :param db_path: Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(_CREATE_TABLE_SQL)
        conn.execute(_CREATE_IDX_ORDER_ID)
        conn.execute(_CREATE_IDX_STATE)
        conn.execute(_CREATE_IDX_TIMESTAMP)
        conn.commit()
    finally:
        conn.close()
    logger.info("Order lifecycle event table initialised at %s", db_path)


def insert_lifecycle_event(db_path: str, event: Dict[str, Any]) -> bool:
    """Insert an order lifecycle event record.

    The insert is *idempotent*: if a row with the same ``event_id`` already
    exists the call succeeds but returns ``False`` without modifying the
    existing row.

    :param db_path: Path to the SQLite database file.
    :param event: Flat event dictionary as produced by
        :func:`~hgraph_oap_adapter.order_analytics_adapter.order_to_lifecycle_dict`.
    :returns: ``True`` if the row was newly inserted, ``False`` if it was a duplicate.
    :raises ValueError: If mandatory fields are missing.
    """
    _validate_event_dict(event)

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO order_lifecycle_events (
                event_id, order_id, timestamp, state,
                salesperson_id, desk, instrument_category, underlying_asset,
                counterparty, notional_qty, notional_unit,
                price, price_currency,
                ev_amount, ev_currency,
                missed_reason, missed_detail, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["order_id"],
                str(event["timestamp"]),
                str(event["state"]),
                event.get("salesperson_id", ""),
                event.get("desk", ""),
                event.get("instrument_category", ""),
                event.get("underlying_asset", ""),
                event.get("counterparty", ""),
                float(event.get("notional_qty", 0.0)),
                event.get("notional_unit", ""),
                float(event.get("price", 0.0)),
                event.get("price_currency", ""),
                float(event.get("ev_amount", 0.0)),
                event.get("ev_currency", ""),
                event.get("missed_reason", ""),
                event.get("missed_detail", ""),
                event.get("notes", ""),
            ),
        )
        conn.commit()
        inserted = cursor.rowcount > 0
    finally:
        conn.close()

    if inserted:
        logger.debug("Inserted lifecycle event %s for order %s", event["event_id"], event["order_id"])
    else:
        logger.debug("Duplicate lifecycle event ignored: %s", event["event_id"])
    return inserted


def get_events_for_order(db_path: str, order_id: str) -> List[Dict[str, Any]]:
    """Return all lifecycle events for a given order, ordered by timestamp ascending.

    :param db_path: Path to the SQLite database file.
    :param order_id: The order identifier to look up.
    :returns: List of event dicts (may be empty if order not found).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM order_lifecycle_events WHERE order_id = ? ORDER BY timestamp ASC",
            (order_id,),
        ).fetchall()
    finally:
        conn.close()
    return [_row_to_dict(r) for r in rows]


def get_latest_event_for_order(db_path: str, order_id: str) -> Dict[str, Any] | None:
    """Return the most recent lifecycle event for a given order.

    "Most recent" is determined by the ``timestamp`` column (lexicographic
    ordering of ISO-8601 strings).

    :param db_path: Path to the SQLite database file.
    :param order_id: The order identifier to look up.
    :returns: The latest event dict, or ``None`` if the order has no events.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM order_lifecycle_events WHERE order_id = ? ORDER BY timestamp DESC LIMIT 1",
            (order_id,),
        ).fetchone()
    finally:
        conn.close()
    return _row_to_dict(row) if row is not None else None


def get_events_by_state(db_path: str, state: str) -> List[Dict[str, Any]]:
    """Return all lifecycle events in a specific state, ordered by timestamp ascending.

    :param db_path: Path to the SQLite database file.
    :param state: The state string to filter on (e.g. ``"RECEIVED"``, ``"MISSED"``).
    :returns: List of event dicts matching the given state.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM order_lifecycle_events WHERE state = ? ORDER BY timestamp ASC",
            (state,),
        ).fetchall()
    finally:
        conn.close()
    return [_row_to_dict(r) for r in rows]


def get_all_events(db_path: str, *, limit: int = 1000) -> List[Dict[str, Any]]:
    """Return up to *limit* lifecycle events ordered by timestamp ascending.

    :param db_path: Path to the SQLite database file.
    :param limit: Maximum number of rows to return (default: 1000).
    :returns: List of event dicts.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM order_lifecycle_events ORDER BY timestamp ASC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [_row_to_dict(r) for r in rows]
