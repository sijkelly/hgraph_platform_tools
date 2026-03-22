"""
Simple SQLite order store for the OMS API.

Provides CRUD operations for orders submitted through the UI.
This is a lightweight persistence layer — the canonical order lifecycle
is tracked via :mod:`hgraph_oap_adapter.order_lifecycle_store`.
"""

import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

__all__ = (
    "init_order_db",
    "create_order",
    "get_order",
    "get_all_orders",
    "update_order_status",
    "cancel_order",
)

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id         TEXT PRIMARY KEY,
    instrument       TEXT NOT NULL,
    side             TEXT NOT NULL,
    quantity         TEXT NOT NULL,
    unit             TEXT NOT NULL DEFAULT '',
    price            TEXT NOT NULL DEFAULT '',
    counterparty     TEXT NOT NULL DEFAULT '',
    delivery_period  TEXT NOT NULL DEFAULT '',
    order_type       TEXT NOT NULL DEFAULT 'Limit',
    portfolio        TEXT NOT NULL DEFAULT '',
    book             TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'Pending',
    filled_quantity  TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    created_by       TEXT NOT NULL DEFAULT ''
)
"""

_CREATE_IDX_STATUS = "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status)"
_CREATE_IDX_COUNTERPARTY = "CREATE INDEX IF NOT EXISTS idx_orders_counterparty ON orders (counterparty)"


def init_order_db(db_path: str) -> None:
    """Create the orders table if it does not exist."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(_CREATE_TABLE_SQL)
        conn.execute(_CREATE_IDX_STATUS)
        conn.execute(_CREATE_IDX_COUNTERPARTY)
        conn.commit()
    finally:
        conn.close()
    logger.info("Order table initialised at %s", db_path)


def create_order(db_path: str, data: dict[str, Any], *, created_by: str = "") -> dict[str, Any]:
    """Insert a new order and return the full order dict including generated ID and timestamps."""
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    now = datetime.now(timezone.utc).isoformat()

    order = {
        "order_id": order_id,
        "instrument": data["instrument"],
        "side": data["side"],
        "quantity": str(data["quantity"]),
        "unit": data.get("unit", ""),
        "price": str(data.get("price", "")),
        "counterparty": data.get("counterparty", ""),
        "delivery_period": data.get("delivery_period", ""),
        "order_type": data.get("order_type", "Limit"),
        "portfolio": data.get("portfolio", ""),
        "book": data.get("book", ""),
        "status": "Pending",
        "filled_quantity": "",
        "created_at": now,
        "updated_at": now,
        "created_by": created_by,
    }

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO orders (
                order_id, instrument, side, quantity, unit, price,
                counterparty, delivery_period, order_type, portfolio, book,
                status, filled_quantity, created_at, updated_at, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            tuple(order.values()),
        )
        conn.commit()
    finally:
        conn.close()

    logger.info("Created order %s: %s %s %s", order_id, data["side"], data["quantity"], data["instrument"])
    return order


def get_order(db_path: str, order_id: str) -> dict[str, Any] | None:
    """Return a single order by ID, or None if not found."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def get_all_orders(db_path: str, *, limit: int = 500) -> list[dict[str, Any]]:
    """Return all orders, most recent first."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def update_order_status(
    db_path: str, order_id: str, status: str, *, filled_quantity: str = ""
) -> dict[str, Any] | None:
    """Update the status (and optionally filled_quantity) of an order. Returns updated order or None."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "UPDATE orders SET status = ?, filled_quantity = ?, updated_at = ? WHERE order_id = ?",
            (status, filled_quantity, now, order_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def cancel_order(db_path: str, order_id: str) -> dict[str, Any] | None:
    """Cancel an order. Only Pending/Working orders can be cancelled."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
        if not row:
            return None
        order = dict(row)
        if order["status"] not in ("Pending", "Working"):
            return order  # Cannot cancel — return current state
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE orders SET status = 'Cancelled', updated_at = ? WHERE order_id = ?",
            (now, order_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None
