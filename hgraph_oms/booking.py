"""
Books trade tickets through the booking pipeline and records the outcome.

:class:`TradeBooker` is the engine's trade sink.  For each ticket it builds the
pipeline input, maps it to the FpML-like trade message and writes it to the
output directory — the same steps as ``hgraph-tools book`` — then records in
SQLite whether the trade was booked or why it failed.

Booking is idempotent per trade ID: a ticket that is already booked is skipped,
and one that failed is tried again.  That lets the engine re-offer every fill
from its event log on start without creating duplicates.
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from hgraph_oms.trades import DEFAULT_PRODUCTS, ProductSpec, TradeTicket, ticket_to_trade_data

__all__ = ("BOOKED", "FAILED", "PENDING", "SENT", "TradeBooking", "init_booking_db", "TradeBooker")

logger = logging.getLogger(__name__)

BOOKED = "Booked"
FAILED = "Failed"

# Delivery states of a booked trade
PENDING = "Pending"
SENT = "Sent"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trade_bookings (
    trade_id     TEXT PRIMARY KEY,
    order_id     TEXT NOT NULL,
    fill_id      TEXT NOT NULL,
    quantity     REAL NOT NULL,
    price        REAL NOT NULL,
    executed_at  TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL,
    detail       TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL
)
"""
_CREATE_IDX_ORDER_ID = "CREATE INDEX IF NOT EXISTS idx_trade_bookings_order_id ON trade_bookings (order_id)"

# One row per booked trade that has been offered to the downstream system at least once
_CREATE_DELIVERIES_SQL = """
CREATE TABLE IF NOT EXISTS trade_deliveries (
    trade_id     TEXT PRIMARY KEY,
    status       TEXT NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    detail       TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL
)
"""

_COLUMNS = "trade_id, order_id, fill_id, quantity, price, executed_at, status, detail, updated_at"
_SELECT = (
    "SELECT b.trade_id, b.order_id, b.fill_id, b.quantity, b.price, b.executed_at, b.status, b.detail, b.updated_at, "
    "COALESCE(d.status, '') AS delivery_status, COALESCE(d.attempts, 0) AS delivery_attempts, "
    "COALESCE(d.detail, '') AS delivery_detail, COALESCE(d.updated_at, '') AS delivery_updated_at "
    "FROM trade_bookings b LEFT JOIN trade_deliveries d ON d.trade_id = b.trade_id"
)


@dataclass(frozen=True, kw_only=True)
class TradeBooking:
    """The booking outcome of one trade.

    :param status: :data:`BOOKED` or :data:`FAILED`.
    :param detail: The booked file's path, or why booking failed.
    :param delivery_status: ``Sent`` once the downstream system has acknowledged the trade, ``Pending``
        while delivery is being retried, empty if delivery has not been attempted.
    :param delivery_detail: Where the trade was sent, or the latest delivery error.
    """

    trade_id: str
    order_id: str
    fill_id: str
    quantity: float
    price: float
    executed_at: str
    status: str
    detail: str
    updated_at: str
    delivery_status: str = ""
    delivery_attempts: int = 0
    delivery_detail: str = ""
    delivery_updated_at: str = ""


def init_booking_db(db_path: str) -> None:
    """Create the ``trade_bookings`` and ``trade_deliveries`` tables if they do not exist."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(_CREATE_TABLE_SQL)
        conn.execute(_CREATE_IDX_ORDER_ID)
        conn.execute(_CREATE_DELIVERIES_SQL)
        conn.commit()
    finally:
        conn.close()


class TradeBooker:
    """Books tickets into ``output_dir`` and records each outcome in ``db_path``.

    :param db_path: SQLite file for the booking records.
    :param output_dir: Directory the booked trade messages are written to.
    :param internal_party: Our own legal entity on every trade.
    :param products: Product reference data used to resolve instrument names.
    """

    def __init__(
        self,
        db_path: str,
        output_dir: str,
        *,
        internal_party: str,
        products: Iterable[ProductSpec] = DEFAULT_PRODUCTS,
    ):
        self.db_path = db_path
        self.output_dir = output_dir
        self.internal_party = internal_party
        self.products = tuple(products)
        init_booking_db(db_path)

    def book(self, tickets: Iterable[TradeTicket]) -> list[TradeBooking]:
        """Book each ticket that is not already booked and return the outcomes of those attempted.

        Never raises for a ticket that cannot be booked; the failure is recorded instead.
        """
        outcomes = []
        for ticket in tickets:
            existing = self.get(ticket.trade_id)
            if existing is not None and existing.status == BOOKED:
                continue
            try:
                status, detail = BOOKED, self._book_one(ticket)
                logger.info("Booked trade %s to %s", ticket.trade_id, detail)
            except Exception as exc:
                status, detail = FAILED, str(exc)
                logger.error("Trade %s could not be booked: %s", ticket.trade_id, exc)
            outcomes.append(self._record(ticket, status, detail))
        return outcomes

    def get(self, trade_id: str) -> TradeBooking | None:
        rows = self._query("WHERE b.trade_id = ?", (trade_id,))
        return rows[0] if rows else None

    def bookings(
        self, *, order_id: str | None = None, status: str | None = None, undelivered: bool = False
    ) -> list[TradeBooking]:
        """Return booking records, oldest first.

        :param order_id: Only this order's trades.
        :param status: Only trades with this booking status.
        :param undelivered: Only booked trades the downstream system has not yet acknowledged.
        """
        clauses, params = [], []
        if order_id is not None:
            clauses.append("b.order_id = ?")
            params.append(order_id)
        if status is not None:
            clauses.append("b.status = ?")
            params.append(status)
        if undelivered:
            clauses.append(f"b.status = '{BOOKED}' AND COALESCE(d.status, '') != '{SENT}'")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._query(where, tuple(params))

    def record_delivery(self, trade_id: str, *, sent: bool, detail: str) -> None:
        """Record one delivery attempt for a booked trade.

        :param sent: ``True`` if the downstream system acknowledged the trade; otherwise it stays pending.
        :param detail: Where it was sent, or why the attempt failed.
        """
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "INSERT INTO trade_deliveries (trade_id, status, attempts, detail, updated_at) VALUES (?, ?, 1, ?, ?) "
                "ON CONFLICT(trade_id) DO UPDATE SET status = excluded.status, attempts = attempts + 1, "
                "detail = excluded.detail, updated_at = excluded.updated_at",
                (trade_id, SENT if sent else PENDING, detail, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _book_one(self, ticket: TradeTicket) -> str:
        """Map and write one trade; return the booked file's path."""
        from hgraph_trade.hgraph_trade_booker.trade_booker import book_trades_batch
        from hgraph_trade.hgraph_trade_booker.trade_mapper import map_trade_to_model

        trade_data = ticket_to_trade_data(ticket, internal_party=self.internal_party, products=self.products)
        messages = map_trade_to_model(trade_data, fail_fast=True)
        if len(messages) != 1:
            raise ValueError(f"Expected one trade message, mapping produced {len(messages)}")

        result = book_trades_batch(messages, self.output_dir)
        if result["quarantined"] or not result["booked"]:
            raise IOError(f"Trade could not be written to {self.output_dir}")
        return result["booked"][0]

    def _record(self, ticket: TradeTicket, status: str, detail: str) -> TradeBooking:
        booking = TradeBooking(
            trade_id=ticket.trade_id,
            order_id=ticket.order_id,
            fill_id=ticket.fill_id,
            quantity=ticket.quantity,
            price=ticket.price,
            executed_at=ticket.executed_at.isoformat() if ticket.executed_at else "",
            status=status,
            detail=detail,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                f"INSERT OR REPLACE INTO trade_bookings ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    booking.trade_id,
                    booking.order_id,
                    booking.fill_id,
                    booking.quantity,
                    booking.price,
                    booking.executed_at,
                    booking.status,
                    booking.detail,
                    booking.updated_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return booking

    def _query(self, where: str, params: tuple) -> list[TradeBooking]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(f"{_SELECT} {where} ORDER BY b.rowid", params).fetchall()
        finally:
            conn.close()
        return [TradeBooking(**dict(row)) for row in rows]
