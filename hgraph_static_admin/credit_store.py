"""
SQLite-backed storage for credit limit and utilization data.

Provides CRUD operations for two tables:

- ``credit_limits`` — stores :class:`~hg_oap.credit.credit_limit.CreditLimit`
  records keyed by ``(counterparty_symbol, limit_type)``.
- ``credit_utilizations`` — stores
  :class:`~hg_oap.credit.credit_limit.CreditUtilization` snapshots
  keyed by ``counterparty_symbol`` (latest snapshot only).

This module is the persistence layer for the credit Kafka subscriber and
can also be used standalone for local credit data management.
"""

import logging
import sqlite3
from datetime import date, datetime, timezone
from typing import List

from hg_oap.credit.credit_limit import (
    CreditLimit,
    CreditLimitType,
    CreditStatus,
    CreditUtilization,
)

__all__ = (
    "init_credit_db",
    "upsert_credit_limit",
    "get_credit_limit",
    "get_credit_limits_for_counterparty",
    "get_all_credit_limits",
    "delete_credit_limit",
    "upsert_credit_utilization",
    "get_credit_utilization",
    "get_all_credit_utilizations",
    "delete_credit_utilization",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_CREDIT_LIMITS_SQL = """
CREATE TABLE IF NOT EXISTS credit_limits (
    counterparty_symbol TEXT NOT NULL,
    limit_type TEXT NOT NULL,
    limit_amount REAL NOT NULL,
    limit_currency TEXT NOT NULL DEFAULT 'USD',
    effective_date TEXT,
    expiry_date TEXT,
    status TEXT NOT NULL DEFAULT 'Active',
    approved_by TEXT,
    last_review_date TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (counterparty_symbol, limit_type)
)
"""

_CREATE_CREDIT_UTILIZATIONS_SQL = """
CREATE TABLE IF NOT EXISTS credit_utilizations (
    counterparty_symbol TEXT PRIMARY KEY,
    utilized_amount REAL NOT NULL,
    utilization_currency TEXT NOT NULL DEFAULT 'USD',
    limit_amount REAL NOT NULL DEFAULT 0.0,
    available_amount REAL NOT NULL DEFAULT 0.0,
    utilization_percentage REAL NOT NULL DEFAULT 0.0,
    as_of_timestamp TEXT,
    updated_at TEXT NOT NULL
)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _parse_date(value: str | None) -> date | None:
    """Parse an ISO date string, returning ``None`` if empty."""
    if value is None:
        return None
    return date.fromisoformat(value)


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------


def init_credit_db(db_path: str) -> None:
    """Create the ``credit_limits`` and ``credit_utilizations`` tables.

    :param db_path: Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(_CREATE_CREDIT_LIMITS_SQL)
        conn.execute(_CREATE_CREDIT_UTILIZATIONS_SQL)
        conn.commit()
    finally:
        conn.close()
    logger.info("Credit database initialised at %s", db_path)


# ---------------------------------------------------------------------------
# Credit Limit CRUD
# ---------------------------------------------------------------------------


def upsert_credit_limit(db_path: str, limit: CreditLimit) -> None:
    """Insert or update a credit limit record.

    :param db_path: Path to the SQLite database file.
    :param limit: The credit limit to store.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO credit_limits (counterparty_symbol, limit_type, limit_amount,
                                       limit_currency, effective_date, expiry_date,
                                       status, approved_by, last_review_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(counterparty_symbol, limit_type) DO UPDATE SET
                limit_amount = excluded.limit_amount,
                limit_currency = excluded.limit_currency,
                effective_date = excluded.effective_date,
                expiry_date = excluded.expiry_date,
                status = excluded.status,
                approved_by = excluded.approved_by,
                last_review_date = excluded.last_review_date,
                updated_at = excluded.updated_at
            """,
            (
                limit.counterparty_symbol,
                limit.limit_type.value,
                limit.limit_amount,
                limit.limit_currency,
                limit.effective_date.isoformat() if limit.effective_date else None,
                limit.expiry_date.isoformat() if limit.expiry_date else None,
                limit.status.value,
                limit.approved_by,
                limit.last_review_date.isoformat() if limit.last_review_date else None,
                _now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_credit_limit(db_path: str, counterparty_symbol: str, limit_type: str) -> CreditLimit | None:
    """Retrieve a credit limit by counterparty and type.

    :param db_path: Path to the SQLite database file.
    :param counterparty_symbol: The counterparty's short identifier.
    :param limit_type: The limit type value (e.g. ``"Bilateral"``).
    :returns: The :class:`CreditLimit`, or ``None`` if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM credit_limits WHERE counterparty_symbol = ? AND limit_type = ?",
            (counterparty_symbol, limit_type),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return CreditLimit(
        counterparty_symbol=row["counterparty_symbol"],
        limit_type=CreditLimitType(row["limit_type"]),
        limit_amount=row["limit_amount"],
        limit_currency=row["limit_currency"],
        effective_date=_parse_date(row["effective_date"]),
        expiry_date=_parse_date(row["expiry_date"]),
        status=CreditStatus(row["status"]),
        approved_by=row["approved_by"],
        last_review_date=_parse_date(row["last_review_date"]),
    )


def get_credit_limits_for_counterparty(db_path: str, counterparty_symbol: str) -> List[CreditLimit]:
    """Return all credit limits for a single counterparty.

    This is the primary query for the salesperson view — it returns
    every limit type (Bilateral, Cleared, etc.) for the selected
    counterparty.

    :param db_path: Path to the SQLite database file.
    :param counterparty_symbol: The counterparty's short identifier.
    :returns: List of :class:`CreditLimit` instances.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM credit_limits WHERE counterparty_symbol = ? ORDER BY limit_type",
            (counterparty_symbol,),
        ).fetchall()
    finally:
        conn.close()

    return [
        CreditLimit(
            counterparty_symbol=r["counterparty_symbol"],
            limit_type=CreditLimitType(r["limit_type"]),
            limit_amount=r["limit_amount"],
            limit_currency=r["limit_currency"],
            effective_date=_parse_date(r["effective_date"]),
            expiry_date=_parse_date(r["expiry_date"]),
            status=CreditStatus(r["status"]),
            approved_by=r["approved_by"],
            last_review_date=_parse_date(r["last_review_date"]),
        )
        for r in rows
    ]


def get_all_credit_limits(db_path: str) -> List[CreditLimit]:
    """Return all credit limits.

    :param db_path: Path to the SQLite database file.
    :returns: List of :class:`CreditLimit` instances.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM credit_limits ORDER BY counterparty_symbol, limit_type"
        ).fetchall()
    finally:
        conn.close()

    return [
        CreditLimit(
            counterparty_symbol=r["counterparty_symbol"],
            limit_type=CreditLimitType(r["limit_type"]),
            limit_amount=r["limit_amount"],
            limit_currency=r["limit_currency"],
            effective_date=_parse_date(r["effective_date"]),
            expiry_date=_parse_date(r["expiry_date"]),
            status=CreditStatus(r["status"]),
            approved_by=r["approved_by"],
            last_review_date=_parse_date(r["last_review_date"]),
        )
        for r in rows
    ]


def delete_credit_limit(db_path: str, counterparty_symbol: str, limit_type: str) -> bool:
    """Delete a credit limit by counterparty and type.

    :param db_path: Path to the SQLite database file.
    :param counterparty_symbol: The counterparty's short identifier.
    :param limit_type: The limit type value (e.g. ``"Bilateral"``).
    :returns: ``True`` if a row was deleted, ``False`` if not found.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM credit_limits WHERE counterparty_symbol = ? AND limit_type = ?",
            (counterparty_symbol, limit_type),
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Credit Utilization CRUD
# ---------------------------------------------------------------------------


def upsert_credit_utilization(db_path: str, utilization: CreditUtilization) -> None:
    """Insert or update a credit utilization snapshot.

    Only the latest snapshot per counterparty is stored.

    :param db_path: Path to the SQLite database file.
    :param utilization: The utilization snapshot to store.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO credit_utilizations (counterparty_symbol, utilized_amount,
                                             utilization_currency, limit_amount,
                                             available_amount, utilization_percentage,
                                             as_of_timestamp, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(counterparty_symbol) DO UPDATE SET
                utilized_amount = excluded.utilized_amount,
                utilization_currency = excluded.utilization_currency,
                limit_amount = excluded.limit_amount,
                available_amount = excluded.available_amount,
                utilization_percentage = excluded.utilization_percentage,
                as_of_timestamp = excluded.as_of_timestamp,
                updated_at = excluded.updated_at
            """,
            (
                utilization.counterparty_symbol,
                utilization.utilized_amount,
                utilization.utilization_currency,
                utilization.limit_amount,
                utilization.available_amount,
                utilization.utilization_percentage,
                utilization.as_of_timestamp,
                _now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_credit_utilization(db_path: str, counterparty_symbol: str) -> CreditUtilization | None:
    """Retrieve the latest utilization snapshot for a counterparty.

    :param db_path: Path to the SQLite database file.
    :param counterparty_symbol: The counterparty's short identifier.
    :returns: The :class:`CreditUtilization`, or ``None`` if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM credit_utilizations WHERE counterparty_symbol = ?",
            (counterparty_symbol,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return CreditUtilization(
        counterparty_symbol=row["counterparty_symbol"],
        utilized_amount=row["utilized_amount"],
        utilization_currency=row["utilization_currency"],
        limit_amount=row["limit_amount"],
        available_amount=row["available_amount"],
        utilization_percentage=row["utilization_percentage"],
        as_of_timestamp=row["as_of_timestamp"],
    )


def get_all_credit_utilizations(db_path: str) -> List[CreditUtilization]:
    """Return all credit utilization snapshots.

    :param db_path: Path to the SQLite database file.
    :returns: List of :class:`CreditUtilization` instances.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM credit_utilizations ORDER BY counterparty_symbol"
        ).fetchall()
    finally:
        conn.close()

    return [
        CreditUtilization(
            counterparty_symbol=r["counterparty_symbol"],
            utilized_amount=r["utilized_amount"],
            utilization_currency=r["utilization_currency"],
            limit_amount=r["limit_amount"],
            available_amount=r["available_amount"],
            utilization_percentage=r["utilization_percentage"],
            as_of_timestamp=r["as_of_timestamp"],
        )
        for r in rows
    ]


def delete_credit_utilization(db_path: str, counterparty_symbol: str) -> bool:
    """Delete a credit utilization snapshot.

    :param db_path: Path to the SQLite database file.
    :param counterparty_symbol: The counterparty's short identifier.
    :returns: ``True`` if a row was deleted, ``False`` if not found.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM credit_utilizations WHERE counterparty_symbol = ?",
            (counterparty_symbol,),
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()
