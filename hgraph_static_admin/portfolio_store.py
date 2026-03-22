"""
SQLite-backed storage for portfolio and book static data.

Provides CRUD operations for two tables:

- ``portfolios`` — stores :class:`~hg_oap.portfolio.portfolio_info.PortfolioInfo`
  records keyed by symbol.
- ``books`` — stores :class:`~hg_oap.portfolio.portfolio_info.BookInfo`
  records keyed by symbol.

This module is the persistence layer for the portfolio Kafka subscriber and
can also be used standalone for local portfolio data management.
"""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import List

from hg_oap.portfolio.portfolio_info import (
    BookInfo,
    BookType,
    PortfolioInfo,
    PortfolioStatus,
    PortfolioType,
)

__all__ = (
    "init_portfolio_db",
    "upsert_portfolio",
    "get_portfolio",
    "get_all_portfolios",
    "delete_portfolio",
    "upsert_book",
    "get_book",
    "get_all_books",
    "delete_book",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_PORTFOLIOS_SQL = """
CREATE TABLE IF NOT EXISTS portfolios (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    portfolio_type TEXT NOT NULL,
    owner TEXT,
    base_currency TEXT,
    legal_entity_symbol TEXT,
    status TEXT NOT NULL DEFAULT 'Active',
    parent_portfolio_symbol TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (parent_portfolio_symbol) REFERENCES portfolios(symbol)
)
"""

_CREATE_BOOKS_SQL = """
CREATE TABLE IF NOT EXISTS books (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    book_type TEXT NOT NULL,
    owner TEXT,
    base_currency TEXT,
    legal_entity_symbol TEXT,
    portfolio_symbol TEXT,
    status TEXT NOT NULL DEFAULT 'Active',
    updated_at TEXT NOT NULL,
    FOREIGN KEY (portfolio_symbol) REFERENCES portfolios(symbol)
)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------


def init_portfolio_db(db_path: str) -> None:
    """Create the ``portfolios`` and ``books`` tables.

    :param db_path: Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(_CREATE_PORTFOLIOS_SQL)
        conn.execute(_CREATE_BOOKS_SQL)
        conn.commit()
    finally:
        conn.close()
    logger.info("Portfolio database initialised at %s", db_path)


# ---------------------------------------------------------------------------
# Portfolio CRUD
# ---------------------------------------------------------------------------


def upsert_portfolio(db_path: str, portfolio: PortfolioInfo) -> None:
    """Insert or update a portfolio record.

    :param db_path: Path to the SQLite database file.
    :param portfolio: The portfolio info to store.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO portfolios (symbol, name, portfolio_type, owner,
                                    base_currency, legal_entity_symbol,
                                    status, parent_portfolio_symbol, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = excluded.name,
                portfolio_type = excluded.portfolio_type,
                owner = excluded.owner,
                base_currency = excluded.base_currency,
                legal_entity_symbol = excluded.legal_entity_symbol,
                status = excluded.status,
                parent_portfolio_symbol = excluded.parent_portfolio_symbol,
                updated_at = excluded.updated_at
            """,
            (
                portfolio.symbol,
                portfolio.name,
                portfolio.portfolio_type.value,
                portfolio.owner,
                portfolio.base_currency,
                portfolio.legal_entity_symbol,
                portfolio.status.value,
                portfolio.parent_portfolio_symbol,
                _now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_portfolio(db_path: str, symbol: str) -> PortfolioInfo | None:
    """Retrieve a portfolio by symbol.

    :param db_path: Path to the SQLite database file.
    :param symbol: The portfolio's short identifier.
    :returns: The :class:`PortfolioInfo`, or ``None`` if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM portfolios WHERE symbol = ?", (symbol,)
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return PortfolioInfo(
        symbol=row["symbol"],
        name=row["name"],
        portfolio_type=PortfolioType(row["portfolio_type"]),
        owner=row["owner"],
        base_currency=row["base_currency"],
        legal_entity_symbol=row["legal_entity_symbol"],
        status=PortfolioStatus(row["status"]),
        parent_portfolio_symbol=row["parent_portfolio_symbol"],
    )


def get_all_portfolios(db_path: str) -> List[PortfolioInfo]:
    """Return all portfolios.

    :param db_path: Path to the SQLite database file.
    :returns: List of :class:`PortfolioInfo` instances.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM portfolios ORDER BY symbol"
        ).fetchall()
    finally:
        conn.close()

    return [
        PortfolioInfo(
            symbol=r["symbol"],
            name=r["name"],
            portfolio_type=PortfolioType(r["portfolio_type"]),
            owner=r["owner"],
            base_currency=r["base_currency"],
            legal_entity_symbol=r["legal_entity_symbol"],
            status=PortfolioStatus(r["status"]),
            parent_portfolio_symbol=r["parent_portfolio_symbol"],
        )
        for r in rows
    ]


def delete_portfolio(db_path: str, symbol: str) -> bool:
    """Delete a portfolio by symbol.

    :param db_path: Path to the SQLite database file.
    :param symbol: The portfolio's short identifier.
    :returns: ``True`` if a row was deleted, ``False`` if not found.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM portfolios WHERE symbol = ?", (symbol,)
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Book CRUD
# ---------------------------------------------------------------------------


def upsert_book(db_path: str, book: BookInfo) -> None:
    """Insert or update a book record.

    :param db_path: Path to the SQLite database file.
    :param book: The book info to store.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO books (symbol, name, book_type, owner,
                               base_currency, legal_entity_symbol,
                               portfolio_symbol, status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = excluded.name,
                book_type = excluded.book_type,
                owner = excluded.owner,
                base_currency = excluded.base_currency,
                legal_entity_symbol = excluded.legal_entity_symbol,
                portfolio_symbol = excluded.portfolio_symbol,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                book.symbol,
                book.name,
                book.book_type.value,
                book.owner,
                book.base_currency,
                book.legal_entity_symbol,
                book.portfolio_symbol,
                book.status.value,
                _now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_book(db_path: str, symbol: str) -> BookInfo | None:
    """Retrieve a book by symbol.

    :param db_path: Path to the SQLite database file.
    :param symbol: The book's short identifier.
    :returns: The :class:`BookInfo`, or ``None`` if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM books WHERE symbol = ?", (symbol,)
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return BookInfo(
        symbol=row["symbol"],
        name=row["name"],
        book_type=BookType(row["book_type"]),
        owner=row["owner"],
        base_currency=row["base_currency"],
        legal_entity_symbol=row["legal_entity_symbol"],
        portfolio_symbol=row["portfolio_symbol"],
        status=PortfolioStatus(row["status"]),
    )


def get_all_books(db_path: str) -> List[BookInfo]:
    """Return all books.

    :param db_path: Path to the SQLite database file.
    :returns: List of :class:`BookInfo` instances.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM books ORDER BY symbol"
        ).fetchall()
    finally:
        conn.close()

    return [
        BookInfo(
            symbol=r["symbol"],
            name=r["name"],
            book_type=BookType(r["book_type"]),
            owner=r["owner"],
            base_currency=r["base_currency"],
            legal_entity_symbol=r["legal_entity_symbol"],
            portfolio_symbol=r["portfolio_symbol"],
            status=PortfolioStatus(r["status"]),
        )
        for r in rows
    ]


def delete_book(db_path: str, symbol: str) -> bool:
    """Delete a book by symbol.

    :param db_path: Path to the SQLite database file.
    :param symbol: The book's short identifier.
    :returns: ``True`` if a row was deleted, ``False`` if not found.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM books WHERE symbol = ?", (symbol,)
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()
