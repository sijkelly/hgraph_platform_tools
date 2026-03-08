"""
SQLite-backed storage for EV (Expected Value) rate configuration.

Provides CRUD operations for the ``ev_rates`` table which maps
instrument categories and quantity units to commission percentages.
The OMS calls :func:`get_ev_rate` at order time to look up the
applicable rate for a given trade type and unit.

Follows the same SQLite-based static-data pattern established by
``hgraph_static_admin.example_code``.
"""

import logging
import sqlite3
from datetime import date
from typing import Any, Dict, List

from hg_oap.orders.sales.ev import EVRate, InstrumentCategory
from hg_oap.units.unit import Unit

__all__ = (
    "init_ev_rate_db",
    "upsert_ev_rate",
    "get_ev_rate",
    "get_all_ev_rates",
    "delete_ev_rate",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ev_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_category TEXT NOT NULL,
    unit TEXT NOT NULL,
    rate_per_unit REAL NOT NULL,
    currency_symbol TEXT NOT NULL DEFAULT 'USD',
    effective_date TEXT,
    expiry_date TEXT,
    UNIQUE(instrument_category, unit)
)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unit_key(unit: Unit) -> str:
    """Produce a stable string key for a :class:`Unit`."""
    return str(unit)


def _row_to_ev_rate(row: Dict[str, Any], unit_registry: Dict[str, Unit] | None = None) -> Dict[str, Any]:
    """Convert a SQLite row dict to a dictionary of EVRate constructor args.

    .. note::

       The *unit* field is stored as a string.  To reconstruct a full
       :class:`EVRate` the caller must supply a *unit_registry* mapping
       or handle unit resolution externally.  When *unit_registry* is
       ``None`` the raw string is placed under the ``"unit_str"`` key
       instead.
    """
    eff = row["effective_date"]
    exp = row["expiry_date"]
    return {
        "instrument_category": InstrumentCategory(row["instrument_category"]),
        "unit_str": row["unit"],
        "rate_per_unit": row["rate_per_unit"],
        "currency_symbol": row["currency_symbol"],
        "effective_date": date.fromisoformat(eff) if eff else None,
        "expiry_date": date.fromisoformat(exp) if exp else None,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_ev_rate_db(db_path: str) -> None:
    """Create the ``ev_rates`` table if it does not exist.

    :param db_path: Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(_CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()
    logger.info("EV rate table initialised at %s", db_path)


def upsert_ev_rate(db_path: str, ev_rate: EVRate) -> None:
    """Insert or update an EV rate record.

    The ``(instrument_category, unit)`` pair forms the unique key.  If a
    record with the same key already exists it is replaced.

    :param db_path: Path to the SQLite database file.
    :param ev_rate: The EV rate to store.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO ev_rates (instrument_category, unit, rate_per_unit,
                                  currency_symbol, effective_date, expiry_date)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_category, unit) DO UPDATE SET
                rate_per_unit = excluded.rate_per_unit,
                currency_symbol = excluded.currency_symbol,
                effective_date = excluded.effective_date,
                expiry_date = excluded.expiry_date
            """,
            (
                ev_rate.instrument_category.value,
                _unit_key(ev_rate.unit),
                ev_rate.rate_per_unit,
                ev_rate.currency_symbol,
                ev_rate.effective_date.isoformat() if ev_rate.effective_date else None,
                ev_rate.expiry_date.isoformat() if ev_rate.expiry_date else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_ev_rate(
    db_path: str,
    instrument_category: InstrumentCategory,
    unit: Unit,
    *,
    as_of: date | None = None,
) -> EVRate | None:
    """Look up the applicable EV rate for an instrument category and unit.

    When *as_of* is supplied the query additionally filters to rates
    whose effective/expiry window covers that date.

    :param db_path: Path to the SQLite database file.
    :param instrument_category: The instrument family to look up.
    :param unit: The quantity unit to match.
    :param as_of: Optional date for effective/expiry filtering.
    :returns: The matching :class:`EVRate`, or ``None`` if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if as_of is not None:
            row = conn.execute(
                """
                SELECT * FROM ev_rates
                WHERE instrument_category = ? AND unit = ?
                  AND (effective_date IS NULL OR effective_date <= ?)
                  AND (expiry_date IS NULL OR expiry_date >= ?)
                """,
                (instrument_category.value, _unit_key(unit), as_of.isoformat(), as_of.isoformat()),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM ev_rates WHERE instrument_category = ? AND unit = ?",
                (instrument_category.value, _unit_key(unit)),
            ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    parsed = _row_to_ev_rate(dict(row))
    return EVRate(
        instrument_category=parsed["instrument_category"],
        unit=unit,
        rate_per_unit=parsed["rate_per_unit"],
        currency_symbol=parsed["currency_symbol"],
        effective_date=parsed["effective_date"],
        expiry_date=parsed["expiry_date"],
    )


def get_all_ev_rates(db_path: str) -> List[Dict[str, Any]]:
    """Return all configured EV rates as dictionaries.

    Each dict contains the raw database values.  The ``"unit"`` field
    is a string; callers should resolve it to a :class:`Unit` using
    their own unit registry.

    :param db_path: Path to the SQLite database file.
    :returns: List of rate dictionaries.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM ev_rates ORDER BY instrument_category, unit").fetchall()
    finally:
        conn.close()

    return [_row_to_ev_rate(dict(r)) for r in rows]


def delete_ev_rate(db_path: str, instrument_category: InstrumentCategory, unit: Unit) -> bool:
    """Delete a specific EV rate.

    :param db_path: Path to the SQLite database file.
    :param instrument_category: The instrument family.
    :param unit: The quantity unit.
    :returns: ``True`` if a row was deleted, ``False`` if not found.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM ev_rates WHERE instrument_category = ? AND unit = ?",
            (instrument_category.value, _unit_key(unit)),
        )
        conn.commit()
        deleted = cursor.rowcount > 0
    finally:
        conn.close()
    return deleted
