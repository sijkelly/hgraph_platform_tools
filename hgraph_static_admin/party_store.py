"""
SQLite-backed storage for party/counterparty static data.

Provides CRUD operations for two tables:

- ``legal_entities`` — stores :class:`~hg_oap.parties.party.LegalEntity`
  records keyed by symbol.
- ``trading_relationships`` — stores
  :class:`~hg_oap.parties.relationship.TradingRelationship` records
  keyed by ``(internal_party_symbol, external_party_symbol)``.

This module is the persistence layer for the party Kafka subscriber and
can also be used standalone for local party data management.
"""

import logging
import sqlite3
from datetime import date, datetime, timezone
from typing import Any, Dict, List

from hg_oap.parties.agreement import MasterAgreement, MasterAgreementType
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship

__all__ = (
    "init_party_db",
    "upsert_legal_entity",
    "get_legal_entity",
    "get_all_legal_entities",
    "delete_legal_entity",
    "upsert_trading_relationship",
    "get_trading_relationship",
    "get_all_trading_relationships",
    "delete_trading_relationship",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_LEGAL_ENTITIES_SQL = """
CREATE TABLE IF NOT EXISTS legal_entities (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    classification TEXT NOT NULL,
    lei TEXT,
    jurisdiction TEXT,
    registration_id TEXT,
    tax_id TEXT,
    address TEXT,
    updated_at TEXT NOT NULL
)
"""

_CREATE_TRADING_RELATIONSHIPS_SQL = """
CREATE TABLE IF NOT EXISTS trading_relationships (
    internal_party_symbol TEXT NOT NULL,
    external_party_symbol TEXT NOT NULL,
    clearing_status TEXT NOT NULL DEFAULT 'Bilateral',
    isda_type TEXT,
    isda_version TEXT,
    isda_date TEXT,
    isda_csa INTEGER DEFAULT 0,
    isda_threshold REAL,
    isda_governing_law TEXT,
    naesb_type TEXT,
    naesb_version TEXT,
    naesb_date TEXT,
    eei_type TEXT,
    eei_version TEXT,
    eei_date TEXT,
    dropcopy_enabled INTEGER DEFAULT 0,
    internal_portfolio TEXT,
    external_portfolio TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (internal_party_symbol, external_party_symbol),
    FOREIGN KEY (internal_party_symbol) REFERENCES legal_entities(symbol),
    FOREIGN KEY (external_party_symbol) REFERENCES legal_entities(symbol)
)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _agreement_to_row(agreement: MasterAgreement | None, prefix: str) -> Dict[str, Any]:
    """Flatten a MasterAgreement into column-prefixed values."""
    if agreement is None:
        return {
            f"{prefix}_type": None,
            f"{prefix}_version": None,
            f"{prefix}_date": None,
        }
    d: Dict[str, Any] = {
        f"{prefix}_type": agreement.agreement_type.value,
        f"{prefix}_version": agreement.version,
        f"{prefix}_date": agreement.agreement_date.isoformat() if agreement.agreement_date else None,
    }
    if prefix == "isda":
        d["isda_csa"] = 1 if agreement.credit_support_annex else 0
        d["isda_threshold"] = agreement.threshold_amount
        d["isda_governing_law"] = agreement.governing_law
    return d


def _row_to_agreement(row: Dict[str, Any], prefix: str) -> MasterAgreement | None:
    """Reconstruct a MasterAgreement from column-prefixed row values."""
    type_val = row.get(f"{prefix}_type")
    if type_val is None:
        return None

    kwargs: Dict[str, Any] = {
        "agreement_type": MasterAgreementType(type_val),
        "version": row.get(f"{prefix}_version"),
        "agreement_date": date.fromisoformat(row[f"{prefix}_date"]) if row.get(f"{prefix}_date") else None,
    }
    if prefix == "isda":
        kwargs["credit_support_annex"] = bool(row.get("isda_csa", 0))
        kwargs["threshold_amount"] = row.get("isda_threshold")
        kwargs["governing_law"] = row.get("isda_governing_law")

    return MasterAgreement(**kwargs)


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------


def init_party_db(db_path: str) -> None:
    """Create the ``legal_entities`` and ``trading_relationships`` tables.

    :param db_path: Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(_CREATE_LEGAL_ENTITIES_SQL)
        conn.execute(_CREATE_TRADING_RELATIONSHIPS_SQL)
        conn.commit()
    finally:
        conn.close()
    logger.info("Party database initialised at %s", db_path)


# ---------------------------------------------------------------------------
# Legal Entity CRUD
# ---------------------------------------------------------------------------


def upsert_legal_entity(db_path: str, entity: LegalEntity) -> None:
    """Insert or update a legal entity record.

    :param db_path: Path to the SQLite database file.
    :param entity: The legal entity to store.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO legal_entities (symbol, name, classification, lei,
                                        jurisdiction, registration_id, tax_id,
                                        address, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = excluded.name,
                classification = excluded.classification,
                lei = excluded.lei,
                jurisdiction = excluded.jurisdiction,
                registration_id = excluded.registration_id,
                tax_id = excluded.tax_id,
                address = excluded.address,
                updated_at = excluded.updated_at
            """,
            (
                entity.symbol,
                entity.name,
                entity.classification.value,
                entity.lei,
                entity.jurisdiction,
                entity.registration_id,
                entity.tax_id,
                entity.address,
                _now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_legal_entity(db_path: str, symbol: str) -> LegalEntity | None:
    """Retrieve a legal entity by symbol.

    :param db_path: Path to the SQLite database file.
    :param symbol: The entity's short identifier.
    :returns: The :class:`LegalEntity`, or ``None`` if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM legal_entities WHERE symbol = ?", (symbol,)
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return LegalEntity(
        symbol=row["symbol"],
        name=row["name"],
        classification=PartyClassification(row["classification"]),
        lei=row["lei"],
        jurisdiction=row["jurisdiction"],
        registration_id=row["registration_id"],
        tax_id=row["tax_id"],
        address=row["address"],
    )


def get_all_legal_entities(db_path: str) -> List[LegalEntity]:
    """Return all legal entities.

    :param db_path: Path to the SQLite database file.
    :returns: List of :class:`LegalEntity` instances.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM legal_entities ORDER BY symbol"
        ).fetchall()
    finally:
        conn.close()

    return [
        LegalEntity(
            symbol=r["symbol"],
            name=r["name"],
            classification=PartyClassification(r["classification"]),
            lei=r["lei"],
            jurisdiction=r["jurisdiction"],
            registration_id=r["registration_id"],
            tax_id=r["tax_id"],
            address=r["address"],
        )
        for r in rows
    ]


def delete_legal_entity(db_path: str, symbol: str) -> bool:
    """Delete a legal entity by symbol.

    :param db_path: Path to the SQLite database file.
    :param symbol: The entity's short identifier.
    :returns: ``True`` if a row was deleted, ``False`` if not found.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM legal_entities WHERE symbol = ?", (symbol,)
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Trading Relationship CRUD
# ---------------------------------------------------------------------------


def upsert_trading_relationship(db_path: str, relationship: TradingRelationship) -> None:
    """Insert or update a trading relationship record.

    :param db_path: Path to the SQLite database file.
    :param relationship: The trading relationship to store.
    """
    isda_cols = _agreement_to_row(relationship.isda, "isda")
    naesb_cols = _agreement_to_row(relationship.naesb, "naesb")
    eei_cols = _agreement_to_row(relationship.eei, "eei")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO trading_relationships (
                internal_party_symbol, external_party_symbol, clearing_status,
                isda_type, isda_version, isda_date, isda_csa, isda_threshold, isda_governing_law,
                naesb_type, naesb_version, naesb_date,
                eei_type, eei_version, eei_date,
                dropcopy_enabled, internal_portfolio, external_portfolio, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(internal_party_symbol, external_party_symbol) DO UPDATE SET
                clearing_status = excluded.clearing_status,
                isda_type = excluded.isda_type,
                isda_version = excluded.isda_version,
                isda_date = excluded.isda_date,
                isda_csa = excluded.isda_csa,
                isda_threshold = excluded.isda_threshold,
                isda_governing_law = excluded.isda_governing_law,
                naesb_type = excluded.naesb_type,
                naesb_version = excluded.naesb_version,
                naesb_date = excluded.naesb_date,
                eei_type = excluded.eei_type,
                eei_version = excluded.eei_version,
                eei_date = excluded.eei_date,
                dropcopy_enabled = excluded.dropcopy_enabled,
                internal_portfolio = excluded.internal_portfolio,
                external_portfolio = excluded.external_portfolio,
                updated_at = excluded.updated_at
            """,
            (
                relationship.internal_party.symbol,
                relationship.external_party.symbol,
                relationship.clearing_status.value,
                isda_cols.get("isda_type"),
                isda_cols.get("isda_version"),
                isda_cols.get("isda_date"),
                isda_cols.get("isda_csa", 0),
                isda_cols.get("isda_threshold"),
                isda_cols.get("isda_governing_law"),
                naesb_cols.get("naesb_type"),
                naesb_cols.get("naesb_version"),
                naesb_cols.get("naesb_date"),
                eei_cols.get("eei_type"),
                eei_cols.get("eei_version"),
                eei_cols.get("eei_date"),
                1 if relationship.dropcopy_enabled else 0,
                relationship.internal_portfolio,
                relationship.external_portfolio,
                _now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_trading_relationship(
    db_path: str,
    internal_symbol: str,
    external_symbol: str,
) -> TradingRelationship | None:
    """Retrieve a trading relationship by party symbols.

    Both the internal and external party must exist in ``legal_entities``.

    :param db_path: Path to the SQLite database file.
    :param internal_symbol: Internal party symbol.
    :param external_symbol: External party symbol.
    :returns: The :class:`TradingRelationship`, or ``None`` if not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """SELECT * FROM trading_relationships
               WHERE internal_party_symbol = ? AND external_party_symbol = ?""",
            (internal_symbol, external_symbol),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    # Look up the party objects
    internal = get_legal_entity(db_path, internal_symbol)
    external = get_legal_entity(db_path, external_symbol)
    if internal is None or external is None:
        logger.warning(
            "Relationship found but party missing: internal=%s external=%s",
            internal_symbol,
            external_symbol,
        )
        return None

    row_dict = dict(row)
    return TradingRelationship(
        internal_party=internal,
        external_party=external,
        clearing_status=ClearingStatus(row_dict["clearing_status"]),
        isda=_row_to_agreement(row_dict, "isda"),
        naesb=_row_to_agreement(row_dict, "naesb"),
        eei=_row_to_agreement(row_dict, "eei"),
        dropcopy_enabled=bool(row_dict.get("dropcopy_enabled", 0)),
        internal_portfolio=row_dict.get("internal_portfolio"),
        external_portfolio=row_dict.get("external_portfolio"),
    )


def get_all_trading_relationships(db_path: str) -> List[TradingRelationship]:
    """Return all trading relationships.

    :param db_path: Path to the SQLite database file.
    :returns: List of :class:`TradingRelationship` instances.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM trading_relationships ORDER BY internal_party_symbol, external_party_symbol"
        ).fetchall()
    finally:
        conn.close()

    results: List[TradingRelationship] = []
    for row in rows:
        rel = get_trading_relationship(db_path, row["internal_party_symbol"], row["external_party_symbol"])
        if rel is not None:
            results.append(rel)
    return results


def delete_trading_relationship(
    db_path: str,
    internal_symbol: str,
    external_symbol: str,
) -> bool:
    """Delete a trading relationship by party symbols.

    :param db_path: Path to the SQLite database file.
    :param internal_symbol: Internal party symbol.
    :param external_symbol: External party symbol.
    :returns: ``True`` if a row was deleted, ``False`` if not found.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """DELETE FROM trading_relationships
               WHERE internal_party_symbol = ? AND external_party_symbol = ?""",
            (internal_symbol, external_symbol),
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()
