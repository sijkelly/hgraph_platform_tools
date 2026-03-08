"""
Adapter converting hg_oap party domain objects to platform-tools formats.

Provides two conversion paths:

1. :func:`legal_entity_to_counterparty_row` — converts a
   :class:`~hg_oap.parties.party.LegalEntity` into the dictionary
   format used by ``hgraph_static_admin``'s counterparties table.

2. :func:`relationship_to_trade_parties` — extracts the party
   identifier fields needed by the trade header from a
   :class:`~hg_oap.parties.relationship.TradingRelationship`.
"""

from typing import Any, Dict

from hg_oap.parties.party import LegalEntity
from hg_oap.parties.relationship import TradingRelationship

__all__ = (
    "legal_entity_to_counterparty_row",
    "relationship_to_trade_parties",
)


def legal_entity_to_counterparty_row(entity: LegalEntity) -> Dict[str, Any]:
    """
    Convert a :class:`LegalEntity` into a static-admin counterparty row.

    The returned dictionary matches the column names of the
    ``counterparties`` table in ``hgraph_static_admin``:

    ============== ===========================
    Column         Source
    ============== ===========================
    legal_name     ``entity.name``
    short_name     ``entity.symbol``
    identifier1    ``entity.lei``
    identifier2    ``entity.registration_id``
    identifier3    ``entity.tax_id``
    identifier4    ``entity.classification.value``
    cleared_otc    (empty — set per relationship)
    ============== ===========================

    :param entity: The legal entity to convert.
    :returns: A dictionary compatible with the counterparties table.
    """
    return {
        "legal_name": entity.name,
        "short_name": entity.symbol,
        "identifier1": entity.lei or "",
        "identifier2": entity.registration_id or "",
        "identifier3": entity.tax_id or "",
        "identifier4": entity.classification.value,
        "cleared_otc": "",
        "dropcopy_enabled": "",
        "notification_preferences": "",
        "notification_contacts": "",
    }


def relationship_to_trade_parties(
    relationship: TradingRelationship,
    *,
    internal_trader: str = "",
    external_trader: str = "",
) -> Dict[str, Any]:
    """
    Extract trade-header party fields from a :class:`TradingRelationship`.

    Returns the nested ``counterparty``, ``portfolio``, and ``traders``
    dictionaries plus the flat ``internal_*`` / ``external_*`` fields
    expected by the booking pipeline's ``trade_data`` format.

    :param relationship: The bilateral trading relationship.
    :param internal_trader: Internal trader identifier.
    :param external_trader: External trader identifier.
    :returns: A dictionary of party-related fields.
    """
    int_sym = relationship.internal_party.symbol
    ext_sym = relationship.external_party.symbol
    int_port = relationship.internal_portfolio or ""
    ext_port = relationship.external_portfolio or ""

    return {
        "internal_party": int_sym,
        "external_party": ext_sym,
        "counterparty": {"internal": int_sym, "external": ext_sym},
        "portfolio": {"internal": int_port, "external": ext_port},
        "traders": {"internal": internal_trader, "external": external_trader},
        "internal_portfolio": int_port,
        "external_portfolio": ext_port,
        "internal_trader": internal_trader,
        "external_trader": external_trader,
    }
