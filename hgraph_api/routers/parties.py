"""Party/counterparty REST endpoints."""

import os

from fastapi import APIRouter, HTTPException, Request

from hgraph_api.config import api_config
from hgraph_api.schemas.parties import (
    LegalEntityResponse,
    MasterAgreementResponse,
    TradingRelationshipResponse,
)
from hgraph_static_admin.party_store import (
    get_all_legal_entities,
    get_all_trading_relationships,
    get_legal_entity,
    get_trading_relationship,
)

__all__ = ("router",)

router = APIRouter(prefix="/api/v1/parties", tags=["parties"])


def _db_path(request: Request) -> str:
    db_dir = getattr(request.app.state, "db_dir", ".")
    return os.path.join(db_dir, api_config.get("PARTY_DB_PATH", "party_data.db"))


def _agreement_to_response(agreement) -> MasterAgreementResponse | None:
    if agreement is None:
        return None
    return MasterAgreementResponse(
        agreement_type=agreement.agreement_type.value,
        version=agreement.version,
        agreement_date=agreement.agreement_date.isoformat() if agreement.agreement_date else None,
        credit_support_annex=agreement.credit_support_annex,
        threshold_amount=agreement.threshold_amount,
        governing_law=agreement.governing_law,
    )


def _entity_to_response(entity) -> LegalEntityResponse:
    return LegalEntityResponse(
        symbol=entity.symbol,
        name=entity.name,
        classification=entity.classification.value,
        lei=entity.lei,
        jurisdiction=entity.jurisdiction,
        registration_id=entity.registration_id,
        tax_id=entity.tax_id,
        address=entity.address,
    )


def _relationship_to_response(rel) -> TradingRelationshipResponse:
    return TradingRelationshipResponse(
        internal_party_symbol=rel.internal_party.symbol,
        external_party_symbol=rel.external_party.symbol,
        clearing_status=rel.clearing_status.value,
        isda=_agreement_to_response(rel.isda),
        naesb=_agreement_to_response(rel.naesb),
        eei=_agreement_to_response(rel.eei),
        dropcopy_enabled=rel.dropcopy_enabled,
        internal_portfolio=rel.internal_portfolio,
        external_portfolio=rel.external_portfolio,
    )


@router.get("/entities", response_model=list[LegalEntityResponse])
def list_entities(request: Request) -> list[LegalEntityResponse]:
    """Return all legal entities."""
    db = _db_path(request)
    results = get_all_legal_entities(db)
    return [_entity_to_response(r) for r in results]


@router.get("/entities/{symbol}", response_model=LegalEntityResponse)
def get_entity(request: Request, symbol: str) -> LegalEntityResponse:
    """Return a single legal entity by symbol."""
    db = _db_path(request)
    result = get_legal_entity(db, symbol)
    if result is None:
        raise HTTPException(status_code=404, detail="Legal entity not found")
    return _entity_to_response(result)


@router.get("/relationships", response_model=list[TradingRelationshipResponse])
def list_relationships(request: Request) -> list[TradingRelationshipResponse]:
    """Return all trading relationships."""
    db = _db_path(request)
    results = get_all_trading_relationships(db)
    return [_relationship_to_response(r) for r in results]


@router.get(
    "/relationships/{internal_symbol}/{external_symbol}",
    response_model=TradingRelationshipResponse,
)
def get_relationship(request: Request, internal_symbol: str, external_symbol: str) -> TradingRelationshipResponse:
    """Return a single trading relationship."""
    db = _db_path(request)
    result = get_trading_relationship(db, internal_symbol, external_symbol)
    if result is None:
        raise HTTPException(status_code=404, detail="Trading relationship not found")
    return _relationship_to_response(result)
