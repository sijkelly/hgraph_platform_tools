"""Pydantic models for party API request/response serialisation."""

from typing import Optional

from pydantic import BaseModel

__all__ = (
    "LegalEntityResponse",
    "TradingRelationshipResponse",
    "MasterAgreementResponse",
)


class LegalEntityResponse(BaseModel):
    """Response model for a legal entity."""

    symbol: str
    name: str
    classification: str
    lei: Optional[str] = None
    jurisdiction: Optional[str] = None
    registration_id: Optional[str] = None
    tax_id: Optional[str] = None
    address: Optional[str] = None

    model_config = {"from_attributes": True}


class MasterAgreementResponse(BaseModel):
    """Response model for a master agreement."""

    agreement_type: str
    version: Optional[str] = None
    agreement_date: Optional[str] = None
    credit_support_annex: bool = False
    threshold_amount: Optional[float] = None
    governing_law: Optional[str] = None


class TradingRelationshipResponse(BaseModel):
    """Response model for a trading relationship."""

    internal_party_symbol: str
    external_party_symbol: str
    clearing_status: str
    isda: Optional[MasterAgreementResponse] = None
    naesb: Optional[MasterAgreementResponse] = None
    eei: Optional[MasterAgreementResponse] = None
    dropcopy_enabled: bool = False
    internal_portfolio: Optional[str] = None
    external_portfolio: Optional[str] = None
