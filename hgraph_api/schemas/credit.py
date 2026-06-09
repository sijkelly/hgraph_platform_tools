"""Pydantic models for credit API request/response serialisation."""

from datetime import date
from typing import Optional

from pydantic import BaseModel

__all__ = (
    "CreditLimitResponse",
    "CreditUtilizationResponse",
)


class CreditLimitResponse(BaseModel):
    """Response model for a single credit limit."""

    counterparty_symbol: str
    limit_type: str
    limit_amount: float
    limit_currency: str
    effective_date: Optional[date] = None
    expiry_date: Optional[date] = None
    status: str
    approved_by: Optional[str] = None
    last_review_date: Optional[date] = None

    model_config = {"from_attributes": True}


class CreditUtilizationResponse(BaseModel):
    """Response model for a credit utilization snapshot."""

    counterparty_symbol: str
    utilized_amount: float
    utilization_currency: str
    limit_amount: float
    available_amount: float
    utilization_percentage: float
    as_of_timestamp: Optional[str] = None

    model_config = {"from_attributes": True}
