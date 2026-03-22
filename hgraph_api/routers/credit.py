"""Credit limit and utilization REST endpoints."""

import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from hgraph_api.config import api_config
from hgraph_api.schemas.credit import CreditLimitResponse, CreditUtilizationResponse
from hgraph_static_admin.credit_store import (
    get_all_credit_limits,
    get_all_credit_utilizations,
    get_credit_limit,
    get_credit_limits_for_counterparty,
    get_credit_utilization,
)

__all__ = ("router",)

router = APIRouter(prefix="/api/v1/credit", tags=["credit"])


def _db_path(request: Request) -> str:
    db_dir = getattr(request.app.state, "db_dir", ".")
    return os.path.join(db_dir, api_config.get("CREDIT_DB_PATH", "credit_data.db"))


def _limit_to_response(cl) -> CreditLimitResponse:
    return CreditLimitResponse(
        counterparty_symbol=cl.counterparty_symbol,
        limit_type=cl.limit_type.value,
        limit_amount=cl.limit_amount,
        limit_currency=cl.limit_currency,
        effective_date=cl.effective_date,
        expiry_date=cl.expiry_date,
        status=cl.status.value,
        approved_by=cl.approved_by,
        last_review_date=cl.last_review_date,
    )


def _utilization_to_response(cu) -> CreditUtilizationResponse:
    return CreditUtilizationResponse(
        counterparty_symbol=cu.counterparty_symbol,
        utilized_amount=cu.utilized_amount,
        utilization_currency=cu.utilization_currency,
        limit_amount=cu.limit_amount,
        available_amount=cu.available_amount,
        utilization_percentage=cu.utilization_percentage,
        as_of_timestamp=cu.as_of_timestamp,
    )


@router.get("/limits", response_model=list[CreditLimitResponse])
def list_credit_limits(request: Request, counterparty: Optional[str] = None) -> list[CreditLimitResponse]:
    """Return credit limits, optionally filtered by counterparty."""
    db = _db_path(request)
    if counterparty:
        results = get_credit_limits_for_counterparty(db, counterparty)
    else:
        results = get_all_credit_limits(db)
    return [_limit_to_response(r) for r in results]


@router.get("/limits/{counterparty_symbol}/{limit_type}", response_model=CreditLimitResponse)
def get_credit_limit_detail(request: Request, counterparty_symbol: str, limit_type: str) -> CreditLimitResponse:
    """Return a single credit limit by counterparty and type."""
    db = _db_path(request)
    result = get_credit_limit(db, counterparty_symbol, limit_type)
    if result is None:
        raise HTTPException(status_code=404, detail="Credit limit not found")
    return _limit_to_response(result)


@router.get("/utilizations", response_model=list[CreditUtilizationResponse])
def list_credit_utilizations(request: Request) -> list[CreditUtilizationResponse]:
    """Return all credit utilization snapshots."""
    db = _db_path(request)
    results = get_all_credit_utilizations(db)
    return [_utilization_to_response(r) for r in results]


@router.get("/utilizations/{counterparty_symbol}", response_model=CreditUtilizationResponse)
def get_credit_utilization_detail(request: Request, counterparty_symbol: str) -> CreditUtilizationResponse:
    """Return the latest utilization snapshot for a counterparty."""
    db = _db_path(request)
    result = get_credit_utilization(db, counterparty_symbol)
    if result is None:
        raise HTTPException(status_code=404, detail="Credit utilization not found")
    return _utilization_to_response(result)
