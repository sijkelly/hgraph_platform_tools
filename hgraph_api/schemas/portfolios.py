"""Pydantic models for portfolio API request/response serialisation."""

from typing import Optional

from pydantic import BaseModel

__all__ = (
    "PortfolioResponse",
    "BookResponse",
)


class PortfolioResponse(BaseModel):
    """Response model for a portfolio."""

    symbol: str
    name: str
    portfolio_type: str
    owner: Optional[str] = None
    base_currency: Optional[str] = None
    legal_entity_symbol: Optional[str] = None
    status: str
    parent_portfolio_symbol: Optional[str] = None

    model_config = {"from_attributes": True}


class BookResponse(BaseModel):
    """Response model for a book."""

    symbol: str
    name: str
    book_type: str
    owner: Optional[str] = None
    base_currency: Optional[str] = None
    legal_entity_symbol: Optional[str] = None
    portfolio_symbol: Optional[str] = None
    status: str

    model_config = {"from_attributes": True}
