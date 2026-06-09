"""Portfolio and book REST endpoints."""

import os

from fastapi import APIRouter, HTTPException, Request

from hgraph_api.config import api_config
from hgraph_api.schemas.portfolios import BookResponse, PortfolioResponse
from hgraph_static_admin.portfolio_store import (
    get_all_books,
    get_all_portfolios,
    get_book,
    get_portfolio,
)

__all__ = ("router",)

router = APIRouter(prefix="/api/v1", tags=["portfolios"])


def _db_path(request: Request) -> str:
    db_dir = getattr(request.app.state, "db_dir", ".")
    return os.path.join(db_dir, api_config.get("PORTFOLIO_DB_PATH", "portfolio_data.db"))


def _portfolio_to_response(p) -> PortfolioResponse:
    return PortfolioResponse(
        symbol=p.symbol,
        name=p.name,
        portfolio_type=p.portfolio_type.value,
        owner=p.owner,
        base_currency=p.base_currency,
        legal_entity_symbol=p.legal_entity_symbol,
        status=p.status.value,
        parent_portfolio_symbol=p.parent_portfolio_symbol,
    )


def _book_to_response(b) -> BookResponse:
    return BookResponse(
        symbol=b.symbol,
        name=b.name,
        book_type=b.book_type.value,
        owner=b.owner,
        base_currency=b.base_currency,
        legal_entity_symbol=b.legal_entity_symbol,
        portfolio_symbol=b.portfolio_symbol,
        status=b.status.value,
    )


@router.get("/portfolios", response_model=list[PortfolioResponse])
def list_portfolios(request: Request) -> list[PortfolioResponse]:
    """Return all portfolios."""
    db = _db_path(request)
    results = get_all_portfolios(db)
    return [_portfolio_to_response(r) for r in results]


@router.get("/portfolios/{symbol}", response_model=PortfolioResponse)
def get_portfolio_detail(request: Request, symbol: str) -> PortfolioResponse:
    """Return a single portfolio by symbol."""
    db = _db_path(request)
    result = get_portfolio(db, symbol)
    if result is None:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    return _portfolio_to_response(result)


@router.get("/books", response_model=list[BookResponse])
def list_books(request: Request) -> list[BookResponse]:
    """Return all books."""
    db = _db_path(request)
    results = get_all_books(db)
    return [_book_to_response(r) for r in results]


@router.get("/books/{symbol}", response_model=BookResponse)
def get_book_detail(request: Request, symbol: str) -> BookResponse:
    """Return a single book by symbol."""
    db = _db_path(request)
    result = get_book(db, symbol)
    if result is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return _book_to_response(result)
