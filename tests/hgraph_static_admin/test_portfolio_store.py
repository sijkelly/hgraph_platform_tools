"""Tests for portfolio and book SQLite store."""

import os

import pytest

from hg_oap.portfolio.portfolio_info import (
    BookInfo,
    BookType,
    PortfolioInfo,
    PortfolioStatus,
    PortfolioType,
)

from hgraph_static_admin.portfolio_store import (
    delete_book,
    delete_portfolio,
    get_all_books,
    get_all_portfolios,
    get_book,
    get_portfolio,
    init_portfolio_db,
    upsert_book,
    upsert_portfolio,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_portfolio.db")
    init_portfolio_db(path)
    return path


@pytest.fixture()
def trading_portfolio():
    return PortfolioInfo(
        symbol="GAS-TRADING",
        name="Gas Trading Portfolio",
        portfolio_type=PortfolioType.TRADING,
        owner="Gas Desk",
        base_currency="USD",
        legal_entity_symbol="HGDEALER",
        status=PortfolioStatus.ACTIVE,
    )


@pytest.fixture()
def hedging_portfolio():
    return PortfolioInfo(
        symbol="GAS-HEDGE",
        name="Gas Hedging Portfolio",
        portfolio_type=PortfolioType.HEDGING,
        owner="Risk Desk",
        base_currency="USD",
        legal_entity_symbol="HGDEALER",
    )


@pytest.fixture()
def trading_book():
    return BookInfo(
        symbol="GAS-BOOK-1",
        name="Gas Trading Book 1",
        book_type=BookType.TRADING,
        owner="Gas Desk",
        base_currency="USD",
        legal_entity_symbol="HGDEALER",
        portfolio_symbol="GAS-TRADING",
        status=PortfolioStatus.ACTIVE,
    )


@pytest.fixture()
def hedging_book():
    return BookInfo(
        symbol="GAS-HEDGE-BOOK",
        name="Gas Hedging Book",
        book_type=BookType.HEDGING,
        owner="Risk Desk",
        base_currency="USD",
        portfolio_symbol="GAS-HEDGE",
    )


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------


def test_init_creates_db(tmp_path):
    path = str(tmp_path / "new.db")
    assert not os.path.exists(path)
    init_portfolio_db(path)
    assert os.path.exists(path)


def test_init_idempotent(db_path):
    # Second call should not raise
    init_portfolio_db(db_path)


# ---------------------------------------------------------------------------
# Portfolio CRUD
# ---------------------------------------------------------------------------


def test_upsert_and_get_portfolio(db_path, trading_portfolio):
    upsert_portfolio(db_path, trading_portfolio)
    result = get_portfolio(db_path, "GAS-TRADING")
    assert result is not None
    assert result.symbol == "GAS-TRADING"
    assert result.name == "Gas Trading Portfolio"
    assert result.portfolio_type == PortfolioType.TRADING
    assert result.owner == "Gas Desk"
    assert result.base_currency == "USD"
    assert result.legal_entity_symbol == "HGDEALER"
    assert result.status == PortfolioStatus.ACTIVE


def test_upsert_updates_existing_portfolio(db_path, trading_portfolio):
    upsert_portfolio(db_path, trading_portfolio)
    updated = PortfolioInfo(
        symbol="GAS-TRADING",
        name="Gas Trading Portfolio (Updated)",
        portfolio_type=PortfolioType.TRADING,
        owner="Senior Gas Desk",
        base_currency="USD",
        legal_entity_symbol="HGDEALER",
        status=PortfolioStatus.ACTIVE,
    )
    upsert_portfolio(db_path, updated)
    result = get_portfolio(db_path, "GAS-TRADING")
    assert result.name == "Gas Trading Portfolio (Updated)"
    assert result.owner == "Senior Gas Desk"
    # Should still be one record
    assert len(get_all_portfolios(db_path)) == 1


def test_get_missing_portfolio_returns_none(db_path):
    assert get_portfolio(db_path, "NONEXISTENT") is None


def test_get_all_portfolios_empty(db_path):
    assert get_all_portfolios(db_path) == []


def test_get_all_portfolios_multiple(db_path, trading_portfolio, hedging_portfolio):
    upsert_portfolio(db_path, trading_portfolio)
    upsert_portfolio(db_path, hedging_portfolio)
    results = get_all_portfolios(db_path)
    assert len(results) == 2
    symbols = [r.symbol for r in results]
    assert "GAS-HEDGE" in symbols
    assert "GAS-TRADING" in symbols


def test_delete_portfolio(db_path, trading_portfolio):
    upsert_portfolio(db_path, trading_portfolio)
    assert delete_portfolio(db_path, "GAS-TRADING") is True
    assert get_portfolio(db_path, "GAS-TRADING") is None


def test_delete_missing_portfolio(db_path):
    assert delete_portfolio(db_path, "NONEXISTENT") is False


@pytest.mark.parametrize("ptype", list(PortfolioType))
def test_portfolio_type_round_trip(db_path, ptype):
    p = PortfolioInfo(
        symbol=f"PORT-{ptype.name}",
        name=f"Portfolio {ptype.value}",
        portfolio_type=ptype,
    )
    upsert_portfolio(db_path, p)
    result = get_portfolio(db_path, p.symbol)
    assert result.portfolio_type == ptype


def test_portfolio_with_none_optional_fields(db_path):
    p = PortfolioInfo(
        symbol="MIN",
        name="Minimal",
        portfolio_type=PortfolioType.PROPRIETARY,
    )
    upsert_portfolio(db_path, p)
    result = get_portfolio(db_path, "MIN")
    assert result.owner is None
    assert result.base_currency is None
    assert result.legal_entity_symbol is None
    assert result.parent_portfolio_symbol is None


def test_portfolio_with_parent(db_path):
    parent = PortfolioInfo(
        symbol="ENERGY",
        name="Energy",
        portfolio_type=PortfolioType.TRADING,
    )
    upsert_portfolio(db_path, parent)
    child = PortfolioInfo(
        symbol="GAS",
        name="Gas",
        portfolio_type=PortfolioType.TRADING,
        parent_portfolio_symbol="ENERGY",
    )
    upsert_portfolio(db_path, child)
    result = get_portfolio(db_path, "GAS")
    assert result.parent_portfolio_symbol == "ENERGY"


# ---------------------------------------------------------------------------
# Book CRUD
# ---------------------------------------------------------------------------


def test_upsert_and_get_book(db_path, trading_portfolio, trading_book):
    upsert_portfolio(db_path, trading_portfolio)
    upsert_book(db_path, trading_book)
    result = get_book(db_path, "GAS-BOOK-1")
    assert result is not None
    assert result.symbol == "GAS-BOOK-1"
    assert result.name == "Gas Trading Book 1"
    assert result.book_type == BookType.TRADING
    assert result.owner == "Gas Desk"
    assert result.base_currency == "USD"
    assert result.legal_entity_symbol == "HGDEALER"
    assert result.portfolio_symbol == "GAS-TRADING"
    assert result.status == PortfolioStatus.ACTIVE


def test_upsert_updates_existing_book(db_path, trading_portfolio, trading_book):
    upsert_portfolio(db_path, trading_portfolio)
    upsert_book(db_path, trading_book)
    updated = BookInfo(
        symbol="GAS-BOOK-1",
        name="Gas Trading Book 1 (Updated)",
        book_type=BookType.TRADING,
        owner="Senior Trader",
        base_currency="USD",
        portfolio_symbol="GAS-TRADING",
    )
    upsert_book(db_path, updated)
    result = get_book(db_path, "GAS-BOOK-1")
    assert result.name == "Gas Trading Book 1 (Updated)"
    assert result.owner == "Senior Trader"
    assert len(get_all_books(db_path)) == 1


def test_get_missing_book_returns_none(db_path):
    assert get_book(db_path, "NONEXISTENT") is None


def test_get_all_books_empty(db_path):
    assert get_all_books(db_path) == []


def test_get_all_books_multiple(db_path, trading_portfolio, hedging_portfolio, trading_book, hedging_book):
    upsert_portfolio(db_path, trading_portfolio)
    upsert_portfolio(db_path, hedging_portfolio)
    upsert_book(db_path, trading_book)
    upsert_book(db_path, hedging_book)
    results = get_all_books(db_path)
    assert len(results) == 2
    symbols = [r.symbol for r in results]
    assert "GAS-BOOK-1" in symbols
    assert "GAS-HEDGE-BOOK" in symbols


def test_delete_book(db_path, trading_portfolio, trading_book):
    upsert_portfolio(db_path, trading_portfolio)
    upsert_book(db_path, trading_book)
    assert delete_book(db_path, "GAS-BOOK-1") is True
    assert get_book(db_path, "GAS-BOOK-1") is None


def test_delete_missing_book(db_path):
    assert delete_book(db_path, "NONEXISTENT") is False


@pytest.mark.parametrize("btype", list(BookType))
def test_book_type_round_trip(db_path, btype):
    b = BookInfo(
        symbol=f"BOOK-{btype.name}",
        name=f"Book {btype.value}",
        book_type=btype,
    )
    upsert_book(db_path, b)
    result = get_book(db_path, b.symbol)
    assert result.book_type == btype


def test_book_with_none_optional_fields(db_path):
    b = BookInfo(
        symbol="MIN",
        name="Minimal",
        book_type=BookType.WASH,
    )
    upsert_book(db_path, b)
    result = get_book(db_path, "MIN")
    assert result.owner is None
    assert result.base_currency is None
    assert result.legal_entity_symbol is None
    assert result.portfolio_symbol is None
