"""Tests for portfolio Kafka subscriber message parsing and processing."""

import pytest

from hg_oap.portfolio.portfolio_info import (
    BookInfo,
    BookType,
    PortfolioInfo,
    PortfolioStatus,
    PortfolioType,
)

from hgraph_static_admin.portfolio_store import (
    get_all_books,
    get_all_portfolios,
    get_book,
    get_portfolio,
    init_portfolio_db,
)
from hgraph_static_admin.portfolio_kafka_subscriber import (
    PortfolioKafkaSubscriber,
    parse_book_message,
    parse_portfolio_message,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_portfolio_subscriber.db")
    init_portfolio_db(path)
    return path


@pytest.fixture()
def portfolio_upsert_msg():
    return {
        "action": "UPSERT",
        "symbol": "GAS-TRADING",
        "name": "Gas Trading Portfolio",
        "portfolio_type": "Trading",
        "owner": "Gas Desk",
        "base_currency": "USD",
        "legal_entity_symbol": "HGDEALER",
        "status": "Active",
        "parent_portfolio_symbol": None,
    }


@pytest.fixture()
def portfolio_delete_msg():
    return {"action": "DELETE", "symbol": "GAS-TRADING"}


@pytest.fixture()
def book_upsert_msg():
    return {
        "action": "UPSERT",
        "symbol": "GAS-BOOK-1",
        "name": "Gas Trading Book 1",
        "book_type": "Trading",
        "owner": "Gas Desk",
        "base_currency": "USD",
        "legal_entity_symbol": "HGDEALER",
        "portfolio_symbol": "GAS-TRADING",
        "status": "Active",
    }


@pytest.fixture()
def book_delete_msg():
    return {"action": "DELETE", "symbol": "GAS-BOOK-1"}


@pytest.fixture()
def subscriber(db_path):
    return PortfolioKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        portfolio_topic="portfolio.portfolio",
        book_topic="portfolio.book",
    )


# ---------------------------------------------------------------------------
# parse_portfolio_message
# ---------------------------------------------------------------------------


def test_parse_portfolio_upsert(portfolio_upsert_msg):
    action, portfolio = parse_portfolio_message(portfolio_upsert_msg)
    assert action == "UPSERT"
    assert isinstance(portfolio, PortfolioInfo)
    assert portfolio.symbol == "GAS-TRADING"
    assert portfolio.name == "Gas Trading Portfolio"
    assert portfolio.portfolio_type == PortfolioType.TRADING
    assert portfolio.owner == "Gas Desk"
    assert portfolio.base_currency == "USD"
    assert portfolio.legal_entity_symbol == "HGDEALER"
    assert portfolio.status == PortfolioStatus.ACTIVE
    assert portfolio.parent_portfolio_symbol is None


def test_parse_portfolio_delete(portfolio_delete_msg):
    action, portfolio = parse_portfolio_message(portfolio_delete_msg)
    assert action == "DELETE"
    assert portfolio is None


def test_parse_portfolio_trading_type():
    msg = {"action": "UPSERT", "symbol": "P1", "name": "P", "portfolio_type": "Trading"}
    _, p = parse_portfolio_message(msg)
    assert p.portfolio_type == PortfolioType.TRADING


def test_parse_portfolio_hedging_type():
    msg = {"action": "UPSERT", "symbol": "P1", "name": "P", "portfolio_type": "Hedging"}
    _, p = parse_portfolio_message(msg)
    assert p.portfolio_type == PortfolioType.HEDGING


def test_parse_portfolio_proprietary_type():
    msg = {"action": "UPSERT", "symbol": "P1", "name": "P", "portfolio_type": "Proprietary"}
    _, p = parse_portfolio_message(msg)
    assert p.portfolio_type == PortfolioType.PROPRIETARY


def test_parse_portfolio_missing_symbol():
    msg = {"action": "UPSERT", "name": "P", "portfolio_type": "Trading"}
    with pytest.raises(ValueError, match="symbol"):
        parse_portfolio_message(msg)


def test_parse_portfolio_missing_name():
    msg = {"action": "UPSERT", "symbol": "P1", "portfolio_type": "Trading"}
    with pytest.raises(ValueError, match="name"):
        parse_portfolio_message(msg)


def test_parse_portfolio_unknown_action():
    msg = {"action": "REMOVE", "symbol": "P1"}
    with pytest.raises(ValueError, match="Unknown action"):
        parse_portfolio_message(msg)


def test_parse_portfolio_unknown_type():
    msg = {"action": "UPSERT", "symbol": "P1", "name": "P", "portfolio_type": "Unknown"}
    with pytest.raises(ValueError, match="Unknown portfolio_type"):
        parse_portfolio_message(msg)


def test_parse_portfolio_case_insensitive_action():
    msg = {"action": "upsert", "symbol": "P1", "name": "P", "portfolio_type": "Trading"}
    action, p = parse_portfolio_message(msg)
    assert action == "UPSERT"
    assert p is not None


def test_parse_portfolio_default_status():
    msg = {"action": "UPSERT", "symbol": "P1", "name": "P", "portfolio_type": "Trading"}
    _, p = parse_portfolio_message(msg)
    assert p.status == PortfolioStatus.ACTIVE


def test_parse_portfolio_unknown_status():
    msg = {"action": "UPSERT", "symbol": "P1", "name": "P", "portfolio_type": "Trading", "status": "Deleted"}
    with pytest.raises(ValueError, match="Unknown status"):
        parse_portfolio_message(msg)


# ---------------------------------------------------------------------------
# parse_book_message
# ---------------------------------------------------------------------------


def test_parse_book_upsert(book_upsert_msg):
    action, book = parse_book_message(book_upsert_msg)
    assert action == "UPSERT"
    assert isinstance(book, BookInfo)
    assert book.symbol == "GAS-BOOK-1"
    assert book.name == "Gas Trading Book 1"
    assert book.book_type == BookType.TRADING
    assert book.owner == "Gas Desk"
    assert book.base_currency == "USD"
    assert book.legal_entity_symbol == "HGDEALER"
    assert book.portfolio_symbol == "GAS-TRADING"
    assert book.status == PortfolioStatus.ACTIVE


def test_parse_book_delete(book_delete_msg):
    action, book = parse_book_message(book_delete_msg)
    assert action == "DELETE"
    assert book is None


def test_parse_book_missing_symbol():
    msg = {"action": "UPSERT", "name": "B", "book_type": "Trading"}
    with pytest.raises(ValueError, match="symbol"):
        parse_book_message(msg)


def test_parse_book_missing_name():
    msg = {"action": "UPSERT", "symbol": "B1", "book_type": "Trading"}
    with pytest.raises(ValueError, match="name"):
        parse_book_message(msg)


def test_parse_book_unknown_type():
    msg = {"action": "UPSERT", "symbol": "B1", "name": "B", "book_type": "Unknown"}
    with pytest.raises(ValueError, match="Unknown book_type"):
        parse_book_message(msg)


def test_parse_book_hedging_type():
    msg = {"action": "UPSERT", "symbol": "B1", "name": "B", "book_type": "Hedging"}
    _, b = parse_book_message(msg)
    assert b.book_type == BookType.HEDGING


def test_parse_book_wash_type():
    msg = {"action": "UPSERT", "symbol": "B1", "name": "B", "book_type": "Wash"}
    _, b = parse_book_message(msg)
    assert b.book_type == BookType.WASH


def test_parse_book_transit_type():
    msg = {"action": "UPSERT", "symbol": "B1", "name": "B", "book_type": "Transit"}
    _, b = parse_book_message(msg)
    assert b.book_type == BookType.TRANSIT


# ---------------------------------------------------------------------------
# process_message (via subscriber)
# ---------------------------------------------------------------------------


def test_process_portfolio_upsert(subscriber, db_path, portfolio_upsert_msg):
    subscriber.process_message("portfolio.portfolio", portfolio_upsert_msg)
    result = get_portfolio(db_path, "GAS-TRADING")
    assert result is not None
    assert result.symbol == "GAS-TRADING"
    assert result.portfolio_type == PortfolioType.TRADING


def test_process_portfolio_delete(subscriber, db_path, portfolio_upsert_msg, portfolio_delete_msg):
    subscriber.process_message("portfolio.portfolio", portfolio_upsert_msg)
    assert get_portfolio(db_path, "GAS-TRADING") is not None
    subscriber.process_message("portfolio.portfolio", portfolio_delete_msg)
    assert get_portfolio(db_path, "GAS-TRADING") is None


def test_process_book_upsert(subscriber, db_path, portfolio_upsert_msg, book_upsert_msg):
    # Insert portfolio first (FK)
    subscriber.process_message("portfolio.portfolio", portfolio_upsert_msg)
    subscriber.process_message("portfolio.book", book_upsert_msg)
    result = get_book(db_path, "GAS-BOOK-1")
    assert result is not None
    assert result.book_type == BookType.TRADING


def test_process_unknown_topic(subscriber):
    # Should not raise — just logs a warning
    subscriber.process_message("unknown.topic", {"action": "UPSERT", "symbol": "X"})


# ---------------------------------------------------------------------------
# Full round-trips
# ---------------------------------------------------------------------------


def test_full_portfolio_round_trip(subscriber, db_path):
    msg = {
        "action": "UPSERT",
        "symbol": "POWER-TRADING",
        "name": "Power Trading Portfolio",
        "portfolio_type": "Trading",
        "owner": "Power Desk",
        "base_currency": "USD",
        "legal_entity_symbol": "HGDEALER",
        "status": "Active",
        "parent_portfolio_symbol": None,
    }
    subscriber.process_message("portfolio.portfolio", msg)
    result = get_portfolio(db_path, "POWER-TRADING")
    assert result.symbol == "POWER-TRADING"
    assert result.name == "Power Trading Portfolio"
    assert result.portfolio_type == PortfolioType.TRADING
    assert result.owner == "Power Desk"
    assert result.base_currency == "USD"
    assert result.legal_entity_symbol == "HGDEALER"
    assert result.status == PortfolioStatus.ACTIVE
    assert result.parent_portfolio_symbol is None


def test_full_book_round_trip(subscriber, db_path):
    # Create portfolio first
    subscriber.process_message("portfolio.portfolio", {
        "action": "UPSERT",
        "symbol": "POWER-TRADING",
        "name": "Power Trading",
        "portfolio_type": "Trading",
    })
    msg = {
        "action": "UPSERT",
        "symbol": "POWER-BOOK-1",
        "name": "Power Trading Book 1",
        "book_type": "Trading",
        "owner": "Power Desk",
        "base_currency": "USD",
        "legal_entity_symbol": "HGDEALER",
        "portfolio_symbol": "POWER-TRADING",
        "status": "Active",
    }
    subscriber.process_message("portfolio.book", msg)
    result = get_book(db_path, "POWER-BOOK-1")
    assert result.symbol == "POWER-BOOK-1"
    assert result.name == "Power Trading Book 1"
    assert result.book_type == BookType.TRADING
    assert result.owner == "Power Desk"
    assert result.portfolio_symbol == "POWER-TRADING"


def test_multiple_portfolios_round_trip(subscriber, db_path):
    messages = [
        {"action": "UPSERT", "symbol": "GAS", "name": "Gas", "portfolio_type": "Trading"},
        {"action": "UPSERT", "symbol": "POWER", "name": "Power", "portfolio_type": "Hedging"},
        {"action": "UPSERT", "symbol": "OIL", "name": "Oil", "portfolio_type": "Proprietary"},
    ]
    for msg in messages:
        subscriber.process_message("portfolio.portfolio", msg)

    results = get_all_portfolios(db_path)
    assert len(results) == 3
    symbols = {r.symbol for r in results}
    assert symbols == {"GAS", "POWER", "OIL"}
