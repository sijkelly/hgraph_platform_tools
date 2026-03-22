"""Tests for the hgraph OMS API endpoints."""

import json
from datetime import date

import pytest
from fastapi.testclient import TestClient

from hg_oap.credit.credit_limit import (
    CreditLimit,
    CreditLimitType,
    CreditStatus,
    CreditUtilization,
)
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.agreement import MasterAgreement, MasterAgreementType
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship
from hg_oap.portfolio.portfolio_info import (
    BookInfo,
    BookType,
    PortfolioInfo,
    PortfolioStatus,
    PortfolioType,
)

from hgraph_api.app import create_app
from hgraph_static_admin.credit_store import (
    init_credit_db,
    upsert_credit_limit,
    upsert_credit_utilization,
)
from hgraph_static_admin.party_store import (
    init_party_db,
    upsert_legal_entity,
    upsert_trading_relationship,
)
from hgraph_static_admin.portfolio_store import (
    init_portfolio_db,
    upsert_book,
    upsert_portfolio,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_dir(tmp_path):
    """Create and initialise all databases in a temp directory."""
    credit_db = str(tmp_path / "credit_data.db")
    party_db = str(tmp_path / "party_data.db")
    portfolio_db = str(tmp_path / "portfolio_data.db")
    init_credit_db(credit_db)
    init_party_db(party_db)
    init_portfolio_db(portfolio_db)
    return str(tmp_path)


@pytest.fixture()
def client(db_dir):
    """Create a FastAPI TestClient with databases pointed at the temp dir."""
    app = create_app()
    app.state.db_dir = db_dir
    return TestClient(app, headers={"X-User-Id": "test-user"})


@pytest.fixture()
def seeded_credit_db(db_dir):
    """Seed credit database with test data."""
    import os

    db = os.path.join(db_dir, "credit_data.db")
    upsert_credit_limit(
        db,
        CreditLimit(
            counterparty_symbol="ACME",
            limit_type=CreditLimitType.BILATERAL,
            limit_amount=50_000_000.0,
            limit_currency="USD",
            effective_date=date(2025, 1, 1),
            expiry_date=date(2026, 1, 1),
            status=CreditStatus.ACTIVE,
            approved_by="Credit Committee",
            last_review_date=date(2025, 6, 15),
        ),
    )
    upsert_credit_limit(
        db,
        CreditLimit(
            counterparty_symbol="ACME",
            limit_type=CreditLimitType.CLEARED,
            limit_amount=100_000_000.0,
        ),
    )
    upsert_credit_limit(
        db,
        CreditLimit(
            counterparty_symbol="BIGCO",
            limit_type=CreditLimitType.TOTAL,
            limit_amount=200_000_000.0,
        ),
    )
    upsert_credit_utilization(
        db,
        CreditUtilization(
            counterparty_symbol="ACME",
            utilized_amount=15_000_000.0,
            limit_amount=50_000_000.0,
            available_amount=35_000_000.0,
            utilization_percentage=30.0,
            as_of_timestamp="2025-07-01T14:30:00+00:00",
        ),
    )
    return db_dir


@pytest.fixture()
def seeded_party_db(db_dir):
    """Seed party database with test data."""
    import os

    db = os.path.join(db_dir, "party_data.db")
    dealer = LegalEntity(
        symbol="HGDEALER",
        name="HGraph Dealer Corp",
        classification=PartyClassification.SWAP_DEALER,
        lei="529900HGDEALER00LEI",
        jurisdiction="US",
    )
    counterparty = LegalEntity(
        symbol="ACME",
        name="ACME Energy Corp",
        classification=PartyClassification.NON_FINANCIAL_END_USER,
        lei="529900ACMEENERGY0LEI",
        jurisdiction="US",
    )
    upsert_legal_entity(db, dealer)
    upsert_legal_entity(db, counterparty)

    relationship = TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.BILATERAL,
        isda=MasterAgreement(
            agreement_type=MasterAgreementType.ISDA,
            version="2002",
            credit_support_annex=True,
            governing_law="New York",
        ),
        dropcopy_enabled=False,
        internal_portfolio="GAS-BOOK-1",
    )
    upsert_trading_relationship(db, relationship)
    return db_dir


@pytest.fixture()
def seeded_portfolio_db(db_dir):
    """Seed portfolio database with test data."""
    import os

    db = os.path.join(db_dir, "portfolio_data.db")
    upsert_portfolio(
        db,
        PortfolioInfo(
            symbol="GAS-TRADING",
            name="Gas Trading Portfolio",
            portfolio_type=PortfolioType.TRADING,
            owner="Gas Desk",
            base_currency="USD",
            legal_entity_symbol="HGDEALER",
            status=PortfolioStatus.ACTIVE,
        ),
    )
    upsert_book(
        db,
        BookInfo(
            symbol="GAS-BOOK-1",
            name="Gas Trading Book 1",
            book_type=BookType.TRADING,
            owner="Gas Desk",
            base_currency="USD",
            portfolio_symbol="GAS-TRADING",
            status=PortfolioStatus.ACTIVE,
        ),
    )
    return db_dir


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


def test_health(client):
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Credit endpoints
# ---------------------------------------------------------------------------


def test_credit_limits_empty(client):
    r = client.get("/api/v1/credit/limits")
    assert r.status_code == 200
    assert r.json() == []


def test_credit_limits_all(client, seeded_credit_db):
    r = client.get("/api/v1/credit/limits")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 3
    symbols = {(d["counterparty_symbol"], d["limit_type"]) for d in data}
    assert ("ACME", "Bilateral") in symbols
    assert ("ACME", "Cleared") in symbols
    assert ("BIGCO", "Total") in symbols


def test_credit_limits_by_counterparty(client, seeded_credit_db):
    r = client.get("/api/v1/credit/limits", params={"counterparty": "ACME"})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert all(d["counterparty_symbol"] == "ACME" for d in data)


def test_credit_limits_by_counterparty_empty(client, seeded_credit_db):
    r = client.get("/api/v1/credit/limits", params={"counterparty": "NONEXISTENT"})
    assert r.status_code == 200
    assert r.json() == []


def test_credit_limit_detail(client, seeded_credit_db):
    r = client.get("/api/v1/credit/limits/ACME/Bilateral")
    assert r.status_code == 200
    data = r.json()
    assert data["counterparty_symbol"] == "ACME"
    assert data["limit_type"] == "Bilateral"
    assert data["limit_amount"] == 50_000_000.0
    assert data["effective_date"] == "2025-01-01"
    assert data["approved_by"] == "Credit Committee"
    assert data["status"] == "Active"


def test_credit_limit_detail_not_found(client, seeded_credit_db):
    r = client.get("/api/v1/credit/limits/NONEXISTENT/Bilateral")
    assert r.status_code == 404


def test_credit_utilizations_all(client, seeded_credit_db):
    r = client.get("/api/v1/credit/utilizations")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["counterparty_symbol"] == "ACME"
    assert data[0]["utilization_percentage"] == 30.0


def test_credit_utilization_detail(client, seeded_credit_db):
    r = client.get("/api/v1/credit/utilizations/ACME")
    assert r.status_code == 200
    data = r.json()
    assert data["utilized_amount"] == 15_000_000.0
    assert data["available_amount"] == 35_000_000.0
    assert data["as_of_timestamp"] == "2025-07-01T14:30:00+00:00"


def test_credit_utilization_not_found(client, seeded_credit_db):
    r = client.get("/api/v1/credit/utilizations/NONEXISTENT")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Party endpoints
# ---------------------------------------------------------------------------


def test_entities_empty(client):
    r = client.get("/api/v1/parties/entities")
    assert r.status_code == 200
    assert r.json() == []


def test_entities_all(client, seeded_party_db):
    r = client.get("/api/v1/parties/entities")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    symbols = {d["symbol"] for d in data}
    assert symbols == {"HGDEALER", "ACME"}


def test_entity_detail(client, seeded_party_db):
    r = client.get("/api/v1/parties/entities/ACME")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "ACME Energy Corp"
    assert data["classification"] == "NFEU"
    assert data["lei"] == "529900ACMEENERGY0LEI"


def test_entity_not_found(client, seeded_party_db):
    r = client.get("/api/v1/parties/entities/NONEXISTENT")
    assert r.status_code == 404


def test_relationships_all(client, seeded_party_db):
    r = client.get("/api/v1/parties/relationships")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    rel = data[0]
    assert rel["internal_party_symbol"] == "HGDEALER"
    assert rel["external_party_symbol"] == "ACME"
    assert rel["clearing_status"] == "Bilateral"
    assert rel["isda"] is not None
    assert rel["isda"]["version"] == "2002"
    assert rel["isda"]["credit_support_annex"] is True
    assert rel["isda"]["governing_law"] == "New York"


def test_relationship_detail(client, seeded_party_db):
    r = client.get("/api/v1/parties/relationships/HGDEALER/ACME")
    assert r.status_code == 200
    data = r.json()
    assert data["internal_party_symbol"] == "HGDEALER"
    assert data["internal_portfolio"] == "GAS-BOOK-1"


def test_relationship_not_found(client, seeded_party_db):
    r = client.get("/api/v1/parties/relationships/X/Y")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Portfolio endpoints
# ---------------------------------------------------------------------------


def test_portfolios_empty(client):
    r = client.get("/api/v1/portfolios")
    assert r.status_code == 200
    assert r.json() == []


def test_portfolios_all(client, seeded_portfolio_db):
    r = client.get("/api/v1/portfolios")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["symbol"] == "GAS-TRADING"
    assert data[0]["portfolio_type"] == "Trading"
    assert data[0]["status"] == "Active"


def test_portfolio_detail(client, seeded_portfolio_db):
    r = client.get("/api/v1/portfolios/GAS-TRADING")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "Gas Trading Portfolio"
    assert data["owner"] == "Gas Desk"
    assert data["legal_entity_symbol"] == "HGDEALER"


def test_portfolio_not_found(client, seeded_portfolio_db):
    r = client.get("/api/v1/portfolios/NONEXISTENT")
    assert r.status_code == 404


def test_books_empty(client):
    r = client.get("/api/v1/books")
    assert r.status_code == 200
    assert r.json() == []


def test_books_all(client, seeded_portfolio_db):
    r = client.get("/api/v1/books")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["symbol"] == "GAS-BOOK-1"
    assert data[0]["book_type"] == "Trading"


def test_book_detail(client, seeded_portfolio_db):
    r = client.get("/api/v1/books/GAS-BOOK-1")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "Gas Trading Book 1"
    assert data["portfolio_symbol"] == "GAS-TRADING"


def test_book_not_found(client, seeded_portfolio_db):
    r = client.get("/api/v1/books/NONEXISTENT")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Entitlements endpoints
# ---------------------------------------------------------------------------


def test_list_roles(client):
    r = client.get("/api/v1/entitlements/")
    assert r.status_code == 200
    data = r.json()
    assert "roles" in data
    assert isinstance(data["roles"], dict)


def test_entitlements_user_not_found(client):
    r = client.get("/api/v1/entitlements/nonexistent_user")
    assert r.status_code == 404


def test_check_permission_user_not_found(client):
    r = client.get("/api/v1/entitlements/nonexistent_user/check/execute_trade")
    assert r.status_code == 200
    data = r.json()
    assert data["allowed"] is False


# ---------------------------------------------------------------------------
# NLP order parsing endpoints (Phase 1c)
# ---------------------------------------------------------------------------


def test_parse_nlp_empty_text(client):
    r = client.post("/api/v1/orders/parse-nlp", json={"text": ""})
    assert r.status_code == 400
    assert "empty" in r.json()["detail"].lower()


def test_parse_nlp_whitespace_text(client):
    r = client.post("/api/v1/orders/parse-nlp", json={"text": "   "})
    assert r.status_code == 400


def test_parse_nlp_success(client, monkeypatch):
    """Test NLP parsing with a mocked Anthropic client."""
    mock_response = {
        "instrument": {"value": "Henry Hub Natural Gas", "confidence": 0.95},
        "side": {"value": "Buy", "confidence": 1.0},
        "quantity": {"value": "50000", "confidence": 0.9},
        "unit": {"value": "MMBtu", "confidence": 0.9},
        "price": {"value": "3.45", "confidence": 0.95},
        "counterparty": {"value": "ACME", "confidence": 0.8},
        "delivery_period": {"value": "January 2026", "confidence": 0.9},
        "order_type": {"value": "Limit", "confidence": 1.0},
    }

    class FakeContent:
        text = json.dumps(mock_response)

    class FakeResponse:
        content = [FakeContent()]

    class FakeMessages:
        def create(self, **kwargs):
            return FakeResponse()

    class FakeClient:
        messages = FakeMessages()

    monkeypatch.setattr(
        "hgraph_api.services.nlp_order_parser._get_client",
        lambda: FakeClient(),
    )

    r = client.post(
        "/api/v1/orders/parse-nlp",
        json={"text": "Buy 50k MMBtu Henry Hub gas Jan 26 at 3.45 with ACME"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["instrument"]["value"] == "Henry Hub Natural Gas"
    assert data["side"]["value"] == "Buy"
    assert data["quantity"]["value"] == "50000"
    assert data["raw_text"] == "Buy 50k MMBtu Henry Hub gas Jan 26 at 3.45 with ACME"


def test_parse_nlp_with_context(client, monkeypatch):
    """Test NLP parsing passes context to Claude."""
    captured_messages = []

    class FakeContent:
        text = json.dumps({"side": {"value": "Sell", "confidence": 1.0}})

    class FakeResponse:
        content = [FakeContent()]

    class FakeMessages:
        def create(self, **kwargs):
            captured_messages.append(kwargs.get("messages", []))
            return FakeResponse()

    class FakeClient:
        messages = FakeMessages()

    monkeypatch.setattr(
        "hgraph_api.services.nlp_order_parser._get_client",
        lambda: FakeClient(),
    )

    r = client.post(
        "/api/v1/orders/parse-nlp",
        json={"text": "sell 10 lots", "context": "counterparty is BIGCO"},
    )
    assert r.status_code == 200
    # Verify context was included in the message
    assert len(captured_messages) == 1
    user_msg = captured_messages[0][0]["content"]
    assert "counterparty is BIGCO" in user_msg
    assert "sell 10 lots" in user_msg


def test_parse_nlp_no_api_key(client, monkeypatch):
    """Test graceful error when ANTHROPIC_API_KEY is missing."""
    monkeypatch.setattr(
        "hgraph_api.services.nlp_order_parser._get_client",
        lambda: (_ for _ in ()).throw(RuntimeError("ANTHROPIC_API_KEY environment variable must be set")),
    )

    r = client.post(
        "/api/v1/orders/parse-nlp",
        json={"text": "Buy gas"},
    )
    assert r.status_code == 503


def test_parse_nlp_markdown_fences(client, monkeypatch):
    """Test that markdown code fences in Claude response are stripped."""
    raw_json = json.dumps({"side": {"value": "Buy", "confidence": 1.0}})
    fenced_response = f"```json\n{raw_json}\n```"

    class FakeContent:
        text = fenced_response

    class FakeResponse:
        content = [FakeContent()]

    class FakeMessages:
        def create(self, **kwargs):
            return FakeResponse()

    class FakeClient:
        messages = FakeMessages()

    monkeypatch.setattr(
        "hgraph_api.services.nlp_order_parser._get_client",
        lambda: FakeClient(),
    )

    r = client.post("/api/v1/orders/parse-nlp", json={"text": "Buy gas"})
    assert r.status_code == 200
    assert r.json()["side"]["value"] == "Buy"


# ---------------------------------------------------------------------------
# WebSocket credit endpoint (Phase 1b)
# ---------------------------------------------------------------------------


def test_websocket_credit_initial_snapshot(client, seeded_credit_db):
    """Test that connecting to /ws/credit sends initial limit and utilization snapshots."""
    with client.websocket_connect("/ws/credit") as ws:
        # First message should be limits
        msg1 = json.loads(ws.receive_text())
        assert msg1["type"] == "credit.limits"
        assert len(msg1["data"]) == 3

        # Second message should be utilizations
        msg2 = json.loads(ws.receive_text())
        assert msg2["type"] == "credit.utilizations"
        assert len(msg2["data"]) == 1
        assert msg2["data"][0]["counterparty_symbol"] == "ACME"

        # Tell the server to close the loop cleanly
        ws.send_text("close")


def test_websocket_credit_empty_db(client):
    """Test WebSocket with empty database sends empty snapshots."""
    with client.websocket_connect("/ws/credit") as ws:
        msg1 = json.loads(ws.receive_text())
        assert msg1["type"] == "credit.limits"
        assert msg1["data"] == []

        msg2 = json.loads(ws.receive_text())
        assert msg2["type"] == "credit.utilizations"
        assert msg2["data"] == []

        ws.send_text("close")


# ---------------------------------------------------------------------------
# Order CRUD endpoints
# ---------------------------------------------------------------------------


def _create_order_payload(**overrides):
    """Build a valid CreateOrderRequest payload with sensible defaults."""
    payload = {
        "instrument": "Henry Hub Natural Gas",
        "side": "Buy",
        "quantity": "50000",
        "unit": "MMBtu",
        "price": "3.45",
        "counterparty": "",
        "delivery_period": "January 2026",
        "order_type": "Limit",
        "portfolio": "GAS-TRADING",
        "book": "GAS-BOOK-1",
    }
    payload.update(overrides)
    return payload


def test_create_order(client):
    r = client.post("/api/v1/orders", json=_create_order_payload())
    assert r.status_code == 201
    data = r.json()
    assert data["instrument"] == "Henry Hub Natural Gas"
    assert data["side"] == "Buy"
    assert data["quantity"] == "50000"
    assert data["status"] == "Pending"
    assert data["order_id"].startswith("ORD-")
    assert data["created_at"]
    assert data["updated_at"]


def test_create_order_empty_instrument(client):
    r = client.post("/api/v1/orders", json=_create_order_payload(instrument=""))
    assert r.status_code == 400
    assert "instrument" in r.json()["detail"].lower()


def test_create_order_invalid_side(client):
    r = client.post("/api/v1/orders", json=_create_order_payload(side="Hold"))
    assert r.status_code == 400
    assert "side" in r.json()["detail"].lower()


def test_create_order_empty_quantity(client):
    r = client.post("/api/v1/orders", json=_create_order_payload(quantity=""))
    assert r.status_code == 400
    assert "quantity" in r.json()["detail"].lower()


def test_list_orders_empty(client):
    r = client.get("/api/v1/orders")
    assert r.status_code == 200
    assert r.json() == []


def test_list_orders(client):
    client.post("/api/v1/orders", json=_create_order_payload())
    client.post("/api/v1/orders", json=_create_order_payload(side="Sell", instrument="WTI Crude"))
    r = client.get("/api/v1/orders")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2


def test_get_order(client):
    create_r = client.post("/api/v1/orders", json=_create_order_payload())
    order_id = create_r.json()["order_id"]

    r = client.get(f"/api/v1/orders/{order_id}")
    assert r.status_code == 200
    assert r.json()["order_id"] == order_id


def test_get_order_not_found(client):
    r = client.get("/api/v1/orders/ORD-NONEXIST")
    assert r.status_code == 404


def test_update_order_status(client):
    create_r = client.post("/api/v1/orders", json=_create_order_payload())
    order_id = create_r.json()["order_id"]

    r = client.patch(f"/api/v1/orders/{order_id}", json={"status": "Working"})
    assert r.status_code == 200
    assert r.json()["status"] == "Working"


def test_update_order_filled(client):
    create_r = client.post("/api/v1/orders", json=_create_order_payload())
    order_id = create_r.json()["order_id"]

    r = client.patch(
        f"/api/v1/orders/{order_id}",
        json={"status": "Partially Filled", "filled_quantity": "25000"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "Partially Filled"
    assert r.json()["filled_quantity"] == "25000"


def test_update_order_invalid_status(client):
    create_r = client.post("/api/v1/orders", json=_create_order_payload())
    order_id = create_r.json()["order_id"]

    r = client.patch(f"/api/v1/orders/{order_id}", json={"status": "Bogus"})
    assert r.status_code == 400
    assert "invalid status" in r.json()["detail"].lower()


def test_update_order_not_found(client):
    r = client.patch("/api/v1/orders/ORD-NONEXIST", json={"status": "Working"})
    assert r.status_code == 404


def test_cancel_order(client):
    create_r = client.post("/api/v1/orders", json=_create_order_payload())
    order_id = create_r.json()["order_id"]

    r = client.post(f"/api/v1/orders/{order_id}/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == "Cancelled"


def test_cancel_order_already_filled(client):
    create_r = client.post("/api/v1/orders", json=_create_order_payload())
    order_id = create_r.json()["order_id"]

    # Move to Filled first
    client.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"})

    # Attempt cancel
    r = client.post(f"/api/v1/orders/{order_id}/cancel")
    assert r.status_code == 409
    assert "cannot cancel" in r.json()["detail"].lower()


def test_cancel_order_not_found(client):
    r = client.post("/api/v1/orders/ORD-NONEXIST/cancel")
    assert r.status_code == 404


def test_create_order_with_credit_check(client, seeded_credit_db):
    """Order with counterparty at <100% utilization should succeed."""
    r = client.post(
        "/api/v1/orders",
        json=_create_order_payload(counterparty="ACME"),
    )
    assert r.status_code == 201
    assert r.json()["counterparty"] == "ACME"


def test_create_order_credit_exceeded(client, seeded_credit_db):
    """Order with counterparty at >=100% utilization should be rejected."""
    import os

    db = os.path.join(seeded_credit_db, "credit_data.db")
    upsert_credit_utilization(
        db,
        CreditUtilization(
            counterparty_symbol="BIGCO",
            utilized_amount=200_000_000.0,
            limit_amount=200_000_000.0,
            available_amount=0.0,
            utilization_percentage=100.0,
            as_of_timestamp="2025-07-01T14:30:00+00:00",
        ),
    )
    r = client.post(
        "/api/v1/orders",
        json=_create_order_payload(counterparty="BIGCO"),
    )
    assert r.status_code == 422
    assert "credit limit exceeded" in r.json()["detail"].lower()


# ---------------------------------------------------------------------------
# WebSocket orders endpoint
# ---------------------------------------------------------------------------


def test_websocket_orders_initial_snapshot(client):
    """Test that connecting to /ws/orders sends initial snapshot."""
    # Create an order first
    client.post("/api/v1/orders", json=_create_order_payload())

    with client.websocket_connect("/ws/orders") as ws:
        msg = json.loads(ws.receive_text())
        assert msg["type"] == "orders.snapshot"
        assert len(msg["data"]) == 1
        assert msg["data"][0]["instrument"] == "Henry Hub Natural Gas"
        ws.send_text("close")


def test_websocket_orders_empty(client):
    """Test orders WebSocket with no orders sends empty snapshot."""
    with client.websocket_connect("/ws/orders") as ws:
        msg = json.loads(ws.receive_text())
        assert msg["type"] == "orders.snapshot"
        assert msg["data"] == []
        ws.send_text("close")
