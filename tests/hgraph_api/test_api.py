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
    # The context manager runs the app's shutdown, which stops the order engine thread
    with TestClient(app, headers={"X-User-Id": "test-user"}) as test_client:
        yield test_client


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


class _FakeAnthropic:
    """Stands in for the Anthropic client: returns a canned reply and records the request."""

    def __init__(self, reply=None, stop_reason="end_turn", error=None):
        self.requests = []
        self.messages = self
        self._reply, self._stop_reason, self._error = reply or {}, stop_reason, error

    def create(self, **kwargs):
        from types import SimpleNamespace

        from hgraph_api.services.nlp_order_parser import ORDER_FIELDS

        self.requests.append(kwargs)
        if self._error:
            raise self._error
        # Structured output always returns every field, null when absent
        full = {**{name: None for name in (*ORDER_FIELDS, "notes")}, **self._reply}
        thinking = SimpleNamespace(type="thinking", thinking="")
        text = SimpleNamespace(type="text", text=json.dumps(full))
        return SimpleNamespace(content=[thinking, text], stop_reason=self._stop_reason)


def _field(value, confidence=0.9):
    return {"value": value, "confidence": confidence}


_PARSED_ORDER = {
    "instrument": _field("Henry Hub Natural Gas", 0.95),
    "side": _field("Buy", 1.0),
    "quantity": _field("50000"),
    "unit": _field("MMBtu"),
    "price": _field("3.45", 0.95),
    "counterparty": _field("ACME", 0.8),
    "delivery_period": _field("January 2027"),
    "order_type": _field("Limit", 1.0),
}


def _use_fake_anthropic(monkeypatch, **kwargs) -> _FakeAnthropic:
    fake = _FakeAnthropic(**kwargs)
    monkeypatch.setattr("hgraph_api.services.nlp_order_parser._get_client", lambda: fake)
    return fake


def test_parse_nlp_success(client, monkeypatch):
    _use_fake_anthropic(monkeypatch, reply=_PARSED_ORDER)

    r = client.post(
        "/api/v1/orders/parse-nlp",
        json={"text": "Buy 50k MMBtu Henry Hub gas Jan 27 at 3.45 with ACME"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["instrument"] == {"value": "Henry Hub Natural Gas", "confidence": 0.95}
    assert data["side"]["value"] == "Buy"
    assert data["quantity"]["value"] == "50000"
    assert data["portfolio"] is None  # not mentioned in the text
    assert data["warnings"] == []
    assert data["raw_text"] == "Buy 50k MMBtu Henry Hub gas Jan 27 at 3.45 with ACME"
    assert client.get("/api/v1/orders").json() == []  # parsing never submits anything


def test_parse_nlp_request_uses_structured_output_and_reference_data(client, monkeypatch, seeded_party_db):
    from hgraph_api.services.nlp_order_parser import DEFAULT_MODEL, OUTPUT_SCHEMA

    fake = _use_fake_anthropic(monkeypatch, reply=_PARSED_ORDER)
    monkeypatch.delenv("ORDER_PARSER_MODEL", raising=False)

    client.post("/api/v1/orders/parse-nlp", json={"text": "sell 10 lots", "context": "counterparty is BIGCO"})

    (request,) = fake.requests
    assert request["model"] == DEFAULT_MODEL
    assert request["output_config"]["format"] == {"type": "json_schema", "schema": OUTPUT_SCHEMA}
    assert "Henry Hub Natural Gas" in request["system"]  # products the desk can book
    user_message = request["messages"][0]["content"]
    assert "counterparty is BIGCO" in user_message
    assert "sell 10 lots" in user_message
    assert "Known counterparties: " in user_message and "ACME" in user_message
    assert "Today's date: " in user_message


def test_parse_nlp_model_can_be_overridden(client, monkeypatch):
    fake = _use_fake_anthropic(monkeypatch, reply=_PARSED_ORDER)
    monkeypatch.setenv("ORDER_PARSER_MODEL", "claude-haiku-5-5")

    client.post("/api/v1/orders/parse-nlp", json={"text": "Buy gas"})
    assert fake.requests[0]["model"] == "claude-haiku-5-5"


def test_parse_nlp_warns_about_what_would_not_submit_or_book(client, monkeypatch):
    reply = {
        "instrument": _field("Unobtainium"),
        "side": _field("Buy"),
        "quantity": _field("lots"),
        "delivery_period": _field("next winter"),
        "order_type": _field("Limit"),
        "notes": "Quantity was not given as a number",
    }
    _use_fake_anthropic(monkeypatch, reply=reply)

    data = client.post("/api/v1/orders/parse-nlp", json={"text": "buy lots of unobtainium next winter"}).json()

    warnings = " | ".join(data["warnings"])
    assert "Unobtainium' is not in the product reference data" in warnings
    assert "Quantity must be a number" in warnings
    assert "Delivery period 'next winter' is not understood" in warnings
    assert "No counterparty was found" in warnings
    assert "A Limit order needs a price" in warnings
    assert data["notes"] == "Quantity was not given as a number"


@pytest.mark.parametrize(
    "fake_kwargs,expected",
    [
        ({"stop_reason": "refusal"}, "declined"),
        ({"stop_reason": "max_tokens"}, "cut off"),
        ({"error": ConnectionError("no route to host")}, "unavailable"),
    ],
)
def test_parse_nlp_reports_model_failures_as_unavailable(client, monkeypatch, fake_kwargs, expected):
    _use_fake_anthropic(monkeypatch, **fake_kwargs)

    r = client.post("/api/v1/orders/parse-nlp", json={"text": "Buy gas"})
    assert r.status_code == 503
    assert expected in r.json()["detail"]


def test_parse_nlp_no_api_key(client, monkeypatch):
    """Test graceful error when no Anthropic credentials are configured."""
    monkeypatch.setattr(
        "hgraph_api.services.nlp_order_parser._get_client",
        lambda: (_ for _ in ()).throw(RuntimeError("No Anthropic credentials are configured")),
    )

    r = client.post(
        "/api/v1/orders/parse-nlp",
        json={"text": "Buy gas"},
    )
    assert r.status_code == 503


# ---------------------------------------------------------------------------
# Order proposals: drafted by an agent, confirmed by a person
# ---------------------------------------------------------------------------

_AGENT = {"X-User-Id": "ai-agent"}
_TRADER = {"X-User-Id": "trader1"}


def _propose(client, **overrides):
    payload = _create_order_payload(
        counterparty="ACME",
        source_text="pls buy 50k HH Jan at 3.45 w/ ACME",
        rationale="Read HH as Henry Hub; Jan as January 2026",
        **overrides,
    )
    return client.post("/api/v1/proposals", json=payload, headers=_AGENT)


def test_proposal_does_not_touch_the_order_book(client):
    r = _propose(client)

    assert r.status_code == 201
    proposal = r.json()
    assert proposal["proposal_id"].startswith("PRP-")
    assert (proposal["status"], proposal["proposed_by"], proposal["order_id"]) == ("Proposed", "ai-agent", "")
    assert proposal["order"]["instrument"] == "Henry Hub Natural Gas"
    assert proposal["source_text"] == "pls buy 50k HH Jan at 3.45 w/ ACME"
    assert client.get("/api/v1/orders").json() == []
    assert client.get(f"/api/v1/proposals/{proposal['proposal_id']}").json() == proposal


def test_malformed_proposal_is_refused(client):
    r = _propose(client, quantity="lots")
    assert r.status_code == 400
    assert client.get("/api/v1/proposals").json() == []


def test_confirming_a_proposal_submits_the_order_as_the_confirming_user(client):
    proposal_id = _propose(client).json()["proposal_id"]

    r = client.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_TRADER)

    assert r.status_code == 201
    order = r.json()
    assert (order["status"], order["created_by"], order["counterparty"]) == ("Pending", "trader1", "ACME")
    proposal = client.get(f"/api/v1/proposals/{proposal_id}").json()
    assert (proposal["status"], proposal["decided_by"], proposal["order_id"]) == (
        "Confirmed",
        "trader1",
        order["order_id"],
    )
    assert [o["order_id"] for o in client.get("/api/v1/orders").json()] == [order["order_id"]]


def test_author_cannot_confirm_their_own_proposal(client):
    proposal_id = _propose(client).json()["proposal_id"]

    r = client.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_AGENT)

    assert r.status_code == 403
    assert "someone other than its author" in r.json()["detail"]
    assert client.get(f"/api/v1/proposals/{proposal_id}").json()["status"] == "Proposed"
    assert client.get("/api/v1/orders").json() == []


def test_proposal_can_only_be_decided_once(client):
    proposal_id = _propose(client).json()["proposal_id"]
    assert client.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_TRADER).status_code == 201

    assert client.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_TRADER).status_code == 409
    assert client.post(f"/api/v1/proposals/{proposal_id}/dismiss", headers=_TRADER).status_code == 409
    assert len(client.get("/api/v1/orders").json()) == 1
    assert client.post("/api/v1/proposals/PRP-NONE/confirm", headers=_TRADER).status_code == 404


def test_dismissed_proposal_submits_nothing(client):
    proposal_id = _propose(client).json()["proposal_id"]

    r = client.post(f"/api/v1/proposals/{proposal_id}/dismiss", json={"reason": "client withdrew"}, headers=_TRADER)

    assert r.status_code == 200
    assert (r.json()["status"], r.json()["decided_by"], r.json()["reason"]) == (
        "Dismissed",
        "trader1",
        "client withdrew",
    )
    assert client.get("/api/v1/orders").json() == []
    assert [p["proposal_id"] for p in client.get("/api/v1/proposals", params={"status": "Dismissed"}).json()] == [
        proposal_id
    ]
    assert client.get("/api/v1/proposals", params={"status": "Proposed"}).json() == []


def test_confirmed_proposal_still_goes_through_the_credit_check(client, seeded_credit_db):
    """ACME has 35M of headroom; a 40M proposal is confirmed by a person but refused by the engine."""
    proposal_id = _propose(client, quantity="20000000", price="2.0").json()["proposal_id"]

    r = client.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_TRADER)

    assert r.status_code == 422
    proposal = client.get(f"/api/v1/proposals/{proposal_id}").json()
    assert proposal["status"] == "Confirmed"
    rejected = client.get(f"/api/v1/orders/{proposal['order_id']}").json()
    assert rejected["status"] == "Rejected"
    assert "would breach credit limit" in rejected["reason"]


def test_proposal_stays_open_if_the_order_could_not_be_submitted_at_all(client, monkeypatch):
    proposal_id = _propose(client).json()["proposal_id"]

    def _boom(db_path, counterparty_symbol):
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr("hgraph_static_admin.credit_store.get_credit_utilization", _boom)
    assert client.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_TRADER).status_code == 503
    assert client.get(f"/api/v1/proposals/{proposal_id}").json()["status"] == "Proposed"

    monkeypatch.undo()
    assert client.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_TRADER).status_code == 201


def test_deciding_a_proposal_needs_trading_permission_but_drafting_does_not(db_dir, monkeypatch):
    app = create_app()
    app.state.db_dir = db_dir
    app.state.enforce_entitlements = True
    monkeypatch.setattr("hgraph_entitlements.checker.check_permission", lambda user_id, action, conn=None: False)

    with TestClient(app) as c:
        drafted = _propose(c)
        assert drafted.status_code == 201
        proposal_id = drafted.json()["proposal_id"]
        assert c.post(f"/api/v1/proposals/{proposal_id}/confirm", headers=_TRADER).status_code == 403
        assert c.post(f"/api/v1/proposals/{proposal_id}/dismiss", headers=_TRADER).status_code == 403


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
    assert data["created_by"] == "test-user"


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
    client.patch(f"/api/v1/orders/{order_id}", json={"status": "Working"})

    r = client.patch(
        f"/api/v1/orders/{order_id}",
        json={"status": "Partially Filled", "filled_quantity": "25000"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "Partially Filled"
    assert r.json()["filled_quantity"] == "25000"
    assert r.json()["average_price"] == "3.45"  # defaults to the limit price


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

    # Accept, then fill in full
    client.patch(f"/api/v1/orders/{order_id}", json={"status": "Working"})
    assert client.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"}).json()["status"] == "Filled"

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


def test_create_order_exposure_breaches_limit(client, seeded_credit_db):
    """An order whose quantity x price exceeds remaining credit should be rejected."""
    # ACME: 15M utilized of 50M limit -> 35M available; 20M @ 2.0 = 40M exposure
    r = client.post(
        "/api/v1/orders",
        json=_create_order_payload(counterparty="ACME", quantity="20000000", price="2.0"),
    )
    assert r.status_code == 422
    assert "would breach credit limit" in r.json()["detail"].lower()


def test_create_order_exposure_within_limit(client, seeded_credit_db):
    """An order whose exposure fits within remaining credit should succeed."""
    # ACME: 35M available; 1M @ 3.0 = 3M exposure
    r = client.post(
        "/api/v1/orders",
        json=_create_order_payload(counterparty="ACME", quantity="1000000", price="3.0"),
    )
    assert r.status_code == 201


def test_create_order_credit_store_unavailable(client, monkeypatch):
    """If the credit store cannot be read, orders with a counterparty are rejected (fail closed)."""

    def _boom(db_path, counterparty_symbol):
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr("hgraph_static_admin.credit_store.get_credit_utilization", _boom)
    r = client.post(
        "/api/v1/orders",
        json=_create_order_payload(counterparty="ACME"),
    )
    assert r.status_code == 503
    assert "credit check unavailable" in r.json()["detail"].lower()


def _working_order(client, **overrides) -> str:
    order_id = client.post("/api/v1/orders", json=_create_order_payload(**overrides)).json()["order_id"]
    assert client.patch(f"/api/v1/orders/{order_id}", json={"status": "Working"}).status_code == 200
    return order_id


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"quantity": "lots"}, "quantity must be a number"),
        ({"quantity": "-5"}, "quantity must be greater than zero"),
        ({"price": ""}, "limit order needs a price"),
        ({"order_type": "Market"}, "market order must not have a price"),
        ({"order_type": "Iceberg"}, "order type must be"),
    ],
)
def test_create_order_malformed_is_refused_and_not_recorded(client, overrides, expected):
    r = client.post("/api/v1/orders", json=_create_order_payload(**overrides))
    assert r.status_code == 400
    assert expected in r.json()["detail"].lower()
    assert client.get("/api/v1/orders").json() == []


def test_create_market_order(client):
    r = client.post("/api/v1/orders", json=_create_order_payload(order_type="Market", price=""))
    assert r.status_code == 201
    assert (r.json()["order_type"], r.json()["price"]) == ("Market", "")


def test_quantity_accepts_thousands_separators(client):
    r = client.post("/api/v1/orders", json=_create_order_payload(quantity="50,000"))
    assert r.json()["quantity"] == "50000"


@pytest.mark.parametrize(
    "setup_statuses,target,expected_code",
    [
        ([], "Partially Filled", 409),  # must be accepted before it can fill
        ([], "Filled", 409),
        ([], "Pending", 409),
        (["Working"], "Working", 409),
        (["Working"], "Rejected", 409),  # only a Pending order can be rejected
        (["Working", "Filled"], "Working", 409),  # a filled order cannot be reopened
        (["Cancelled"], "Working", 409),
        ([], "Rejected", 200),
        (["Working"], "Cancelled", 200),
    ],
)
def test_update_order_only_allows_lifecycle_transitions(client, setup_statuses, target, expected_code):
    order_id = client.post("/api/v1/orders", json=_create_order_payload()).json()["order_id"]
    for status in setup_statuses:
        assert client.patch(f"/api/v1/orders/{order_id}", json={"status": status}).status_code == 200

    body = {"status": target, "filled_quantity": "100"} if target == "Partially Filled" else {"status": target}
    r = client.patch(f"/api/v1/orders/{order_id}", json=body)
    assert r.status_code == expected_code


def test_successive_fills_accumulate(client):
    order_id = _working_order(client)

    client.patch(f"/api/v1/orders/{order_id}", json={"status": "Partially Filled", "filled_quantity": "20000"})
    r = client.patch(
        f"/api/v1/orders/{order_id}",
        json={"status": "Filled", "filled_quantity": "50000", "fill_price": "3.55"},
    )

    assert r.status_code == 200
    assert (r.json()["status"], r.json()["filled_quantity"]) == ("Filled", "50000")
    assert float(r.json()["average_price"]) == pytest.approx((20000 * 3.45 + 30000 * 3.55) / 50000)


@pytest.mark.parametrize(
    "body,expected",
    [
        ({"status": "Partially Filled"}, "needs a filled_quantity"),
        ({"status": "Partially Filled", "filled_quantity": "50000"}, "below the order quantity"),
        ({"status": "Filled", "filled_quantity": "100"}, "equal the order quantity"),
        ({"status": "Partially Filled", "filled_quantity": "abc"}, "must be a number"),
        ({"status": "Working", "quantity": "10"}, "separately"),
        ({}, "provide a status"),
    ],
)
def test_update_order_bad_request(client, body, expected):
    order_id = _working_order(client)
    r = client.patch(f"/api/v1/orders/{order_id}", json=body)
    assert r.status_code == 400
    assert expected in r.json()["detail"].lower()


def test_fill_endpoint_records_executions(client):
    order_id = _working_order(client)

    first = client.post(
        f"/api/v1/orders/{order_id}/fills", json={"quantity": "10000", "price": "3.40", "fill_id": "X1"}
    )
    duplicate = client.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "10000", "fill_id": "X1"})
    overfill = client.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "45000"})
    rest = client.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "40000"})

    assert (first.status_code, first.json()["status"]) == (200, "Partially Filled")
    assert duplicate.status_code == 409
    assert overfill.status_code == 422
    assert (rest.json()["status"], rest.json()["filled_quantity"]) == ("Filled", "50000")
    assert client.post("/api/v1/orders/ORD-NONEXIST/fills", json={"quantity": "1"}).status_code == 404


def test_fill_on_market_order_needs_price(client):
    order_id = _working_order(client, order_type="Market", price="")

    assert client.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "100"}).status_code == 400
    r = client.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "100", "price": "3.50"})
    assert r.status_code == 200


def test_amend_order(client):
    order_id = _working_order(client)

    r = client.patch(f"/api/v1/orders/{order_id}", json={"quantity": "60000", "price": "3.50"})
    assert r.status_code == 200
    assert (r.json()["quantity"], r.json()["price"], r.json()["status"]) == ("60000", "3.5", "Working")

    client.post(f"/api/v1/orders/{order_id}/cancel")
    assert client.patch(f"/api/v1/orders/{order_id}", json={"price": "3.60"}).status_code == 409


def test_cancel_keeps_partial_fill(client):
    order_id = _working_order(client)
    client.patch(f"/api/v1/orders/{order_id}", json={"status": "Partially Filled", "filled_quantity": "25000"})

    r = client.post(f"/api/v1/orders/{order_id}/cancel")
    assert (r.json()["status"], r.json()["filled_quantity"]) == ("Cancelled", "25000")


def test_credit_rejection_is_recorded_as_rejected_order(client, seeded_credit_db):
    r = client.post(
        "/api/v1/orders",
        json=_create_order_payload(counterparty="ACME", quantity="20000000", price="2.0"),
    )
    assert r.status_code == 422

    orders = client.get("/api/v1/orders").json()
    assert [(o["status"], o["counterparty"]) for o in orders] == [("Rejected", "ACME")]
    assert "would breach credit limit" in orders[0]["reason"]


def test_open_orders_reserve_credit(client, seeded_credit_db):
    """ACME has 35M of headroom: two 20M orders cannot both be accepted, until one is cancelled."""
    payload = _create_order_payload(counterparty="ACME", quantity="10000000", price="2.0")

    first = client.post("/api/v1/orders", json=payload)
    assert first.status_code == 201
    assert client.post("/api/v1/orders", json=payload).status_code == 422

    client.post(f"/api/v1/orders/{first.json()['order_id']}/cancel")
    assert client.post("/api/v1/orders", json=payload).status_code == 201


def test_amend_is_credit_checked(client, seeded_credit_db):
    payload = _create_order_payload(counterparty="ACME", quantity="10000000", price="2.0")
    order_id = client.post("/api/v1/orders", json=payload).json()["order_id"]

    assert client.patch(f"/api/v1/orders/{order_id}", json={"quantity": "20000000"}).status_code == 422
    assert client.patch(f"/api/v1/orders/{order_id}", json={"quantity": "15000000"}).status_code == 200


def test_orders_survive_api_restart(db_dir):
    def _client():
        app = create_app()
        app.state.db_dir = db_dir
        return TestClient(app, headers={"X-User-Id": "test-user"})

    with _client() as first:
        order_id = _working_order(first)
        before = first.get(f"/api/v1/orders/{order_id}").json()

    with _client() as second:
        assert second.get(f"/api/v1/orders/{order_id}").json() == before
        assert second.post(f"/api/v1/orders/{order_id}/cancel").json()["status"] == "Cancelled"


def test_list_orders_most_recent_first(client):
    ids = [client.post("/api/v1/orders", json=_create_order_payload()).json()["order_id"] for _ in range(3)]
    assert [o["order_id"] for o in client.get("/api/v1/orders").json()] == ids[::-1]


# ---------------------------------------------------------------------------
# Trades booked from fills
# ---------------------------------------------------------------------------


def test_fill_books_a_trade_through_the_pipeline(client, db_dir):
    import os

    order_id = _working_order(client, counterparty="ACME")
    assert client.get(f"/api/v1/orders/{order_id}/trades").json() == []

    client.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "20000", "price": "3.40"})
    client.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"})

    trades = client.get(f"/api/v1/orders/{order_id}/trades").json()
    assert [(t["trade_id"], t["status"], t["quantity"], t["price"]) for t in trades] == [
        (f"{order_id}-F1", "Booked", "20000", "3.4"),
        (f"{order_id}-F2", "Booked", "30000", "3.45"),
    ]
    assert trades[0]["detail"] == os.path.join(db_dir, "booked_trades", f"{order_id}-F1.json")
    with open(trades[0]["detail"]) as fh:
        message = json.load(fh)
    assert message["tradeHeader"]["tradeHeader"]["parties"][1] == {"externalParty": "ACME"}
    assert message["tradeEconomics"]["commoditySwap"]["fixedLeg"]["notionalQuantity"]["quantity"] == 20000.0

    assert client.get("/api/v1/trades").json() == trades
    assert client.get("/api/v1/trades", params={"status": "Failed"}).json() == []
    assert client.get("/api/v1/orders/ORD-NONEXIST/trades").status_code == 404


def test_fill_that_cannot_be_booked_is_still_filled_and_reported_as_failed(client):
    """The execution happened, so the order fills; the booking failure is visible, not swallowed."""
    order_id = _working_order(client, instrument="Unobtainium", counterparty="ACME")

    r = client.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"})
    assert r.json()["status"] == "Filled"

    (trade,) = client.get("/api/v1/trades", params={"status": "Failed"}).json()
    assert trade["order_id"] == order_id
    assert "not in the product reference data" in trade["detail"]


def test_fill_id_must_be_safe_for_a_trade_id(client):
    order_id = _working_order(client)
    r = client.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "1", "fill_id": "../escape"})
    assert r.status_code == 400
    assert "fill_id" in r.json()["detail"]


def test_booked_trades_are_published_when_a_topic_is_configured(db_dir, monkeypatch):
    import threading

    monkeypatch.setenv("TRADE_KAFKA_TOPIC", "trades.booked")
    published, arrived = [], threading.Event()

    def _send(topic, trade_id, message):
        published.append((topic, trade_id, message["tradeHeader"]["tradeHeader"]["parties"][1]))
        arrived.set()

    app = create_app()
    app.state.db_dir = db_dir
    app.state.trade_sender = _send

    with TestClient(app, headers={"X-User-Id": "test-user"}) as c:
        order_id = _working_order(c, counterparty="ACME")
        c.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"})
        assert arrived.wait(timeout=5)

    assert published == [("trades.booked", f"{order_id}-F1", {"externalParty": "ACME"})]

    with TestClient(app, headers={"X-User-Id": "test-user"}) as c:  # restart: nothing is sent twice
        (trade,) = c.get("/api/v1/trades").json()
        assert c.get("/api/v1/trades", params={"undelivered": True}).json() == []
    assert (trade["delivery_status"], trade["delivery_detail"]) == ("Sent", "topic trades.booked")
    assert len(published) == 1


def test_undelivered_trades_show_as_pending_with_the_error(db_dir, monkeypatch):
    monkeypatch.setenv("TRADE_KAFKA_TOPIC", "trades.booked")
    attempted = __import__("threading").Event()

    def _send(topic, trade_id, message):
        attempted.set()
        raise ConnectionError("broker unavailable")

    app = create_app()
    app.state.db_dir = db_dir
    app.state.trade_sender = _send

    with TestClient(app, headers={"X-User-Id": "test-user"}) as c:
        order_id = _working_order(c, counterparty="ACME")
        filled = c.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"})
        assert filled.json()["status"] == "Filled"  # a broker outage does not hold up the order
        assert attempted.wait(timeout=5)
        (trade,) = c.get("/api/v1/trades", params={"undelivered": True}).json()

    assert (trade["status"], trade["delivery_status"]) == ("Booked", "Pending")


def test_delivery_status_is_blank_when_no_topic_is_configured(client):
    order_id = _working_order(client, counterparty="ACME")
    client.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"})

    (trade,) = client.get("/api/v1/trades").json()
    assert (trade["status"], trade["delivery_status"]) == ("Booked", "")


# ---------------------------------------------------------------------------
# Auth middleware fail-closed behaviour
# ---------------------------------------------------------------------------


def test_auth_denies_without_permission(db_dir, monkeypatch):
    """With enforcement on, a user lacking the action permission gets 403."""
    app = create_app()
    app.state.db_dir = db_dir
    app.state.enforce_entitlements = True
    monkeypatch.setattr("hgraph_entitlements.checker.check_permission", lambda user_id, action, conn=None: False)

    c = TestClient(app, headers={"X-User-Id": "test-user"})
    r = c.post("/api/v1/orders", json=_create_order_payload())
    assert r.status_code == 403
    assert "lacks permission" in r.json()["detail"]


def test_engine_enforces_entitlements_behind_the_middleware(db_dir, monkeypatch):
    """With enforcement on, the order engine checks the user's role itself, not only the middleware."""
    app = create_app()
    app.state.db_dir = db_dir
    app.state.enforce_entitlements = True
    # Middleware waves everyone through; the roles the engine looks up say otherwise
    monkeypatch.setattr("hgraph_entitlements.checker.check_permission", lambda user_id, action, conn=None: True)
    roles = {"trader1": "Trader"}
    monkeypatch.setattr("hgraph_entitlements.example_code.get_user_role", lambda conn, user_id: roles.get(user_id))

    with TestClient(app) as c:
        allowed = c.post("/api/v1/orders", json=_create_order_payload(), headers={"X-User-Id": "trader1"})
        denied = c.post("/api/v1/orders", json=_create_order_payload(), headers={"X-User-Id": "stranger"})

    assert allowed.status_code == 201
    assert denied.status_code == 403
    assert "lacks permission" in denied.json()["detail"]


def test_auth_fails_closed_on_checker_error(db_dir, monkeypatch):
    """With enforcement on, an erroring entitlements checker denies the request (503)."""
    app = create_app()
    app.state.db_dir = db_dir
    app.state.enforce_entitlements = True

    def _boom(user_id, action, conn=None):
        raise RuntimeError("entitlements db unavailable")

    monkeypatch.setattr("hgraph_entitlements.checker.check_permission", _boom)

    c = TestClient(app, headers={"X-User-Id": "test-user"})
    r = c.post("/api/v1/orders", json=_create_order_payload())
    assert r.status_code == 503
    assert "request denied" in r.json()["detail"].lower()


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


def test_websocket_orders_pushes_changes(client):
    """A change made through the REST API is pushed to connected clients."""
    with client.websocket_connect("/ws/orders") as ws:
        assert json.loads(ws.receive_text())["data"] == []

        order_id = client.post("/api/v1/orders", json=_create_order_payload()).json()["order_id"]
        created = json.loads(ws.receive_text())
        assert [(o["order_id"], o["status"]) for o in created["data"]] == [(order_id, "Pending")]

        client.post(f"/api/v1/orders/{order_id}/cancel")
        assert json.loads(ws.receive_text())["data"][0]["status"] == "Cancelled"
        ws.send_text("close")
