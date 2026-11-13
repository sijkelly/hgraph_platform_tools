"""The API with the order core in a single database (SQLite and, when configured, PostgreSQL)."""

import pytest
from fastapi.testclient import TestClient

from hgraph_api.app import create_app

_TRADER = {"X-User-Id": "trader1"}
_AGENT = {"X-User-Id": "ai-agent"}
_ORDER = {
    "instrument": "Henry Hub Natural Gas",
    "side": "Buy",
    "quantity": "10000",
    "unit": "MMBtu",
    "price": "3.5",
    "counterparty": "ACME",
    "delivery_period": "January 2027",
    "order_type": "Limit",
}


def _client(tmp_path, oms_db) -> TestClient:
    app = create_app()
    app.state.db_dir = str(tmp_path)
    app.state.database_url = oms_db
    return TestClient(app)


def test_orders_trades_and_proposals_survive_a_restart(tmp_path, oms_db):
    with _client(tmp_path, oms_db) as api:
        order_id = api.post("/api/v1/orders", json=_ORDER, headers=_TRADER).json()["order_id"]
        assert api.patch(f"/api/v1/orders/{order_id}", json={"status": "Working"}, headers=_TRADER).status_code == 200
        fill = api.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "4000", "price": "3.45"}, headers=_TRADER)
        assert fill.status_code == 200

        proposal = api.post("/api/v1/proposals", json={**_ORDER, "rationale": "client asked"}, headers=_AGENT).json()
        confirmed = api.post(f"/api/v1/proposals/{proposal['proposal_id']}/confirm", headers=_TRADER)
        assert confirmed.status_code == 201
        proposed_order_id = confirmed.json()["order_id"]

    # A new process on the same database sees everything the first one wrote
    with _client(tmp_path, oms_db) as api:
        order = api.get(f"/api/v1/orders/{order_id}").json()
        assert (order["status"], order["filled_quantity"], order["average_price"]) == (
            "Partially Filled",
            "4000",
            "3.45",
        )
        assert api.get(f"/api/v1/orders/{proposed_order_id}").json()["created_by"] == "trader1"

        (trade,) = api.get(f"/api/v1/orders/{order_id}/trades").json()
        assert (trade["status"], trade["quantity"], trade["price"]) == ("Booked", "4000", "3.45")

        (stored,) = api.get("/api/v1/proposals").json()
        assert (stored["status"], stored["decided_by"], stored["order_id"]) == (
            "Confirmed",
            "trader1",
            proposed_order_id,
        )

        # ...and carries on from there
        fill = api.post(f"/api/v1/orders/{order_id}/fills", json={"quantity": "6000", "price": "3.55"}, headers=_TRADER)
        assert fill.json()["status"] == "Filled"
        assert len(api.get(f"/api/v1/orders/{order_id}/trades").json()) == 2


def test_nothing_is_written_to_sqlite_files_when_a_database_url_is_set(tmp_path, oms_db):
    with _client(tmp_path, oms_db) as api:
        assert api.post("/api/v1/orders", json=_ORDER, headers=_TRADER).status_code == 201
        api.post("/api/v1/proposals", json={**_ORDER, "rationale": "client asked"}, headers=_AGENT)

    for name in ("order_events.db", "trade_bookings.db", "order_proposals.db"):
        assert not (tmp_path / name).exists()


def test_reference_data_and_credit_check_use_the_same_database(tmp_path, oms_db):
    from datetime import date

    from hg_oap.credit.credit_limit import CreditLimit, CreditLimitType, CreditStatus, CreditUtilization
    from hgraph_static_admin.credit_store import init_credit_db, upsert_credit_limit, upsert_credit_utilization

    # Written the way the credit Kafka subscriber writes it
    init_credit_db(oms_db)
    upsert_credit_limit(
        oms_db,
        CreditLimit(
            counterparty_symbol="ACME",
            limit_type=CreditLimitType.BILATERAL,
            limit_amount=50_000.0,
            limit_currency="USD",
            effective_date=date(2026, 1, 1),
            expiry_date=date(2028, 1, 1),
            status=CreditStatus.ACTIVE,
            approved_by="Credit Committee",
            last_review_date=date(2026, 6, 15),
        ),
    )
    upsert_credit_utilization(
        oms_db,
        CreditUtilization(
            counterparty_symbol="ACME",
            utilized_amount=30_000.0,
            utilization_currency="USD",
            limit_amount=50_000.0,
            available_amount=20_000.0,
            utilization_percentage=60.0,
        ),
    )

    with _client(tmp_path, oms_db) as api:
        (limit,) = api.get("/api/v1/credit/limits").json()
        assert (limit["counterparty_symbol"], limit["limit_amount"]) == ("ACME", 50_000.0)
        assert api.get("/api/v1/credit/utilizations/ACME").json()["utilized_amount"] == 30_000.0

        # 10,000 x 3.5 = 35,000 on top of 30,000 used would pass the 50,000 limit
        refused = api.post("/api/v1/orders", json=_ORDER, headers=_TRADER)
        assert refused.status_code == 422, refused.text
        assert "credit" in refused.json()["detail"].lower()

        accepted = api.post("/api/v1/orders", json={**_ORDER, "quantity": "5000"}, headers=_TRADER)
        assert accepted.status_code == 201

    for name in ("credit_data.db", "party_data.db", "portfolio_data.db"):
        assert not (tmp_path / name).exists()


def test_entitlements_follow_the_configured_database(tmp_path, oms_db, monkeypatch):
    from hgraph_entitlements.checker import check_permission
    from hgraph_entitlements.example_code import get_db_connection, initialize_db, update_user_role

    if "://" in oms_db:
        monkeypatch.setenv("DATABASE_URL", oms_db.split("?options=")[0].split("&options=")[0])
        monkeypatch.setenv("DATABASE_SCHEMA", oms_db.rsplit("%3D", 1)[1])
    else:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("ENTITLEMENTS_DB_PATH", oms_db)

    conn = get_db_connection()
    try:
        initialize_db(conn)
        update_user_role(conn, "trader1", "Trader")
    finally:
        conn.close()

    assert check_permission("trader1", "execute_trade")
    assert not check_permission("someone-else", "execute_trade")
