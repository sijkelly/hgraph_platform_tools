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
