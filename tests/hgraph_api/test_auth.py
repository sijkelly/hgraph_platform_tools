"""Tests for OMS API authentication, entitlements enforcement and bearer tokens."""

import base64
import json
import os

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from hgraph_api.app import create_app
from hgraph_api.middleware import auth as auth_module
from hgraph_api.middleware.auth import AuthSettings
from hgraph_api.middleware.tokens import TokenError, create_token, verify_token
from hgraph_entitlements.example_code import get_db_connection, initialize_db, update_user_role
from hgraph_static_admin.credit_store import init_credit_db

SECRET = "s" * 40
ORDER = {"instrument": "Henry Hub Natural Gas", "side": "Buy", "quantity": "10000", "unit": "MMBtu"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_dir(tmp_path):
    entitlements_db = str(tmp_path / "entitlements.db")
    conn = get_db_connection(entitlements_db)
    try:
        initialize_db(conn)
        update_user_role(conn, "trader1", "Trader")
        update_user_role(conn, "viewer1", "Read_only")
    finally:
        conn.close()
    init_credit_db(str(tmp_path / "credit_data.db"))
    return str(tmp_path)


def _make_client(db_dir, monkeypatch, mode, secret=""):
    monkeypatch.setenv("API_AUTH_MODE", mode)
    monkeypatch.setenv("API_AUTH_SECRET", secret)
    monkeypatch.setenv("ENTITLEMENTS_DB_PATH", os.path.join(db_dir, "entitlements.db"))
    app = create_app()
    app.state.db_dir = db_dir
    return TestClient(app)


@pytest.fixture()
def token_client(db_dir, monkeypatch):
    return _make_client(db_dir, monkeypatch, "token", SECRET)


@pytest.fixture()
def dev_client(db_dir, monkeypatch):
    return _make_client(db_dir, monkeypatch, "dev")


def _bearer(user_id, secret=SECRET, **kwargs):
    return {"Authorization": f"Bearer {create_token(user_id, secret, **kwargs)}"}


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------


def test_token_round_trip():
    assert verify_token(create_token("trader1", SECRET), SECRET) == "trader1"


@pytest.mark.parametrize(
    "token_factory,match",
    [
        (lambda: create_token("trader1", "x" * 40), "signature"),
        (lambda: create_token("trader1", SECRET, ttl_seconds=60, now=1_000), "expired"),
        (lambda: "not-a-token", "Malformed"),
        (lambda: "a.b", "Malformed"),
    ],
)
def test_token_rejected(token_factory, match):
    with pytest.raises(TokenError, match=match):
        verify_token(token_factory(), SECRET)


def test_token_alg_none_rejected():
    def seg(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    forged = f"{seg({'alg': 'none', 'typ': 'JWT'})}.{seg({'sub': 'admin', 'exp': 9_999_999_999})}."
    with pytest.raises(TokenError, match="algorithm"):
        verify_token(forged, SECRET)


def test_token_tampered_payload_rejected():
    header, _, signature = create_token("viewer1", SECRET).split(".")
    payload = base64.urlsafe_b64encode(json.dumps({"sub": "trader1", "exp": 9_999_999_999}).encode())
    with pytest.raises(TokenError, match="signature"):
        verify_token(f"{header}.{payload.rstrip(b'=').decode()}.{signature}", SECRET)


def test_short_secret_rejected():
    with pytest.raises(ValueError, match="at least"):
        create_token("trader1", "short")


# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------


def test_token_mode_without_secret_refuses_to_start(db_dir, monkeypatch):
    with pytest.raises(ValueError, match="API_AUTH_SECRET"):
        _make_client(db_dir, monkeypatch, "token", "")


def test_unknown_auth_mode_refuses_to_start(db_dir, monkeypatch):
    with pytest.raises(ValueError, match="API_AUTH_MODE"):
        _make_client(db_dir, monkeypatch, "none")


def test_wildcard_cors_refuses_to_start(db_dir, monkeypatch):
    monkeypatch.setenv("API_CORS_ORIGINS", "*")
    with pytest.raises(ValueError, match="CORS"):
        _make_client(db_dir, monkeypatch, "dev")


def test_cors_preflight_not_blocked_by_auth(token_client):
    r = token_client.options(
        "/api/v1/orders",
        headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "POST"},
    )
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "http://localhost:5173"


# ---------------------------------------------------------------------------
# Token mode — HTTP
# ---------------------------------------------------------------------------


def test_health_is_public(token_client):
    assert token_client.get("/api/v1/health").status_code == 200


@pytest.mark.parametrize("path", ["/api/v1/orders", "/api/v1/credit/limits", "/api/v1/parties/entities"])
def test_reads_require_authentication(token_client, path):
    r = token_client.get(path)
    assert r.status_code == 401
    assert r.headers["www-authenticate"] == "Bearer"


def test_spoofed_user_header_alone_is_rejected(token_client):
    r = token_client.post("/api/v1/orders", json=ORDER, headers={"X-User-Id": "trader1"})
    assert r.status_code == 401


def test_user_header_must_match_token(token_client):
    headers = {**_bearer("viewer1"), "X-User-Id": "trader1"}
    assert token_client.get("/api/v1/orders", headers=headers).status_code == 401


def test_valid_token_can_read(token_client):
    assert token_client.get("/api/v1/orders", headers=_bearer("viewer1")).status_code == 200


def test_trader_can_create_order_and_is_recorded_as_creator(token_client, db_dir):
    from hgraph_api.services.order_store import get_order

    r = token_client.post("/api/v1/orders", json=ORDER, headers=_bearer("trader1"))
    assert r.status_code == 201
    stored = get_order(os.path.join(db_dir, "order_data.db"), r.json()["order_id"])
    assert stored["created_by"] == "trader1"


def test_read_only_user_cannot_create_order(token_client):
    r = token_client.post("/api/v1/orders", json=ORDER, headers=_bearer("viewer1"))
    assert r.status_code == 403
    assert "execute_trade" in r.json()["detail"]


def test_unknown_user_with_valid_token_is_denied(token_client):
    assert token_client.get("/api/v1/orders", headers=_bearer("ghost")).status_code == 403


def test_expired_token_rejected(token_client):
    headers = _bearer("trader1", ttl_seconds=60, now=1_000)
    assert token_client.get("/api/v1/orders", headers=headers).status_code == 401


def test_unmapped_write_is_denied(token_client):
    r = token_client.post("/api/v1/credit/limits", json={}, headers=_bearer("trader1"))
    assert r.status_code == 403
    assert "No entitlement is mapped" in r.json()["detail"]


def test_prefix_lookalike_path_is_not_public(token_client):
    assert token_client.get("/api/v1/healthz").status_code == 401


def test_entitlements_failure_denies_request(token_client, monkeypatch):
    def boom(*_args, **_kwargs):
        raise RuntimeError("database locked")

    monkeypatch.setattr("hgraph_entitlements.example_code.get_db_connection", boom)
    r = token_client.post("/api/v1/orders", json=ORDER, headers=_bearer("trader1"))
    assert r.status_code == 503


def test_missing_auth_settings_denies_request(token_client):
    token_client.app.state.auth_settings = None
    assert token_client.get("/api/v1/orders", headers=_bearer("trader1")).status_code == 503


# ---------------------------------------------------------------------------
# Dev mode — HTTP
# ---------------------------------------------------------------------------


def test_dev_mode_trusts_user_header(dev_client):
    r = dev_client.post("/api/v1/orders", json=ORDER, headers={"X-User-Id": "trader1"})
    assert r.status_code == 201


def test_dev_mode_still_enforces_entitlements(dev_client):
    r = dev_client.post("/api/v1/orders", json=ORDER, headers={"X-User-Id": "viewer1"})
    assert r.status_code == 403


def test_dev_mode_requires_user_header(dev_client):
    assert dev_client.get("/api/v1/orders").status_code == 401


# ---------------------------------------------------------------------------
# WebSockets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/ws/orders", "/ws/credit"])
def test_websocket_without_credentials_is_rejected(token_client, path):
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with token_client.websocket_connect(path):
            pass
    assert exc_info.value.code == 1008


@pytest.mark.parametrize("path", ["/ws/orders", "/ws/credit"])
def test_websocket_with_token_query_param(token_client, path):
    with token_client.websocket_connect(f"{path}?token={create_token('viewer1', SECRET)}") as ws:
        assert json.loads(ws.receive_text())["type"].split(".")[0] in ("orders", "credit")
        ws.send_text("close")


def test_websocket_rejects_unknown_user(token_client):
    with pytest.raises(WebSocketDisconnect):
        with token_client.websocket_connect(f"/ws/orders?token={create_token('ghost', SECRET)}"):
            pass


def test_websocket_dev_mode_user_id_query_param(dev_client):
    with dev_client.websocket_connect("/ws/orders?user_id=viewer1") as ws:
        assert json.loads(ws.receive_text())["type"] == "orders.snapshot"
        ws.send_text("close")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def test_settings_from_env_defaults_to_token_mode(monkeypatch):
    monkeypatch.delenv("API_AUTH_MODE", raising=False)
    monkeypatch.setenv("API_AUTH_SECRET", SECRET)
    assert AuthSettings.from_env({}).mode == auth_module.AUTH_MODE_TOKEN
