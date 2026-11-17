"""Tests for token verification, OIDC-mode API authentication and client sign-in."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from hgraph_api.app import create_app
from hgraph_api.client_auth import BearerTokenAuth, ClientAuthError, ClientCredentialsAuth, client_auth_from_env
from hgraph_api.services import tokens
from hgraph_api.services.tokens import OIDC, TokenError, TokenServiceError, TokenVerifier, auth_settings

ISSUER = "https://idp.example.test/"
AUDIENCE = "oms-api"

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def make_token(key=_KEY, *, algorithm="RS256", kid="k1", **overrides) -> str:
    claims = {"iss": ISSUER, "aud": AUDIENCE, "sub": "trader1", "exp": int(time.time()) + 300, **overrides}
    claims = {name: value for name, value in claims.items() if value is not None}
    return jwt.encode(claims, key, algorithm=algorithm, headers={"kid": kid})


@pytest.fixture()
def verifier():
    return TokenVerifier(issuer=ISSUER, audience=AUDIENCE, key_resolver=lambda _token: _KEY.public_key())


# ---------------------------------------------------------------------------
# TokenVerifier
# ---------------------------------------------------------------------------


def test_valid_token_gives_user_id(verifier):
    assert verifier.verify(make_token()) == "trader1"


def test_user_claim_is_configurable():
    by_email = TokenVerifier(
        issuer=ISSUER, audience=AUDIENCE, user_claim="email", key_resolver=lambda _token: _KEY.public_key()
    )
    assert by_email.verify(make_token(email="trader1@example.test")) == "trader1@example.test"


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"exp": int(time.time()) - 3600}, "expired"),
        ({"aud": "another-api"}, "not issued for this API"),
        ({"iss": "https://evil.example.test/"}, "issuer this API does not trust"),
        ({"exp": None}, "could not be verified"),
        ({"sub": None}, "no 'sub' claim"),
        ({"sub": "  "}, "no 'sub' claim"),
    ],
)
def test_unacceptable_claims_are_rejected(verifier, overrides, expected):
    with pytest.raises(TokenError, match=expected):
        verifier.verify(make_token(**overrides))


def test_token_signed_by_another_key_is_rejected(verifier):
    with pytest.raises(TokenError, match="could not be verified"):
        verifier.verify(make_token(_OTHER_KEY))


@pytest.mark.parametrize("token", ["", "not-a-token", "a.b.c"])
def test_malformed_token_is_rejected(verifier, token):
    with pytest.raises(TokenError):
        verifier.verify(token)


def test_shared_secret_token_is_rejected(verifier):
    # A token signed with a symmetric algorithm must never verify, whatever the secret
    forged = jwt.encode(
        {"iss": ISSUER, "aud": AUDIENCE, "sub": "trader1", "exp": int(time.time()) + 300},
        "a-secret-that-is-long-enough-for-hs256-signing",
        algorithm="HS256",
    )
    with pytest.raises(TokenError):
        verifier.verify(forged)


def test_unsigned_token_is_rejected(verifier):
    unsigned = jwt.encode(
        {"iss": ISSUER, "aud": AUDIENCE, "sub": "trader1", "exp": int(time.time()) + 300}, None, algorithm="none"
    )
    with pytest.raises(TokenError):
        verifier.verify(unsigned)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"issuer": "", "audience": AUDIENCE},
        {"issuer": ISSUER, "audience": ""},
        {"issuer": "file:///etc/passwd", "audience": AUDIENCE},
        {"issuer": ISSUER, "audience": AUDIENCE, "jwks_url": "file:///tmp/keys.json"},
    ],
)
def test_verifier_refuses_bad_configuration(kwargs):
    with pytest.raises(ValueError):
        TokenVerifier(**kwargs)


@pytest.fixture()
def identity_provider():
    """A local identity provider serving a discovery document and its signing keys."""
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(_KEY.public_key()))
    jwk.update({"kid": "k1", "use": "sig", "alg": "RS256"})

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            base = f"http://127.0.0.1:{self.server.server_port}"
            documents = {
                "/.well-known/openid-configuration": {"issuer": base, "jwks_uri": f"{base}/keys"},
                "/keys": {"keys": [jwk]},
            }
            body = json.dumps(documents.get(self.path, {})).encode()
            self.send_response(200 if self.path in documents else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join()


def test_keys_are_found_through_discovery(identity_provider):
    verifier = TokenVerifier(issuer=identity_provider, audience=AUDIENCE)
    assert verifier.verify(make_token(iss=identity_provider)) == "trader1"
    with pytest.raises(TokenError):
        verifier.verify(make_token(_OTHER_KEY, iss=identity_provider))


def test_unreachable_provider_is_a_service_error():
    verifier = TokenVerifier(issuer="http://127.0.0.1:9/", audience=AUDIENCE)
    with pytest.raises(TokenServiceError):
        verifier.verify(make_token(iss="http://127.0.0.1:9/"))


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@pytest.fixture()
def no_auth_settings(monkeypatch):
    for name in ("AUTH_MODE", "OIDC_ISSUER", "OIDC_AUDIENCE", "OIDC_USER_CLAIM", "OIDC_JWKS_URL"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.setitem(tokens.api_config, name, "")


def test_default_mode_is_header(no_auth_settings):
    assert auth_settings() == ("header", None)


def test_oidc_mode_builds_a_verifier(no_auth_settings, monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "OIDC")
    monkeypatch.setenv("OIDC_ISSUER", ISSUER)
    monkeypatch.setenv("OIDC_AUDIENCE", AUDIENCE)
    monkeypatch.setenv("OIDC_USER_CLAIM", "email")
    mode, verifier = auth_settings()
    assert mode == OIDC
    assert (verifier.issuer, verifier.audience, verifier.user_claim) == (ISSUER, AUDIENCE, "email")


@pytest.mark.parametrize(
    "env",
    [
        {"AUTH_MODE": "oidc"},
        {"AUTH_MODE": "oidc", "OIDC_ISSUER": ISSUER},
        {"AUTH_MODE": "oidc", "OIDC_AUDIENCE": AUDIENCE},
        {"AUTH_MODE": "none"},
    ],
)
def test_misconfigured_auth_stops_the_app_starting(no_auth_settings, monkeypatch, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(RuntimeError):
        create_app()


# ---------------------------------------------------------------------------
# The API in oidc mode
# ---------------------------------------------------------------------------

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


def bearer(**claims) -> dict[str, str]:
    return {"Authorization": f"Bearer {make_token(**claims)}"}


@pytest.fixture()
def api(tmp_path, verifier, no_auth_settings):
    app = create_app()
    app.state.db_dir = str(tmp_path)
    app.state.auth_mode = OIDC
    app.state.token_verifier = verifier
    with TestClient(app) as client:
        yield client


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-User-Id": "trader1"},
        {"Authorization": "Basic dHJhZGVyMTpwdw=="},
        {"Authorization": "Bearer "},
        {"Authorization": "Bearer not-a-token"},
    ],
)
@pytest.mark.parametrize("method,path", [("GET", "/api/v1/orders"), ("POST", "/api/v1/orders"), ("GET", "/api/v1/me")])
def test_requests_without_a_valid_token_are_refused(api, headers, method, path):
    response = api.request(method, path, headers=headers, json=_ORDER if method == "POST" else None)
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_expired_token_is_refused_with_the_reason(api):
    response = api.get("/api/v1/orders", headers=bearer(exp=int(time.time()) - 3600))
    assert response.status_code == 401
    assert "expired" in response.json()["detail"]


def test_health_needs_no_token(api):
    assert api.get("/api/v1/health").status_code == 200


def test_order_is_created_by_the_token_user_not_the_header(api):
    response = api.post("/api/v1/orders", json=_ORDER, headers={**bearer(sub="trader1"), "X-User-Id": "someone-else"})
    assert response.status_code == 201
    assert response.json()["created_by"] == "trader1"


def test_me_reports_the_token_user(api):
    body = api.get("/api/v1/me", headers=bearer(sub="trader1")).json()
    assert body["user_id"] == "trader1"
    assert body["auth_mode"] == OIDC


def test_agent_cannot_confirm_its_own_proposal_by_changing_a_header(api):
    proposal = api.post(
        "/api/v1/proposals", json={**_ORDER, "rationale": "client asked"}, headers=bearer(sub="ai-agent")
    ).json()
    confirm = f"/api/v1/proposals/{proposal['proposal_id']}/confirm"

    spoofed = api.post(confirm, headers={**bearer(sub="ai-agent"), "X-User-Id": "trader1"})
    assert spoofed.status_code == 403

    confirmed = api.post(confirm, headers=bearer(sub="trader1"))
    assert confirmed.status_code == 201
    assert confirmed.json()["created_by"] == "trader1"


def test_unavailable_keys_deny_the_request(api):
    def unavailable(_token):
        raise jwt.PyJWKClientConnectionError("identity provider is down")

    api.app.state.token_verifier = TokenVerifier(issuer=ISSUER, audience=AUDIENCE, key_resolver=unavailable)
    assert api.get("/api/v1/orders", headers=bearer()).status_code == 503


def test_missing_verifier_denies_the_request(api):
    api.app.state.token_verifier = None
    assert api.get("/api/v1/orders", headers=bearer()).status_code == 503


def test_cors_preflight_is_not_blocked(api):
    response = api.options(
        "/api/v1/orders",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization",
        },
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"


def test_refusals_carry_cors_headers(api):
    response = api.get("/api/v1/orders", headers={"Origin": "http://localhost:5173"})
    assert response.status_code == 401
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"


@pytest.mark.parametrize("path", ["/ws/orders", "/ws/credit"])
def test_websocket_without_a_token_is_refused(api, path):
    with pytest.raises(WebSocketDisconnect):
        with api.websocket_connect(path):
            pass


def test_websocket_accepts_a_token_in_the_header(api):
    with api.websocket_connect("/ws/orders", headers=bearer()) as ws:
        assert ws.receive_json()["type"] == "orders.snapshot"
        ws.send_text("close")


def test_websocket_accepts_a_token_as_subprotocol(api):
    with api.websocket_connect("/ws/orders", subprotocols=["bearer", make_token()]) as ws:
        assert ws.accepted_subprotocol == "bearer"
        assert ws.receive_json()["type"] == "orders.snapshot"
        ws.send_text("close")


def test_websocket_refuses_a_bad_subprotocol_token(api):
    with pytest.raises(WebSocketDisconnect):
        with api.websocket_connect("/ws/orders", subprotocols=["bearer", make_token(_OTHER_KEY)]):
            pass


# ---------------------------------------------------------------------------
# Client sign-in
# ---------------------------------------------------------------------------


class _Provider:
    """A token endpoint and an API behind one mock transport."""

    def __init__(self, *, expires_in=3600, token_status=200):
        self.issued = 0
        self.expires_in = expires_in
        self.token_status = token_status
        self.revoked: set[str] = set()
        self.token_requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            self.token_requests.append(request)
            if self.token_status != 200:
                return httpx.Response(self.token_status, json={"error": "invalid_client"})
            self.issued += 1
            return httpx.Response(200, json={"access_token": f"token-{self.issued}", "expires_in": self.expires_in})
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if not token or token in self.revoked:
            return httpx.Response(401, json={"detail": "Invalid token"})
        return httpx.Response(200, json={"token": token})


def _client(provider, **kwargs) -> httpx.Client:
    auth = ClientCredentialsAuth("https://idp.example.test/token", "agent", "s3cret", **kwargs)
    return httpx.Client(base_url="https://oms.example.test", auth=auth, transport=httpx.MockTransport(provider))


def test_client_credentials_token_is_fetched_once_and_reused():
    provider = _Provider()
    with _client(provider, scope="oms", audience=AUDIENCE) as client:
        assert client.get("/api/v1/orders").json() == {"token": "token-1"}
        assert client.get("/api/v1/orders").json() == {"token": "token-1"}

    assert provider.issued == 1
    request = provider.token_requests[0]
    assert request.headers["Authorization"] == "Basic YWdlbnQ6czNjcmV0"
    assert request.content == b"grant_type=client_credentials&scope=oms&audience=oms-api"


def test_expiring_token_is_renewed():
    provider = _Provider(expires_in=10)  # inside the renewal margin, so already due
    with _client(provider) as client:
        client.get("/api/v1/orders")
        assert client.get("/api/v1/orders").json() == {"token": "token-2"}


def test_rejected_token_is_replaced_once():
    provider = _Provider()
    with _client(provider) as client:
        client.get("/api/v1/orders")
        provider.revoked.add("token-1")
        assert client.get("/api/v1/orders").json() == {"token": "token-2"}


def test_refused_token_request_is_reported():
    with _client(_Provider(token_status=401)) as client:
        with pytest.raises(ClientAuthError, match="HTTP 401"):
            client.get("/api/v1/orders")


def test_mcp_tools_report_a_failed_sign_in():
    from hgraph_api.mcp_server import OmsApiError, OmsApiTools

    with _client(_Provider(token_status=401)) as client:
        with pytest.raises(OmsApiError, match="Could not sign in"):
            OmsApiTools(client).list_orders()


def test_client_auth_from_env_prefers_client_credentials():
    env = {"OMS_TOKEN_URL": "https://idp.example.test/token", "OMS_CLIENT_ID": "agent", "OMS_CLIENT_SECRET": "s3cret"}
    assert isinstance(client_auth_from_env({**env, "OMS_API_TOKEN": "abc"}), ClientCredentialsAuth)
    assert isinstance(client_auth_from_env({"OMS_API_TOKEN": "abc"}), BearerTokenAuth)
    assert client_auth_from_env({}) is None


def test_partial_client_credentials_are_an_error():
    with pytest.raises(ValueError):
        client_auth_from_env({"OMS_TOKEN_URL": "https://idp.example.test/token", "OMS_CLIENT_ID": "agent"})


def test_fixed_bearer_token_is_sent():
    with httpx.Client(
        base_url="https://oms.example.test", auth=BearerTokenAuth("abc"), transport=httpx.MockTransport(_Provider())
    ) as client:
        assert client.get("/api/v1/orders").json() == {"token": "abc"}
