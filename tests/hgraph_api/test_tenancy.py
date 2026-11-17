"""Tests for tenancy: the tenant setting, schema-per-tenant and the token's organisation check."""

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from hgraph_api.app import create_app
from hgraph_api.services import tenancy, tokens
from hgraph_api.services.tenancy import database_schema, tenant_id
from hgraph_api.services.tokens import OIDC, TokenError, TokenVerifier
from hgraph_oms.db import Database, with_schema

ISSUER = "https://idp.example.test/"
AUDIENCE = "oms-api"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)

_SETTINGS = (
    "TENANT_ID",
    "DATABASE_SCHEMA",
    "DATABASE_URL",
    "AUTH_MODE",
    "OIDC_ISSUER",
    "OIDC_AUDIENCE",
    "OIDC_USER_CLAIM",
    "OIDC_JWKS_URL",
    "OIDC_TENANT_CLAIM",
)

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


@pytest.fixture(autouse=True)
def clean_settings(monkeypatch):
    for name in _SETTINGS:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.setitem(tenancy.api_config, name, "")


def make_token(**overrides) -> str:
    claims = {"iss": ISSUER, "aud": AUDIENCE, "sub": "trader1", "exp": int(time.time()) + 300, **overrides}
    return jwt.encode(claims, _KEY, algorithm="RS256")


def verifier(**kwargs) -> TokenVerifier:
    return TokenVerifier(issuer=ISSUER, audience=AUDIENCE, key_resolver=lambda _token: _KEY.public_key(), **kwargs)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def test_no_tenant_by_default():
    assert tenant_id() == ""
    assert database_schema() == ""


@pytest.mark.parametrize(
    "tenant,schema",
    [
        ("acme", "tenant_acme"),
        ("Acme-Energy", "tenant_acme_energy"),
        ("acme.uk", "tenant_acme_uk"),
        ("42", "tenant_42"),
    ],
)
def test_schema_follows_from_the_tenant(monkeypatch, tenant, schema):
    monkeypatch.setenv("TENANT_ID", tenant)
    assert tenant_id() == tenant
    assert database_schema() == schema


def test_schema_can_be_set_explicitly(monkeypatch):
    monkeypatch.setenv("TENANT_ID", "acme")
    monkeypatch.setenv("DATABASE_SCHEMA", "oms_acme")
    assert database_schema() == "oms_acme"


@pytest.mark.parametrize("tenant", ["acme energy", "-acme", "acme;drop", "a" * 64, 'acme"'])
def test_bad_tenant_id_stops_the_app_starting(monkeypatch, tenant):
    monkeypatch.setenv("TENANT_ID", tenant)
    with pytest.raises(RuntimeError, match="TENANT_ID"):
        create_app()


def test_tenant_claim_without_a_tenant_stops_the_app_starting(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "oidc")
    monkeypatch.setenv("OIDC_ISSUER", ISSUER)
    monkeypatch.setenv("OIDC_AUDIENCE", AUDIENCE)
    monkeypatch.setenv("OIDC_TENANT_CLAIM", "org_id")
    with pytest.raises(RuntimeError, match="TENANT_ID"):
        create_app()


def test_tenant_claim_setting_reaches_the_verifier(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "oidc")
    monkeypatch.setenv("OIDC_ISSUER", ISSUER)
    monkeypatch.setenv("OIDC_AUDIENCE", AUDIENCE)
    monkeypatch.setenv("OIDC_TENANT_CLAIM", "org_id")
    monkeypatch.setenv("TENANT_ID", "acme")
    _mode, built = tokens.auth_settings()
    assert (built.tenant_claim, built.tenant) == ("org_id", "acme")


# ---------------------------------------------------------------------------
# Schema per tenant
# ---------------------------------------------------------------------------


def test_with_schema_narrows_a_postgres_url():
    url = with_schema("postgresql://oms:pw@db.example.test/oms", "tenant_acme")
    assert url == "postgresql://oms:pw@db.example.test/oms?options=-csearch_path%3Dtenant_acme"
    database = Database(url)
    assert database.schema == "tenant_acme"
    assert database.description == "postgresql://db.example.test/oms (schema tenant_acme)"

    assert with_schema("postgresql://db.example.test/oms?sslmode=require", "t1").endswith(
        "?sslmode=require&options=-csearch_path%3Dt1"
    )


def test_with_schema_leaves_other_targets_alone():
    assert with_schema("/data/order_events.db", "tenant_acme") == "/data/order_events.db"
    assert with_schema("postgresql://db.example.test/oms", "") == "postgresql://db.example.test/oms"
    assert Database("postgresql://db.example.test/oms").schema == ""


@pytest.mark.parametrize("schema", ["Tenant", "tenant-acme", "1tenant", 'x"; DROP SCHEMA public; --', "a b", "x" * 64])
def test_with_schema_refuses_unsafe_names(schema):
    with pytest.raises(ValueError):
        with_schema("postgresql://db.example.test/oms", schema)


def test_with_schema_refuses_a_url_that_already_has_options():
    with pytest.raises(ValueError):
        with_schema("postgresql://db.example.test/oms?options=-csearch_path%3Dother", "tenant_acme")


def _tenant_app(tmp_path, url: str, schema: str, name: str) -> TestClient:
    app = create_app()
    app.state.db_dir = str(tmp_path / name)
    (tmp_path / name).mkdir(exist_ok=True)
    app.state.database_url = url
    app.state.database_schema = schema
    return TestClient(app, headers={"X-User-Id": "trader1"})


def test_tenants_sharing_a_database_do_not_see_each_others_orders(tmp_path, postgres_schemas):
    url, (schema_a, schema_b) = postgres_schemas

    # Both run at once: the engine's lock is per schema, so neither blocks the other
    with _tenant_app(tmp_path, url, schema_a, "a") as api_a, _tenant_app(tmp_path, url, schema_b, "b") as api_b:
        order_a = api_a.post("/api/v1/orders", json=_ORDER).json()["order_id"]
        order_b = api_b.post("/api/v1/orders", json={**_ORDER, "side": "Sell"}).json()["order_id"]
        api_a.post("/api/v1/proposals", json={**_ORDER, "rationale": "for tenant a"})

        assert [o["order_id"] for o in api_a.get("/api/v1/orders").json()] == [order_a]
        assert [o["order_id"] for o in api_b.get("/api/v1/orders").json()] == [order_b]
        assert api_a.get(f"/api/v1/orders/{order_b}").status_code == 404
        assert api_b.get(f"/api/v1/orders/{order_a}").status_code == 404
        assert len(api_a.get("/api/v1/proposals").json()) == 1
        assert api_b.get("/api/v1/proposals").json() == []

    # Each tenant's data is where it should be, and comes back for that tenant only
    with _tenant_app(tmp_path, url, schema_b, "b") as api_b:
        assert [o["order_id"] for o in api_b.get("/api/v1/orders").json()] == [order_b]


def test_second_deployment_for_the_same_tenant_is_refused(tmp_path, postgres_schemas):
    url, (schema_a, _schema_b) = postgres_schemas
    with _tenant_app(tmp_path, url, schema_a, "a") as first:
        assert first.post("/api/v1/orders", json=_ORDER).status_code == 201

        # A second process is a second Database on the same schema
        from hgraph_oms.engine import OrderEngine

        other_process = Database(with_schema(url, schema_a))
        try:
            duplicate = OrderEngine(with_schema(url, schema_a))
            duplicate._db = other_process
            with pytest.raises(RuntimeError, match="Another order engine"):
                duplicate.start()
        finally:
            other_process.close()


# ---------------------------------------------------------------------------
# Tokens carry the organisation
# ---------------------------------------------------------------------------


def test_token_for_this_tenant_is_accepted():
    assert verifier(tenant_claim="org_id", tenant="acme").verify(make_token(org_id="acme")) == "trader1"


@pytest.mark.parametrize("claims", [{"org_id": "globex"}, {}, {"org_id": ""}, {"org_id": ["acme"]}, {"org_id": "ACME"}])
def test_token_for_another_tenant_is_refused(claims):
    with pytest.raises(TokenError, match="different organisation"):
        verifier(tenant_claim="org_id", tenant="acme").verify(make_token(**claims))


def test_tenant_claim_is_not_required_unless_configured():
    assert verifier().verify(make_token(org_id="globex")) == "trader1"


def test_verifier_needs_a_tenant_to_check_the_claim_against():
    with pytest.raises(ValueError):
        verifier(tenant_claim="org_id")


@pytest.fixture()
def acme_api(tmp_path, monkeypatch):
    monkeypatch.setenv("TENANT_ID", "acme")
    app = create_app()
    app.state.db_dir = str(tmp_path)
    app.state.auth_mode = OIDC
    app.state.token_verifier = verifier(tenant_claim="org_id", tenant="acme")
    with TestClient(app) as client:
        yield client


def test_api_serves_its_own_tenants_users(acme_api):
    headers = {"Authorization": f"Bearer {make_token(org_id='acme')}"}
    assert acme_api.get("/api/v1/me", headers=headers).json()["tenant_id"] == "acme"
    assert acme_api.post("/api/v1/orders", json=_ORDER, headers=headers).status_code == 201


def test_api_refuses_another_tenants_users(acme_api):
    headers = {"Authorization": f"Bearer {make_token(org_id='globex')}"}
    for method, path in (("GET", "/api/v1/orders"), ("POST", "/api/v1/orders"), ("GET", "/api/v1/me")):
        response = acme_api.request(method, path, headers=headers, json=_ORDER if method == "POST" else None)
        assert response.status_code == 401
        assert "different organisation" in response.json()["detail"]
