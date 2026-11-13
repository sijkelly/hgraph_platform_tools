"""
Authentication for programs that call the OMS API, such as the MCP server.

When the API runs in ``oidc`` mode a client needs a bearer token.  A service
(as opposed to a person) gets one from the identity provider with the OAuth 2.0
client-credentials grant: :class:`ClientCredentialsAuth` does that, caches the
token and renews it before it expires.  :func:`client_auth_from_env` picks the
right scheme from the environment.
"""

import base64
import os
import time
from typing import Generator, Mapping

import httpx

__all__ = ("ClientAuthError", "BearerTokenAuth", "ClientCredentialsAuth", "client_auth_from_env")

# Renew this many seconds before the provider says the token expires
_EXPIRY_MARGIN = 30.0
_DEFAULT_LIFETIME = 300.0


class ClientAuthError(httpx.HTTPError):
    """A token could not be obtained from the identity provider. The message says why."""


class BearerTokenAuth(httpx.Auth):
    """Sends a fixed bearer token."""

    def __init__(self, token: str):
        self._token = token

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


class ClientCredentialsAuth(httpx.Auth):
    """Obtains bearer tokens with the OAuth 2.0 client-credentials grant and keeps them fresh.

    :param token_url: The identity provider's token endpoint.
    :param scope: Scopes to request, space separated, if the provider needs them.
    :param audience: The API's audience, for providers that take it as a request parameter.
    """

    requires_response_body = True

    def __init__(self, token_url: str, client_id: str, client_secret: str, *, scope: str = "", audience: str = ""):
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._audience = audience
        self._token = ""
        self._expires_at = 0.0

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        if not self._token or time.monotonic() >= self._expires_at:
            self._store((yield self._token_request()))
        request.headers["Authorization"] = f"Bearer {self._token}"
        response = yield request

        if response.status_code == 401:
            # The API no longer accepts the token (revoked, or keys rotated): get a new one, once
            self._store((yield self._token_request()))
            request.headers["Authorization"] = f"Bearer {self._token}"
            yield request

    def _token_request(self) -> httpx.Request:
        form = {"grant_type": "client_credentials"}
        if self._scope:
            form["scope"] = self._scope
        if self._audience:
            form["audience"] = self._audience
        basic = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        return httpx.Request("POST", self._token_url, data=form, headers={"Authorization": f"Basic {basic}"})

    def _store(self, response: httpx.Response) -> None:
        self._token = ""
        if response.status_code != 200:
            raise ClientAuthError(f"the identity provider refused the token request (HTTP {response.status_code})")
        try:
            body = response.json()
            token = body["access_token"]
            lifetime = float(body.get("expires_in") or _DEFAULT_LIFETIME)
        except (ValueError, KeyError, TypeError):
            raise ClientAuthError("the identity provider's token response could not be read") from None
        self._token = token
        self._expires_at = time.monotonic() + max(lifetime - _EXPIRY_MARGIN, 0.0)


def client_auth_from_env(env: Mapping[str, str] | None = None) -> httpx.Auth | None:
    """Return the authentication a client should use, from its environment.

    * ``OMS_TOKEN_URL``, ``OMS_CLIENT_ID`` and ``OMS_CLIENT_SECRET`` (optionally ``OMS_TOKEN_SCOPE``
      and ``OMS_TOKEN_AUDIENCE``): client credentials, renewed automatically.
    * ``OMS_API_TOKEN``: a fixed bearer token.
    * Neither: ``None``; the client identifies itself with ``X-User-Id`` (development).

    :raises ValueError: If the client-credentials settings are only partly given.
    """
    env = os.environ if env is None else env
    credentials = [env.get(name, "") for name in ("OMS_TOKEN_URL", "OMS_CLIENT_ID", "OMS_CLIENT_SECRET")]
    if all(credentials):
        return ClientCredentialsAuth(
            *credentials, scope=env.get("OMS_TOKEN_SCOPE", ""), audience=env.get("OMS_TOKEN_AUDIENCE", "")
        )
    if any(credentials):
        raise ValueError("OMS_TOKEN_URL, OMS_CLIENT_ID and OMS_CLIENT_SECRET must all be set, or none of them")
    if env.get("OMS_API_TOKEN"):
        return BearerTokenAuth(env["OMS_API_TOKEN"])
    return None
