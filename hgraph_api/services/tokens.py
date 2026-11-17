"""
Verification of OpenID Connect access tokens.

In ``oidc`` mode the API accepts only requests carrying a bearer token issued
by the configured identity provider.  :class:`TokenVerifier` checks the token's
signature against the provider's published keys, its issuer, audience and
expiry, and returns the user it identifies.  The user ID is whatever the
configured claim holds (``sub`` by default), and is the ID the entitlements
store and the order log know the user by.

Only asymmetric signature algorithms are accepted, so a token can never be
verified with a shared secret or with no signature at all.
"""

import json
import logging
import os
import threading
import urllib.request
from typing import Any, Callable
from urllib.parse import urlparse

from hgraph_api.config import api_config
from hgraph_api.services.tenancy import tenant_id

__all__ = (
    "HEADER",
    "OIDC",
    "ALGORITHMS",
    "TokenError",
    "TokenServiceError",
    "TokenVerifier",
    "auth_settings",
    "verifier_from_settings",
)

logger = logging.getLogger(__name__)

# Authentication modes
HEADER = "header"  # trust the X-User-Id header; development only
OIDC = "oidc"  # require a bearer token from the configured identity provider

ALGORITHMS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512")

_DISCOVERY_PATH = "/.well-known/openid-configuration"
_HTTP_TIMEOUT = 10


class TokenError(Exception):
    """The token is not acceptable. The message says why and is safe to return to the caller."""


class TokenServiceError(Exception):
    """The token could not be checked because the identity provider's keys were unavailable."""


def _require_http_url(url: str, what: str) -> str:
    if urlparse(url).scheme not in ("http", "https"):
        raise ValueError(f"{what} must be an http(s) URL")
    return url


class TokenVerifier:
    """Verifies bearer tokens from one identity provider.

    :param issuer: The provider's issuer URL; must equal the token's ``iss`` claim exactly.
    :param audience: The audience this API is registered as; must be in the token's ``aud`` claim.
    :param user_claim: The claim holding the user ID.
    :param jwks_url: Where the provider publishes its signing keys.  If empty, it is read from the
        provider's discovery document on first use.
    :param tenant_claim: A claim naming the organisation the user belongs to (for example ``tid`` or
        ``org_id``).  If given, it must equal ``tenant``, so that a token the same provider issued
        to another organisation is refused.
    :param tenant: The organisation this API serves; required with ``tenant_claim``.
    :param key_resolver: Returns the verification key for a token, replacing the JWKS lookup.
    :param leeway: Clock skew, in seconds, tolerated when checking expiry.
    """

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        user_claim: str = "sub",
        jwks_url: str = "",
        tenant_claim: str = "",
        tenant: str = "",
        key_resolver: Callable[[str], Any] | None = None,
        leeway: float = 30.0,
    ):
        if not issuer or not audience:
            raise ValueError("Token verification needs both an issuer and an audience")
        if tenant_claim and not tenant:
            raise ValueError("a tenant claim can only be checked when TENANT_ID is set")
        if key_resolver is None:
            _require_http_url(jwks_url or issuer, "The JWKS URL" if jwks_url else "The issuer")
        self.issuer = issuer
        self.audience = audience
        self.user_claim = user_claim or "sub"
        self.tenant_claim = tenant_claim
        self.tenant = tenant
        self.leeway = leeway
        self._jwks_url = jwks_url
        self._key_resolver = key_resolver
        self._jwks_client: Any = None
        self._lock = threading.Lock()

    def verify(self, token: str) -> str:
        """Return the user ID a token identifies.

        :raises TokenError: If the token is malformed, wrongly signed, expired, or not for this API.
        :raises TokenServiceError: If the provider's signing keys could not be fetched.
        """
        import jwt

        try:
            key = self._key_resolver(token) if self._key_resolver else self._signing_key(token)
            claims = jwt.decode(
                token,
                key,
                algorithms=list(ALGORITHMS),
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.leeway,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.PyJWKClientConnectionError as exc:
            raise TokenServiceError(str(exc)) from exc
        except jwt.ExpiredSignatureError:
            raise TokenError("the token has expired") from None
        except jwt.InvalidAudienceError:
            raise TokenError("the token was not issued for this API") from None
        except jwt.InvalidIssuerError:
            raise TokenError("the token is from an issuer this API does not trust") from None
        except jwt.PyJWTError as exc:
            logger.info("Rejected token: %s: %s", type(exc).__name__, exc)
            raise TokenError("the token could not be verified") from None

        if self.tenant_claim and claims.get(self.tenant_claim) != self.tenant:
            raise TokenError("the token belongs to a different organisation")

        user_id = claims.get(self.user_claim)
        if not isinstance(user_id, str) or not user_id.strip():
            raise TokenError(f"the token has no '{self.user_claim}' claim")
        return user_id.strip()

    def _signing_key(self, token: str) -> Any:
        import jwt

        with self._lock:
            if self._jwks_client is None:
                self._jwks_client = jwt.PyJWKClient(
                    self._jwks_url or self._discover_jwks_url(), cache_keys=True, timeout=_HTTP_TIMEOUT
                )
            client = self._jwks_client
        return client.get_signing_key_from_jwt(token).key

    def _discover_jwks_url(self) -> str:
        url = self.issuer.rstrip("/") + _DISCOVERY_PATH
        try:
            with urllib.request.urlopen(url, timeout=_HTTP_TIMEOUT) as response:  # scheme checked in __init__
                jwks_url = json.load(response)["jwks_uri"]
            return _require_http_url(jwks_url, "The provider's jwks_uri")
        except Exception as exc:
            raise TokenServiceError(f"Could not read {url}: {type(exc).__name__}") from exc


def _setting(name: str, default: str = "") -> str:
    return os.getenv(name) or api_config.get(name) or default


def auth_settings() -> tuple[str, TokenVerifier | None]:
    """Read the authentication mode and, for ``oidc``, build the verifier from the settings.

    :raises RuntimeError: If the mode is unknown or ``oidc`` is not fully configured, so that a
        misconfigured server refuses to start rather than run without authentication.
    """
    mode = _setting("AUTH_MODE", HEADER).strip().lower()
    if mode == HEADER:
        return HEADER, None
    if mode != OIDC:
        raise RuntimeError(f"AUTH_MODE must be '{HEADER}' or '{OIDC}', not '{mode}'")
    return OIDC, verifier_from_settings()


def verifier_from_settings() -> TokenVerifier:
    """Build the verifier from ``OIDC_ISSUER``, ``OIDC_AUDIENCE``, ``OIDC_USER_CLAIM`` and ``OIDC_JWKS_URL``.

    :raises RuntimeError: If the issuer or audience is missing or a URL is not http(s).
    """
    try:
        return TokenVerifier(
            issuer=_setting("OIDC_ISSUER"),
            audience=_setting("OIDC_AUDIENCE"),
            user_claim=_setting("OIDC_USER_CLAIM", "sub"),
            jwks_url=_setting("OIDC_JWKS_URL"),
            tenant_claim=_setting("OIDC_TENANT_CLAIM"),
            tenant=tenant_id(),
        )
    except ValueError as exc:
        raise RuntimeError(f"AUTH_MODE=oidc is not fully configured: {exc}") from exc
