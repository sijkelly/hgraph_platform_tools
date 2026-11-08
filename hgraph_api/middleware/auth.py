"""
Authentication and entitlements enforcement for the OMS API.

Every request outside :data:`_PUBLIC_PATHS` must carry an authenticated
identity, and that identity must exist in the entitlements database.
Write requests additionally require the entitlements action mapped in
:data:`_WRITE_ACTIONS`; writes to unmapped paths are denied.

Two authentication modes are supported (``API_AUTH_MODE``):

- ``token`` (default) — ``Authorization: Bearer <token>`` where the token is
  an HS256 JWT signed with ``API_AUTH_SECRET`` (see :mod:`.tokens`).  If an
  ``X-User-Id`` header is also sent it must match the token's subject.
- ``dev`` — the ``X-User-Id`` header is trusted as-is.  For local
  development only: anyone who can reach the server can claim any identity.

WebSocket handshakes cannot carry custom headers from a browser, so
:func:`authenticate_websocket` also accepts ``?token=`` (token mode) or
``?user_id=`` (dev mode) query parameters.

Any failure while checking entitlements denies the request (fail closed).
"""

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass

from fastapi import Request, WebSocket
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware

from hgraph_api.middleware.tokens import MIN_SECRET_LENGTH, TokenError, verify_token

__all__ = (
    "AUTH_MODE_DEV",
    "AUTH_MODE_TOKEN",
    "AuthError",
    "AuthMiddleware",
    "AuthSettings",
    "authenticate",
    "authenticate_websocket",
    "authorize",
)

logger = logging.getLogger(__name__)

AUTH_MODE_TOKEN = "token"
AUTH_MODE_DEV = "dev"
_AUTH_MODES = (AUTH_MODE_TOKEN, AUTH_MODE_DEV)

# Paths (exact, or as a "/"-separated prefix) that do not require authentication
_PUBLIC_PATHS = (
    "/api/v1/health",
    "/docs",
    "/openapi.json",
    "/redoc",
)

# Entitlements action required for write requests, by path prefix.
_WRITE_ACTIONS = (("/api/v1/orders", "execute_trade"),)

_READ_METHODS = frozenset({"GET", "HEAD"})

# WebSocket close code for policy violations (RFC 6455)
_WS_POLICY_VIOLATION = 1008


class AuthError(Exception):
    """Authentication or authorisation failure carrying the HTTP status to return."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class AuthSettings:
    """Resolved authentication settings for an app instance."""

    mode: str
    secret: str
    entitlements_db_path: str

    def __post_init__(self) -> None:
        if self.mode not in _AUTH_MODES:
            raise ValueError(f"API_AUTH_MODE must be one of {_AUTH_MODES}, got {self.mode!r}")
        if self.mode == AUTH_MODE_TOKEN and len(self.secret) < MIN_SECRET_LENGTH:
            raise ValueError(
                f"API_AUTH_MODE=token requires API_AUTH_SECRET of at least {MIN_SECRET_LENGTH} characters. "
                "Set API_AUTH_SECRET, or set API_AUTH_MODE=dev for local development only."
            )

    @classmethod
    def from_env(cls, defaults: Mapping[str, str]) -> "AuthSettings":
        """Build settings from environment variables, falling back to ``defaults``.

        Read at app-creation time so CLI flags that set env vars take effect.
        """

        def _get(key: str, fallback: str) -> str:
            return os.getenv(key, defaults.get(key, fallback))

        return cls(
            mode=_get("API_AUTH_MODE", AUTH_MODE_TOKEN).strip().lower(),
            secret=_get("API_AUTH_SECRET", ""),
            entitlements_db_path=_get("ENTITLEMENTS_DB_PATH", "hgraph_entitlements.db"),
        )


def _matches(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix.rstrip("/") + "/")


def _is_public(path: str) -> bool:
    return any(_matches(path, prefix) for prefix in _PUBLIC_PATHS)


def _required_action(method: str, path: str) -> str | None:
    """Return the action a request needs, ``None`` for reads, or raise for unmapped writes."""
    if method in _READ_METHODS:
        return None
    for prefix, action in _WRITE_ACTIONS:
        if _matches(path, prefix):
            return action
    raise AuthError(403, f"No entitlement is mapped for {method} {path}")


def authenticate(
    settings: AuthSettings,
    headers: Mapping[str, str],
    query: Mapping[str, str] | None = None,
) -> str:
    """Resolve the caller's user id from request headers (or WebSocket query params).

    :raises AuthError: 401 if no valid identity is presented.
    """
    query = query or {}
    claimed_user = headers.get("x-user-id") or ""

    if settings.mode == AUTH_MODE_DEV:
        user_id = claimed_user or query.get("user_id", "")
        if not user_id:
            raise AuthError(401, "Missing X-User-Id header")
        return user_id

    token = ""
    authorization = headers.get("authorization") or ""
    scheme, _, credentials = authorization.partition(" ")
    if scheme.lower() == "bearer" and credentials.strip():
        token = credentials.strip()
    elif query.get("token"):
        token = query["token"]
    if not token:
        raise AuthError(401, "Missing bearer token")

    try:
        user_id = verify_token(token, settings.secret)
    except TokenError as exc:
        raise AuthError(401, f"Invalid bearer token: {exc}") from exc

    if claimed_user and claimed_user != user_id:
        raise AuthError(401, "X-User-Id does not match bearer token")
    return user_id


def authorize(settings: AuthSettings, user_id: str, action: str | None) -> None:
    """Check that ``user_id`` is a known user and, if given, holds ``action``.

    Blocking (SQLite); call via a thread pool from async code.

    :raises AuthError: 403 if denied, 503 if entitlements cannot be checked.
    """
    from hgraph_entitlements.checker import get_allowed_actions
    from hgraph_entitlements.example_code import get_db_connection, get_user_role, initialize_db

    try:
        conn = get_db_connection(settings.entitlements_db_path)
        try:
            initialize_db(conn)
            role = get_user_role(conn, user_id)
        finally:
            conn.close()
    except Exception:
        logger.exception("Entitlements check failed for user=%s; denying request", user_id)
        raise AuthError(503, "Entitlements service unavailable")

    if role is None:
        logger.warning("Access denied: unknown user=%s", user_id)
        raise AuthError(403, f"User '{user_id}' has no entitlements")

    if action is not None and action not in get_allowed_actions(role):
        logger.warning("Permission denied: user=%s role=%s action=%s", user_id, role, action)
        raise AuthError(403, f"User '{user_id}' lacks permission '{action}'")


def _settings(app) -> AuthSettings:
    settings = getattr(app.state, "auth_settings", None)
    if not isinstance(settings, AuthSettings):
        # Misconfigured app: refuse rather than serve unauthenticated.
        raise AuthError(503, "Authentication is not configured")
    return settings


class AuthMiddleware(BaseHTTPMiddleware):
    """Authenticate every non-public HTTP request and enforce entitlements on writes.

    Sets ``request.state.user_id`` for downstream handlers.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method

        # CORS preflights carry no credentials; CORSMiddleware answers them.
        if method == "OPTIONS" or _is_public(path):
            return await call_next(request)

        try:
            settings = _settings(request.app)
            user_id = authenticate(settings, request.headers)
            action = _required_action(method, path)
            await run_in_threadpool(authorize, settings, user_id, action)
        except AuthError as exc:
            headers = {"WWW-Authenticate": "Bearer"} if exc.status_code == 401 else None
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=headers)

        request.state.user_id = user_id
        return await call_next(request)


async def authenticate_websocket(websocket: WebSocket) -> str | None:
    """Authenticate a WebSocket handshake before it is accepted.

    HTTP middleware does not run for WebSocket connections, so every
    WebSocket endpoint must call this first.  On failure the handshake is
    rejected (close code 1008) and ``None`` is returned.
    """
    try:
        settings = _settings(websocket.app)
        user_id = authenticate(settings, websocket.headers, websocket.query_params)
        await run_in_threadpool(authorize, settings, user_id, None)
    except AuthError as exc:
        logger.info("WebSocket %s rejected: %s", websocket.url.path, exc.detail)
        await websocket.close(code=_WS_POLICY_VIOLATION)
        return None
    return user_id
