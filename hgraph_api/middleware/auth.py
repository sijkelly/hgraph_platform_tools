"""
Authentication and entitlements middleware.

Who the caller is depends on the app's authentication mode:

* ``header`` (development): the ``X-User-Id`` header is trusted as given, and
  reads are allowed without it.
* ``oidc``: every request outside the public paths must carry a bearer token
  from the configured identity provider; ``X-User-Id`` is ignored.

Once the caller is known, write endpoints are checked against the entitlements
system when enforcement is enabled.
"""

import logging

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware

from hgraph_api.services.tokens import HEADER, OIDC, TokenError, TokenServiceError

__all__ = ("AuthError", "AuthMiddleware", "auth_mode", "authenticate", "accept_websocket")

logger = logging.getLogger(__name__)

_WS_TOKEN_PROTOCOL = "bearer"

# Paths that do not require authentication
_PUBLIC_PREFIXES = (
    "/api/v1/health",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/ws/",
)

# Mapping of (method, path_prefix) to required action
_ACTION_MAP = {
    ("POST", "/api/v1/orders"): "execute_trade",
    ("PATCH", "/api/v1/orders"): "execute_trade",
    # Confirming or dismissing a proposal is a trading decision; drafting one is not
    ("POST", "/api/v1/proposals/"): "execute_trade",
    ("POST", "/api/v1/credit"): "manage_credit",
    ("PUT", "/api/v1/credit"): "manage_credit",
    ("DELETE", "/api/v1/credit"): "manage_credit",
}


def _required_action(method: str, path: str) -> str | None:
    """Return the entitlements action required for this request, or None if public."""
    for prefix in _PUBLIC_PREFIXES:
        if path.startswith(prefix):
            return None

    # GET requests only require basic read access — no specific action check
    if method == "GET":
        return None

    for (m, prefix), action in _ACTION_MAP.items():
        if method == m and path.startswith(prefix):
            return action

    # Default: require authentication header but no specific action
    return None


class AuthError(Exception):
    """The caller could not be identified."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def auth_mode(app: FastAPI) -> str:
    return getattr(app.state, "auth_mode", HEADER)


def authenticate(app: FastAPI, *, authorization: str | None, user_header: str | None) -> str | None:
    """Return the caller's user ID, or ``None`` for an anonymous caller in ``header`` mode.

    :param authorization: The ``Authorization`` header, if any.
    :param user_header: The ``X-User-Id`` header, if any; only used in ``header`` mode.
    :raises AuthError: In ``oidc`` mode, if there is no valid bearer token (401) or the identity
        provider's keys cannot be fetched (503).
    """
    if auth_mode(app) != OIDC:
        return user_header or None

    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise AuthError(401, "A bearer token is required")
    verifier = getattr(app.state, "token_verifier", None)
    if verifier is None:
        raise AuthError(503, "Token verification is not configured — request denied")
    try:
        return verifier.verify(token.strip())
    except TokenServiceError as exc:
        logger.error("Token could not be checked: %s (denying)", exc)
        raise AuthError(503, "Token verification unavailable — request denied") from exc
    except TokenError as exc:
        raise AuthError(401, f"Invalid token: {exc}") from exc


async def accept_websocket(websocket: WebSocket) -> bool:
    """Accept a WebSocket connection if its caller is allowed to connect.

    In ``oidc`` mode the token comes from the ``Authorization`` header or, for browsers (which
    cannot set headers on a WebSocket), from the subprotocol list ``["bearer", "<token>"]``.
    A caller without a valid token is refused before the connection is accepted.

    :returns: ``True`` if the connection was accepted; ``False`` if it was closed.
    """
    if auth_mode(websocket.app) != OIDC:
        await websocket.accept()
        return True

    authorization = websocket.headers.get("authorization")
    subprotocol = None
    offered = websocket.scope.get("subprotocols") or []
    if not authorization and len(offered) == 2 and offered[0].lower() == _WS_TOKEN_PROTOCOL:
        authorization, subprotocol = f"Bearer {offered[1]}", offered[0]
    try:
        user_id = await run_in_threadpool(authenticate, websocket.app, authorization=authorization, user_header=None)
    except AuthError as exc:
        logger.warning("WebSocket connection to %s refused: %s", websocket.url.path, exc.detail)
        await websocket.close(code=1008)
        return False
    websocket.state.user_id = user_id
    await websocket.accept(subprotocol=subprotocol)
    return True


class AuthMiddleware(BaseHTTPMiddleware):
    """Identifies the caller and checks entitlements on protected endpoints.

    Returns 401 if the caller cannot be identified where that is required, and 403 if the
    user lacks the required permission.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method

        # Public paths, and CORS preflight requests, which browsers send without credentials
        if method == "OPTIONS" or any(path.startswith(prefix) for prefix in _PUBLIC_PREFIXES):
            return await call_next(request)

        try:
            user_id = await run_in_threadpool(
                authenticate,
                request.app,
                authorization=request.headers.get("Authorization"),
                user_header=request.headers.get("X-User-Id"),
            )
        except AuthError as exc:
            headers = {"WWW-Authenticate": "Bearer"} if exc.status_code == 401 else None
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=headers)

        if not user_id:
            # header mode only: allow unauthenticated GET requests (public read)
            if method == "GET":
                return await call_next(request)
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing X-User-Id header"},
            )

        # Store user_id in request state for downstream use
        request.state.user_id = user_id

        # Check specific action permission if required and enforcement is enabled
        enforce = getattr(request.app.state, "enforce_entitlements", False)
        action = _required_action(method, path)
        if action and enforce:
            try:
                from hgraph_entitlements.checker import check_permission

                allowed = check_permission(user_id, action)
                if not allowed:
                    logger.warning(
                        "Permission denied: user=%s action=%s path=%s",
                        user_id,
                        action,
                        path,
                    )
                    return JSONResponse(
                        status_code=403,
                        content={"detail": f"User '{user_id}' lacks permission '{action}'"},
                    )
            except Exception as exc:
                # Fail closed: if the entitlements check errors, deny the action
                logger.error("Auth check failed for %s action=%s: %s (denying)", user_id, action, exc)
                return JSONResponse(
                    status_code=503,
                    content={"detail": "Entitlements check unavailable — request denied"},
                )

        return await call_next(request)
