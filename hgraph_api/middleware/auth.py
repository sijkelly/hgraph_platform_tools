"""
Entitlements-based authentication middleware.

Checks the ``X-User-Id`` header on protected endpoints and verifies
the user has the required permission via the entitlements system.

Unprotected paths (health, docs, WebSocket upgrades) are passed through.
"""

import logging

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

__all__ = ("AuthMiddleware",)

logger = logging.getLogger(__name__)

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


class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware that checks entitlements on protected endpoints.

    Reads ``X-User-Id`` from request headers.  If the header is missing
    on a protected endpoint, returns 401.  If the user lacks the required
    permission, returns 403.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method

        # Skip auth for public paths
        for prefix in _PUBLIC_PREFIXES:
            if path.startswith(prefix):
                return await call_next(request)

        # Check for user header
        user_id = request.headers.get("X-User-Id")
        if not user_id:
            # Allow unauthenticated GET requests (public read)
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
                        user_id, action, path,
                    )
                    return JSONResponse(
                        status_code=403,
                        content={"detail": f"User '{user_id}' lacks permission '{action}'"},
                    )
            except Exception as exc:
                logger.warning("Auth check failed for %s: %s (allowing)", user_id, exc)

        return await call_next(request)
