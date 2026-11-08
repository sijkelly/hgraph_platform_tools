"""
FastAPI application factory.

Creates and configures the main FastAPI app with all routers, middleware,
and CORS settings.  Call :func:`create_app` to get a ready-to-run instance.
"""

import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hgraph_api.config import api_config
from hgraph_api.routers import credit, orders, parties, portfolios, entitlements, health
from hgraph_api.middleware.auth import AUTH_MODE_DEV, AuthMiddleware, AuthSettings
from hgraph_api.websockets import credit_ws, orders_ws

__all__ = ("create_app",)

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="hgraph OMS API",
        description="REST + WebSocket API for the hgraph Order Management System",
        version="0.1.0",
    )

    # Store db_dir in app state so routers can access it
    # Read from env at factory time so CLI --db-dir takes effect
    app.state.db_dir = os.getenv("API_DB_DIR", api_config.get("API_DB_DIR", "."))

    # Register routers
    app.include_router(health.router)
    app.include_router(credit.router)
    app.include_router(parties.router)
    app.include_router(portfolios.router)
    app.include_router(entitlements.router)
    app.include_router(orders.router)
    app.include_router(credit_ws.router)
    app.include_router(orders_ws.router)

    # Authentication settings are resolved at factory time (so CLI flags that set
    # env vars take effect) and validated: token mode without a secret fails here.
    app.state.auth_settings = AuthSettings.from_env(api_config)
    if app.state.auth_settings.mode == AUTH_MODE_DEV:
        logger.warning(
            "API_AUTH_MODE=dev: X-User-Id headers are trusted without verification. "
            "Never expose this server beyond localhost."
        )

    # Middleware added last runs first: CORS must wrap auth so that preflight
    # requests and error responses still carry CORS headers.
    app.add_middleware(AuthMiddleware)

    cors_origins = os.getenv("API_CORS_ORIGINS", api_config.get("API_CORS_ORIGINS", ""))
    origins = [o.strip() for o in cors_origins.split(",") if o.strip()]
    if "*" in origins:
        raise ValueError("API_CORS_ORIGINS must list explicit origins; '*' is not allowed with credentials")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-User-Id"],
    )

    return app
