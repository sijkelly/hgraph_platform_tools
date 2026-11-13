"""
FastAPI application factory.

Creates and configures the main FastAPI app with all routers, middleware,
and CORS settings.  Call :func:`create_app` to get a ready-to-run instance.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hgraph_api.config import api_config
from hgraph_api.routers import credit, orders, parties, portfolios, proposals, entitlements, health
from hgraph_api.middleware.auth import AuthMiddleware
from hgraph_api.services.order_engine import stop_order_engine
from hgraph_api.services.tokens import HEADER, auth_settings
from hgraph_api.websockets import credit_ws, orders_ws
from hgraph_oms.db import close_databases

__all__ = ("create_app",)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # The order engine starts on first use (see get_order_engine); stop it on shutdown
    yield
    stop_order_engine(app)
    close_databases()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="hgraph OMS API",
        description="REST + WebSocket API for the hgraph Order Management System",
        version="0.1.0",
        lifespan=_lifespan,
    )

    # Authentication mode: "header" trusts X-User-Id (development); "oidc" requires bearer tokens.
    # Added before CORS so that CORS wraps it and error responses carry the CORS headers.
    app.state.auth_mode, app.state.token_verifier = auth_settings()
    if app.state.auth_mode == HEADER:
        logger.warning("AUTH_MODE=header: callers are identified by an unverified header. Development use only.")
    app.add_middleware(AuthMiddleware)

    # CORS — allow the UI dev server and any configured origins
    cors_origins = os.getenv("API_CORS_ORIGINS", api_config.get("API_CORS_ORIGINS", ""))
    origins = [o.strip() for o in cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store db_dir in app state so routers can access it
    # Read from env at factory time so CLI --db-dir takes effect
    app.state.db_dir = os.getenv("API_DB_DIR", api_config.get("API_DB_DIR", "."))

    # Entitlements enforcement on write endpoints (off by default for dev;
    # when on, errors in the checker fail closed)
    app.state.enforce_entitlements = os.getenv("API_ENFORCE_ENTITLEMENTS", "").lower() in ("1", "true", "yes")

    # Register routers
    app.include_router(health.router)
    app.include_router(credit.router)
    app.include_router(parties.router)
    app.include_router(portfolios.router)
    app.include_router(entitlements.router)
    app.include_router(entitlements.me_router)
    app.include_router(orders.router)
    app.include_router(orders.trades_router)
    app.include_router(proposals.router)
    app.include_router(credit_ws.router)
    app.include_router(orders_ws.router)

    return app
