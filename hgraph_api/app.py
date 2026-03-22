"""
FastAPI application factory.

Creates and configures the main FastAPI app with all routers, middleware,
and CORS settings.  Call :func:`create_app` to get a ready-to-run instance.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hgraph_api.config import api_config
from hgraph_api.routers import credit, orders, parties, portfolios, entitlements, health
from hgraph_api.middleware.auth import AuthMiddleware
from hgraph_api.websockets import credit_ws, orders_ws

__all__ = ("create_app",)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="hgraph OMS API",
        description="REST + WebSocket API for the hgraph Order Management System",
        version="0.1.0",
    )

    # CORS — allow the UI dev server and any configured origins
    origins = [o.strip() for o in api_config["API_CORS_ORIGINS"].split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store db_dir in app state so routers can access it
    app.state.db_dir = api_config["API_DB_DIR"]

    # Register routers
    app.include_router(health.router)
    app.include_router(credit.router)
    app.include_router(parties.router)
    app.include_router(portfolios.router)
    app.include_router(entitlements.router)
    app.include_router(orders.router)
    app.include_router(credit_ws.router)
    app.include_router(orders_ws.router)

    # Auth middleware — checks X-User-Id header and entitlements on write endpoints
    app.add_middleware(AuthMiddleware)

    return app
