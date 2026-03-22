"""
hgraph_api — FastAPI service exposing OMS data over REST and WebSocket.

Wraps existing store functions and order services with HTTP endpoints
for consumption by the hgraph_oms_ui frontend.
"""

__all__ = ("create_app",)

from hgraph_api.app import create_app
