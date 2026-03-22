"""
WebSocket endpoint for live order state updates.

Clients connect to ``/ws/orders`` and receive JSON messages whenever
orders change in the store.  The server polls the SQLite database
on a configurable interval and pushes diffs.
"""

import asyncio
import json
import logging
import os

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

__all__ = ("router",)

logger = logging.getLogger(__name__)

router = APIRouter()

_clients: set[WebSocket] = set()


def _db_path(websocket: WebSocket) -> str:
    db_dir = getattr(websocket.app.state, "db_dir", ".")
    return os.path.join(db_dir, "order_data.db")


def _snapshot_orders(db_path: str) -> list[dict]:
    """Load all orders and return as dicts."""
    from hgraph_api.services.order_store import init_order_db, get_all_orders

    init_order_db(db_path)
    return get_all_orders(db_path)


@router.websocket("/ws/orders")
async def orders_websocket(websocket: WebSocket):
    """WebSocket endpoint for live order data.

    On connect, sends a full snapshot.  Then polls for changes and
    pushes updates to the client.  Send ``"close"`` to cleanly disconnect.
    """
    await websocket.accept()
    _clients.add(websocket)
    logger.info("Orders WebSocket client connected (%d total)", len(_clients))

    db_path = _db_path(websocket)
    prev_orders = None

    try:
        while True:
            orders = _snapshot_orders(db_path)

            if orders != prev_orders or prev_orders is None:
                await websocket.send_text(
                    json.dumps({"type": "orders.snapshot", "data": orders})
                )
                prev_orders = orders

            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
                if msg == "close":
                    break
            except asyncio.TimeoutError:
                pass

    except WebSocketDisconnect:
        logger.info("Orders WebSocket client disconnected")
    except Exception as exc:
        logger.error("Orders WebSocket error: %s", exc)
    finally:
        _clients.discard(websocket)
