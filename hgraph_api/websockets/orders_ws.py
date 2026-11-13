"""
WebSocket endpoint for live order state updates.

Clients connect to ``/ws/orders`` and receive the full list of orders on
connect and again whenever the order engine changes any order.
"""

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.concurrency import run_in_threadpool

from hgraph_api.services.order_engine import get_order_engine, orders_to_dicts

__all__ = ("router",)

logger = logging.getLogger(__name__)

router = APIRouter()

_clients: set[WebSocket] = set()


@router.websocket("/ws/orders")
async def orders_websocket(websocket: WebSocket):
    """WebSocket endpoint for live order data.

    Sends an ``orders.snapshot`` message on connect and after every change the
    order engine publishes.  Send ``"close"`` to cleanly disconnect.
    """
    await websocket.accept()
    _clients.add(websocket)
    logger.info("Orders WebSocket client connected (%d total)", len(_clients))

    loop = asyncio.get_running_loop()
    changed = asyncio.Event()
    remove_listener = None
    receive: asyncio.Future | None = None

    try:
        engine = await run_in_threadpool(get_order_engine, websocket.app)
        # The engine calls listeners on its own thread; hand the signal over to this loop
        remove_listener = engine.add_listener(lambda _orders: loop.call_soon_threadsafe(changed.set))
        receive = asyncio.ensure_future(websocket.receive_text())
        send = True

        while True:
            if send:
                changed.clear()  # before reading, so a change during the send is not lost
                await websocket.send_text(json.dumps({"type": "orders.snapshot", "data": orders_to_dicts(engine)}))

            waiter = asyncio.ensure_future(changed.wait())
            await asyncio.wait({receive, waiter}, return_when=asyncio.FIRST_COMPLETED)
            send = waiter.done()
            waiter.cancel()

            if receive.done():
                if receive.result() == "close":
                    break
                receive = asyncio.ensure_future(websocket.receive_text())

    except WebSocketDisconnect:
        logger.info("Orders WebSocket client disconnected")
    except Exception as exc:
        logger.error("Orders WebSocket error: %s", exc)
    finally:
        if receive is not None:
            receive.cancel()
        if remove_listener is not None:
            remove_listener()
        _clients.discard(websocket)
