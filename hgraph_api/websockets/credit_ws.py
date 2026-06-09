"""
WebSocket endpoint for live credit data updates.

Clients connect to ``/ws/credit`` and receive JSON messages whenever
credit limits or utilizations change in the store.  The server polls
the SQLite database on a configurable interval and pushes diffs.

This is a polling-based implementation suitable for the current SQLite
backend.  A future version could subscribe directly to Kafka for
lower latency.
"""

import asyncio
import json
import logging
import os

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

__all__ = ("router",)

logger = logging.getLogger(__name__)

router = APIRouter()

# Connected clients
_clients: set[WebSocket] = set()


def _db_path(websocket: WebSocket) -> str:
    db_dir = getattr(websocket.app.state, "db_dir", ".")
    return os.path.join(db_dir, "credit_data.db")


def _snapshot_limits(db_path: str) -> list[dict]:
    """Load all credit limits and return as dicts."""
    from hgraph_static_admin.credit_store import get_all_credit_limits

    return [
        {
            "counterparty_symbol": cl.counterparty_symbol,
            "limit_type": cl.limit_type.value,
            "limit_amount": cl.limit_amount,
            "limit_currency": cl.limit_currency,
            "effective_date": cl.effective_date.isoformat() if cl.effective_date else None,
            "expiry_date": cl.expiry_date.isoformat() if cl.expiry_date else None,
            "status": cl.status.value,
            "approved_by": cl.approved_by,
            "last_review_date": cl.last_review_date.isoformat() if cl.last_review_date else None,
        }
        for cl in get_all_credit_limits(db_path)
    ]


def _snapshot_utilizations(db_path: str) -> list[dict]:
    """Load all credit utilizations and return as dicts."""
    from hgraph_static_admin.credit_store import get_all_credit_utilizations

    return [
        {
            "counterparty_symbol": cu.counterparty_symbol,
            "utilized_amount": cu.utilized_amount,
            "utilization_currency": cu.utilization_currency,
            "limit_amount": cu.limit_amount,
            "available_amount": cu.available_amount,
            "utilization_percentage": cu.utilization_percentage,
            "as_of_timestamp": cu.as_of_timestamp,
        }
        for cu in get_all_credit_utilizations(db_path)
    ]


@router.websocket("/ws/credit")
async def credit_websocket(websocket: WebSocket):
    """WebSocket endpoint for live credit data.

    On connect, sends a full snapshot.  Then polls for changes and
    pushes updates to the client.  Send ``"close"`` to cleanly disconnect.
    """
    await websocket.accept()
    _clients.add(websocket)
    logger.info("Credit WebSocket client connected (%d total)", len(_clients))

    db_path = _db_path(websocket)
    prev_limits = None
    prev_utilizations = None

    try:
        while True:
            # Snapshot current state
            limits = _snapshot_limits(db_path)
            utilizations = _snapshot_utilizations(db_path)

            # Send on first connect or when data changes
            if limits != prev_limits or prev_limits is None:
                await websocket.send_text(
                    json.dumps({"type": "credit.limits", "data": limits})
                )
                prev_limits = limits

            if utilizations != prev_utilizations or prev_utilizations is None:
                await websocket.send_text(
                    json.dumps({"type": "credit.utilizations", "data": utilizations})
                )
                prev_utilizations = utilizations

            # Also handle incoming messages (ping/pong, commands)
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
                if msg == "close":
                    break
            except asyncio.TimeoutError:
                pass  # Normal — no client message within poll interval

    except WebSocketDisconnect:
        logger.info("Credit WebSocket client disconnected")
    except Exception as exc:
        logger.error("Credit WebSocket error: %s", exc)
    finally:
        _clients.discard(websocket)
