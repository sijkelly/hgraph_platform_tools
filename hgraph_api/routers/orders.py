"""Order management REST endpoints.

Provides NLP order parsing, order submission with credit check,
order status updates, and order cancellation.
"""

import logging
import os

from fastapi import APIRouter, HTTPException, Request

from hgraph_api.schemas.orders import (
    CreateOrderRequest,
    NLPParseRequest,
    NLPParseResponse,
    OrderResponse,
    UpdateOrderRequest,
)

__all__ = ("router",)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/orders", tags=["orders"])


def _order_db_path(request: Request) -> str:
    db_dir = getattr(request.app.state, "db_dir", ".")
    return os.path.join(db_dir, "order_data.db")


def _credit_db_path(request: Request) -> str:
    db_dir = getattr(request.app.state, "db_dir", ".")
    return os.path.join(db_dir, "credit_data.db")


# ---------------------------------------------------------------------------
# NLP parsing
# ---------------------------------------------------------------------------


@router.post("/parse-nlp", response_model=NLPParseResponse)
def parse_nlp_order(request_body: NLPParseRequest) -> NLPParseResponse:
    """Parse free text into structured order fields using Claude API."""
    from hgraph_api.services.nlp_order_parser import parse_order_text

    if not request_body.text.strip():
        raise HTTPException(status_code=400, detail="Order text must not be empty")

    try:
        parsed = parse_order_text(request_body.text, context=request_body.context)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        logger.error("NLP parsing failed: %s", exc)
        raise HTTPException(status_code=500, detail="Order parsing failed")

    return NLPParseResponse(**parsed)


# ---------------------------------------------------------------------------
# Order CRUD
# ---------------------------------------------------------------------------


@router.post("", response_model=OrderResponse, status_code=201)
def create_order(request_body: CreateOrderRequest, request: Request) -> OrderResponse:
    """Submit a new order with optional credit check."""
    from hgraph_api.services.order_store import init_order_db, create_order as store_create

    if not request_body.instrument.strip():
        raise HTTPException(status_code=400, detail="Instrument must not be empty")
    if request_body.side not in ("Buy", "Sell"):
        raise HTTPException(status_code=400, detail="Side must be 'Buy' or 'Sell'")
    if not request_body.quantity.strip():
        raise HTTPException(status_code=400, detail="Quantity must not be empty")

    # Credit check — if counterparty is specified, verify available credit
    if request_body.counterparty:
        credit_warning = _check_credit(request, request_body.counterparty, request_body.quantity)
        if credit_warning:
            raise HTTPException(status_code=422, detail=credit_warning)

    db = _order_db_path(request)
    init_order_db(db)
    order = store_create(db, request_body.model_dump())
    return OrderResponse(**order)


@router.get("", response_model=list[OrderResponse])
def list_orders(request: Request) -> list[OrderResponse]:
    """List all orders, most recent first."""
    from hgraph_api.services.order_store import init_order_db, get_all_orders

    db = _order_db_path(request)
    init_order_db(db)
    return [OrderResponse(**o) for o in get_all_orders(db)]


@router.get("/{order_id}", response_model=OrderResponse)
def get_order(order_id: str, request: Request) -> OrderResponse:
    """Get a single order by ID."""
    from hgraph_api.services.order_store import init_order_db, get_order as store_get

    db = _order_db_path(request)
    init_order_db(db)
    order = store_get(db, order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return OrderResponse(**order)


@router.patch("/{order_id}", response_model=OrderResponse)
def update_order(order_id: str, request_body: UpdateOrderRequest, request: Request) -> OrderResponse:
    """Update an order's status."""
    from hgraph_api.services.order_store import init_order_db, update_order_status

    valid_statuses = {"Pending", "Working", "Filled", "Partially Filled", "Rejected", "Cancelled"}
    if request_body.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")

    db = _order_db_path(request)
    init_order_db(db)
    order = update_order_status(db, order_id, request_body.status, filled_quantity=request_body.filled_quantity)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return OrderResponse(**order)


@router.post("/{order_id}/cancel", response_model=OrderResponse)
def cancel_order_endpoint(order_id: str, request: Request) -> OrderResponse:
    """Cancel an order. Only Pending/Working orders can be cancelled."""
    from hgraph_api.services.order_store import init_order_db, cancel_order

    db = _order_db_path(request)
    init_order_db(db)
    order = cancel_order(db, order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    if order["status"] != "Cancelled":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel order in '{order['status']}' state",
        )
    return OrderResponse(**order)


# ---------------------------------------------------------------------------
# Credit check helper
# ---------------------------------------------------------------------------


def _check_credit(request: Request, counterparty: str, quantity_str: str) -> str | None:
    """Check if counterparty has sufficient credit. Returns warning message or None."""
    try:
        from hgraph_static_admin.credit_store import get_credit_utilization

        credit_db = _credit_db_path(request)
        utilization = get_credit_utilization(credit_db, counterparty)
        if utilization is None:
            return None  # No credit data — allow (no limit configured)

        if utilization.utilization_percentage >= 100:
            return f"Credit limit exceeded for {counterparty}: {utilization.utilization_percentage:.1f}% utilized"

        if utilization.utilization_percentage >= 90:
            logger.warning(
                "High credit utilization for %s: %.1f%%", counterparty, utilization.utilization_percentage
            )
    except Exception as exc:
        logger.warning("Credit check failed for %s: %s (allowing order)", counterparty, exc)
    return None
