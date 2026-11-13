"""Order management REST endpoints.

Every change to an order is sent as a message to the order engine
(:mod:`hgraph_oms`), which runs the pre-trade checks and the lifecycle state
machine and records the outcome in its event log.  These endpoints translate
HTTP requests into those messages and the engine's refusals into HTTP errors.
"""

import logging
import os
import re

from fastapi import APIRouter, HTTPException, Request

from hgraph_api.schemas.orders import (
    CreateOrderRequest,
    FillRequest,
    NLPParseRequest,
    NLPParseResponse,
    OrderResponse,
    TradeBookingResponse,
    UpdateOrderRequest,
)

from hgraph_api.services.order_engine import (
    booking_to_dict,
    details_from_fields,
    get_order_engine,
    get_trade_booker,
    new_order_id,
    order_to_dict,
    orders_to_dicts,
    parse_number,
    sync_credit,
    sync_permissions,
)
from hgraph_oms.engine import OrderEngine
from hgraph_oms.model import (
    AcceptOrder,
    AmendOrder,
    CancelOrder,
    FillOrder,
    NewOrder,
    OrderDetails,
    OrderMessage,
    OrderSnapshot,
    OrderStatus,
    RejectOrder,
)
from hgraph_oms.model import OrderResponse as EngineResponse
from hgraph_oms.state_machine import validate_details

__all__ = ("router", "trades_router", "order_details_from_request", "submit_new_order")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/orders", tags=["orders"])
trades_router = APIRouter(prefix="/api/v1/trades", tags=["trades"])

# Fill IDs become part of the trade ID and the booked file name
_FILL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def _credit_db_path(request: Request) -> str:
    db_dir = getattr(request.app.state, "db_dir", ".")
    return os.path.join(db_dir, "credit_data.db")


# ---------------------------------------------------------------------------
# NLP parsing
# ---------------------------------------------------------------------------


def _known_counterparties(request: Request) -> list[str]:
    """Counterparty symbols from the party store, so the parser can match names to them. Best effort."""
    try:
        from hgraph_static_admin.party_store import get_all_legal_entities, init_party_db

        db = os.path.join(getattr(request.app.state, "db_dir", "."), "party_data.db")
        init_party_db(db)
        return [entity.symbol for entity in get_all_legal_entities(db)]
    except Exception as exc:
        logger.warning("Could not load counterparties for order parsing: %s", exc)
        return []


@router.post("/parse-nlp", response_model=NLPParseResponse)
def parse_nlp_order(request_body: NLPParseRequest, request: Request) -> NLPParseResponse:
    """Parse free text into structured order fields using the Claude API.

    Nothing is submitted: the result is for a person to review, with warnings for anything
    that would stop the order submitting or its trades booking.
    """
    from hgraph_api.services.nlp_order_parser import parse_order_text

    if not request_body.text.strip():
        raise HTTPException(status_code=400, detail="Order text must not be empty")

    try:
        parsed = parse_order_text(
            request_body.text, context=request_body.context, counterparties=_known_counterparties(request)
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        logger.error("NLP parsing failed: %s", exc)
        raise HTTPException(status_code=500, detail="Order parsing failed")

    return NLPParseResponse(**parsed)


# ---------------------------------------------------------------------------
# Engine helpers
# ---------------------------------------------------------------------------


def _engine(request: Request) -> OrderEngine:
    try:
        return get_order_engine(request.app)
    except Exception as exc:
        logger.error("Order engine unavailable: %s", exc)
        raise HTTPException(status_code=503, detail="Order engine unavailable")


def _user_id(request: Request) -> str:
    return getattr(request.state, "user_id", "")


def _number(text: str, field: str) -> float:
    try:
        return parse_number(text, field)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def _existing(engine: OrderEngine, order_id: str) -> OrderSnapshot:
    order = engine.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return order


def _refresh_credit(request: Request, engine: OrderEngine, counterparty: str) -> None:
    """Give the engine the counterparty's latest credit position. Fails closed."""
    if not counterparty:
        return
    try:
        sync_credit(engine, _credit_db_path(request), counterparty)
    except Exception as exc:
        logger.error("Credit check failed for %s: %s (rejecting order)", counterparty, exc)
        raise HTTPException(
            status_code=503,
            detail=f"Credit check unavailable for {counterparty} — order rejected",
        )


def _refusal_status(reason: str) -> int:
    """Map the engine's refusal reason to an HTTP status."""
    if "lacks permission" in reason or reason == "User ID is required":
        return 403
    if reason.endswith("not found"):
        return 404
    if reason.startswith("Cannot ") or reason.endswith(("already exists", "already applied")):
        return 409  # wrong lifecycle state, or a duplicate
    return 422


def _submit(request: Request, engine: OrderEngine, message: OrderMessage) -> EngineResponse:
    """Send a message to the engine and return its response, whether applied or refused."""
    try:
        sync_permissions(engine, message.user_id)
    except Exception as exc:
        logger.error("Entitlements lookup failed for %s: %s (denying)", message.user_id, exc)
        raise HTTPException(status_code=503, detail="Entitlements check unavailable — request denied")
    try:
        return engine.submit(message)
    except (RuntimeError, TimeoutError) as exc:
        logger.error("Order engine did not answer %s: %s", type(message).__name__, exc)
        raise HTTPException(status_code=503, detail="Order engine unavailable")


def _apply(request: Request, engine: OrderEngine, message: OrderMessage) -> OrderResponse:
    """Submit a message and return the updated order, or raise the matching HTTP error."""
    response = _submit(request, engine, message)
    if not response.accepted or response.snapshot is None:
        raise HTTPException(status_code=_refusal_status(response.reason), detail=response.reason)
    return OrderResponse(**order_to_dict(response.snapshot))


def _fill_message(order: OrderSnapshot, user_id: str, quantity: float, price_text: str, fill_id: str) -> FillOrder:
    if price_text.strip():
        price = _number(price_text, "Fill price")
    elif order.details.price is not None:
        price = order.details.price
    else:
        raise HTTPException(status_code=400, detail="A fill on a Market order needs a price")
    if fill_id and not _FILL_ID.match(fill_id):
        raise HTTPException(status_code=400, detail="fill_id may only contain letters, digits, '.', '_' and '-'")
    return FillOrder(
        order_id=order.order_id,
        user_id=user_id,
        fill_id=fill_id or f"F{len(order.fill_ids) + 1}",
        quantity=quantity,
        price=price,
    )


# ---------------------------------------------------------------------------
# Order endpoints
# ---------------------------------------------------------------------------


def order_details_from_request(request_body: CreateOrderRequest) -> OrderDetails:
    """Convert and validate the order fields of a request, raising 400 if they are malformed."""
    if not request_body.instrument.strip():
        raise HTTPException(status_code=400, detail="Instrument must not be empty")
    if request_body.side not in ("Buy", "Sell"):
        raise HTTPException(status_code=400, detail="Side must be 'Buy' or 'Sell'")
    try:
        details = details_from_fields(**request_body.model_dump(include=set(CreateOrderRequest.model_fields)))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    invalid = validate_details(details)
    if invalid:
        raise HTTPException(status_code=400, detail=invalid)
    return details


def submit_new_order(request: Request, details: OrderDetails, *, order_id: str | None = None) -> OrderResponse:
    """Credit check and submit a new order as the requesting user.

    :raises HTTPException: 422 if a pre-trade check refuses it (the order is then on record as
        ``Rejected``), 403 if the user is not entitled, 503 if the engine or a check is unavailable.
    """
    engine = _engine(request)
    _refresh_credit(request, engine, details.counterparty)
    message = NewOrder(order_id=order_id or new_order_id(), user_id=_user_id(request), details=details)
    return _apply(request, engine, message)


@router.post("", response_model=OrderResponse, status_code=201)
def create_order(request_body: CreateOrderRequest, request: Request) -> OrderResponse:
    """Submit a new order.

    A malformed order is refused with 400 and nothing is recorded.  A well-formed order that
    fails the credit check is recorded as ``Rejected`` and refused with 422.
    """
    return submit_new_order(request, order_details_from_request(request_body))


@router.get("", response_model=list[OrderResponse])
def list_orders(request: Request) -> list[OrderResponse]:
    """List all orders, most recent first."""
    return [OrderResponse(**o) for o in orders_to_dicts(_engine(request))]


@router.get("/{order_id}", response_model=OrderResponse)
def get_order(order_id: str, request: Request) -> OrderResponse:
    """Get a single order by ID."""
    return OrderResponse(**order_to_dict(_existing(_engine(request), order_id)))


@router.patch("/{order_id}", response_model=OrderResponse)
def update_order(order_id: str, request_body: UpdateOrderRequest, request: Request) -> OrderResponse:
    """Move an order to a new status, or amend its quantity and price.

    Only the transitions the order lifecycle allows are applied; anything else is refused with 409.
    """
    body = request_body
    amending = body.quantity is not None or body.price is not None
    valid_statuses = {s.value for s in OrderStatus}
    if body.status is None and not amending:
        raise HTTPException(status_code=400, detail="Provide a status, or a quantity and/or price to amend")
    if body.status is not None and body.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {sorted(valid_statuses)}")
    if body.status is not None and amending:
        raise HTTPException(status_code=400, detail="Send an amendment separately from a status change")

    engine = _engine(request)
    order = _existing(engine, order_id)
    user_id = _user_id(request)
    message: OrderMessage

    if body.status is None:
        message = AmendOrder(
            order_id=order_id,
            user_id=user_id,
            quantity=None if body.quantity is None else _number(body.quantity, "Quantity"),
            price=None if body.price is None else _number(body.price, "Price"),
        )
        _refresh_credit(request, engine, order.details.counterparty)
    elif body.status == OrderStatus.PENDING.value:
        raise HTTPException(status_code=409, detail="An order cannot be moved back to 'Pending'")
    elif body.status == OrderStatus.WORKING.value:
        message = AcceptOrder(order_id=order_id, user_id=user_id)
    elif body.status == OrderStatus.REJECTED.value:
        message = RejectOrder(order_id=order_id, user_id=user_id, reason=body.reason)
    elif body.status == OrderStatus.CANCELLED.value:
        message = CancelOrder(order_id=order_id, user_id=user_id, reason=body.reason)
    else:
        message = _fill_to_status(order, body, user_id)

    return _apply(request, engine, message)


def _fill_to_status(order: OrderSnapshot, body: UpdateOrderRequest, user_id: str) -> FillOrder:
    """Build the fill that takes the order to the requested filled status and total filled quantity."""
    full = body.status == OrderStatus.FILLED.value
    if body.filled_quantity is None:
        if not full:
            raise HTTPException(status_code=400, detail="'Partially Filled' needs a filled_quantity")
        total = order.details.quantity
    else:
        total = _number(body.filled_quantity, "Filled quantity")

    if full and total != order.details.quantity:
        raise HTTPException(status_code=400, detail="'Filled' needs filled_quantity to equal the order quantity")
    if not full and total >= order.details.quantity:
        raise HTTPException(status_code=400, detail="'Partially Filled' needs filled_quantity below the order quantity")
    if total <= order.filled_quantity:
        raise HTTPException(
            status_code=400,
            detail=f"filled_quantity must exceed the {order.filled_quantity:g} already filled",
        )
    return _fill_message(order, user_id, total - order.filled_quantity, body.fill_price or "", "")


@router.post("/{order_id}/fills", response_model=OrderResponse)
def fill_order_endpoint(order_id: str, request_body: FillRequest, request: Request) -> OrderResponse:
    """Record one execution against a ``Working`` or ``Partially Filled`` order."""
    engine = _engine(request)
    order = _existing(engine, order_id)
    quantity = _number(request_body.quantity, "Quantity")
    message = _fill_message(order, _user_id(request), quantity, request_body.price, request_body.fill_id.strip())
    return _apply(request, engine, message)


@router.post("/{order_id}/cancel", response_model=OrderResponse)
def cancel_order_endpoint(order_id: str, request: Request) -> OrderResponse:
    """Cancel an open order. Any quantity already filled stays filled."""
    engine = _engine(request)
    _existing(engine, order_id)
    return _apply(request, engine, CancelOrder(order_id=order_id, user_id=_user_id(request)))


# ---------------------------------------------------------------------------
# Trades booked from fills
# ---------------------------------------------------------------------------


def _trade_responses(request: Request, **filters) -> list[TradeBookingResponse]:
    booker = get_trade_booker(request.app)
    delivering = getattr(request.app.state, "trade_publisher", None) is not None
    return [TradeBookingResponse(**booking_to_dict(b, delivery_enabled=delivering)) for b in booker.bookings(**filters)]


@router.get("/{order_id}/trades", response_model=list[TradeBookingResponse])
def list_order_trades(order_id: str, request: Request) -> list[TradeBookingResponse]:
    """List the trades created from an order's fills, with their booking and delivery status."""
    _existing(_engine(request), order_id)
    return _trade_responses(request, order_id=order_id)


@trades_router.get("", response_model=list[TradeBookingResponse])
def list_trades(request: Request, status: str | None = None, undelivered: bool = False) -> list[TradeBookingResponse]:
    """List every trade created from a fill, oldest first.

    ``status`` filters to ``Booked`` or ``Failed``; ``undelivered=true`` shows booked trades the
    downstream system has not yet acknowledged.
    """
    _engine(request)
    return _trade_responses(request, status=status, undelivered=undelivered)
