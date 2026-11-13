"""Pydantic models for order API request/response serialisation."""

from typing import Optional

from pydantic import BaseModel

__all__ = (
    "CreateOrderRequest",
    "FillRequest",
    "NLPParseRequest",
    "NLPParseResponse",
    "OrderField",
    "OrderResponse",
    "ProposalDecisionRequest",
    "ProposalResponse",
    "ProposeOrderRequest",
    "TradeBookingResponse",
    "UpdateOrderRequest",
)


class OrderField(BaseModel):
    """A single parsed order field with confidence."""

    value: str
    confidence: float = 1.0


class NLPParseResponse(BaseModel):
    """Response from NLP order parsing — structured fields extracted from free text.

    ``warnings`` lists what a reviewer must fix or accept before the order would submit and book,
    such as an instrument that is not in the product reference data.
    """

    instrument: Optional[OrderField] = None
    side: Optional[OrderField] = None
    quantity: Optional[OrderField] = None
    unit: Optional[OrderField] = None
    price: Optional[OrderField] = None
    counterparty: Optional[OrderField] = None
    delivery_period: Optional[OrderField] = None
    order_type: Optional[OrderField] = None
    portfolio: Optional[OrderField] = None
    book: Optional[OrderField] = None
    notes: Optional[str] = None
    warnings: list[str] = []
    raw_text: str


class NLPParseRequest(BaseModel):
    """Request to parse free text into structured order fields."""

    text: str
    context: Optional[str] = None


class CreateOrderRequest(BaseModel):
    """Request to create a new order."""

    instrument: str
    side: str
    quantity: str
    unit: str = ""
    price: str = ""
    counterparty: str = ""
    delivery_period: str = ""
    order_type: str = "Limit"
    portfolio: str = ""
    book: str = ""


class UpdateOrderRequest(BaseModel):
    """Request to move an order to a new status, or to amend it.

    With ``status``:

    * ``Working`` accepts a ``Pending`` order; ``Rejected`` declines it.
    * ``Cancelled`` cancels an open order.
    * ``Partially Filled`` / ``Filled`` record a fill.  ``filled_quantity`` is the order's
      total filled quantity after the fill (optional for ``Filled``).  ``fill_price``
      defaults to the order's limit price and is required for a market order.

    Without ``status``, ``quantity`` and/or ``price`` amend an open order.
    """

    status: Optional[str] = None
    filled_quantity: Optional[str] = None
    fill_price: Optional[str] = None
    quantity: Optional[str] = None
    price: Optional[str] = None
    reason: str = ""


class FillRequest(BaseModel):
    """One execution against an order.

    ``price`` defaults to the order's limit price.  ``fill_id`` must be unique per order;
    resending the same ``fill_id`` is refused, so a retry cannot double-count.
    """

    quantity: str
    price: str = ""
    fill_id: str = ""


class OrderResponse(BaseModel):
    """Full order representation."""

    order_id: str
    instrument: str
    side: str
    quantity: str
    unit: str
    price: str
    counterparty: str
    delivery_period: str
    order_type: str
    portfolio: str
    book: str
    status: str
    filled_quantity: str
    average_price: str = ""
    reason: str = ""
    version: int = 0
    created_by: str = ""
    updated_by: str = ""
    created_at: str
    updated_at: str


class TradeBookingResponse(BaseModel):
    """A trade created from one fill, and whether it reached the booking pipeline.

    ``status`` is ``Booked`` or ``Failed``; ``detail`` is the booked file or the reason it failed.

    ``delivery_status`` is ``Sent`` once the downstream system acknowledged the trade, ``Pending``
    while it is waiting or being retried (``delivery_detail`` then holds the latest error), and
    empty when delivery is not configured.
    """

    trade_id: str
    order_id: str
    fill_id: str
    quantity: str
    price: str
    executed_at: str
    status: str
    detail: str
    updated_at: str
    delivery_status: str = ""
    delivery_attempts: int = 0
    delivery_detail: str = ""
    delivery_updated_at: str = ""


class ProposeOrderRequest(CreateOrderRequest):
    """A drafted order for a person to confirm. Nothing is submitted until they do.

    ``source_text`` is the message the order was drafted from and ``rationale`` is the
    drafter's explanation; both are shown to the person deciding.
    """

    source_text: str = ""
    rationale: str = ""


class ProposalDecisionRequest(BaseModel):
    """Optional reason when dismissing a proposal."""

    reason: str = ""


class ProposalResponse(BaseModel):
    """An order proposal and what was decided about it.

    ``status`` is ``Proposed``, ``Confirmed`` or ``Dismissed``.  ``order_id`` is set once confirmed.
    """

    proposal_id: str
    status: str
    order: CreateOrderRequest
    proposed_by: str
    source_text: str
    rationale: str
    created_at: str
    decided_by: str
    decided_at: str
    order_id: str
    reason: str
