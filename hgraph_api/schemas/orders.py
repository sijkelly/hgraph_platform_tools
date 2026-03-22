"""Pydantic models for order API request/response serialisation."""

from typing import Optional

from pydantic import BaseModel

__all__ = (
    "CreateOrderRequest",
    "NLPParseRequest",
    "NLPParseResponse",
    "OrderField",
    "OrderResponse",
    "UpdateOrderRequest",
)


class OrderField(BaseModel):
    """A single parsed order field with confidence."""

    value: str
    confidence: float = 1.0


class NLPParseResponse(BaseModel):
    """Response from NLP order parsing — structured fields extracted from free text."""

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
    """Request to update an order's status."""

    status: str
    filled_quantity: str = ""


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
    created_at: str
    updated_at: str
