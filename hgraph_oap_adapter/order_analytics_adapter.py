"""
Adapter converting hg_oap order domain objects to data lake records.

Provides pure-function converters that transform
:class:`~hg_oap.orders.sales.lifecycle.OrderLifecycleEvent` and
:class:`~hg_oap.orders.sales.missed_reason.OrderOutcome` instances
into flat dictionaries suitable for ingestion into a data lake or
event store.

Also provides a factory function :func:`create_lifecycle_event` that
builds an :class:`OrderLifecycleEvent` from individual order parameters,
making it easy to emit lifecycle events from the order processing
pipeline.
"""

from datetime import datetime, timezone
from typing import Any, Dict
import uuid

from hg_oap.orders.sales.ev import EVCalculation, InstrumentCategory
from hg_oap.orders.sales.lifecycle import OrderLifecycleEvent, OrderLifecycleState
from hg_oap.orders.sales.missed_reason import OrderMissedReason, OrderOutcome
from hg_oap.units.quantity import Quantity

__all__ = (
    "order_to_lifecycle_dict",
    "order_outcome_to_dict",
    "create_lifecycle_event",
)


# ---------------------------------------------------------------------------
# Lifecycle event → flat dict
# ---------------------------------------------------------------------------


def order_to_lifecycle_dict(event: OrderLifecycleEvent) -> Dict[str, Any]:
    """Convert an :class:`OrderLifecycleEvent` to a flat dict for data lake.

    All enum values are serialised as their string value so the dict
    is directly JSON-serialisable.

    :param event: The lifecycle event to convert.
    :returns: A flat dictionary with string keys.
    """
    return {
        "event_id": event.event_id,
        "order_id": event.order_id,
        "timestamp": event.timestamp,
        "state": event.state.value,
        "salesperson_id": event.salesperson_id,
        "desk": event.desk,
        "instrument_category": event.instrument_category,
        "underlying_asset": event.underlying_asset,
        "counterparty": event.counterparty,
        "notional_qty": event.notional_qty,
        "notional_unit": event.notional_unit,
        "price": event.price,
        "price_currency": event.price_currency,
        "ev_amount": event.ev_amount,
        "ev_currency": event.ev_currency,
        "missed_reason": event.missed_reason,
        "missed_detail": event.missed_detail,
        "notes": event.notes,
    }


# ---------------------------------------------------------------------------
# OrderOutcome → flat dict
# ---------------------------------------------------------------------------


def order_outcome_to_dict(
    outcome: OrderOutcome,
    *,
    ev_calculation: EVCalculation | None = None,
    salesperson_id: str = "",
    desk: str = "",
) -> Dict[str, Any]:
    """Convert an :class:`OrderOutcome` to a flat dict for data lake.

    :param outcome: The order outcome to convert.
    :param ev_calculation: Optional EV calculation to include.
    :param salesperson_id: Salesperson identifier.
    :param desk: Trading desk identifier.
    :returns: A flat dictionary with string keys.
    """
    d: Dict[str, Any] = {
        "order_id": outcome.order_id,
        "status": outcome.status.value,
        "missed_reason": outcome.missed_reason.value if outcome.missed_reason is not None else "",
        "missed_detail": outcome.missed_detail,
        "filled_qty": outcome.filled_quantity.qty if outcome.filled_quantity is not None else 0.0,
        "filled_unit": str(outcome.filled_quantity.unit) if outcome.filled_quantity is not None else "",
        "filled_price": outcome.filled_price if outcome.filled_price is not None else 0.0,
        "filled_currency": outcome.filled_currency_symbol,
        "outcome_timestamp": outcome.outcome_timestamp,
        "salesperson_id": salesperson_id,
        "desk": desk,
    }

    if ev_calculation is not None:
        d["ev_amount"] = ev_calculation.ev_amount
        d["ev_currency"] = ev_calculation.ev_currency_symbol
        d["ev_instrument_category"] = ev_calculation.instrument_category.value
        d["ev_rate_per_unit"] = ev_calculation.ev_rate.rate_per_unit
    else:
        d["ev_amount"] = 0.0
        d["ev_currency"] = ""
        d["ev_instrument_category"] = ""
        d["ev_rate_per_unit"] = 0.0

    return d


# ---------------------------------------------------------------------------
# Factory: build OrderLifecycleEvent from parameters
# ---------------------------------------------------------------------------


def create_lifecycle_event(
    order_id: str,
    state: OrderLifecycleState,
    *,
    timestamp: str = "",
    salesperson_id: str = "",
    desk: str = "",
    instrument_category: InstrumentCategory | None = None,
    underlying_asset: str = "",
    counterparty: str = "",
    quantity: Quantity | None = None,
    price: float = 0.0,
    price_currency: str = "",
    ev_calculation: EVCalculation | None = None,
    missed_reason: OrderMissedReason | None = None,
    missed_detail: str = "",
    notes: str = "",
) -> OrderLifecycleEvent:
    """Build an :class:`OrderLifecycleEvent` from order parameters.

    Generates a unique *event_id* and fills in default values where
    the caller has not supplied them.

    :param order_id: The order identifier.
    :param state: The lifecycle state to record.
    :param timestamp: ISO 8601 timestamp (auto-generated if empty).
    :param salesperson_id: Salesperson identifier.
    :param desk: Trading desk identifier.
    :param instrument_category: Instrument family enum value.
    :param underlying_asset: Underlying asset symbol.
    :param counterparty: Counterparty identifier.
    :param quantity: Order notional quantity.
    :param price: Order price value.
    :param price_currency: Price currency symbol.
    :param ev_calculation: Optional computed EV to embed.
    :param missed_reason: Missed reason (for MISSED/EXPIRED states).
    :param missed_detail: Free-text detail.
    :param notes: Additional notes.
    :returns: A new :class:`OrderLifecycleEvent`.
    """
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()

    event_id = f"EVT-{uuid.uuid4().hex[:12].upper()}"

    return OrderLifecycleEvent(
        event_id=event_id,
        order_id=order_id,
        timestamp=timestamp,
        state=state,
        salesperson_id=salesperson_id,
        desk=desk,
        instrument_category=instrument_category.value if instrument_category is not None else "",
        underlying_asset=underlying_asset,
        counterparty=counterparty,
        notional_qty=quantity.qty if quantity is not None else 0.0,
        notional_unit=str(quantity.unit) if quantity is not None else "",
        price=price,
        price_currency=price_currency,
        ev_amount=ev_calculation.ev_amount if ev_calculation is not None else 0.0,
        ev_currency=ev_calculation.ev_currency_symbol if ev_calculation is not None else "",
        missed_reason=missed_reason.value if missed_reason is not None else "",
        missed_detail=missed_detail,
        notes=notes,
    )
