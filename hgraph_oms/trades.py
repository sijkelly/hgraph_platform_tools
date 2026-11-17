"""
From fills to trades.

Every accepted fill is an execution, and every execution is a trade that the
ETRM needs.  This module holds the pure steps of that hand-off:

* :class:`TradeTicket` — what was executed, in the order's own terms.  A ticket
  can always be produced from a fill.
* :func:`ticket_to_trade_data` — the ticket in the input format of the booking
  pipeline (:mod:`hgraph_trade.hgraph_trade_booker`).  This needs reference data
  (which product the instrument name refers to, what dates the delivery period
  covers), so it can fail; the caller records the failure rather than guessing.

Writing the trade out is I/O and lives in :mod:`hgraph_oms.booking`.
"""

import calendar
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from hgraph_oms.event_log import LoggedEvent
from hgraph_oms.model import FillOrder, OrderResponse, OrderSnapshot
from hgraph_oms.state_machine import apply_message

__all__ = (
    "TradeTicket",
    "ProductSpec",
    "DEFAULT_PRODUCTS",
    "ticket_for_fill",
    "tickets_from_responses",
    "tickets_from_events",
    "parse_delivery_period",
    "find_product",
    "ticket_to_trade_data",
)

# Trade IDs become file names and downstream keys, so keep them to a safe alphabet
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


@dataclass(frozen=True, kw_only=True)
class TradeTicket:
    """One execution against an order.

    :param trade_id: ``<order_id>-<fill_id>``; unique because fill IDs are unique per order.
    :param trader: The user who created the order.
    :param executed_at: When the order book processed the fill (UTC).
    """

    trade_id: str
    order_id: str
    fill_id: str
    instrument: str
    side: str
    quantity: float
    price: float
    unit: str = ""
    counterparty: str = ""
    delivery_period: str = ""
    portfolio: str = ""
    book: str = ""
    trader: str = ""
    executed_at: datetime | None = None


@dataclass(frozen=True, kw_only=True)
class ProductSpec:
    """Reference data that turns an order's instrument name into a bookable product.

    :param pricing_instrument: Key into the booking pipeline's instrument mappings (e.g. ``outright``).
    :param commodity_reference: The floating price index the swap settles against.
    """

    name: str
    pricing_instrument: str
    commodity_reference: str
    currency: str
    asset: str = ""


# Demo reference data.  A deployment replaces this with its own product master.
DEFAULT_PRODUCTS: tuple[ProductSpec, ...] = (
    ProductSpec(
        name="Henry Hub Natural Gas",
        pricing_instrument="outright",
        commodity_reference="NATURAL GAS-HENRY HUB-NYMEX",
        currency="USD",
        asset="NaturalGas",
    ),
    ProductSpec(
        name="WTI Crude Oil",
        pricing_instrument="outright",
        commodity_reference="OIL-WTI-NYMEX",
        currency="USD",
        asset="Oil",
    ),
    ProductSpec(
        name="TTF Natural Gas",
        pricing_instrument="outright",
        commodity_reference="NATURAL GAS-TTF-ICE ENDEX",
        currency="EUR",
        asset="NaturalGas",
    ),
    ProductSpec(
        name="PJM Western Hub Power",
        pricing_instrument="outright",
        commodity_reference="ELECTRICITY-PJM-WESTERN HUB-REAL TIME",
        currency="USD",
        asset="Electricity",
    ),
)


# ---------------------------------------------------------------------------
# Fills -> tickets
# ---------------------------------------------------------------------------


def ticket_for_fill(order: OrderSnapshot, fill: FillOrder, executed_at: datetime | None = None) -> TradeTicket:
    """Return the trade ticket for an accepted fill.

    :param order: The order the fill was applied to (before or after the fill; only its terms are used).
    """
    details = order.details
    return TradeTicket(
        trade_id=f"{order.order_id}-{fill.fill_id}",
        order_id=order.order_id,
        fill_id=fill.fill_id,
        instrument=details.instrument,
        side=details.side,
        quantity=fill.quantity,
        price=fill.price,
        unit=details.unit,
        counterparty=details.counterparty,
        delivery_period=details.delivery_period,
        portfolio=details.portfolio,
        book=details.book,
        trader=order.created_by,
        executed_at=executed_at,
    )


def tickets_from_responses(responses: Iterable[OrderResponse]) -> tuple[TradeTicket, ...]:
    """Return one ticket per accepted fill among the order book's responses."""
    return tuple(
        ticket_for_fill(r.snapshot, r.message, r.processed_at)
        for r in responses
        if r.accepted and isinstance(r.message, FillOrder) and r.snapshot is not None
    )


def tickets_from_events(events: Iterable[LoggedEvent | tuple]) -> tuple[TradeTicket, ...]:
    """Return the ticket of every accepted fill in the event log, in order.

    Gives the same tickets the live graph produced, so trades that were logged but not
    booked before a crash can be found and booked on restart.
    """
    orders: dict[str, OrderSnapshot] = {}
    tickets: list[TradeTicket] = []
    for event in events:
        message, accepted, _reason, at = LoggedEvent(*event)
        if not accepted:
            continue
        updated, failure = apply_message(orders.get(message.order_id), message, at=at)
        if failure or updated is None:
            continue
        orders[updated.order_id] = updated
        if isinstance(message, FillOrder):
            tickets.append(ticket_for_fill(updated, message, at))
    return tuple(tickets)


# ---------------------------------------------------------------------------
# Tickets -> booking pipeline input
# ---------------------------------------------------------------------------

_MONTHS = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
_MONTHS.update({name.lower(): i for i, name in enumerate(calendar.month_abbr) if name})

_MONTH_YEAR = re.compile(r"^([A-Za-z]+)[\s-]+(\d{4})$")
_ISO_MONTH = re.compile(r"^(\d{4})-(\d{2})$")
_QUARTER = re.compile(r"^Q([1-4])[\s-]+(\d{4})$", re.IGNORECASE)
_YEAR = re.compile(r"^(?:Cal(?:endar)?[\s-]*)?(\d{4})$", re.IGNORECASE)


def _month_range(year: int, first_month: int, last_month: int) -> tuple[date, date]:
    return date(year, first_month, 1), date(year, last_month, calendar.monthrange(year, last_month)[1])


def parse_delivery_period(text: str) -> tuple[date, date]:
    """Return the first and last day of a delivery period.

    Understands a month (``January 2027``, ``Jan 2027``, ``2027-01``), a quarter (``Q3 2026``)
    and a calendar year (``Cal 2027``, ``2027``).

    :raises ValueError: If the period is empty or not in one of those forms.
    """
    period = text.strip()
    if not period:
        raise ValueError("Delivery period is required to book a trade")

    if match := _MONTH_YEAR.match(period):
        month = _MONTHS.get(match.group(1).lower())
        if month:
            return _month_range(int(match.group(2)), month, month)
    if match := _ISO_MONTH.match(period):
        month = int(match.group(2))
        if 1 <= month <= 12:
            return _month_range(int(match.group(1)), month, month)
    if match := _QUARTER.match(period):
        last_month = int(match.group(1)) * 3
        return _month_range(int(match.group(2)), last_month - 2, last_month)
    if match := _YEAR.match(period):
        return _month_range(int(match.group(1)), 1, 12)

    raise ValueError(f"Delivery period '{text}' is not understood (use e.g. 'January 2027', 'Q3 2026', 'Cal 2027')")


def find_product(instrument: str, products: Iterable[ProductSpec] = DEFAULT_PRODUCTS) -> ProductSpec:
    """Return the product an order's instrument name refers to (case-insensitive).

    :raises ValueError: If no product has that name.
    """
    wanted = instrument.strip().lower()
    for product in products:
        if product.name.lower() == wanted:
            return product
    raise ValueError(f"Instrument '{instrument}' is not in the product reference data")


def ticket_to_trade_data(
    ticket: TradeTicket,
    *,
    internal_party: str,
    products: Iterable[ProductSpec] = DEFAULT_PRODUCTS,
) -> dict[str, Any]:
    """Convert a ticket into the booking pipeline's trade input.

    Orders are booked as fixed-for-floating swaps on the product's price index over the
    delivery period: the buyer pays the fill price and receives the index.

    :param internal_party: Our own legal entity on the trade.
    :raises ValueError: If the ticket cannot be booked as it stands — unknown instrument,
        delivery period not understood, no counterparty, or an unsafe trade ID.
    """
    if not _SAFE_ID.match(ticket.trade_id):
        raise ValueError(f"Trade ID '{ticket.trade_id}' may only contain letters, digits, '.', '_' and '-'")
    if not ticket.counterparty:
        raise ValueError("A counterparty is required to book a trade")
    if ticket.side not in ("Buy", "Sell"):
        raise ValueError(f"Side '{ticket.side}' is not bookable")

    product = find_product(ticket.instrument, products)
    start, end = parse_delivery_period(ticket.delivery_period)
    trade_date = ticket.executed_at.date().isoformat() if ticket.executed_at else ""

    buying = ticket.side == "Buy"
    fixed_payer, fixed_receiver = (
        (internal_party, ticket.counterparty) if buying else (ticket.counterparty, internal_party)
    )
    leg_quantity: Mapping[str, Any] = {
        "quantityUnit": ticket.unit,
        "quantityFrequency": "Term",
        "quantity": ticket.quantity,
        "totalNotionalQuantity": ticket.quantity,
    }

    return {
        "tradeType": "newTrade",
        "instrument": product.pricing_instrument,
        "trade_id": ticket.trade_id,
        "order_id": ticket.order_id,
        "fill_id": ticket.fill_id,
        "trade_date": trade_date,
        "counterparty": {"internal": internal_party, "external": ticket.counterparty},
        "portfolio": {"internal": ticket.portfolio, "external": ""},
        "book": ticket.book,
        "traders": {"internal": ticket.trader, "external": ""},
        "buy_sell": ticket.side.lower(),
        "effective_date": start.isoformat(),
        "termination_date": end.isoformat(),
        "commodity": product.commodity_reference,
        "asset": product.asset,
        "qty": ticket.quantity,
        "unit": ticket.unit,
        "price": ticket.price,
        "currency": product.currency,
        "fixed_leg_price": ticket.price,
        "float_leg_reference": product.commodity_reference,
        # The keys the swap builder reads for the economics
        "effectiveDate.unadjustedDate": start.isoformat(),
        "terminationDate.unadjustedDate": end.isoformat(),
        "settlementCurrency": product.currency,
        "fixedLeg.payerPartyReference": fixed_payer,
        "fixedLeg.receiverPartyReference": fixed_receiver,
        "fixedLeg.price": ticket.price,
        "fixedLeg.priceCurrency": product.currency,
        "fixedLeg.priceUnit": ticket.unit,
        **{f"fixedLeg.{key}": value for key, value in leg_quantity.items()},
        "floatingLeg.payerPartyReference": fixed_receiver,
        "floatingLeg.receiverPartyReference": fixed_payer,
        "floatingLeg.instrumentId": product.commodity_reference,
        "floatingLeg.specifiedPrice": "Settlement",
        **{f"floatingLeg.{key}": value for key, value in leg_quantity.items()},
    }
