"""Tests for turning fills into trade tickets and booking-pipeline input."""

from dataclasses import replace
from datetime import date, datetime, timezone

import pytest

from hgraph_oms.model import AcceptOrder, CancelOrder, FillOrder, NewOrder, OrderDetails, OrderResponse
from hgraph_oms.state_machine import apply_message
from hgraph_oms.trades import (
    ProductSpec,
    TradeTicket,
    find_product,
    parse_delivery_period,
    ticket_to_trade_data,
    tickets_from_events,
    tickets_from_responses,
)
from hgraph_trade.hgraph_trade_booker.trade_mapper import map_trade_to_model

AT = datetime(2026, 11, 13, 9, 30, tzinfo=timezone.utc)

DETAILS = OrderDetails(
    instrument="Henry Hub Natural Gas",
    side="Buy",
    quantity=50_000.0,
    unit="MMBtu",
    price=3.45,
    counterparty="ACME",
    delivery_period="January 2027",
    portfolio="GAS-TRADING",
    book="GAS-BOOK-1",
)

TICKET = TradeTicket(
    trade_id="ORD-1-F1",
    order_id="ORD-1",
    fill_id="F1",
    instrument="Henry Hub Natural Gas",
    side="Buy",
    quantity=20_000.0,
    price=3.40,
    unit="MMBtu",
    counterparty="ACME",
    delivery_period="January 2027",
    portfolio="GAS-TRADING",
    book="GAS-BOOK-1",
    trader="trader1",
    executed_at=AT,
)

_EVENTS = [
    (NewOrder(order_id="ORD-1", user_id="trader1", details=DETAILS), True, "", AT),
    (
        FillOrder(order_id="ORD-1", fill_id="F0", quantity=1.0, price=3.0),
        False,
        "Cannot fill order in 'Pending' state",
        AT,
    ),
    (AcceptOrder(order_id="ORD-1", user_id="desk"), True, "", AT),
    (FillOrder(order_id="ORD-1", user_id="desk", fill_id="F1", quantity=20_000.0, price=3.40), True, "", AT),
    (CancelOrder(order_id="ORD-1"), True, "", AT),
]


def _responses() -> list[OrderResponse]:
    """Run ``_EVENTS`` through the state machine as the order book would."""
    order, responses = None, []
    for message, _accepted, _reason, at in _EVENTS:
        updated, reason = apply_message(order, message, at=at)
        order = updated if updated is not None else order
        responses.append(
            OrderResponse(message=message, accepted=not reason, reason=reason, snapshot=updated, processed_at=at)
        )
    return responses


# ---------- fills -> tickets ----------


def test_only_accepted_fills_become_tickets():
    assert tickets_from_responses(_responses()) == (TICKET,)


def test_event_log_yields_the_same_tickets_as_the_live_responses():
    assert tickets_from_events(_EVENTS) == tickets_from_responses(_responses())


# ---------- delivery periods ----------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("January 2027", (date(2027, 1, 1), date(2027, 1, 31))),
        ("feb 2028", (date(2028, 2, 1), date(2028, 2, 29))),  # leap year
        ("2026-04", (date(2026, 4, 1), date(2026, 4, 30))),
        ("Q3 2026", (date(2026, 7, 1), date(2026, 9, 30))),
        ("q1-2027", (date(2027, 1, 1), date(2027, 3, 31))),
        ("Cal 2027", (date(2027, 1, 1), date(2027, 12, 31))),
        ("Calendar 2027", (date(2027, 1, 1), date(2027, 12, 31))),
        (" 2027 ", (date(2027, 1, 1), date(2027, 12, 31))),
    ],
)
def test_parse_delivery_period(text, expected):
    assert parse_delivery_period(text) == expected


@pytest.mark.parametrize("text", ["", "next winter", "Smarch 2027", "2027-13", "Q5 2027"])
def test_parse_delivery_period_rejects_what_it_does_not_understand(text):
    with pytest.raises(ValueError, match="[Dd]elivery period"):
        parse_delivery_period(text)


# ---------- products ----------


def test_find_product_is_case_insensitive():
    assert find_product(" henry hub natural gas ").commodity_reference == "NATURAL GAS-HENRY HUB-NYMEX"


def test_find_product_uses_supplied_reference_data():
    brent = ProductSpec(
        name="Brent", pricing_instrument="outright", commodity_reference="OIL-BRENT-IPE", currency="USD"
    )
    assert find_product("brent", [brent]) is brent
    with pytest.raises(ValueError, match="not in the product reference data"):
        find_product("Henry Hub Natural Gas", [brent])


# ---------- tickets -> booking pipeline ----------


def test_ticket_to_trade_data_carries_the_fill_not_the_order():
    trade = ticket_to_trade_data(TICKET, internal_party="OURCO")

    assert trade["trade_id"] == "ORD-1-F1"
    assert (trade["qty"], trade["price"]) == (20_000.0, 3.40)  # the fill, not the 50,000 @ 3.45 order
    assert trade["trade_date"] == "2026-11-13"
    assert (trade["effective_date"], trade["termination_date"]) == ("2027-01-01", "2027-01-31")
    assert trade["counterparty"] == {"internal": "OURCO", "external": "ACME"}
    assert trade["traders"]["internal"] == "trader1"
    assert trade["currency"] == "USD"


@pytest.mark.parametrize(
    "side,fixed_payer,fixed_receiver",
    [("Buy", "OURCO", "ACME"), ("Sell", "ACME", "OURCO")],
)
def test_buyer_pays_fixed_and_receives_floating(side, fixed_payer, fixed_receiver):
    trade = ticket_to_trade_data(replace(TICKET, side=side), internal_party="OURCO")

    assert trade["fixedLeg.payerPartyReference"] == trade["floatingLeg.receiverPartyReference"] == fixed_payer
    assert trade["fixedLeg.receiverPartyReference"] == trade["floatingLeg.payerPartyReference"] == fixed_receiver


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"instrument": "Unobtainium"}, "not in the product reference data"),
        ({"delivery_period": "soon"}, "not understood"),
        ({"delivery_period": ""}, "Delivery period is required"),
        ({"counterparty": ""}, "counterparty is required"),
        ({"trade_id": "../../etc/passwd"}, "may only contain"),
    ],
)
def test_ticket_that_cannot_be_booked_says_why(changes, expected):
    with pytest.raises(ValueError, match=expected):
        ticket_to_trade_data(replace(TICKET, **changes), internal_party="OURCO")


def test_trade_data_maps_to_a_swap_with_its_economics_filled_in():
    """The pipeline's mapper must find the fields it needs, not emit an empty swap."""
    (message,) = map_trade_to_model(ticket_to_trade_data(TICKET, internal_party="OURCO"), fail_fast=True)

    header = message["tradeHeader"]["tradeHeader"]
    assert header["partyTradeIdentifier"]["tradeId"] == "ORD-1-F1"
    assert header["parties"] == [{"internalParty": "OURCO"}, {"externalParty": "ACME"}]

    swap = message["tradeEconomics"]["commoditySwap"]
    assert swap["effectiveDate"]["adjustableDate"]["unadjustedDate"] == "2027-01-01"
    assert swap["terminationDate"]["adjustableDate"]["unadjustedDate"] == "2027-01-31"
    assert swap["settlementCurrency"] == "USD"
    assert swap["fixedLeg"]["fixedPrice"] == {"price": 3.40, "priceCurrency": "USD", "priceUnit": "MMBtu"}
    assert swap["fixedLeg"]["notionalQuantity"]["quantity"] == 20_000.0
    assert swap["floatingLeg"]["commodity"]["instrumentId"] == "NATURAL GAS-HENRY HUB-NYMEX"
    assert swap["floatingLeg"]["payerPartyReference"] == {"href": "ACME"}
