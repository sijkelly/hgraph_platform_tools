"""Tests for the order analytics data lake adapter."""

import pytest

from hg_oap.orders.sales.ev import (
    EVCalculation,
    EVRate,
    InstrumentCategory,
    calculate_ev,
)
from hg_oap.orders.sales.lifecycle import OrderLifecycleEvent, OrderLifecycleState
from hg_oap.orders.sales.missed_reason import (
    OrderMissedReason,
    OrderOutcome,
    OrderOutcomeStatus,
)
from hg_oap.units.default_unit_system import U
from hg_oap.units.quantity import Quantity
from hgraph_oap_adapter.order_analytics_adapter import (
    create_lifecycle_event,
    order_outcome_to_dict,
    order_to_lifecycle_dict,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def gas_qty():
    return Quantity(qty=10000.0, unit=U.MMBtu)


@pytest.fixture()
def power_qty():
    return Quantity(qty=50000.0, unit=U.kWh)


@pytest.fixture()
def gas_ev_rate():
    return EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
        currency_symbol="USD",
    )


@pytest.fixture()
def gas_ev_calculation(gas_qty, gas_ev_rate):
    return calculate_ev("ORD-100", InstrumentCategory.FIXED_FLOAT_SWAP, gas_qty, gas_ev_rate)


@pytest.fixture()
def received_event():
    return OrderLifecycleEvent(
        event_id="EVT-001",
        order_id="ORD-100",
        timestamp="2026-03-07T09:00:00Z",
        state=OrderLifecycleState.RECEIVED,
        salesperson_id="SP-100",
        desk="Gas-South",
        instrument_category="FixedFloatSwap",
        underlying_asset="HENRY_HUB",
        counterparty="ACME-ENERGY",
        notional_qty=10000.0,
        notional_unit="MMBtu",
        price=3.50,
        price_currency="USD",
        ev_amount=200.0,
        ev_currency="USD",
    )


@pytest.fixture()
def filled_outcome(gas_qty):
    return OrderOutcome(
        order_id="ORD-100",
        status=OrderOutcomeStatus.FILLED,
        filled_quantity=gas_qty,
        filled_price=3.50,
        filled_currency_symbol="USD",
        outcome_timestamp="2026-03-07T10:00:00Z",
    )


@pytest.fixture()
def missed_outcome():
    return OrderOutcome(
        order_id="ORD-200",
        status=OrderOutcomeStatus.MISSED,
        missed_reason=OrderMissedReason.CLIENT_TRADED_AWAY,
        missed_detail="Client went to competitor",
        outcome_timestamp="2026-03-07T11:00:00Z",
    )


# ---------------------------------------------------------------------------
# order_to_lifecycle_dict
# ---------------------------------------------------------------------------


def test_lifecycle_dict_keys(received_event):
    d = order_to_lifecycle_dict(received_event)
    expected_keys = {
        "event_id", "order_id", "timestamp", "state", "salesperson_id",
        "desk", "instrument_category", "underlying_asset", "counterparty",
        "notional_qty", "notional_unit", "price", "price_currency",
        "ev_amount", "ev_currency", "missed_reason", "missed_detail", "notes",
    }
    assert set(d.keys()) == expected_keys


def test_lifecycle_dict_values(received_event):
    d = order_to_lifecycle_dict(received_event)
    assert d["event_id"] == "EVT-001"
    assert d["order_id"] == "ORD-100"
    assert d["state"] == "Received"
    assert d["salesperson_id"] == "SP-100"
    assert d["desk"] == "Gas-South"
    assert d["instrument_category"] == "FixedFloatSwap"
    assert d["underlying_asset"] == "HENRY_HUB"
    assert d["counterparty"] == "ACME-ENERGY"
    assert d["notional_qty"] == 10000.0
    assert d["price"] == 3.50
    assert d["ev_amount"] == 200.0


def test_lifecycle_dict_state_serialised_as_string(received_event):
    d = order_to_lifecycle_dict(received_event)
    assert isinstance(d["state"], str)


def test_lifecycle_dict_defaults():
    event = OrderLifecycleEvent(
        event_id="EVT-002",
        order_id="ORD-200",
        timestamp="2026-03-07T12:00:00Z",
        state=OrderLifecycleState.WORKING,
    )
    d = order_to_lifecycle_dict(event)
    assert d["salesperson_id"] == ""
    assert d["desk"] == ""
    assert d["ev_amount"] == 0.0
    assert d["missed_reason"] == ""


# ---------------------------------------------------------------------------
# order_outcome_to_dict
# ---------------------------------------------------------------------------


def test_outcome_dict_filled(filled_outcome):
    d = order_outcome_to_dict(filled_outcome, salesperson_id="SP-100", desk="Gas-South")
    assert d["order_id"] == "ORD-100"
    assert d["status"] == "Filled"
    assert d["filled_qty"] == 10000.0
    assert d["filled_price"] == 3.50
    assert d["filled_currency"] == "USD"
    assert d["salesperson_id"] == "SP-100"
    assert d["desk"] == "Gas-South"
    assert d["missed_reason"] == ""


def test_outcome_dict_missed(missed_outcome):
    d = order_outcome_to_dict(missed_outcome)
    assert d["status"] == "Missed"
    assert d["missed_reason"] == "Client traded away"
    assert d["missed_detail"] == "Client went to competitor"
    assert d["filled_qty"] == 0.0
    assert d["filled_unit"] == ""


def test_outcome_dict_with_ev(filled_outcome, gas_ev_calculation):
    d = order_outcome_to_dict(filled_outcome, ev_calculation=gas_ev_calculation)
    assert d["ev_amount"] == pytest.approx(200.0)
    assert d["ev_currency"] == "USD"
    assert d["ev_instrument_category"] == "FixedFloatSwap"
    assert d["ev_rate_per_unit"] == pytest.approx(0.02)


def test_outcome_dict_without_ev(filled_outcome):
    d = order_outcome_to_dict(filled_outcome)
    assert d["ev_amount"] == 0.0
    assert d["ev_currency"] == ""
    assert d["ev_instrument_category"] == ""
    assert d["ev_rate_per_unit"] == 0.0


def test_outcome_dict_all_keys_are_strings():
    outcome = OrderOutcome(
        order_id="ORD-300",
        status=OrderOutcomeStatus.CANCELLED,
    )
    d = order_outcome_to_dict(outcome)
    assert all(isinstance(k, str) for k in d.keys())


def test_outcome_dict_cancelled():
    outcome = OrderOutcome(
        order_id="ORD-400",
        status=OrderOutcomeStatus.CANCELLED,
        outcome_timestamp="2026-03-07T14:00:00Z",
    )
    d = order_outcome_to_dict(outcome)
    assert d["status"] == "Cancelled"
    assert d["missed_reason"] == ""
    assert d["outcome_timestamp"] == "2026-03-07T14:00:00Z"


# ---------------------------------------------------------------------------
# create_lifecycle_event
# ---------------------------------------------------------------------------


def test_create_lifecycle_event_basic():
    event = create_lifecycle_event(
        "ORD-100",
        OrderLifecycleState.RECEIVED,
        timestamp="2026-03-07T09:00:00Z",
        salesperson_id="SP-100",
        desk="Gas-South",
    )
    assert isinstance(event, OrderLifecycleEvent)
    assert event.order_id == "ORD-100"
    assert event.state == OrderLifecycleState.RECEIVED
    assert event.salesperson_id == "SP-100"
    assert event.desk == "Gas-South"
    assert event.event_id.startswith("EVT-")


def test_create_lifecycle_event_auto_timestamp():
    event = create_lifecycle_event("ORD-200", OrderLifecycleState.WORKING)
    assert event.timestamp != ""
    assert "T" in event.timestamp


def test_create_lifecycle_event_auto_event_id():
    event = create_lifecycle_event("ORD-300", OrderLifecycleState.FILLED)
    assert event.event_id.startswith("EVT-")
    assert len(event.event_id) == 16  # "EVT-" + 12 hex chars


def test_create_lifecycle_event_unique_ids():
    e1 = create_lifecycle_event("ORD-400", OrderLifecycleState.RECEIVED)
    e2 = create_lifecycle_event("ORD-400", OrderLifecycleState.WORKING)
    assert e1.event_id != e2.event_id


def test_create_lifecycle_event_with_quantity(gas_qty):
    event = create_lifecycle_event(
        "ORD-500",
        OrderLifecycleState.WORKING,
        quantity=gas_qty,
    )
    assert event.notional_qty == 10000.0
    assert event.notional_unit != ""


def test_create_lifecycle_event_with_instrument_category():
    event = create_lifecycle_event(
        "ORD-600",
        OrderLifecycleState.RECEIVED,
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
    )
    assert event.instrument_category == "FixedFloatSwap"


def test_create_lifecycle_event_with_ev(gas_ev_calculation):
    event = create_lifecycle_event(
        "ORD-700",
        OrderLifecycleState.FILLED,
        ev_calculation=gas_ev_calculation,
    )
    assert event.ev_amount == pytest.approx(200.0)
    assert event.ev_currency == "USD"


def test_create_lifecycle_event_missed(gas_qty):
    event = create_lifecycle_event(
        "ORD-800",
        OrderLifecycleState.MISSED,
        missed_reason=OrderMissedReason.MARKET_TOO_FAR,
        missed_detail="Bid was $0.15 away",
        quantity=gas_qty,
    )
    assert event.state == OrderLifecycleState.MISSED
    assert event.missed_reason == "Market too far away"
    assert event.missed_detail == "Bid was $0.15 away"


def test_create_lifecycle_event_defaults():
    event = create_lifecycle_event("ORD-900", OrderLifecycleState.ACKNOWLEDGED)
    assert event.salesperson_id == ""
    assert event.desk == ""
    assert event.instrument_category == ""
    assert event.underlying_asset == ""
    assert event.counterparty == ""
    assert event.notional_qty == 0.0
    assert event.notional_unit == ""
    assert event.price == 0.0
    assert event.ev_amount == 0.0
    assert event.missed_reason == ""
    assert event.notes == ""


def test_create_lifecycle_event_with_notes():
    event = create_lifecycle_event(
        "ORD-1000",
        OrderLifecycleState.WORKING,
        notes="Urgent fill requested by COB",
    )
    assert event.notes == "Urgent fill requested by COB"


def test_create_lifecycle_event_full_params(gas_qty, gas_ev_calculation):
    event = create_lifecycle_event(
        "ORD-FULL",
        OrderLifecycleState.FILLED,
        timestamp="2026-03-07T15:00:00Z",
        salesperson_id="SP-999",
        desk="Power-East",
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        underlying_asset="PJM-WEST",
        counterparty="MEGAWATT-LLC",
        quantity=gas_qty,
        price=3.50,
        price_currency="USD",
        ev_calculation=gas_ev_calculation,
        notes="Large block trade",
    )
    assert event.order_id == "ORD-FULL"
    assert event.state == OrderLifecycleState.FILLED
    assert event.salesperson_id == "SP-999"
    assert event.desk == "Power-East"
    assert event.instrument_category == "FixedFloatSwap"
    assert event.underlying_asset == "PJM-WEST"
    assert event.counterparty == "MEGAWATT-LLC"
    assert event.notional_qty == 10000.0
    assert event.price == 3.50
    assert event.price_currency == "USD"
    assert event.ev_amount == pytest.approx(200.0)
    assert event.notes == "Large block trade"
