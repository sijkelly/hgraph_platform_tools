"""Tests for the energy swap adapter — hg_oap domain objects to booking pipeline."""

from datetime import date

import pytest

from hg_oap.impl.assets.currency import Currencies
from hg_oap.instruments.index import Index
from hg_oap.instruments.swap import (
    BasisSwap,
    FixedFloatSwap,
    FixedLeg,
    FloatingLeg,
    PaymentFrequency,
    SwapSettlementMethod,
)
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship
from hg_oap.units.quantity import Quantity
from hg_oap.units.default_unit_system import U

from hgraph_oap_adapter.energy_swap_adapter import (
    basis_swap_to_trade_data,
    fixed_float_swap_to_trade_data,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def usd():
    return Currencies.USD.value


@pytest.fixture()
def hh_index():
    return Index(symbol="NATURAL_GAS-HENRY_HUB")


@pytest.fixture()
def algonquin_index():
    return Index(symbol="NATURAL_GAS-ALGONQUIN")


@pytest.fixture()
def dealer():
    return LegalEntity(
        symbol="DEALER",
        name="Acme Energy Trading LLC",
        classification=PartyClassification.SWAP_DEALER,
        lei="529900ACMEDEALER0LEI",
        jurisdiction="US",
    )


@pytest.fixture()
def counterparty():
    return LegalEntity(
        symbol="UTIL_CO",
        name="SunState Electric Co",
        classification=PartyClassification.NON_FINANCIAL_END_USER,
        lei="529900SUNSTATEUTL0LEI",
        jurisdiction="US",
    )


@pytest.fixture()
def relationship(dealer, counterparty):
    return TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.BILATERAL,
        internal_portfolio="GAS_DESK",
        external_portfolio="HEDGE_BOOK",
    )


@pytest.fixture()
def fixed_leg(usd):
    return FixedLeg(
        fixed_price=Quantity(qty=3.50, unit=U.MMBtu),
        notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
        currency=usd,
        payment_frequency=PaymentFrequency.MONTHLY,
    )


@pytest.fixture()
def floating_leg(usd, hh_index):
    return FloatingLeg(
        index=hh_index,
        notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
        currency=usd,
        payment_frequency=PaymentFrequency.MONTHLY,
    )


@pytest.fixture()
def fixed_float_swap(fixed_leg, floating_leg):
    return FixedFloatSwap(
        symbol="NG_HH_CAL26",
        fixed_leg=fixed_leg,
        floating_leg=floating_leg,
        effective_date=date(2026, 1, 1),
        termination_date=date(2026, 12, 31),
    )


@pytest.fixture()
def basis_swap_fixture(usd, hh_index, algonquin_index):
    leg_a = FloatingLeg(
        index=hh_index,
        notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
        currency=usd,
        payment_frequency=PaymentFrequency.MONTHLY,
    )
    leg_b = FloatingLeg(
        index=algonquin_index,
        notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
        currency=usd,
        payment_frequency=PaymentFrequency.MONTHLY,
        spread=Quantity(qty=0.75, unit=U.MMBtu),
    )
    return BasisSwap(
        symbol="NG_HH_ALG_CAL26",
        leg_a=leg_a,
        leg_b=leg_b,
        effective_date=date(2026, 1, 1),
        termination_date=date(2026, 12, 31),
    )


# ---------------------------------------------------------------------------
# Fixed-float swap adapter
# ---------------------------------------------------------------------------


def test_fixed_float_returns_dict(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    assert isinstance(result, dict)


def test_fixed_float_base_fields(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    assert result["trade_id"] == "SWAP-001"
    assert result["trade_date"] == "2026-01-05"
    assert result["tradeType"] == "newTrade"
    assert result["instrument"] == "outright"
    assert result["buy_sell"] == "Buy"


def test_fixed_float_party_fields(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    assert result["internal_party"] == "DEALER"
    assert result["external_party"] == "UTIL_CO"
    assert result["counterparty"] == {"internal": "DEALER", "external": "UTIL_CO"}
    assert result["portfolio"] == {"internal": "GAS_DESK", "external": "HEDGE_BOOK"}


def test_fixed_float_dates(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    assert result["effective_date"] == "2026-01-01"
    assert result["termination_date"] == "2026-12-31"


def test_fixed_float_fixed_leg(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    assert result["fixedLeg.price"] == 3.50
    assert result["fixedLeg.quantity"] == 10000.0
    assert result["fixedLeg.quantityFrequency"] == "PerCalendarMonth"


def test_fixed_float_floating_leg(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    assert result["floatingLeg.instrumentId"] == "NATURAL_GAS-HENRY_HUB"
    assert result["floatingLeg.specifiedPrice"] == "Settlement"
    assert result["floatingLeg.quantity"] == 10000.0
    assert result["floatingLeg.quantityFrequency"] == "PerCalendarMonth"


def test_fixed_float_buy_direction(fixed_float_swap, relationship):
    """Buy = we pay fixed, counterparty pays floating."""
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
        buy_sell="Buy",
    )
    assert result["fixedLeg.payerPartyReference"] == "DEALER"
    assert result["fixedLeg.receiverPartyReference"] == "UTIL_CO"
    assert result["floatingLeg.payerPartyReference"] == "UTIL_CO"
    assert result["floatingLeg.receiverPartyReference"] == "DEALER"


def test_fixed_float_sell_direction(fixed_float_swap, relationship):
    """Sell = we pay floating, counterparty pays fixed."""
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
        buy_sell="Sell",
    )
    assert result["fixedLeg.payerPartyReference"] == "UTIL_CO"
    assert result["fixedLeg.receiverPartyReference"] == "DEALER"
    assert result["floatingLeg.payerPartyReference"] == "DEALER"
    assert result["floatingLeg.receiverPartyReference"] == "UTIL_CO"


def test_fixed_float_settlement_currency(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    assert result["settlementCurrency"] == "USD"


def test_fixed_float_custom_trade_type(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
        trade_type="amendTrade",
    )
    assert result["tradeType"] == "amendTrade"


def test_fixed_float_traders(fixed_float_swap, relationship):
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
        internal_trader="JSmith",
        external_trader="JDoe",
    )
    assert result["traders"] == {"internal": "JSmith", "external": "JDoe"}
    assert result["internal_trader"] == "JSmith"
    assert result["external_trader"] == "JDoe"


def test_fixed_float_has_all_required_pipeline_keys(fixed_float_swap, relationship):
    """Verify all keys required by the booking pipeline are present."""
    result = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "SWAP-001", date(2026, 1, 5),
    )
    required_keys = [
        "trade_id", "trade_date", "tradeType", "instrument",
        "counterparty", "portfolio", "traders",
        "effective_date", "termination_date", "settlementCurrency",
        "fixedLeg.price", "fixedLeg.quantity",
        "floatingLeg.instrumentId", "floatingLeg.quantity",
    ]
    for key in required_keys:
        assert key in result, f"Missing required key: {key}"


# ---------------------------------------------------------------------------
# Basis swap adapter
# ---------------------------------------------------------------------------


def test_basis_swap_returns_dict(basis_swap_fixture, relationship):
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    assert isinstance(result, dict)


def test_basis_swap_instrument_type(basis_swap_fixture, relationship):
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    assert result["instrument"] == "calender_spread_flfl"


def test_basis_swap_base_fields(basis_swap_fixture, relationship):
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    assert result["trade_id"] == "BASIS-001"
    assert result["trade_date"] == "2026-01-05"
    assert result["tradeType"] == "newTrade"
    assert result["buy_sell"] == "Buy"


def test_basis_swap_dates(basis_swap_fixture, relationship):
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    assert result["effective_date"] == "2026-01-01"
    assert result["termination_date"] == "2026-12-31"


def test_basis_swap_leg1_fields(basis_swap_fixture, relationship):
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    assert result["floatLeg1.instrumentId"] == "NATURAL_GAS-HENRY_HUB"
    assert result["floatLeg1.quantity"] == 10000.0
    assert result["floatLeg1.specifiedPrice"] == "Settlement"


def test_basis_swap_leg2_fields(basis_swap_fixture, relationship):
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    assert result["floatLeg2.instrumentId"] == "NATURAL_GAS-ALGONQUIN"
    assert result["floatLeg2.quantity"] == 10000.0
    assert result["floatLeg2.specifiedPrice"] == "Settlement"


def test_basis_swap_spread(basis_swap_fixture, relationship):
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    assert result["floatLeg2.spreadAmount"] == 0.75


def test_basis_swap_buy_direction(basis_swap_fixture, relationship):
    """Buy = we pay leg_a, counterparty pays leg_b."""
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
        buy_sell="Buy",
    )
    assert result["floatLeg1.payerPartyReference"] == "DEALER"
    assert result["floatLeg1.receiverPartyReference"] == "UTIL_CO"
    assert result["floatLeg2.payerPartyReference"] == "UTIL_CO"
    assert result["floatLeg2.receiverPartyReference"] == "DEALER"


def test_basis_swap_sell_direction(basis_swap_fixture, relationship):
    """Sell = counterparty pays leg_a, we pay leg_b."""
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
        buy_sell="Sell",
    )
    assert result["floatLeg1.payerPartyReference"] == "UTIL_CO"
    assert result["floatLeg1.receiverPartyReference"] == "DEALER"
    assert result["floatLeg2.payerPartyReference"] == "DEALER"
    assert result["floatLeg2.receiverPartyReference"] == "UTIL_CO"


def test_basis_swap_has_all_required_pipeline_keys(basis_swap_fixture, relationship):
    """Verify all keys required by the booking pipeline are present."""
    result = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "BASIS-001", date(2026, 1, 5),
    )
    required_keys = [
        "trade_id", "trade_date", "tradeType", "instrument",
        "counterparty", "portfolio", "traders",
        "effective_date", "termination_date", "settlementCurrency",
        "floatLeg1.instrumentId", "floatLeg1.quantity",
        "floatLeg2.instrumentId", "floatLeg2.quantity",
    ]
    for key in required_keys:
        assert key in result, f"Missing required key: {key}"


# ---------------------------------------------------------------------------
# Pipeline integration — verify output can be consumed by swap model
# ---------------------------------------------------------------------------


def test_fixed_float_output_compatible_with_swap_model(fixed_float_swap, relationship):
    """The adapter output should be consumable by create_commodity_swap."""
    from hgraph_trade.hgraph_trade_model.swap import create_commodity_swap

    trade_data = fixed_float_swap_to_trade_data(
        fixed_float_swap, relationship, "INTEG-001", date(2026, 1, 5),
    )
    result = create_commodity_swap(trade_data, sub_instrument_type="fixedFloat")

    assert isinstance(result, list)
    assert len(result) == 1
    assert "commoditySwap" in result[0]

    swap_msg = result[0]["commoditySwap"]
    assert "fixedLeg" in swap_msg
    assert "floatingLeg" in swap_msg
    assert swap_msg["fixedLeg"]["fixedPrice"]["price"] == 3.50
    assert swap_msg["floatingLeg"]["commodity"]["instrumentId"] == "NATURAL_GAS-HENRY_HUB"
    assert swap_msg["fixedLeg"]["notionalQuantity"]["quantity"] == 10000.0


def test_basis_swap_output_compatible_with_swap_model(basis_swap_fixture, relationship):
    """The adapter output should be consumable by create_commodity_swap."""
    from hgraph_trade.hgraph_trade_model.swap import create_commodity_swap

    trade_data = basis_swap_to_trade_data(
        basis_swap_fixture, relationship, "INTEG-002", date(2026, 1, 5),
    )
    result = create_commodity_swap(trade_data, sub_instrument_type="floatFloat")

    assert isinstance(result, list)
    assert len(result) == 1
    assert "commoditySwap" in result[0]

    swap_msg = result[0]["commoditySwap"]
    assert "floatLeg1" in swap_msg
    assert "floatLeg2" in swap_msg
    assert swap_msg["floatLeg1"]["commodity"]["instrumentId"] == "NATURAL_GAS-HENRY_HUB"
    assert swap_msg["floatLeg2"]["commodity"]["instrumentId"] == "NATURAL_GAS-ALGONQUIN"
