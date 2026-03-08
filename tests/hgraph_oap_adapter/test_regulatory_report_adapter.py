"""Tests for the CFTC regulatory report adapter."""
from datetime import date, datetime

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
from hg_oap.parties.agreement import MasterAgreement, MasterAgreementType
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship
from hg_oap.regulatory.cftc import (
    ActionType,
    BlockTradeIndicator,
    CftcAssetClass,
    EventType,
    ExecutionVenueType,
    Part43PublicReport,
    Part45CreationReport,
    PriceNotationType,
    SettlementType,
)
from hg_oap.units.default_unit_system import U
from hg_oap.units.quantity import Quantity

from hgraph_oap_adapter.regulatory_report_adapter import (
    swap_to_part43_dict,
    swap_to_part43_record,
    swap_to_part45_dict,
    swap_to_part45_record,
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
def isda_agreement():
    return MasterAgreement(
        agreement_type=MasterAgreementType.ISDA,
        agreement_date=date(2020, 6, 1),
        version="2002",
        credit_support_annex=True,
        threshold_amount=10_000_000.0,
        governing_law="New York",
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
def relationship_with_isda(dealer, counterparty, isda_agreement):
    return TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.BILATERAL,
        isda=isda_agreement,
        internal_portfolio="GAS_DESK",
        external_portfolio="HEDGE_BOOK",
    )


@pytest.fixture()
def exempt_relationship(dealer, counterparty):
    return TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.EXEMPT,
        internal_portfolio="GAS_DESK",
        external_portfolio="HEDGE_BOOK",
    )


@pytest.fixture()
def fixed_float_swap(usd, hh_index):
    return FixedFloatSwap(
        symbol="NG_HH_CAL26",
        fixed_leg=FixedLeg(
            fixed_price=Quantity(qty=3.50, unit=U.MMBtu),
            notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
            currency=usd,
            payment_frequency=PaymentFrequency.MONTHLY,
        ),
        floating_leg=FloatingLeg(
            index=hh_index,
            notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
            currency=usd,
            payment_frequency=PaymentFrequency.MONTHLY,
        ),
        effective_date=date(2026, 1, 1),
        termination_date=date(2026, 12, 31),
    )


@pytest.fixture()
def basis_swap(usd, hh_index, algonquin_index):
    return BasisSwap(
        symbol="NG_HH_ALG_CAL26",
        leg_a=FloatingLeg(
            index=hh_index,
            notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
            currency=usd,
            payment_frequency=PaymentFrequency.MONTHLY,
        ),
        leg_b=FloatingLeg(
            index=algonquin_index,
            notional_quantity=Quantity(qty=10000.0, unit=U.MMBtu),
            currency=usd,
            payment_frequency=PaymentFrequency.MONTHLY,
            spread=Quantity(qty=0.75, unit=U.MMBtu),
        ),
        effective_date=date(2026, 1, 1),
        termination_date=date(2026, 12, 31),
    )


@pytest.fixture()
def exec_ts():
    return datetime(2026, 1, 5, 14, 30, 0)


# ===========================================================================
# Part 43 — Public Dissemination
# ===========================================================================


def test_part43_returns_correct_type(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert isinstance(record, Part43PublicReport)


def test_part43_base_fields(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.dissemination_id == "T001"
    assert record.asset_class == CftcAssetClass.ENERGY
    assert record.action_type == ActionType.NEWT
    assert record.execution_venue_type == ExecutionVenueType.OFF_FACILITY


def test_part43_economics(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.notional_amount == 10000.0
    assert record.notional_unit != ""
    assert record.price == 3.50
    assert record.price_notation_type == PriceNotationType.FIXED_PRICE
    assert record.settlement_type == SettlementType.CASH


def test_part43_dates(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.effective_date == date(2026, 1, 1)
    assert record.termination_date == date(2026, 12, 31)
    assert record.execution_timestamp == exec_ts
    assert record.tenor != ""


def test_part43_payment_frequency(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.payment_frequency == "Monthly"


def test_part43_block_trade_default(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.block_trade_indicator == BlockTradeIndicator.NOT_BLOCK


def test_part43_block_trade_override(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(
        fixed_float_swap, relationship, "T001", exec_ts, block_trade_indicator=BlockTradeIndicator.BLOCK
    )
    assert record.block_trade_indicator == BlockTradeIndicator.BLOCK


def test_part43_basis_swap(basis_swap, relationship, exec_ts):
    record = swap_to_part43_record(basis_swap, relationship, "T002", exec_ts)
    assert isinstance(record, Part43PublicReport)
    assert record.underlying_asset == "NATURAL_GAS-HENRY_HUB"
    assert record.price_notation_type == PriceNotationType.SPREAD
    assert record.price == 0.75


def test_part43_action_type_modi(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part43_record(fixed_float_swap, relationship, "T001", exec_ts, action_type=ActionType.MODI)
    assert record.action_type == ActionType.MODI


def test_part43_dict_is_flat(fixed_float_swap, relationship, exec_ts):
    result = swap_to_part43_dict(fixed_float_swap, relationship, "T001", exec_ts)
    assert isinstance(result, dict)
    assert "dissemination_id" in result
    assert result["dissemination_id"] == "T001"


# ===========================================================================
# Part 45 — SDR Reporting
# ===========================================================================


def test_part45_returns_correct_type(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert isinstance(record, Part45CreationReport)


def test_part45_usi_format(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert len(record.usi) >= 21
    assert len(record.usi) <= 52
    # USI starts with the reporting party's LEI
    assert record.usi.startswith("529900ACMEDEALER0LEI")


def test_part45_uti_matches_usi(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.uti == record.usi


def test_part45_upi_populated(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.upi != ""
    assert record.upi.startswith("QC")


def test_part45_reporting_party_is_dealer(fixed_float_swap, relationship, exec_ts):
    """SD vs NFEU: the swap dealer reports."""
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.reporting_counterparty_lei == "529900ACMEDEALER0LEI"
    assert record.reporting_counterparty_classification == "SD"
    assert record.non_reporting_counterparty_classification == "NFEU"


def test_part45_both_leis_populated(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.reporting_counterparty_lei != ""
    assert record.non_reporting_counterparty_lei != ""


def test_part45_reporting_side(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.reporting_counterparty_side in ("Payer", "Receiver")


def test_part45_clearing_status(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.clearing_status == "Bilateral"


def test_part45_economics(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.notional_amount == 10000.0
    assert record.price == 3.50
    assert record.floating_rate_index == "NATURAL_GAS-HENRY_HUB"
    assert record.payment_frequency == "Monthly"
    assert record.effective_date == date(2026, 1, 1)
    assert record.termination_date == date(2026, 12, 31)


def test_part45_settlement_type(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.settlement_type == SettlementType.CASH


def test_part45_master_agreement(fixed_float_swap, relationship_with_isda, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship_with_isda, "T001", exec_ts)
    assert record.master_agreement_type == "ISDA"
    assert record.master_agreement_version == "2002"
    assert record.collateralisation == "Full"


def test_part45_no_master_agreement(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.master_agreement_type is None
    assert record.master_agreement_version is None
    assert record.collateralisation is None


def test_part45_end_user_exception_when_exempt(fixed_float_swap, exempt_relationship, exec_ts):
    """NFEU counterparty with EXEMPT clearing status qualifies."""
    record = swap_to_part45_record(fixed_float_swap, exempt_relationship, "T001", exec_ts)
    assert record.end_user_exception is True


def test_part45_no_end_user_exception_when_bilateral(fixed_float_swap, relationship, exec_ts):
    """NFEU counterparty with BILATERAL clearing status does not qualify."""
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    assert record.end_user_exception is False


def test_part45_action_type_modi(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts, action_type=ActionType.MODI)
    assert record.action_type == ActionType.MODI


def test_part45_event_type_nova(fixed_float_swap, relationship, exec_ts):
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts, event_type=EventType.NOVA)
    assert record.event_type == EventType.NOVA


def test_part45_prior_usi(fixed_float_swap, relationship, exec_ts):
    prior = "529900ACMEDEALER0LEIORIGINAL12345678901234"
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts, prior_usi=prior)
    assert record.prior_usi == prior


def test_part45_basis_swap(basis_swap, relationship, exec_ts):
    record = swap_to_part45_record(basis_swap, relationship, "T002", exec_ts)
    assert isinstance(record, Part45CreationReport)
    assert record.floating_rate_index == "NATURAL_GAS-HENRY_HUB"
    assert record.spread == 0.75
    assert record.price_notation_type == PriceNotationType.SPREAD
    assert "BSIS" in record.upi


def test_part45_dict_is_flat(fixed_float_swap, relationship, exec_ts):
    result = swap_to_part45_dict(fixed_float_swap, relationship, "T001", exec_ts)
    assert isinstance(result, dict)
    assert "usi" in result
    assert "reporting_counterparty_lei" in result
    assert "clearing_status" in result


def test_part45_dict_enums_serialised(fixed_float_swap, relationship, exec_ts):
    result = swap_to_part45_dict(fixed_float_swap, relationship, "T001", exec_ts)
    # Enum values should be serialised to their string values
    assert result["asset_class"] == "Energy"
    assert result["action_type"] == "NEWT"
    assert result["settlement_type"] == "Cash"


def test_part45_has_all_required_sdr_fields(fixed_float_swap, relationship, exec_ts):
    """Verify that the Part 45 record contains all key SDR fields."""
    record = swap_to_part45_record(fixed_float_swap, relationship, "T001", exec_ts)
    required_fields = [
        "usi",
        "uti",
        "upi",
        "action_type",
        "event_type",
        "execution_timestamp",
        "reporting_counterparty_lei",
        "non_reporting_counterparty_lei",
        "asset_class",
        "underlying_asset",
        "settlement_type",
        "effective_date",
        "termination_date",
        "notional_amount",
        "notional_currency",
        "price",
        "floating_rate_index",
        "payment_frequency",
        "clearing_status",
        "execution_venue_type",
        "block_trade_indicator",
    ]
    for field in required_fields:
        val = getattr(record, field)
        assert val is not None and val != "", f"Field {field!r} is empty or None: {val!r}"
