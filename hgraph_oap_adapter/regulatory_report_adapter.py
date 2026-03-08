"""
Adapter generating CFTC Part 43 and Part 45 report records.

Converts hg_oap swap instruments, trading relationships, and
regulatory domain objects into structured report records suitable
for submission to a Swap Data Repository (SDR).

The output records are frozen dataclasses from
:mod:`hg_oap.regulatory.cftc` and can be serialised to FpML,
CSV, or JSON for SDR submission.

Usage::

    from hgraph_oap_adapter.regulatory_report_adapter import (
        swap_to_part43_record,
        swap_to_part45_record,
    )

    report43 = swap_to_part43_record(
        swap=my_swap,
        relationship=my_relationship,
        trade_id="T-20260105-001",
        execution_timestamp=datetime.utcnow(),
    )
"""
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, Optional

from hg_oap.impl.regulatory.us_energy_upi import classify_energy_swap_upi
from hg_oap.instruments.swap import (
    BasisSwap,
    FixedFloatSwap,
    PaymentFrequency,
    Swap,
    SwapSettlementMethod,
)
from hg_oap.parties.agreement import MasterAgreementType
from hg_oap.parties.relationship import TradingRelationship
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
from hg_oap.regulatory.identifiers import generate_usi
from hg_oap.regulatory.obligation import (
    determine_reporting_counterparty,
    is_end_user_exception_eligible,
)

__all__ = (
    "swap_to_part43_dict",
    "swap_to_part43_record",
    "swap_to_part45_dict",
    "swap_to_part45_record",
)


# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------

_FREQUENCY_MAP: Dict[PaymentFrequency, str] = {
    PaymentFrequency.DAILY: "Daily",
    PaymentFrequency.WEEKLY: "Weekly",
    PaymentFrequency.MONTHLY: "Monthly",
    PaymentFrequency.QUARTERLY: "Quarterly",
    PaymentFrequency.SEMI_ANNUAL: "SemiAnnual",
    PaymentFrequency.ANNUAL: "Annual",
}

_SETTLEMENT_MAP: Dict[SwapSettlementMethod, SettlementType] = {
    SwapSettlementMethod.FINANCIAL: SettlementType.CASH,
    SwapSettlementMethod.PHYSICAL: SettlementType.PHYSICAL,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_underlying_asset(swap: Swap) -> str:
    """Extract a human-readable underlying asset description from the swap."""
    if isinstance(swap, FixedFloatSwap):
        return swap.floating_leg.index.symbol
    if isinstance(swap, BasisSwap):
        return swap.leg_a.index.symbol
    return swap.symbol


def _extract_notional(swap: Swap) -> tuple[Optional[float], str]:
    """Extract notional amount and unit from the swap's pay leg."""
    leg = swap.pay_leg
    return leg.notional_quantity.qty, str(leg.notional_quantity.unit)


def _extract_price_fields(swap: Swap) -> tuple[Optional[float], str, PriceNotationType]:
    """Extract price, price unit, and notation type from the swap."""
    if isinstance(swap, FixedFloatSwap):
        price = swap.fixed_leg.fixed_price.qty
        currency_str = str(swap.fixed_leg.currency.unit)
        qty_unit_str = str(swap.fixed_leg.notional_quantity.unit)
        price_unit = f"{currency_str}/{qty_unit_str}"
        return price, price_unit, PriceNotationType.FIXED_PRICE
    if isinstance(swap, BasisSwap):
        # For basis swaps, report the spread on leg_b if present
        if swap.leg_b.spread is not None:
            spread_val = swap.leg_b.spread.qty
            currency_str = str(swap.leg_b.currency.unit)
            qty_unit_str = str(swap.leg_b.notional_quantity.unit)
            price_unit = f"{currency_str}/{qty_unit_str}"
            return spread_val, price_unit, PriceNotationType.SPREAD
        return None, "", PriceNotationType.INDEX
    return None, "", PriceNotationType.FIXED_PRICE


def _extract_floating_index(swap: Swap) -> str:
    """Extract the floating rate index reference from the swap."""
    if isinstance(swap, FixedFloatSwap):
        return swap.floating_leg.index.symbol
    if isinstance(swap, BasisSwap):
        return swap.leg_a.index.symbol
    return ""


def _extract_spread(swap: Swap) -> Optional[float]:
    """Extract spread from the swap if present."""
    if isinstance(swap, FixedFloatSwap) and swap.floating_leg.spread is not None:
        return swap.floating_leg.spread.qty
    if isinstance(swap, BasisSwap) and swap.leg_b.spread is not None:
        return swap.leg_b.spread.qty
    return None


def _extract_payment_frequency(swap: Swap) -> str:
    """Extract payment frequency as a string from the swap's pay leg."""
    return _FREQUENCY_MAP.get(swap.pay_leg.payment_frequency, "Monthly")


def _extract_currency(swap: Swap) -> str:
    """Extract the notional currency code from the swap."""
    return str(swap.pay_leg.currency.symbol)


def _determine_master_agreement(relationship: TradingRelationship) -> tuple[Optional[str], Optional[str]]:
    """Determine the applicable master agreement type and version."""
    for agreement in (relationship.isda, relationship.naesb, relationship.eei):
        if agreement is not None:
            return agreement.agreement_type.value, agreement.version
    return None, None


# ---------------------------------------------------------------------------
# Part 43 — Public dissemination record
# ---------------------------------------------------------------------------


def swap_to_part43_record(
    swap: Swap,
    relationship: TradingRelationship,
    trade_id: str,
    execution_timestamp: datetime,
    *,
    action_type: ActionType = ActionType.NEWT,
    execution_venue_type: ExecutionVenueType = ExecutionVenueType.OFF_FACILITY,
    block_trade_indicator: BlockTradeIndicator = BlockTradeIndicator.NOT_BLOCK,
) -> Part43PublicReport:
    """
    Convert a swap and relationship into a Part 43 public dissemination record.

    :param swap: The swap instrument (FixedFloatSwap or BasisSwap).
    :param relationship: The bilateral trading relationship.
    :param trade_id: Internal trade identifier (used as dissemination_id placeholder).
    :param execution_timestamp: UTC execution timestamp.
    :param action_type: NEWT, MODI, TERM, or CORR.
    :param execution_venue_type: SEF, DCM, or Off-facility.
    :param block_trade_indicator: Block trade status.
    :returns: A :class:`Part43PublicReport` domain object.
    """
    notional_amount, notional_unit = _extract_notional(swap)
    price, price_unit, price_notation_type = _extract_price_fields(swap)
    settlement_type = _SETTLEMENT_MAP.get(swap.settlement_method, SettlementType.CASH)

    return Part43PublicReport(
        dissemination_id=trade_id,
        asset_class=CftcAssetClass.ENERGY,
        underlying_asset=_extract_underlying_asset(swap),
        action_type=action_type,
        execution_timestamp=execution_timestamp,
        notional_amount=notional_amount,
        notional_unit=notional_unit,
        notional_currency=_extract_currency(swap),
        price=price,
        price_unit=price_unit,
        price_notation_type=price_notation_type,
        settlement_type=settlement_type,
        effective_date=swap.effective_date,
        termination_date=swap.termination_date,
        tenor=swap.tenor,
        payment_frequency=_extract_payment_frequency(swap),
        block_trade_indicator=block_trade_indicator,
        execution_venue_type=execution_venue_type,
    )


# ---------------------------------------------------------------------------
# Part 45 — SDR creation report
# ---------------------------------------------------------------------------


def swap_to_part45_record(
    swap: Swap,
    relationship: TradingRelationship,
    trade_id: str,
    execution_timestamp: datetime,
    *,
    action_type: ActionType = ActionType.NEWT,
    event_type: EventType = EventType.TRAD,
    execution_venue_type: ExecutionVenueType = ExecutionVenueType.OFF_FACILITY,
    execution_venue_name: Optional[str] = None,
    block_trade_indicator: BlockTradeIndicator = BlockTradeIndicator.NOT_BLOCK,
    prior_usi: Optional[str] = None,
) -> Part45CreationReport:
    """
    Convert a swap and relationship into a Part 45 SDR creation record.

    Generates USI/UTI using the reporting counterparty's LEI.  Determines
    the reporting obligation using the CFTC hierarchy.  Extracts all
    economic terms from the swap instrument and relationship.

    :param swap: The swap instrument (FixedFloatSwap or BasisSwap).
    :param relationship: The bilateral trading relationship.
    :param trade_id: Internal trade identifier.
    :param execution_timestamp: UTC execution timestamp.
    :param action_type: NEWT, MODI, TERM, CORR.
    :param event_type: TRAD, NOVA, COMP, ETRM, CLRG.
    :param execution_venue_type: SEF, DCM, or Off-facility.
    :param execution_venue_name: Name of execution venue if SEF/DCM.
    :param block_trade_indicator: Block trade status.
    :param prior_usi: USI of the prior trade for lifecycle events.
    :returns: A :class:`Part45CreationReport` domain object.
    """
    # Determine who reports
    reporting_side = determine_reporting_counterparty(relationship)

    # Generate identifiers
    reporter_lei = reporting_side.reporting_party.lei or ""
    usi = generate_usi(reporter_lei, unique_suffix=trade_id.replace("-", "").upper()[:32]) if reporter_lei else ""
    uti = usi  # UTI = USI under CFTC rules
    upi = classify_energy_swap_upi(swap)

    # Extract economics
    notional_amount, notional_unit = _extract_notional(swap)
    price, price_unit, price_notation_type = _extract_price_fields(swap)
    settlement_type = _SETTLEMENT_MAP.get(swap.settlement_method, SettlementType.CASH)

    # Determine end-user exception eligibility
    end_user_exception = is_end_user_exception_eligible(
        reporting_side.non_reporting_party,
        relationship.clearing_status,
    )

    # Master agreement
    ma_type, ma_version = _determine_master_agreement(relationship)

    # Collateralisation from CSA
    collateralisation = None
    if relationship.isda and relationship.isda.credit_support_annex:
        collateralisation = "Full"

    # Determine reporting side (Payer/Receiver)
    if reporting_side.is_internal_reporter:
        reporting_side_str = "Payer"
    else:
        reporting_side_str = "Receiver"

    return Part45CreationReport(
        # Identifiers
        usi=usi,
        uti=uti,
        upi=upi,
        prior_usi=prior_usi,
        # Action / event
        action_type=action_type,
        event_type=event_type,
        event_timestamp=execution_timestamp,
        execution_timestamp=execution_timestamp,
        # Parties
        reporting_counterparty_lei=reporter_lei,
        non_reporting_counterparty_lei=reporting_side.non_reporting_party.lei or "",
        reporting_counterparty_classification=reporting_side.reporting_party.classification.value,
        non_reporting_counterparty_classification=reporting_side.non_reporting_party.classification.value,
        reporting_counterparty_side=reporting_side_str,
        # Product
        asset_class=CftcAssetClass.ENERGY,
        underlying_asset=_extract_underlying_asset(swap),
        settlement_type=settlement_type,
        effective_date=swap.effective_date,
        termination_date=swap.termination_date,
        # Economics
        notional_amount=notional_amount,
        notional_unit=notional_unit,
        notional_currency=_extract_currency(swap),
        price=price,
        price_unit=price_unit,
        price_notation_type=price_notation_type,
        floating_rate_index=_extract_floating_index(swap),
        spread=_extract_spread(swap),
        payment_frequency=_extract_payment_frequency(swap),
        # Clearing
        clearing_status=relationship.clearing_status.value,
        clearing_venue=None,
        end_user_exception=end_user_exception,
        # Execution
        execution_venue_type=execution_venue_type,
        execution_venue_name=execution_venue_name,
        block_trade_indicator=block_trade_indicator,
        # Legal
        collateralisation=collateralisation,
        master_agreement_type=ma_type,
        master_agreement_version=ma_version,
    )


# ---------------------------------------------------------------------------
# Dict convenience wrappers
# ---------------------------------------------------------------------------


def swap_to_part43_dict(
    swap: Swap,
    relationship: TradingRelationship,
    trade_id: str,
    execution_timestamp: datetime,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Convert a swap into a Part 43 flat dictionary record.

    Convenience wrapper that calls :func:`swap_to_part43_record`
    and serialises the result to a flat dictionary.

    :returns: A flat dictionary of Part 43 fields.
    """
    record = swap_to_part43_record(swap, relationship, trade_id, execution_timestamp, **kwargs)
    result = asdict(record)
    # Convert enum values to their string representations
    for key, val in result.items():
        if hasattr(val, "value"):
            result[key] = val.value
    return result


def swap_to_part45_dict(
    swap: Swap,
    relationship: TradingRelationship,
    trade_id: str,
    execution_timestamp: datetime,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Convert a swap into a Part 45 flat dictionary record.

    Convenience wrapper that calls :func:`swap_to_part45_record`
    and serialises the result to a flat dictionary.

    :returns: A flat dictionary of Part 45 fields.
    """
    record = swap_to_part45_record(swap, relationship, trade_id, execution_timestamp, **kwargs)
    result = asdict(record)
    # Convert enum values to their string representations
    for key, val in result.items():
        if hasattr(val, "value"):
            result[key] = val.value
    return result
