"""
Adapter converting hg_oap swap domain objects to booking pipeline dicts.

Transforms :class:`~hg_oap.instruments.swap.FixedFloatSwap` and
:class:`~hg_oap.instruments.swap.BasisSwap` instances, together with a
:class:`~hg_oap.parties.relationship.TradingRelationship`, into the
flat ``trade_data`` dictionary format expected by
``hgraph_trade.hgraph_trade_booker``.

The output dicts can be passed directly to the existing pipeline
(``trade_loader`` → ``trade_mapper`` → ``trade_booker``) without any
modifications to the core pipeline code.
"""

from datetime import date
from typing import Any, Dict

from hg_oap.instruments.swap import (
    BasisSwap,
    FixedFloatSwap,
    FixedLeg,
    FloatingLeg,
    PaymentFrequency,
)
from hg_oap.parties.relationship import TradingRelationship

__all__ = (
    "fixed_float_swap_to_trade_data",
    "basis_swap_to_trade_data",
)


# ---------------------------------------------------------------------------
# Payment frequency mapping (hg_oap enum → FpML convention)
# ---------------------------------------------------------------------------

_FREQUENCY_MAP: Dict[PaymentFrequency, str] = {
    PaymentFrequency.DAILY: "PerCalendarDay",
    PaymentFrequency.WEEKLY: "PerCalendarWeek",
    PaymentFrequency.MONTHLY: "PerCalendarMonth",
    PaymentFrequency.QUARTERLY: "PerCalendarQuarter",
    PaymentFrequency.SEMI_ANNUAL: "PerCalendarSemiAnnual",
    PaymentFrequency.ANNUAL: "PerCalendarYear",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _unit_str(unit) -> str:
    """Extract a string representation from a hg_oap Unit."""
    return str(unit)


def _base_trade_fields(
    relationship: TradingRelationship,
    trade_id: str,
    trade_date: date,
    *,
    trade_type: str,
    buy_sell: str,
    internal_trader: str,
    external_trader: str,
) -> Dict[str, Any]:
    """Build the base trade_data fields common to all swap types."""
    int_sym = relationship.internal_party.symbol
    ext_sym = relationship.external_party.symbol
    int_port = relationship.internal_portfolio or ""
    ext_port = relationship.external_portfolio or ""

    return {
        "trade_id": trade_id,
        "trade_date": trade_date.isoformat(),
        "tradeType": trade_type,
        "buy_sell": buy_sell,
        "internal_party": int_sym,
        "external_party": ext_sym,
        "counterparty": {"internal": int_sym, "external": ext_sym},
        "portfolio": {"internal": int_port, "external": ext_port},
        "traders": {"internal": internal_trader, "external": external_trader},
        "internal_portfolio": int_port,
        "external_portfolio": ext_port,
        "internal_trader": internal_trader,
        "external_trader": external_trader,
    }


def _fixed_leg_fields(
    leg: FixedLeg,
    payer: str,
    receiver: str,
) -> Dict[str, Any]:
    """Build fixed-leg dot-separated fields for the trade_data dict."""
    return {
        "fixedLeg.payerPartyReference": payer,
        "fixedLeg.receiverPartyReference": receiver,
        "fixedLeg.price": leg.fixed_price.qty,
        "fixedLeg.priceCurrency": _unit_str(leg.currency.unit),
        "fixedLeg.priceUnit": _unit_str(leg.notional_quantity.unit),
        "fixedLeg.quantityUnit": _unit_str(leg.notional_quantity.unit),
        "fixedLeg.quantityFrequency": _FREQUENCY_MAP.get(
            leg.payment_frequency, "PerCalendarMonth"
        ),
        "fixedLeg.quantity": leg.notional_quantity.qty,
    }


def _floating_leg_fields(
    leg: FloatingLeg,
    payer: str,
    receiver: str,
    *,
    prefix: str = "floatingLeg",
) -> Dict[str, Any]:
    """Build floating-leg dot-separated fields for the trade_data dict."""
    fields: Dict[str, Any] = {
        f"{prefix}.payerPartyReference": payer,
        f"{prefix}.receiverPartyReference": receiver,
        f"{prefix}.instrumentId": leg.index.symbol,
        f"{prefix}.specifiedPrice": "Settlement",
        f"{prefix}.quantityUnit": _unit_str(leg.notional_quantity.unit),
        f"{prefix}.quantityFrequency": _FREQUENCY_MAP.get(
            leg.payment_frequency, "PerCalendarMonth"
        ),
        f"{prefix}.quantity": leg.notional_quantity.qty,
    }
    if leg.spread is not None:
        fields[f"{prefix}.spreadCurrency"] = _unit_str(leg.spread.unit)
        fields[f"{prefix}.spreadAmount"] = leg.spread.qty
    return fields


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fixed_float_swap_to_trade_data(
    swap: FixedFloatSwap,
    relationship: TradingRelationship,
    trade_id: str,
    trade_date: date,
    *,
    trade_type: str = "newTrade",
    buy_sell: str = "Buy",
    internal_trader: str = "",
    external_trader: str = "",
) -> Dict[str, Any]:
    """
    Convert an hg_oap :class:`FixedFloatSwap` into a booking pipeline dict.

    :param swap: The fixed-float swap instrument.
    :param relationship: The trading relationship (internal ↔ external party).
    :param trade_id: Unique trade identifier.
    :param trade_date: Trade execution date.
    :param trade_type: Pipeline trade type (``"newTrade"``, ``"amendTrade"``, etc.).
    :param buy_sell: Direction — ``"Buy"`` means we pay fixed / receive floating.
    :param internal_trader: Internal trader identifier.
    :param external_trader: External trader identifier.
    :returns: A flat dictionary compatible with the booking pipeline.
    """
    int_sym = relationship.internal_party.symbol
    ext_sym = relationship.external_party.symbol

    # Determine payer/receiver based on buy/sell direction
    if buy_sell == "Buy":
        # We pay fixed, we receive floating
        fixed_payer, fixed_receiver = int_sym, ext_sym
        float_payer, float_receiver = ext_sym, int_sym
    else:
        # We receive fixed, we pay floating
        fixed_payer, fixed_receiver = ext_sym, int_sym
        float_payer, float_receiver = int_sym, ext_sym

    data = _base_trade_fields(
        relationship,
        trade_id,
        trade_date,
        trade_type=trade_type,
        buy_sell=buy_sell,
        internal_trader=internal_trader,
        external_trader=external_trader,
    )

    data.update(
        {
            "instrument": "outright",
            "effective_date": swap.effective_date.isoformat(),
            "termination_date": swap.termination_date.isoformat(),
            "settlementCurrency": _unit_str(swap.fixed_leg.currency.unit),
            "qty": swap.fixed_leg.notional_quantity.qty,
            "unit": _unit_str(swap.fixed_leg.notional_quantity.unit),
            "currency": _unit_str(swap.fixed_leg.currency.unit),
        }
    )

    data.update(_fixed_leg_fields(swap.fixed_leg, fixed_payer, fixed_receiver))
    data.update(
        _floating_leg_fields(swap.floating_leg, float_payer, float_receiver)
    )

    return data


def basis_swap_to_trade_data(
    swap: BasisSwap,
    relationship: TradingRelationship,
    trade_id: str,
    trade_date: date,
    *,
    trade_type: str = "newTrade",
    buy_sell: str = "Buy",
    internal_trader: str = "",
    external_trader: str = "",
) -> Dict[str, Any]:
    """
    Convert an hg_oap :class:`BasisSwap` into a booking pipeline dict.

    :param swap: The basis (float-float) swap instrument.
    :param relationship: The trading relationship (internal ↔ external party).
    :param trade_id: Unique trade identifier.
    :param trade_date: Trade execution date.
    :param trade_type: Pipeline trade type.
    :param buy_sell: Direction — ``"Buy"`` means we pay leg_a / receive leg_b.
    :param internal_trader: Internal trader identifier.
    :param external_trader: External trader identifier.
    :returns: A flat dictionary compatible with the booking pipeline.
    """
    int_sym = relationship.internal_party.symbol
    ext_sym = relationship.external_party.symbol

    # Determine payer/receiver based on buy/sell direction
    if buy_sell == "Buy":
        leg1_payer, leg1_receiver = int_sym, ext_sym
        leg2_payer, leg2_receiver = ext_sym, int_sym
    else:
        leg1_payer, leg1_receiver = ext_sym, int_sym
        leg2_payer, leg2_receiver = int_sym, ext_sym

    data = _base_trade_fields(
        relationship,
        trade_id,
        trade_date,
        trade_type=trade_type,
        buy_sell=buy_sell,
        internal_trader=internal_trader,
        external_trader=external_trader,
    )

    data.update(
        {
            "instrument": "calender_spread_flfl",
            "effective_date": swap.effective_date.isoformat(),
            "termination_date": swap.termination_date.isoformat(),
            "settlementCurrency": _unit_str(swap.leg_a.currency.unit),
            "qty": swap.leg_a.notional_quantity.qty,
            "unit": _unit_str(swap.leg_a.notional_quantity.unit),
            "currency": _unit_str(swap.leg_a.currency.unit),
        }
    )

    data.update(
        _floating_leg_fields(
            swap.leg_a, leg1_payer, leg1_receiver, prefix="floatLeg1"
        )
    )
    data.update(
        _floating_leg_fields(
            swap.leg_b, leg2_payer, leg2_receiver, prefix="floatLeg2"
        )
    )

    return data
