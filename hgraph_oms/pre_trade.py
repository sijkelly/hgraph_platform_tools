"""
Pre-trade checks.

Pure functions that return a rejection reason, or an empty string when the
order may proceed.  They are given everything they need as arguments, so they
can be run live inside the order book graph and re-run offline for audit.
"""

from typing import Mapping

from hgraph_oms.model import CreditPosition, OrderDetails

__all__ = ("EXECUTE_TRADE", "order_notional", "check_credit", "check_entitlement")

# Entitlements action required to create or change an order
EXECUTE_TRADE = "execute_trade"


def order_notional(details: OrderDetails, quantity: float | None = None) -> float | None:
    """Return ``quantity x price`` for the order, or ``None`` for a market order."""
    if details.price is None:
        return None
    return (details.quantity if quantity is None else quantity) * details.price


def check_credit(details: OrderDetails, position: CreditPosition | None, *, open_exposure: float = 0.0) -> str:
    """Check the order against the counterparty's credit limit.

    Headroom is the limit minus what booked trades already utilise minus the
    notional of the counterparty's other open orders, so two orders cannot both
    spend the same headroom.

    :param position: The counterparty's credit position, or ``None`` if no limit is configured
        (the order is allowed).
    :param open_exposure: Notional of the counterparty's other open orders.
    :returns: A rejection reason, or an empty string.
    """
    if not details.counterparty or position is None:
        return ""

    committed = position.utilized_amount + open_exposure
    if committed >= position.limit_amount:
        return (
            f"Credit limit exceeded for {details.counterparty}: "
            f"{committed:,.0f} of {position.limit_amount:,.0f} committed"
        )

    notional = order_notional(details)
    if notional is not None and committed + notional > position.limit_amount:
        return (
            f"Order exposure {notional:,.0f} would breach credit limit for {details.counterparty}: "
            f"{committed:,.0f} of {position.limit_amount:,.0f} already committed"
        )
    return ""


def check_entitlement(user_id: str, action: str, permissions: Mapping[str, frozenset[str]] | None) -> str:
    """Check the user holds ``action``.

    :param permissions: Allowed actions per user, or ``None`` when entitlements are not enforced.
    :returns: A rejection reason, or an empty string.
    """
    if permissions is None:
        return ""
    if not user_id:
        return "User ID is required"
    if action not in permissions.get(user_id, frozenset()):
        return f"User '{user_id}' lacks permission '{action}'"
    return ""
