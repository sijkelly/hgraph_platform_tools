"""
trade_mapper.py

This module is responsible for translating raw trade data into fully structured trade messages.
It now supports decomposing certain instruments into multiple bookable trades.

Each decomposed trade is processed independently; a failure in one does not prevent
the remaining trades from being mapped. Errors are logged and can be inspected via
the ``errors`` list returned alongside the messages.

The final output is a list of dictionaries, each representing a fully assembled trade message.
"""

import logging
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from secure_config import config
from hgraph_entitlements.checker import check_permission, PermissionDeniedError
from hgraph_trade.hgraph_trade_booker.message_wrapper import create_message_header, create_message_footer
from hgraph_trade.hgraph_trade_model import (
    create_trade_header,
    create_trade_footer,
    create_commodity_swap,
    create_commodity_option,
    create_commodity_forward,
    create_commodity_future,
    create_commodity_physical,
    create_commodity_swaption,
    create_fx_trade,
    create_cash_trade,
    get_instrument_mapping,
)
from hgraph_trade.hgraph_trade_mapping.instrument_mappings import map_pricing_instrument
from decomposition import decompose_instrument

__all__ = ("map_trade_to_model",)

logger = logging.getLogger(__name__)

# Map trade types to the entitlements action required
_ACTION_FOR_TRADE_TYPE: Dict[str, str] = {
    "newTrade": "execute_trade",
    "amendTrade": "execute_trade",
    "cancelTrade": "execute_trade",
    "settlement": "settle_trade",
    "approval": "approve_trade",
}

# Lookup table: instrument_type -> creator function
_INSTRUMENT_CREATORS = {
    "swap": lambda td, sub: create_commodity_swap(td, sub_instrument_type=sub),
    "option": lambda td, _sub: create_commodity_option(td),
    "forward": lambda td, _sub: create_commodity_forward(td),
    "future": lambda td, _sub: create_commodity_future(td),
    "physical": lambda td, _sub: create_commodity_physical(td),
    "swaption": lambda td, _sub: create_commodity_swaption(td),
    "fx": lambda td, _sub: create_fx_trade(td),
    "cash": lambda td, _sub: create_cash_trade(td),
}


def _build_single_message(
    single_trade_data: Dict[str, Any],
    instrument_type: str,
    sub_instrument_type: str,
) -> Dict[str, Any]:
    """
    Build one fully assembled trade message from a single (possibly decomposed) trade.

    :param single_trade_data: Trade data for one bookable trade.
    :param instrument_type: The resolved instrument type (e.g. "swap").
    :param sub_instrument_type: The resolved sub-instrument type (e.g. "fixedFloat").
    :return: A dictionary containing messageHeader, tradeHeader, tradeEconomics,
             tradeFooter, and messageFooter.
    :raises ValueError: If the instrument type is unsupported.
    """
    message: Dict[str, Any] = {}

    message["messageHeader"] = create_message_header(
        msg_type=single_trade_data.get("tradeType", "newTrade"),
        sender=single_trade_data.get("sender", config["MESSAGE_SENDER_ID"]),
        target=single_trade_data.get("target", config["MESSAGE_TARGET_ID"]),
    )

    message["tradeHeader"] = create_trade_header(dict(single_trade_data))

    creator = _INSTRUMENT_CREATORS.get(instrument_type)
    if creator is None:
        raise ValueError(f"Unsupported instrument: {instrument_type}")

    trades_created = creator(single_trade_data, sub_instrument_type)

    # Normalise: some creators return a list, others a dict
    if isinstance(trades_created, list):
        trade_economics = trades_created[0]
    else:
        trade_economics = trades_created

    message["tradeEconomics"] = trade_economics
    message["tradeFooter"] = create_trade_footer(dict(single_trade_data))
    message["messageFooter"] = create_message_footer()

    return message


def map_trade_to_model(
    trade_data: Dict[str, Any],
    *,
    fail_fast: bool = False,
    user_id: Optional[str] = None,
    entitlements_conn: Optional[sqlite3.Connection] = None,
) -> List[Dict[str, Any]]:
    """
    Map raw trade data to one or more trade messages, depending on decomposition requirements.

    Steps:
    - Optionally check the user's entitlements before proceeding.
    - Determine instrument and sub-instrument from the ``instrument`` field.
    - Decompose into multiple trades if needed.
    - For each decomposed trade data, build a complete trade message.
    - Return a list of all successfully built trade messages.

    When ``fail_fast`` is False (the default), errors in individual decomposed
    trades are logged but do not prevent other trades from being processed.
    When ``fail_fast`` is True, any error raises immediately.

    :param trade_data: Dictionary containing raw trade data.
    :param fail_fast: If True, re-raise the first error instead of continuing.
    :param user_id: If provided, check that this user has permission to perform
                    the trade action before processing. Pass None to skip the check.
    :param entitlements_conn: Optional SQLite connection for entitlements lookups.
                              If user_id is given but conn is None, a default connection
                              is created automatically.
    :return: A list of dictionaries, each representing a compiled trade message.
    :raises PermissionDeniedError: If user_id is provided and lacks the required permission.
    """
    # --- Entitlements pre-check ---
    if user_id is not None:
        trade_type = trade_data.get("tradeType", "newTrade")
        required_action = _ACTION_FOR_TRADE_TYPE.get(trade_type, "execute_trade")
        if not check_permission(user_id, required_action, conn=entitlements_conn):
            raise PermissionDeniedError(user_id, required_action)

    instrument_key = trade_data.get("instrument", "")
    instrument_type, sub_instrument_type = map_pricing_instrument(instrument_key)

    decomposed_trade_data_list = decompose_instrument(trade_data, instrument_type, sub_instrument_type)

    all_messages: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for idx, single_trade_data in enumerate(decomposed_trade_data_list):
        trade_id = single_trade_data.get("trade_id", f"unknown-{idx}")
        try:
            message = _build_single_message(single_trade_data, instrument_type, sub_instrument_type)
            all_messages.append(message)
        except Exception as exc:
            if fail_fast:
                raise
            error_info = {
                "trade_id": trade_id,
                "index": idx,
                "error": str(exc),
                "instrument": instrument_type,
            }
            errors.append(error_info)
            logger.error(
                "Failed to map decomposed trade %d (trade_id=%s): %s",
                idx,
                trade_id,
                exc,
            )

    if errors:
        logger.warning(
            "%d of %d decomposed trade(s) failed mapping for instrument '%s'",
            len(errors),
            len(decomposed_trade_data_list),
            instrument_key,
        )

    return all_messages
