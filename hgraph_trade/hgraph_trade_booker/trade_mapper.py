"""
trade_mapper.py

This module is responsible for translating raw trade data into fully structured trade messages.
It now supports decomposing certain instruments into multiple bookable trades.

The final output is a list of dictionaries, each representing a fully assembled trade message.
"""

import logging
from typing import Dict, Any, List

from hgraph_trade.hgraph_trade_booker.message_wrapper import create_message_header, create_message_footer
from hgraph_trade.hgraph_trade_model import (
    create_trade_header,
    create_trade_footer,
    create_commodity_swap,
    create_commodity_option,
    create_commodity_forward,
    create_commodity_future,
    create_commodity_swaption,
    create_fx_trade,
    get_instrument_mapping
)
from hgraph_trade.hgraph_trade_mapping.instrument_mappings import map_pricing_instrument
from decomposition import decompose_instrument

logger = logging.getLogger(__name__)

def map_trade_to_model(trade_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Map raw trade data to one or more trade messages, depending on decomposition requirements.

    Steps:
    - Determine instrument and sub-instrument from the "instrument" field in trade data.
    - Decompose into multiple trades if needed.
    - For each decomposed trade data:
        - Create message header, trade header, trade footer.
        - Create trade economics using the appropriate create_ functions.
        - Wrap everything into a single trade message.
    - Return a list of all such trade messages.

    :param trade_data: Dictionary containing raw trade data.
    :return: A list of dictionaries, each representing a compiled trade message.
    """
    # UPDATED: use the "instrument" field instead of "pricing_instrument"
    instrument_key = trade_data.get("instrument", "")
    instrument_type, sub_instrument_type = map_pricing_instrument(instrument_key)

    decomposed_trade_data_list = decompose_instrument(trade_data, instrument_type, sub_instrument_type)
    all_messages = []

    for single_trade_data in decomposed_trade_data_list:
        message = {}
        message["messageHeader"] = create_message_header(
            msg_type=single_trade_data.get("tradeType", "newTrade"),
            sender=single_trade_data.get("sender", "DefaultSender"),
            target=single_trade_data.get("target", "DefaultTarget")
        )

        # Create trade header/footer
        header_data = dict(single_trade_data)
        message["tradeHeader"] = create_trade_header(header_data)

        # Determine which creator to call based on instrument_type
        if instrument_type == "swap":
            trades_created = create_commodity_swap(single_trade_data, sub_instrument_type=sub_instrument_type)
        elif instrument_type == "option":
            trades_created = create_commodity_option(single_trade_data)
        elif instrument_type == "forward":
            trades_created = create_commodity_forward(single_trade_data)
        elif instrument_type == "future":
            trades_created = create_commodity_future(single_trade_data)
        elif instrument_type == "physical":
            # Add logic if physical instruments are supported
            raise ValueError("Physical instrument type not yet supported.")
        elif instrument_type == "swaption":
            trades_created = create_commodity_swaption(single_trade_data)
        elif instrument_type == "fx":
            trades_created = create_fx_trade(single_trade_data)
        elif instrument_type == "cash":
            trades_created = [{"cashTrade": {}}]
        else:
            raise ValueError(f"Unsupported instrument: {instrument_type}")

        # If we get a list, take the first trade. If we get a dict, just use it directly.
        if isinstance(trades_created, list):
            trade_economics = trades_created[0]
        else:
            trade_economics = trades_created

        message["tradeEconomics"] = trade_economics

        footer_data = dict(single_trade_data)
        message["tradeFooter"] = create_trade_footer(footer_data)
        message["messageFooter"] = create_message_footer()

        all_messages.append(message)

    return all_messages