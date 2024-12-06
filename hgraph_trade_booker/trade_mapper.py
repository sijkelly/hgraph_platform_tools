"""
trade_mapper.py

This module is responsible for translating raw trade data into a fully structured
message conforming to an FpML-like format. It uses various model creation functions
to build headers, footers, and instrument-specific trade economics based on
input fields such as instrument and sub-instrument types.

The final output is a dictionary representing the fully assembled trade message,
complete with header, trade economics, and footer.
"""

import logging
from typing import Dict, Any

from hgraph_trade_booker.message_wrapper import create_message_header, create_message_footer
from hgraph_trade_model import (
    create_trade_header,
    create_trade_footer,
    create_commodity_swap,
    create_commodity_option,
    create_commodity_forward,
    create_commodity_future,
    create_commodity_swaption,
    create_fx_trade,
    get_global_mapping,
    get_instrument_mapping,
    map_sub_instrument_type,
    map_instrument_type
)
from hgraph_trade_model.fpml_mappings import (
    INSTRUMENT_TYPE_MAPPING,
    SUB_INSTRUMENT_TYPE_MAPPING
)

# Create a module-level logger (optional, if logging is configured elsewhere, this will honor that configuration)
logger = logging.getLogger(__name__)

def map_trade_to_model(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw trade data to the correct trade model and compile the full message.

    This function:
    - Determines the instrument and sub-instrument types.
    - Creates the message header, trade header, and trade footer.
    - Calls the appropriate creation function for the trade economics based on the instrument type.
    - Assembles all parts into a single dictionary (message) ready for further processing.

    :param trade_data: Dictionary containing raw trade data.
    :return: A dictionary representing the compiled trade message.
    :raises ValueError: If the instrument or sub-instrument type is unsupported.
    """
    # Determine and validate instrument types
    raw_instrument = trade_data.get("instrument", "").strip()
    instrument_type = map_instrument_type(raw_instrument)
    raw_sub_instrument = trade_data.get("sub_instrument", "").strip()
    sub_instrument_type = map_sub_instrument_type(raw_sub_instrument)

    logger.debug("Raw instrument: %s, Mapped instrument: %s", raw_instrument, instrument_type)
    logger.debug("Raw sub-instrument: %s, Mapped sub-instrument: %s", raw_sub_instrument, sub_instrument_type)

    if not instrument_type:
        raise ValueError(
            f"Unsupported instrument type: {raw_instrument}. "
            f"Ensure `instrument` is one of: {list(INSTRUMENT_TYPE_MAPPING.keys())}"
        )
    if not sub_instrument_type:
        raise ValueError(
            f"Unsupported sub-instrument type: {raw_sub_instrument}. "
            f"Ensure `sub_instrument` is one of: {list(SUB_INSTRUMENT_TYPE_MAPPING.keys())}"
        )

    # Initialize message structure and create message header
    message = {}
    message["messageHeader"] = create_message_header(
        msg_type=trade_data.get("tradeType", "newTrade"),
        sender=trade_data.get("sender", "DefaultSender"),
        target=trade_data.get("target", "DefaultTarget")
    )

    # Pass full trade_data to create_trade_header to ensure global fields are included
    header_data = dict(trade_data)
    message["tradeHeader"] = create_trade_header(header_data)

    # Determine instrument mapping and create trade economics
    instrument_mapping = get_instrument_mapping(raw_instrument)
    if instrument_type == "swap" and sub_instrument_type in ["fixedFloat", "floatFloat"]:
        # Commodity swap logic
        message["tradeEconomics"] = create_commodity_swap(trade_data, sub_instrument_type=sub_instrument_type)
    elif instrument_type == "option":
        # Commodity option logic
        message["tradeEconomics"] = create_commodity_option(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "forward":
        # Commodity forward logic
        message["tradeEconomics"] = create_commodity_forward(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "future":
        # Commodity future logic
        message["tradeEconomics"] = create_commodity_future(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "physical":
        raise ValueError("Physical instrument type not yet supported.")
    elif instrument_type == "swaption":
        # Commodity swaption logic
        message["tradeEconomics"] = create_commodity_swaption(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "fx":
        # FX logic
        message["tradeEconomics"] = create_fx_trade(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    else:
        raise ValueError(
            f"Unsupported instrument or sub-instrument type. "
            f"Instrument: {instrument_type}, Sub-instrument: {sub_instrument_type}"
        )

    # Pass full trade_data to create_trade_footer for consistency
    footer_data = dict(trade_data)
    message["tradeFooter"] = create_trade_footer(footer_data)

    # Add message footer
    message["messageFooter"] = create_message_footer()

    return message