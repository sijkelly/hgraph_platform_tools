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

def map_trade_to_model(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw trade data to the correct trade model and compile the full message.

    :param trade_data: Dictionary containing raw trade data.
    :return: Compiled message with headers, trade economics, and footers.
    """
    # Correctly define raw instrument and sub-instrument types
    raw_instrument = trade_data.get("instrument", "").strip()
    instrument_type = map_instrument_type(raw_instrument)

    raw_sub_instrument = trade_data.get("sub_instrument", "").strip()
    sub_instrument_type = map_sub_instrument_type(raw_sub_instrument)

    # Debugging
    print(f"DEBUG: raw_instrument={raw_instrument}, instrument_type={instrument_type}")
    print(f"DEBUG: raw_sub_instrument={raw_sub_instrument}, sub_instrument_type={sub_instrument_type}")

    # Validate instrument and sub-instrument types
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

    # Initialize message structure
    message = {}

    # Add message header
    message["messageHeader"] = create_message_header(
        msg_type=trade_data.get("tradeType", "newTrade"),
        sender=trade_data.get("sender", "DefaultSender"),
        target=trade_data.get("target", "DefaultTarget")
    )

    # Remove filtering here so global fields (like trade_id, trade_date) are included
    # header_data = {key: trade_data[key] for key in trade_data if key not in global_mapping}
    # Instead, just pass all trade_data:
    header_data = dict(trade_data)  # Copy all fields

    # Create trade header with full data
    message["tradeHeader"] = create_trade_header(header_data)

    # Add trade economics
    instrument_mapping = get_instrument_mapping(raw_instrument)
    if instrument_type == "swap" and sub_instrument_type in ["fixedFloat", "floatFloat"]:
        # Pass the entire trade_data
        message["tradeEconomics"] = create_commodity_swap(
            trade_data,
            sub_instrument_type=sub_instrument_type
        )
    elif instrument_type == "option":
        message["tradeEconomics"] = create_commodity_option(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "forward":
        message["tradeEconomics"] = create_commodity_forward(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "future":
        message["tradeEconomics"] = create_commodity_future(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "physical":
        raise ValueError("Physical instrument type not yet supported.")
    elif instrument_type == "swaption":
        message["tradeEconomics"] = create_commodity_swaption(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    elif instrument_type == "fx":
        message["tradeEconomics"] = create_fx_trade(
            {key: trade_data[key] for key in instrument_mapping if key in trade_data}
        )
    else:
        raise ValueError(
            f"Unsupported instrument or sub-instrument type. "
            f"Instrument: {instrument_type}, Sub-instrument: {sub_instrument_type}"
        )

    # Add trade footer
    # Similar to trade_header, do not filter out global fields here; just pass all data:
    footer_data = dict(trade_data)
    message["tradeFooter"] = create_trade_footer(footer_data)

    # Add message footer
    message["messageFooter"] = create_message_footer()

    return message