from typing import Dict, Any
from hgraph_trade_booker.message_wrapper import create_message_header, create_message_footer
from hgraph_trade_model import (
    create_trade_header,
    create_commodity_swap,
    create_commodity_option,
    create_commodity_forward,
    create_commodity_future,
    create_commodity_swaption,
    create_fx_trade,
    create_trade_footer,
)


def map_trade_to_model(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw trade data to the correct trade model and compile the full message.

    :param trade_data: Dictionary containing raw trade data.
    :return: Compiled message with headers, trade economics, and footers.
    """
    # Determine trade type and subtypes
    trade_type = trade_data.get("tradeType", "UnknownTradeType")

    # Initialize message structure
    message = {}

    # Add message header
    message["messageHeader"] = create_message_header(
        msg_type=trade_data.get("messageType", "NewTrade"),
        sender=trade_data.get("sender", "DefaultSender"),
        target=trade_data.get("target", "DefaultTarget")
    )

    # Add trade header
    message["tradeHeader"] = create_trade_header(trade_data)

    # Add trade economics based on trade type
    if trade_type == "CommoditySwap":
        message["tradeEconomics"] = create_commodity_swap(trade_data)
    elif trade_type == "CommodityOption":
        message["tradeEconomics"] = create_commodity_option(trade_data)
    elif trade_type == "CommodityForward":
        message["tradeEconomics"] = create_commodity_forward(trade_data)
    elif trade_type == "CommodityFuture":
        message["tradeEconomics"] = create_commodity_future(trade_data)
    elif trade_type == "CommoditySwaption":
        message["tradeEconomics"] = create_commodity_swaption(trade_data)
    elif trade_type == "FX":
        message["tradeEconomics"] = create_fx_trade(trade_data)
    else:
        raise ValueError(f"Unsupported trade type: {trade_type}")

    # Add trade footer
    message["tradeFooter"] = create_trade_footer(trade_data)

    # Add message footer
    message["messageFooter"] = create_message_footer()

    return message