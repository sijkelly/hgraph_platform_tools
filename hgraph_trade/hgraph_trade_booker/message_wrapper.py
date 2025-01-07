"""
message_wrapper.py

This module provides functionality to wrap trade messages with a standardized
header and footer, including a checksum for integrity. It filters out unwanted
fields and ensures that only the necessary trade components are included before
constructing a final message suitable for downstream processing.
"""

import json
from typing import Dict, Any
import datetime
import hashlib

def create_message_header(msg_type: str, sender: str, target: str) -> Dict[str, Any]:
    """
    Create a message header with metadata for routing and processing.

    :param msg_type: Message type (e.g., "NewTrade", "CancelTrade").
    :param sender: Sender's identifier (e.g., system or user ID).
    :param target: Target system identifier.
    :return: A dictionary representing the message header.
    """
    return {
        "messageType": msg_type,
        "senderCompID": sender,
        "targetCompID": target,
        "sendingTime": datetime.datetime.utcnow().isoformat() + "Z",
        "messageVersion": "1.0"
    }

def create_message_footer() -> Dict[str, Any]:
    """
    Create a message footer with integrity checks.

    :return: A dictionary representing the message footer.
    """
    return {"checksum": None}

def calculate_checksum(message: str) -> str:
    """
    Calculate a simple checksum for the message.

    :param message: The serialized message string (e.g., JSON).
    :return: The calculated SHA-256 checksum.
    """
    return hashlib.sha256(message.encode("utf-8")).hexdigest()

def filter_trade_data(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter the trade data to ensure only top-level allowed keys are included.

    :param trade_data: The original trade data.
    :return: Filtered trade data dictionary containing only allowed top-level keys.
    """
    allowed_keys = {"tradeHeader", "tradeEconomics", "tradeFooter"}
    return {key: value for key, value in trade_data.items() if key in allowed_keys}

def wrap_message_with_headers_and_footers(
    trade_data: Dict[str, Any],
    msg_type: str,
    sender: str,
    target: str
) -> Dict[str, Any]:
    """
    Wrap trade data with message header and footer.

    Filters the input trade data to include only expected fields, generates a header
    and footer, and calculates a checksum for data integrity.

    :param trade_data: The core trade data to wrap.
    :param msg_type: Message type (e.g., "NewTrade").
    :param sender: Sender's identifier (e.g., "TradeSystemA").
    :param target: Target system identifier (e.g., "BookingSystemB").
    :return: A complete message with header, filtered trade data, and footer (with checksum).
    """
    filtered_trade_data = filter_trade_data(trade_data)
    header = create_message_header(msg_type, sender, target)
    footer = create_message_footer()

    message = {
        "messageHeader": header,
        **filtered_trade_data,
        "messageFooter": footer,
    }

    # Exclude the footer itself from checksum calculation
    serialized_message = json.dumps({"messageHeader": header, **filtered_trade_data}, separators=(",", ":"))
    message["messageFooter"]["checksum"] = calculate_checksum(serialized_message)

    return message

# Example usage (Commented out to avoid running in production)
# if __name__ == "__main__":
#     sample_trade_data = {
#         "tradeHeader": {
#             "partyTradeIdentifier": {
#                 "partyReference": "",
#                 "tradeId": "hgraph-SWAP-002"
#             },
#             "tradeDate": "2024-11-20",
#             "parties": [{"internalParty": ""}, {"externalParty": ""}],
#             "portfolio": {"internalPortfolio": "", "externalPortfolio": ""},
#             "trader": {"internalTrader": "", "externalTrader": ""}
#         },
#         "tradeEconomics": {
#             "commoditySwap": {
#                 "buySell": "Buy",
#                 "effectiveDate": "2024-12-01",
#                 "terminationDate": "2025-12-01",
#                 "notionalQuantity": {"quantity": 100, "unit": "tonne"},
#                 "paymentCurrency": "USD"
#             }
#         },
#         "tradeFooter": {
#             "placeholderField1": "",
#             "placeholderField2": ""
#         },
#         "buy_sell": "buySell",
#         "currency": "paymentCurrency",
#         "price_unit": "priceUnit"
#     }

#     full_message = wrap_message_with_headers_and_footers(
#         trade_data=sample_trade_data,
#         msg_type="NewTrade",
#         sender="TradeSystemA",
#         target="BookingSystemB"
#     )

#     print(json.dumps(full_message, indent=4))