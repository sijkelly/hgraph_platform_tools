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
    return {
        "checksum": None  # Placeholder for checksum calculation.
    }


def calculate_checksum(message: str) -> str:
    """
    Calculate a simple checksum for the message.

    :param message: The serialized message string (e.g., JSON).
    :return: The calculated checksum.
    """
    return hashlib.sha256(message.encode("utf-8")).hexdigest()


def filter_trade_data(trade_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter the trade data to ensure only top-level allowed keys are included.

    :param trade_data: The original trade data.
    :return: Filtered trade data.
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

    :param trade_data: The core trade data to wrap.
    :param msg_type: Message type.
    :param sender: Sender's identifier.
    :param target: Target system identifier.
    :return: A complete message with header and footer.
    """
    # Filter the trade data to remove unwanted keys
    filtered_trade_data = filter_trade_data(trade_data)

    # Generate header and footer
    header = create_message_header(msg_type, sender, target)
    footer = create_message_footer()

    # Construct the message structure
    message = {
        "messageHeader": header,
        **filtered_trade_data,
        "messageFooter": footer,
    }

    # Calculate and set the checksum
    serialized_message = json.dumps(
        {"messageHeader": header, **filtered_trade_data}, separators=(",", ":")
    )  # Exclude the footer itself from checksum calculation
    message["messageFooter"]["checksum"] = calculate_checksum(serialized_message)

    # Debugging outputs
    print(f"DEBUG: Serialized message before checksum: {serialized_message}")
    print(f"DEBUG: Checksum: {message['messageFooter']['checksum']}")

    return message


# Example Usage
if __name__ == "__main__":
    # Sample trade data
    sample_trade_data = {
        "tradeHeader": {
            "partyTradeIdentifier": {
                "partyReference": "",
                "tradeId": "hgraph-SWAP-002"
            },
            "tradeDate": "2024-11-20",
            "parties": [
                {
                    "internalParty": ""
                },
                {
                    "externalParty": ""
                }
            ],
            "portfolio": {
                "internalPortfolio": "",
                "externalPortfolio": ""
            },
            "trader": {
                "internalTrader": "",
                "externalTrader": ""
            }
        },
        "tradeEconomics": {
            "commoditySwap": {
                "buySell": "Buy",
                "effectiveDate": "2024-12-01",
                "terminationDate": "2025-12-01",
                "notionalQuantity": {
                    "quantity": 100,
                    "unit": "tonne"
                },
                "paymentCurrency": "USD"
            }
        },
        "tradeFooter": {
            "placeholderField1": "",
            "placeholderField2": ""
        },
        # Unwanted fields that should not appear in the output
        "buy_sell": "buySell",
        "currency": "paymentCurrency",
        "price_unit": "priceUnit"
    }

    # Wrap trade data with headers and footers
    full_message = wrap_message_with_headers_and_footers(
        trade_data=sample_trade_data,
        msg_type="NewTrade",
        sender="TradeSystemA",
        target="BookingSystemB"
    )

    # Serialize and print the complete message
    print(json.dumps(full_message, indent=4))