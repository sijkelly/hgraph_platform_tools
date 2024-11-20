import json
from typing import Dict, Any
import datetime


def create_message_header(msg_type: str, sender: str, target: str) -> Dict[str, Any]:
    """
    Create a message header with metadata for routing and processing.

    :param msg_type: Message type (e.g., "NewTrade", "CancelTrade").
    :param sender: Sender's identifier (e.g., system or user ID).
    :param target: Target system identifier.
    :return: A dictionary representing the message header.
    """
    return {
        "messageHeader": {
            "messageType": msg_type,
            "senderCompID": sender,
            "targetCompID": target,
            "sendingTime": datetime.datetime.utcnow().isoformat() + "Z",
            "messageVersion": "1.0"
        }
    }


def create_message_footer() -> Dict[str, Any]:
    """
    Create a message footer with integrity checks.

    :return: A dictionary representing the message footer.
    """
    return {
        "messageFooter": {
            "checksum": None  # Placeholder for checksum calculation.
        }
    }


def calculate_checksum(message: str) -> str:
    """
    Calculate a simple checksum for the message.

    :param message: The serialized message string (e.g., JSON).
    :return: The calculated checksum.
    """
    return str(sum(bytearray(message, "utf-8")) % 256)


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
    # Generate header and footer
    header = create_message_header(msg_type, sender, target)
    footer = create_message_footer()

    # Combine header, trade data, and footer
    message = {**header, **trade_data, **footer}

    # Calculate and set the checksum
    serialized_message = json.dumps(message, separators=(",", ":"))
    message["messageFooter"]["checksum"] = calculate_checksum(serialized_message)

    return message


# Example Usage
if __name__ == "__main__":
    # Sample trade data
    sample_trade_data = {
        "tradeHeader": {
            "tradeId": "SWAP-003",
            "tradeDate": "2024-11-10"
        },
        "commoditySwap": {
            "buySell": "Buy",
            "effectiveDate": "2024-12-01",
            "terminationDate": "2025-12-01",
            "notionalQuantity": {
                "quantity": 10000,
                "unit": "barrels"
            },
            "paymentCurrency": "USD"
        }
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