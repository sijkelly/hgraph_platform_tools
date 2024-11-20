import json
import os
from typing import Dict, Any


def compile_booking_message(trade: Dict[str, Any]) -> str:
    """
    Compile the trade into a JSON booking message.

    :param trade: The trade data to compile.
    :return: A formatted JSON string.
    """
    return json.dumps(trade, indent=4)


def save_booking_message_to_file(message: str, file_path: str):
    """
    Save the booking message to a local text file for testing.

    :param message: The booking message as a JSON string.
    :param file_path: The path to save the file.
    """
    with open(file_path, 'w') as file:
        file.write(message)


def book_trade(trade_data: Dict[str, Any], file_name: str, kafka_topic: str = None):
    """
    Book the trade by saving it locally and optionally sending it via Kafka.

    :param trade_data: The trade data to book.
    :param file_name: The name of the file to save locally.
    :param kafka_topic: The Kafka topic to send the message to (placeholder for production use).
    """
    # Compile the booking message
    message = compile_booking_message(trade_data)

    # Save to a local file
    local_file_path = os.path.join("/Users/simonkelly/Documents/GitHub/hgraph_platform_tools/test_trades_booking_outputs", file_name)
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)  # Ensure directory exists
    save_booking_message_to_file(message, local_file_path)
    print(f"Booking message saved to {local_file_path}")

    # Placeholder: Sending to Kafka (for production)
    if kafka_topic:
        send_to_kafka(kafka_topic, message)


# Example usage
if __name__ == "__main__":
    sample_trade_data = {
        "tradeHeader": {
            "partyTradeIdentifier": {
                "partyReference": "Party1",
                "tradeId": "SWAP-003"
            },
            "tradeDate": "2024-11-10",
            "parties": [
                {"internalParty": "Trader A"},
                {"externalParty": "Trader B"}
            ],
            "portfolio": {
                "internalPortfolio": "Portfolio1",
                "externalPortfolio": "Portfolio2"
            },
            "trader": {
                "internalTrader": "Trader A",
                "externalTrader": "Trader B"
            }
        },
        "commoditySwap": {
            "buySell": "Buy",
            "effectiveDate": "2024-12-01",
            "terminationDate": "2025-12-01",
            "underlyer": "WTI",
            "notionalQuantity": {
                "quantity": 10000,
                "unit": "barrels"
            },
            "paymentCurrency": "USD",
            "fixedLeg": {
                "fixedPrice": 85.50,
                "priceUnit": "USD per barrel"
            },
            "floatLeg": {
                "referencePrice": "Brent",
                "resetDates": ["2024-12-15", "2025-01-15"]
            }
        }
    }

    book_trade(sample_trade_data, "fixed_float_BM_swap_001.txt")