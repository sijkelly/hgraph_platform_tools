import json
from typing import Dict, Any
from kafka_sender import send_to_kafka
from utils import save_to_database


def compile_booking_message(trade: Dict[str, Any]) -> str:
    """
    Compile the trade into a JSON booking message.
    """
    return json.dumps(trade, indent=4)


def book_trade(trade_data: Dict[str, Any], kafka_topic: str):
    """
    Book the trade by sending it via Kafka.
    """
    message = compile_booking_message(trade_data)

    # Save to database for tracking
    save_to_database("sent_messages", message)

    # Send to Kafka
    send_to_kafka(kafka_topic, message)