"""
kafka_sender.py

This module provides a KafkaSender class intended for sending trades or other
messages to a Kafka topic. It encapsulates the logic for connecting to a Kafka
cluster, optionally serializing messages to JSON, and safely closing the
producer when done.

Typical usage:
    sender = KafkaSender(bootstrap_servers="localhost:9092")
    sender.send_to_kafka("my_topic", {"key": "value"}, serialize_as_json=True)
    sender.close()
"""

import json
from typing import Any
from kafka import KafkaProducer, KafkaError

class KafkaSender:
    """
    Provides functionality to send messages to a Kafka topic.

    This class uses the KafkaProducer from `kafka-python` to publish messages.
    It supports optional JSON serialization for dictionary messages.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Kafka producer.

        :param bootstrap_servers: A string specifying the Kafka bootstrap servers.
                                 Use a comma-separated list if multiple servers.
        """
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send_to_kafka(self, topic: str, message: Any, serialize_as_json: bool = True) -> None:
        """
        Send a message to the specified Kafka topic.

        If `serialize_as_json` is True and `message` is a dictionary, it will be
        serialized to a JSON string before sending. Otherwise, `message` is assumed
        to be a string and will be encoded as UTF-8.

        :param topic: The name of the Kafka topic to which the message will be sent.
        :param message: The message to send. Can be either a string or a dictionary.
        :param serialize_as_json: If True and `message` is a dict, serialize it as JSON.
        :raises KafkaError: If there is a problem sending the message.
        """
        try:
            if serialize_as_json and isinstance(message, dict):
                message = json.dumps(message)
            self.producer.send(topic, message.encode("utf-8"))
            self.producer.flush()
        except KafkaError as e:
            # In a production environment, consider implementing retries or custom error handling.
            raise KafkaError(f"Failed to send message to topic '{topic}': {e}") from e

    def close(self) -> None:
        """
        Close the Kafka producer connection.

        After calling this method, the producer can no longer be used to send messages.
        """
        try:
            self.producer.close()
        except Exception as e:
            # In production, you might handle exceptions more gracefully.
            raise RuntimeError(f"Error closing Kafka producer: {e}") from e


# Example usage (for testing or demonstration purposes):
# if __name__ == "__main__":
#     sender = KafkaSender(bootstrap_servers="localhost:9092")
#     sample_message = {"tradeId": "SWAP-003", "instrument": "CommoditySwap"}
#     sender.send_to_kafka("test_topic", sample_message, serialize_as_json=True)
#     sender.close()