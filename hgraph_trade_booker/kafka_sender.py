import json
import logging
from kafka import KafkaProducer, KafkaError
from typing import Any

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class KafkaSender:
    """
    A Kafka sender class for sending messages to a Kafka topic.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Kafka producer with the given bootstrap servers.

        :param bootstrap_servers: Kafka broker addresses (comma-separated if multiple).
        """
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send_to_kafka(self, topic: str, message: Any, serialize_as_json: bool = True):
        """
        Send a message to a Kafka topic.

        :param topic: The Kafka topic to send the message to.
        :param message: The message to send. Can be a string or a dictionary.
        :param serialize_as_json: Whether to serialize the message as JSON.
        """
        try:
            # Serialize the message if needed
            if serialize_as_json and isinstance(message, dict):
                message = json.dumps(message)

            # Send the message
            self.producer.send(topic, message.encode("utf-8"))
            self.producer.flush()
            logging.info(f"Message sent to Kafka topic: {topic}")
        except KafkaError as e:
            logging.error(f"Failed to send message to Kafka topic {topic}: {e}")
            raise

    def close(self):
        """
        Close the Kafka producer.
        """
        try:
            self.producer.close()
            logging.info("Kafka producer closed successfully.")
        except Exception as e:
            logging.error(f"Error while closing Kafka producer: {e}")


# Example usage
if __name__ == "__main__":
    # Kafka configuration
    kafka_topic = "test_topic"
    bootstrap_servers = "localhost:9092"

    # Sample message
    sample_message = {
        "tradeId": "SWAP-003",
        "tradeDate": "2024-11-25",
        "instrument": "CommoditySwap"
    }

    # Initialize KafkaSender and send a message
    sender = KafkaSender(bootstrap_servers=bootstrap_servers)
    try:
        sender.send_to_kafka(kafka_topic, sample_message)
    finally:
        sender.close()