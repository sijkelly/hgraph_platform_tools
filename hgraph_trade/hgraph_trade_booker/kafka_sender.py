"""
kafka_sender.py

This module provides a KafkaSender class intended for sending trades or other
messages to a Kafka topic. It encapsulates the logic for connecting to a Kafka
cluster, optionally serializing messages to JSON, and safely closing the
producer when done.

Includes configurable retry logic with exponential back-off.

Typical usage:
    sender = KafkaSender()
    sender.send_to_kafka("my_topic", {"key": "value"}, serialize_as_json=True)
    sender.close()
"""

import json
import logging
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from secure_config import config

__all__ = (
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_RETRY_BACKOFF",
    "DEFAULT_SEND_TIMEOUT",
    "KafkaSender",
)

logger = logging.getLogger(__name__)

# Defaults — can be overridden per-instance
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF = 1.0  # seconds; doubled on each retry
DEFAULT_SEND_TIMEOUT = 10.0  # seconds to wait for the broker to acknowledge a message


class KafkaSender:
    """
    Provides functionality to send messages to a Kafka topic.

    This class uses the KafkaProducer from ``kafka-python`` to publish messages.
    It supports optional JSON serialisation and automatic retry with exponential
    back-off on transient failures.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
        send_timeout: float = DEFAULT_SEND_TIMEOUT,
    ):
        """
        Initialise the Kafka producer.

        :param bootstrap_servers: Kafka bootstrap servers (comma-separated).
                                  Falls back to ``config["KAFKA_BOOTSTRAP_SERVERS"]``.
        :param max_retries: Number of times to retry a failed send before giving up.
        :param retry_backoff: Initial back-off in seconds; doubled after each retry.
        :param send_timeout: Seconds to wait for the broker to acknowledge each message.
        """
        if bootstrap_servers is None:
            bootstrap_servers = config["KAFKA_BOOTSTRAP_SERVERS"]
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.send_timeout = send_timeout
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send_to_kafka(
        self,
        topic: str,
        message: Any,
        serialize_as_json: bool = True,
        key: str | None = None,
    ) -> None:
        """
        Send a message to the specified Kafka topic with automatic retry.

        If ``serialize_as_json`` is True and ``message`` is a dictionary, it will
        be serialised to a JSON string before sending. Otherwise ``message`` is
        assumed to be a string and will be encoded as UTF-8.

        :param topic: The Kafka topic to send to.
        :param message: The message payload (string or dict).
        :param serialize_as_json: Serialise dicts to JSON if True.
        :param key: Optional message key.  Messages with the same key go to the same partition,
                    and consumers can use it to recognise a message they have already processed.
        :raises KafkaError: If all retry attempts are exhausted.

        The call returns only once the broker has acknowledged the message; a message the
        broker did not acknowledge counts as a failed attempt.
        """
        if serialize_as_json and isinstance(message, dict):
            message = json.dumps(message)

        encoded = message.encode("utf-8")
        last_error: Exception | None = None
        backoff = self.retry_backoff

        for attempt in range(1, self.max_retries + 1):
            try:
                future = self.producer.send(topic, encoded, key=key.encode("utf-8") if key else None)
                future.get(timeout=self.send_timeout)  # raises if the broker did not acknowledge
                if attempt > 1:
                    logger.info(
                        "Kafka send succeeded on attempt %d for topic '%s'",
                        attempt,
                        topic,
                    )
                return  # success
            except KafkaError as exc:
                last_error = exc
                logger.warning(
                    "Kafka send attempt %d/%d failed for topic '%s': %s",
                    attempt,
                    self.max_retries,
                    topic,
                    exc,
                )
                if attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff *= 2  # exponential back-off

        raise KafkaError(
            f"Failed to send message to topic '{topic}' after " f"{self.max_retries} attempts: {last_error}"
        )

    def close(self) -> None:
        """
        Close the Kafka producer connection.

        After calling this method, the producer can no longer be used to send messages.
        """
        try:
            self.producer.close()
        except Exception as exc:
            logger.error("Error closing Kafka producer: %s", exc)
            raise RuntimeError(f"Error closing Kafka producer: {exc}") from exc
