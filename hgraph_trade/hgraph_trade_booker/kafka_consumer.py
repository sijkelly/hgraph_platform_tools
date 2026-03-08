"""
kafka_consumer.py

Provides a KafkaReceiver class for consuming messages from Kafka topics.
Mirrors the architecture of :class:`KafkaSender` with config-driven defaults,
JSON deserialisation, and clean resource management.

Typical usage::

    receiver = KafkaReceiver(["party.legal_entity"])
    messages = receiver.poll(timeout_ms=2000)
    for msg in messages:
        process(msg)
    receiver.commit()
    receiver.close()
"""

import json
import logging
from typing import Any, Callable, Dict, List

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from secure_config import config

__all__ = (
    "KafkaReceiver",
)

logger = logging.getLogger(__name__)


def _default_json_deserializer(raw: bytes) -> Dict[str, Any]:
    """Decode UTF-8 bytes and parse as JSON."""
    return json.loads(raw.decode("utf-8"))


class KafkaReceiver:
    """Wraps :class:`KafkaConsumer` with config-driven defaults and JSON deserialisation.

    :param topics: Initial list of topics to subscribe to.
    :param bootstrap_servers: Kafka bootstrap servers (comma-separated).
        Falls back to ``config["KAFKA_BOOTSTRAP_SERVERS"]``.
    :param group_id: Consumer group identifier.
        Falls back to ``config["KAFKA_CONSUMER_GROUP"]``.
    :param auto_offset_reset: Where to start consuming when no committed
        offset exists.  One of ``"earliest"``, ``"latest"``.
    :param value_deserializer: Callable to deserialise message bytes.
        Defaults to JSON deserialisation.
    :param enable_auto_commit: If ``True``, offsets are committed
        automatically.  Set to ``False`` for manual commit via
        :meth:`commit`.
    """

    def __init__(
        self,
        topics: List[str],
        *,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        auto_offset_reset: str = "earliest",
        value_deserializer: Callable[[bytes], Any] | None = None,
        enable_auto_commit: bool = False,
    ):
        if bootstrap_servers is None:
            bootstrap_servers = config["KAFKA_BOOTSTRAP_SERVERS"]
        if group_id is None:
            group_id = config.get("KAFKA_CONSUMER_GROUP", "hgraph_platform")
        if value_deserializer is None:
            value_deserializer = _default_json_deserializer

        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._topics = list(topics)
        self._deserializer = value_deserializer

        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            value_deserializer=value_deserializer,
        )
        logger.info(
            "KafkaReceiver initialised: topics=%s, group=%s, servers=%s",
            topics,
            group_id,
            bootstrap_servers,
        )

    def poll(self, timeout_ms: int = 1000, max_records: int = 100) -> List[Dict[str, Any]]:
        """Poll for new messages and return a list of deserialised records.

        Each record in the returned list is a dict with keys:
        ``topic``, ``partition``, ``offset``, ``key``, ``value``, ``timestamp``.

        :param timeout_ms: Maximum time to block waiting for messages.
        :param max_records: Maximum number of records to return.
        :returns: List of message dicts with metadata and deserialised value.
        """
        raw = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        results: List[Dict[str, Any]] = []

        for tp, records in raw.items():
            for record in records:
                results.append(
                    {
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "key": record.key,
                        "value": record.value,
                        "timestamp": record.timestamp,
                    }
                )
        return results

    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to additional topics (replaces the current subscription).

        :param topics: Full list of topics to subscribe to.
        """
        self._topics = list(topics)
        self.consumer.subscribe(topics)
        logger.info("KafkaReceiver subscription updated: topics=%s", topics)

    def commit(self) -> None:
        """Manually commit current offsets for all assigned partitions."""
        self.consumer.commit()

    def close(self) -> None:
        """Close the Kafka consumer connection.

        After calling this method the consumer can no longer be used.
        """
        try:
            self.consumer.close()
            logger.info("KafkaReceiver closed")
        except Exception as exc:
            logger.error("Error closing Kafka consumer: %s", exc)
            raise RuntimeError(f"Error closing Kafka consumer: {exc}") from exc
