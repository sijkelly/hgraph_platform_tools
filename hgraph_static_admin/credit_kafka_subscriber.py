"""
Kafka subscriber service for credit limit and utilization data.

Subscribes to two Kafka topics — one for credit limit updates and one
for credit utilization snapshots — deserialises incoming JSON messages,
and persists the data into the local SQLite credit store.

The subscriber runs a blocking poll loop via
:meth:`CreditKafkaSubscriber.start` and can be stopped gracefully
with :meth:`CreditKafkaSubscriber.stop`.

Message formats
---------------

**Credit limit topic** (default: ``credit.limit``)::

    {
        "action": "UPSERT" | "DELETE",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": 50000000.0,
        "limit_currency": "USD",
        "effective_date": "2025-01-01",
        "expiry_date": "2026-01-01",
        "status": "Active",
        "approved_by": "Credit Committee",
        "last_review_date": "2025-06-15"
    }

**Credit utilization topic** (default: ``credit.utilization``)::

    {
        "action": "UPSERT" | "DELETE",
        "counterparty_symbol": "ACME",
        "utilized_amount": 15000000.0,
        "utilization_currency": "USD",
        "limit_amount": 50000000.0,
        "available_amount": 35000000.0,
        "utilization_percentage": 30.0,
        "as_of_timestamp": "2025-07-01T14:30:00+00:00"
    }
"""

import logging
from datetime import date
from typing import Any, Dict, Tuple

from hg_oap.credit.credit_limit import (
    CreditLimit,
    CreditLimitType,
    CreditStatus,
    CreditUtilization,
)

from hgraph_static_admin.credit_store import (
    delete_credit_limit,
    delete_credit_utilization,
    upsert_credit_limit,
    upsert_credit_utilization,
)

__all__ = (
    "CreditKafkaSubscriber",
    "parse_credit_limit_message",
    "parse_credit_utilization_message",
)

logger = logging.getLogger(__name__)

# Stop the subscriber after this many consecutive processing failures
# to avoid infinite error loops on persistently bad data.
_MAX_CONSECUTIVE_ERRORS = 50

# Reverse lookup: limit type string value → enum member
_LIMIT_TYPE_MAP: Dict[str, CreditLimitType] = {t.value: t for t in CreditLimitType}

# Reverse lookup: status string value → enum member
_STATUS_MAP: Dict[str, CreditStatus] = {s.value: s for s in CreditStatus}


# ---------------------------------------------------------------------------
# Message parsers (standalone, testable without Kafka)
# ---------------------------------------------------------------------------


def parse_credit_limit_message(msg: Dict[str, Any]) -> Tuple[str, CreditLimit | None]:
    """Parse a credit limit Kafka message.

    :param msg: Deserialised JSON message dict.
    :returns: A tuple of ``(action, credit_limit)``.  For ``DELETE`` actions
        the credit_limit is ``None``.
    :raises ValueError: If required fields are missing or invalid.
    """
    action = msg.get("action", "").upper()
    if action not in ("UPSERT", "DELETE"):
        raise ValueError(f"Unknown action: {msg.get('action')!r}")

    counterparty_symbol = msg.get("counterparty_symbol")
    if not counterparty_symbol:
        raise ValueError("Message missing required 'counterparty_symbol' field")

    limit_type_str = msg.get("limit_type", "")
    if not limit_type_str and action != "DELETE":
        raise ValueError("UPSERT message missing required 'limit_type' field")

    if action == "DELETE":
        return action, None

    limit_type = _LIMIT_TYPE_MAP.get(limit_type_str)
    if limit_type is None:
        raise ValueError(
            f"Unknown limit_type: {limit_type_str!r}. "
            f"Expected one of: {list(_LIMIT_TYPE_MAP.keys())}"
        )

    limit_amount = msg.get("limit_amount")
    if limit_amount is None:
        raise ValueError("UPSERT message missing required 'limit_amount' field")
    try:
        limit_amount = float(limit_amount)
    except (TypeError, ValueError):
        raise ValueError(f"limit_amount must be numeric, got: {type(limit_amount).__name__}")
    if limit_amount < 0:
        raise ValueError(f"limit_amount must be non-negative, got: {limit_amount}")

    status_str = msg.get("status", "Active")
    status = _STATUS_MAP.get(status_str)
    if status is None:
        raise ValueError(
            f"Unknown status: {status_str!r}. "
            f"Expected one of: {list(_STATUS_MAP.keys())}"
        )

    effective_date = None
    if msg.get("effective_date"):
        effective_date = date.fromisoformat(msg["effective_date"])

    expiry_date = None
    if msg.get("expiry_date"):
        expiry_date = date.fromisoformat(msg["expiry_date"])

    last_review_date = None
    if msg.get("last_review_date"):
        last_review_date = date.fromisoformat(msg["last_review_date"])

    credit_limit = CreditLimit(
        counterparty_symbol=counterparty_symbol,
        limit_type=limit_type,
        limit_amount=float(limit_amount),
        limit_currency=msg.get("limit_currency", "USD"),
        effective_date=effective_date,
        expiry_date=expiry_date,
        status=status,
        approved_by=msg.get("approved_by"),
        last_review_date=last_review_date,
    )
    return action, credit_limit


def parse_credit_utilization_message(msg: Dict[str, Any]) -> Tuple[str, CreditUtilization | None]:
    """Parse a credit utilization Kafka message.

    :param msg: Deserialised JSON message dict.
    :returns: A tuple of ``(action, utilization)``.  For ``DELETE`` actions
        the utilization is ``None``.
    :raises ValueError: If required fields are missing or invalid.
    """
    action = msg.get("action", "").upper()
    if action not in ("UPSERT", "DELETE"):
        raise ValueError(f"Unknown action: {msg.get('action')!r}")

    counterparty_symbol = msg.get("counterparty_symbol")
    if not counterparty_symbol:
        raise ValueError("Message missing required 'counterparty_symbol' field")

    if action == "DELETE":
        return action, None

    utilized_amount = msg.get("utilized_amount")
    if utilized_amount is None:
        raise ValueError("UPSERT message missing required 'utilized_amount' field")
    try:
        utilized_amount = float(utilized_amount)
    except (TypeError, ValueError):
        raise ValueError(f"utilized_amount must be numeric, got: {type(utilized_amount).__name__}")
    if utilized_amount < 0:
        raise ValueError(f"utilized_amount must be non-negative, got: {utilized_amount}")

    try:
        limit_amount = float(msg.get("limit_amount", 0.0))
        available_amount = float(msg.get("available_amount", 0.0))
        utilization_percentage = float(msg.get("utilization_percentage", 0.0))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Numeric field has invalid type: {exc}")

    if utilization_percentage < 0 or utilization_percentage > 100:
        raise ValueError(f"utilization_percentage must be 0-100, got: {utilization_percentage}")

    utilization = CreditUtilization(
        counterparty_symbol=counterparty_symbol,
        utilized_amount=utilized_amount,
        utilization_currency=msg.get("utilization_currency", "USD"),
        limit_amount=limit_amount,
        available_amount=available_amount,
        utilization_percentage=utilization_percentage,
        as_of_timestamp=msg.get("as_of_timestamp"),
    )
    return action, utilization


# ---------------------------------------------------------------------------
# Subscriber service
# ---------------------------------------------------------------------------


class CreditKafkaSubscriber:
    """Subscribes to credit Kafka topics and persists updates to the credit store.

    :param db_path: Path to the credit SQLite database.
    :param bootstrap_servers: Kafka bootstrap servers.  Falls back to config.
    :param group_id: Kafka consumer group.  Falls back to config.
    :param limit_topic: Topic for credit limit messages.
    :param utilization_topic: Topic for credit utilization messages.
    """

    def __init__(
        self,
        db_path: str,
        *,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        limit_topic: str | None = None,
        utilization_topic: str | None = None,
    ):
        from secure_config import config

        self.db_path = db_path
        self.bootstrap_servers = bootstrap_servers or config["KAFKA_BOOTSTRAP_SERVERS"]
        self.group_id = group_id or config.get("KAFKA_CONSUMER_GROUP", "hgraph_platform")
        self.limit_topic = limit_topic or config.get(
            "CREDIT_KAFKA_LIMIT_TOPIC", "credit.limit"
        )
        self.utilization_topic = utilization_topic or config.get(
            "CREDIT_KAFKA_UTILIZATION_TOPIC", "credit.utilization"
        )
        self._running = False
        self._receiver = None
        self._processed_count = 0

    def start(self, *, poll_interval_ms: int = 1000, max_messages: int | None = None) -> None:
        """Start the subscriber loop.

        Blocks until :meth:`stop` is called or *max_messages* have been
        processed (useful for testing / batch mode).

        :param poll_interval_ms: Kafka poll timeout in milliseconds.
        :param max_messages: Stop after processing this many messages
            (``None`` = run indefinitely).
        """
        from hgraph_trade.hgraph_trade_booker.kafka_consumer import KafkaReceiver

        topics = [self.limit_topic, self.utilization_topic]
        self._receiver = KafkaReceiver(
            topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )

        self._running = True
        self._processed_count = 0
        logger.info(
            "Credit subscriber started: topics=%s, group=%s",
            topics,
            self.group_id,
        )

        consecutive_errors = 0
        try:
            while self._running:
                records = self._receiver.poll(timeout_ms=poll_interval_ms)
                for record in records:
                    topic = record["topic"]
                    value = record["value"]
                    try:
                        self.process_message(topic, value)
                        self._processed_count += 1
                        consecutive_errors = 0
                    except Exception as exc:
                        consecutive_errors += 1
                        logger.error(
                            "Error processing message on topic %s: %s — counterparty: %s",
                            topic,
                            exc,
                            value.get("counterparty_symbol", "unknown") if isinstance(value, dict) else "unknown",
                        )
                        if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                            logger.critical(
                                "Too many consecutive errors (%d), stopping credit subscriber",
                                consecutive_errors,
                            )
                            self._running = False
                            break

                if records:
                    self._receiver.commit()

                if max_messages is not None and self._processed_count >= max_messages:
                    logger.info(
                        "Reached max_messages=%d, stopping subscriber",
                        max_messages,
                    )
                    break
        finally:
            self._running = False

    def process_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Process a single Kafka message.

        Routes the message to the appropriate handler based on topic.
        Can also be called directly for testing without Kafka.

        :param topic: The Kafka topic the message came from.
        :param message: The deserialised message dict.
        :raises ValueError: If the message cannot be parsed.
        """
        if topic == self.limit_topic:
            self._handle_limit_message(message)
        elif topic == self.utilization_topic:
            self._handle_utilization_message(message)
        else:
            logger.warning("Unknown topic: %s", topic)

    def _handle_limit_message(self, message: Dict[str, Any]) -> None:
        """Handle a credit limit message."""
        action, credit_limit = parse_credit_limit_message(message)
        cpty = message["counterparty_symbol"]

        if action == "UPSERT":
            upsert_credit_limit(self.db_path, credit_limit)
            logger.info("Upserted credit limit: %s / %s", cpty, credit_limit.limit_type.value)
        elif action == "DELETE":
            limit_type = message.get("limit_type", "")
            deleted = delete_credit_limit(self.db_path, cpty, limit_type)
            if deleted:
                logger.info("Deleted credit limit: %s / %s", cpty, limit_type)
            else:
                logger.warning("Credit limit not found for deletion: %s / %s", cpty, limit_type)

    def _handle_utilization_message(self, message: Dict[str, Any]) -> None:
        """Handle a credit utilization message."""
        action, utilization = parse_credit_utilization_message(message)
        cpty = message["counterparty_symbol"]

        if action == "UPSERT":
            upsert_credit_utilization(self.db_path, utilization)
            logger.info("Upserted credit utilization: %s", cpty)
        elif action == "DELETE":
            deleted = delete_credit_utilization(self.db_path, cpty)
            if deleted:
                logger.info("Deleted credit utilization: %s", cpty)
            else:
                logger.warning("Credit utilization not found for deletion: %s", cpty)

    def stop(self) -> None:
        """Signal the subscriber to stop after the current poll cycle."""
        self._running = False
        logger.info("Credit subscriber stop requested")

    def close(self) -> None:
        """Close the Kafka consumer and release resources."""
        self.stop()
        if self._receiver is not None:
            self._receiver.close()
            self._receiver = None

    @property
    def processed_count(self) -> int:
        """Number of messages successfully processed."""
        return self._processed_count
