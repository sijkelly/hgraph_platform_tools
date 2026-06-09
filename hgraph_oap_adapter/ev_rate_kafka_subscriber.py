"""
Kafka subscriber service for EV (Expected Value) rate configuration.

Subscribes to a Kafka topic for EV rate updates, deserialises incoming
JSON messages, and persists the data into the local SQLite EV rate store
via :mod:`hgraph_oap_adapter.ev_rate_store`.

The subscriber runs a blocking poll loop via
:meth:`EVRateKafkaSubscriber.start` and can be stopped gracefully with
:meth:`EVRateKafkaSubscriber.stop`.

Message format
--------------

**EV rate topic** (default: ``oap.ev_rate``)::

    {
        "action": "UPSERT" | "DELETE",
        "instrument_category": "FixedFloatSwap",
        "unit": "MMBtu",
        "rate_per_unit": 0.02,
        "currency_symbol": "USD",
        "effective_date": "2026-01-01",
        "expiry_date": null
    }

The ``instrument_category`` value must match one of the
:class:`~hg_oap.orders.sales.ev.InstrumentCategory` enum values
(e.g. ``"FixedFloatSwap"``, ``"BasisSwap"``, ``"Forward"``).

The ``unit`` value must match a unit symbol in the default unit system
(e.g. ``"MMBtu"``, ``"kWh"``, ``"MWh"``).  Unrecognised unit strings
are rejected with a ``ValueError``.

For ``DELETE`` actions only ``instrument_category`` and ``unit`` are
required; all other fields are ignored.
"""

import logging
from datetime import date
from typing import Any, Dict, Tuple

from hg_oap.orders.sales.ev import EVRate, InstrumentCategory
from hg_oap.units.default_unit_system import U
from hg_oap.units.unit import Unit

from hgraph_oap_adapter.ev_rate_store import (
    delete_ev_rate,
    upsert_ev_rate,
)

__all__ = (
    "EVRateKafkaSubscriber",
    "parse_ev_rate_message",
)

logger = logging.getLogger(__name__)

# Reverse lookup: category string value → enum member
_CATEGORY_MAP: Dict[str, InstrumentCategory] = {c.value: c for c in InstrumentCategory}


# ---------------------------------------------------------------------------
# Message parser (standalone, testable without Kafka)
# ---------------------------------------------------------------------------


def _resolve_unit(unit_str: str) -> Unit:
    """Resolve a unit symbol string to a :class:`Unit` from the default unit system.

    :param unit_str: Unit symbol, e.g. ``"MMBtu"`` or ``"kWh"``.
    :returns: The matching :class:`Unit`.
    :raises ValueError: If the symbol is not found in the default unit system.
    """
    unit = getattr(U, unit_str, None)
    if unit is None:
        raise ValueError(
            f"Unknown unit symbol: {unit_str!r}. "
            f"Must be a symbol in the default unit system (e.g. 'MMBtu', 'kWh', 'MWh')."
        )
    return unit


def parse_ev_rate_message(msg: Dict[str, Any]) -> Tuple[str, EVRate | None, str | None]:
    """Parse an EV rate Kafka message.

    :param msg: Deserialised JSON message dict.
    :returns: A tuple of ``(action, ev_rate, unit_str)``.
        For ``DELETE`` actions *ev_rate* is ``None``.
        *unit_str* is always present (needed for DELETE key lookup).
    :raises ValueError: If required fields are missing, invalid, or the
        unit symbol is not recognised.
    """
    action = msg.get("action", "").upper()
    if action not in ("UPSERT", "DELETE"):
        raise ValueError(f"Unknown action: {msg.get('action')!r}")

    category_str = msg.get("instrument_category")
    if not category_str:
        raise ValueError("Message missing required 'instrument_category' field")

    category = _CATEGORY_MAP.get(category_str)
    if category is None:
        raise ValueError(
            f"Unknown instrument_category: {category_str!r}. " f"Expected one of: {list(_CATEGORY_MAP.keys())}"
        )

    unit_str = msg.get("unit")
    if not unit_str:
        raise ValueError("Message missing required 'unit' field")

    unit = _resolve_unit(unit_str)

    if action == "DELETE":
        return action, None, unit_str

    rate_per_unit = msg.get("rate_per_unit")
    if rate_per_unit is None:
        raise ValueError("UPSERT message missing required 'rate_per_unit' field")

    try:
        rate_per_unit = float(rate_per_unit)
    except (TypeError, ValueError):
        raise ValueError(f"'rate_per_unit' must be a number, got: {rate_per_unit!r}")

    currency_symbol = msg.get("currency_symbol", "USD")

    eff_str = msg.get("effective_date")
    exp_str = msg.get("expiry_date")
    effective_date = date.fromisoformat(eff_str) if eff_str else None
    expiry_date = date.fromisoformat(exp_str) if exp_str else None

    ev_rate = EVRate(
        instrument_category=category,
        unit=unit,
        rate_per_unit=rate_per_unit,
        currency_symbol=currency_symbol,
        effective_date=effective_date,
        expiry_date=expiry_date,
    )
    return action, ev_rate, unit_str


# ---------------------------------------------------------------------------
# Subscriber service
# ---------------------------------------------------------------------------


class EVRateKafkaSubscriber:
    """Subscribes to the EV rate Kafka topic and persists updates to the store.

    :param db_path: Path to the SQLite EV rate database.
    :param bootstrap_servers: Kafka bootstrap servers.  Falls back to config.
    :param group_id: Kafka consumer group.  Falls back to config.
    :param ev_rate_topic: Topic for EV rate messages.  Falls back to config.
    """

    def __init__(
        self,
        db_path: str,
        *,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        ev_rate_topic: str | None = None,
    ):
        from secure_config import config

        self.db_path = db_path
        self.bootstrap_servers = bootstrap_servers or config["KAFKA_BOOTSTRAP_SERVERS"]
        self.group_id = group_id or config.get("KAFKA_CONSUMER_GROUP", "hgraph_platform")
        self.ev_rate_topic = ev_rate_topic or config.get("EV_RATE_KAFKA_TOPIC", "oap.ev_rate")
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

        topics = [self.ev_rate_topic]
        self._receiver = KafkaReceiver(
            topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )

        self._running = True
        self._processed_count = 0
        logger.info(
            "EV rate subscriber started: topic=%s, group=%s",
            self.ev_rate_topic,
            self.group_id,
        )

        try:
            while self._running:
                records = self._receiver.poll(timeout_ms=poll_interval_ms)
                for record in records:
                    topic = record["topic"]
                    value = record["value"]
                    try:
                        self.process_message(topic, value)
                        self._processed_count += 1
                    except Exception as exc:
                        logger.error(
                            "Error processing message on topic %s: %s — message: %s",
                            topic,
                            exc,
                            value,
                        )

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

        Can be called directly for testing without Kafka.

        :param topic: The Kafka topic the message came from.
        :param message: The deserialised message dict.
        :raises ValueError: If the message cannot be parsed.
        """
        if topic == self.ev_rate_topic:
            self._handle_ev_rate_message(message)
        else:
            logger.warning("Unknown topic: %s", topic)

    def _handle_ev_rate_message(self, message: Dict[str, Any]) -> None:
        """Handle an EV rate message."""
        action, ev_rate, unit_str = parse_ev_rate_message(message)
        category_str = message["instrument_category"]

        if action == "UPSERT":
            upsert_ev_rate(self.db_path, ev_rate)
            logger.info("Upserted EV rate: category=%s, unit=%s", category_str, unit_str)
        elif action == "DELETE":
            category = _CATEGORY_MAP[category_str]
            unit = _resolve_unit(unit_str)
            deleted = delete_ev_rate(self.db_path, category, unit)
            if deleted:
                logger.info("Deleted EV rate: category=%s, unit=%s", category_str, unit_str)
            else:
                logger.warning(
                    "EV rate not found for deletion: category=%s, unit=%s",
                    category_str,
                    unit_str,
                )

    def stop(self) -> None:
        """Signal the subscriber to stop after the current poll cycle."""
        self._running = False
        logger.info("EV rate subscriber stop requested")

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
