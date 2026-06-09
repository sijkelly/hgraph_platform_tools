"""
Kafka subscriber service for party/counterparty static data.

Subscribes to two Kafka topics — one for legal entity updates and
one for trading relationship updates — deserialises incoming JSON
messages, and persists the data into the local SQLite party store.

The subscriber runs a blocking poll loop via :meth:`PartyKafkaSubscriber.start`
and can be stopped gracefully with :meth:`PartyKafkaSubscriber.stop`.

Message formats
---------------

**Legal entity topic** (default: ``party.legal_entity``)::

    {
        "action": "UPSERT" | "DELETE",
        "symbol": "ACME",
        "name": "ACME Energy Corp",
        "classification": "NFEU",
        "lei": "529900ACMEENERGY0LEI",
        "jurisdiction": "US",
        "registration_id": "NFA-12345",
        "tax_id": "12-3456789",
        "address": "123 Main St, Houston TX"
    }

**Trading relationship topic** (default: ``party.trading_relationship``)::

    {
        "action": "UPSERT" | "DELETE",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "ACME",
        "clearing_status": "Bilateral",
        "isda": {
            "version": "2002",
            "credit_support_annex": true,
            "governing_law": "New York"
        },
        "naesb": null,
        "eei": null,
        "dropcopy_enabled": false,
        "internal_portfolio": "GAS-BOOK-1",
        "external_portfolio": "EXT-001"
    }
"""

import logging
from datetime import date
from typing import Any, Dict, Tuple

from hg_oap.parties.agreement import MasterAgreement, MasterAgreementType
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship

from hgraph_static_admin.party_store import (
    delete_legal_entity,
    delete_trading_relationship,
    get_legal_entity,
    upsert_legal_entity,
    upsert_trading_relationship,
)

__all__ = (
    "PartyKafkaSubscriber",
    "parse_legal_entity_message",
    "parse_relationship_message",
)

logger = logging.getLogger(__name__)

# Stop the subscriber after this many consecutive processing failures
# to avoid infinite error loops on persistently bad data.
_MAX_CONSECUTIVE_ERRORS = 50

# Reverse lookup: classification string value → enum member
_CLASSIFICATION_MAP: Dict[str, PartyClassification] = {c.value: c for c in PartyClassification}

# Reverse lookup: clearing status string value → enum member
_CLEARING_STATUS_MAP: Dict[str, ClearingStatus] = {c.value: c for c in ClearingStatus}


# ---------------------------------------------------------------------------
# Message parsers (standalone, testable without Kafka)
# ---------------------------------------------------------------------------


def parse_legal_entity_message(msg: Dict[str, Any]) -> Tuple[str, LegalEntity | None]:
    """Parse a legal entity Kafka message.

    :param msg: Deserialised JSON message dict.
    :returns: A tuple of ``(action, entity)``.  For ``DELETE`` actions
        the entity is ``None``.
    :raises ValueError: If required fields are missing or invalid.
    """
    action = msg.get("action", "").upper()
    if action not in ("UPSERT", "DELETE"):
        raise ValueError(f"Unknown action: {msg.get('action')!r}")

    symbol = msg.get("symbol")
    if not symbol:
        raise ValueError("Message missing required 'symbol' field")

    if action == "DELETE":
        return action, None

    name = msg.get("name")
    if not name:
        raise ValueError("UPSERT message missing required 'name' field")

    classification_str = msg.get("classification", "")
    classification = _CLASSIFICATION_MAP.get(classification_str)
    if classification is None:
        raise ValueError(
            f"Unknown classification: {classification_str!r}. "
            f"Expected one of: {list(_CLASSIFICATION_MAP.keys())}"
        )

    entity = LegalEntity(
        symbol=symbol,
        name=name,
        classification=classification,
        lei=msg.get("lei"),
        jurisdiction=msg.get("jurisdiction"),
        registration_id=msg.get("registration_id"),
        tax_id=msg.get("tax_id"),
        address=msg.get("address"),
    )
    return action, entity


def _parse_agreement(data: Dict[str, Any] | None, agreement_type: MasterAgreementType) -> MasterAgreement | None:
    """Parse a nested agreement dict into a MasterAgreement."""
    if data is None:
        return None

    kwargs: Dict[str, Any] = {"agreement_type": agreement_type}

    if "version" in data:
        kwargs["version"] = data["version"]
    if "agreement_date" in data and data["agreement_date"]:
        kwargs["agreement_date"] = date.fromisoformat(data["agreement_date"])
    if "credit_support_annex" in data:
        kwargs["credit_support_annex"] = bool(data["credit_support_annex"])
    if "threshold_amount" in data and data["threshold_amount"] is not None:
        try:
            kwargs["threshold_amount"] = float(data["threshold_amount"])
        except (TypeError, ValueError):
            raise ValueError(f"threshold_amount must be numeric, got: {type(data['threshold_amount']).__name__}")
    if "governing_law" in data:
        kwargs["governing_law"] = data["governing_law"]

    return MasterAgreement(**kwargs)


def parse_relationship_message(
    msg: Dict[str, Any],
    db_path: str,
) -> Tuple[str, TradingRelationship | None]:
    """Parse a trading relationship Kafka message.

    For ``UPSERT`` actions the internal and external parties are looked
    up from the party store.  They must already exist.

    :param msg: Deserialised JSON message dict.
    :param db_path: Path to the party SQLite database (for entity lookup).
    :returns: A tuple of ``(action, relationship)``.  For ``DELETE``
        actions the relationship is ``None``.
    :raises ValueError: If required fields are missing, parties not found,
        or values are invalid.
    """
    action = msg.get("action", "").upper()
    if action not in ("UPSERT", "DELETE"):
        raise ValueError(f"Unknown action: {msg.get('action')!r}")

    int_sym = msg.get("internal_party_symbol")
    ext_sym = msg.get("external_party_symbol")
    if not int_sym or not ext_sym:
        raise ValueError("Message missing required 'internal_party_symbol' and/or 'external_party_symbol'")

    if action == "DELETE":
        return action, None

    # Look up parties from the store
    internal = get_legal_entity(db_path, int_sym)
    if internal is None:
        raise ValueError(f"Internal party not found in store: {int_sym!r}")

    external = get_legal_entity(db_path, ext_sym)
    if external is None:
        raise ValueError(f"External party not found in store: {ext_sym!r}")

    # Clearing status
    clearing_str = msg.get("clearing_status", "Bilateral")
    clearing_status = _CLEARING_STATUS_MAP.get(clearing_str)
    if clearing_status is None:
        raise ValueError(f"Unknown clearing_status: {clearing_str!r}")

    # Agreements
    isda = _parse_agreement(msg.get("isda"), MasterAgreementType.ISDA)
    naesb = _parse_agreement(msg.get("naesb"), MasterAgreementType.NAESB)
    eei = _parse_agreement(msg.get("eei"), MasterAgreementType.EEI)

    relationship = TradingRelationship(
        internal_party=internal,
        external_party=external,
        clearing_status=clearing_status,
        isda=isda,
        naesb=naesb,
        eei=eei,
        dropcopy_enabled=bool(msg.get("dropcopy_enabled", False)),
        internal_portfolio=msg.get("internal_portfolio"),
        external_portfolio=msg.get("external_portfolio"),
    )
    return action, relationship


# ---------------------------------------------------------------------------
# Subscriber service
# ---------------------------------------------------------------------------


class PartyKafkaSubscriber:
    """Subscribes to party Kafka topics and persists updates to the party store.

    :param db_path: Path to the party SQLite database.
    :param bootstrap_servers: Kafka bootstrap servers.  Falls back to config.
    :param group_id: Kafka consumer group.  Falls back to config.
    :param entity_topic: Topic for legal entity messages.
    :param relationship_topic: Topic for trading relationship messages.
    """

    def __init__(
        self,
        db_path: str,
        *,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        entity_topic: str | None = None,
        relationship_topic: str | None = None,
    ):
        from secure_config import config

        self.db_path = db_path
        self.bootstrap_servers = bootstrap_servers or config["KAFKA_BOOTSTRAP_SERVERS"]
        self.group_id = group_id or config.get("KAFKA_CONSUMER_GROUP", "hgraph_platform")
        self.entity_topic = entity_topic or config.get("PARTY_KAFKA_ENTITY_TOPIC", "party.legal_entity")
        self.relationship_topic = relationship_topic or config.get(
            "PARTY_KAFKA_RELATIONSHIP_TOPIC", "party.trading_relationship"
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

        topics = [self.entity_topic, self.relationship_topic]
        self._receiver = KafkaReceiver(
            topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )

        self._running = True
        self._processed_count = 0
        logger.info(
            "Party subscriber started: topics=%s, group=%s",
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
                            "Error processing message on topic %s: %s — symbol: %s",
                            topic,
                            exc,
                            value.get("symbol", "unknown") if isinstance(value, dict) else "unknown",
                        )
                        if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                            logger.critical(
                                "Too many consecutive errors (%d), stopping party subscriber",
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
        if topic == self.entity_topic:
            self._handle_entity_message(message)
        elif topic == self.relationship_topic:
            self._handle_relationship_message(message)
        else:
            logger.warning("Unknown topic: %s", topic)

    def _handle_entity_message(self, message: Dict[str, Any]) -> None:
        """Handle a legal entity message."""
        action, entity = parse_legal_entity_message(message)
        symbol = message["symbol"]

        if action == "UPSERT":
            upsert_legal_entity(self.db_path, entity)
            logger.info("Upserted legal entity: %s", symbol)
        elif action == "DELETE":
            deleted = delete_legal_entity(self.db_path, symbol)
            if deleted:
                logger.info("Deleted legal entity: %s", symbol)
            else:
                logger.warning("Legal entity not found for deletion: %s", symbol)

    def _handle_relationship_message(self, message: Dict[str, Any]) -> None:
        """Handle a trading relationship message."""
        action, relationship = parse_relationship_message(message, self.db_path)
        int_sym = message["internal_party_symbol"]
        ext_sym = message["external_party_symbol"]

        if action == "UPSERT":
            upsert_trading_relationship(self.db_path, relationship)
            logger.info("Upserted trading relationship: %s ↔ %s", int_sym, ext_sym)
        elif action == "DELETE":
            deleted = delete_trading_relationship(self.db_path, int_sym, ext_sym)
            if deleted:
                logger.info("Deleted trading relationship: %s ↔ %s", int_sym, ext_sym)
            else:
                logger.warning(
                    "Trading relationship not found for deletion: %s ↔ %s",
                    int_sym,
                    ext_sym,
                )

    def stop(self) -> None:
        """Signal the subscriber to stop after the current poll cycle."""
        self._running = False
        logger.info("Party subscriber stop requested")

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
