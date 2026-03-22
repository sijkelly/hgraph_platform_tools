"""
Kafka subscriber service for portfolio and book static data.

Subscribes to two Kafka topics — one for portfolio updates and one for
book updates — deserialises incoming JSON messages, and persists the
data into the local SQLite portfolio store.

The subscriber runs a blocking poll loop via
:meth:`PortfolioKafkaSubscriber.start` and can be stopped gracefully
with :meth:`PortfolioKafkaSubscriber.stop`.

Message formats
---------------

**Portfolio topic** (default: ``portfolio.portfolio``)::

    {
        "action": "UPSERT" | "DELETE",
        "symbol": "GAS-TRADING",
        "name": "Gas Trading Portfolio",
        "portfolio_type": "Trading",
        "owner": "Gas Desk",
        "base_currency": "USD",
        "legal_entity_symbol": "HGDEALER",
        "status": "Active",
        "parent_portfolio_symbol": null
    }

**Book topic** (default: ``portfolio.book``)::

    {
        "action": "UPSERT" | "DELETE",
        "symbol": "GAS-BOOK-1",
        "name": "Gas Trading Book 1",
        "book_type": "Trading",
        "owner": "Gas Desk",
        "base_currency": "USD",
        "legal_entity_symbol": "HGDEALER",
        "portfolio_symbol": "GAS-TRADING",
        "status": "Active"
    }
"""

import logging
from typing import Any, Dict, Tuple

from hg_oap.portfolio.portfolio_info import (
    BookInfo,
    BookType,
    PortfolioInfo,
    PortfolioStatus,
    PortfolioType,
)

from hgraph_static_admin.portfolio_store import (
    delete_book,
    delete_portfolio,
    upsert_book,
    upsert_portfolio,
)

__all__ = (
    "PortfolioKafkaSubscriber",
    "parse_portfolio_message",
    "parse_book_message",
)

logger = logging.getLogger(__name__)

# Stop the subscriber after this many consecutive processing failures
# to avoid infinite error loops on persistently bad data.
_MAX_CONSECUTIVE_ERRORS = 50

# Reverse lookup: portfolio type string value → enum member
_PORTFOLIO_TYPE_MAP: Dict[str, PortfolioType] = {t.value: t for t in PortfolioType}

# Reverse lookup: book type string value → enum member
_BOOK_TYPE_MAP: Dict[str, BookType] = {t.value: t for t in BookType}

# Reverse lookup: status string value → enum member
_STATUS_MAP: Dict[str, PortfolioStatus] = {s.value: s for s in PortfolioStatus}


# ---------------------------------------------------------------------------
# Message parsers (standalone, testable without Kafka)
# ---------------------------------------------------------------------------


def parse_portfolio_message(msg: Dict[str, Any]) -> Tuple[str, PortfolioInfo | None]:
    """Parse a portfolio Kafka message.

    :param msg: Deserialised JSON message dict.
    :returns: A tuple of ``(action, portfolio)``.  For ``DELETE`` actions
        the portfolio is ``None``.
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

    portfolio_type_str = msg.get("portfolio_type", "")
    portfolio_type = _PORTFOLIO_TYPE_MAP.get(portfolio_type_str)
    if portfolio_type is None:
        raise ValueError(
            f"Unknown portfolio_type: {portfolio_type_str!r}. "
            f"Expected one of: {list(_PORTFOLIO_TYPE_MAP.keys())}"
        )

    status_str = msg.get("status", "Active")
    status = _STATUS_MAP.get(status_str)
    if status is None:
        raise ValueError(
            f"Unknown status: {status_str!r}. "
            f"Expected one of: {list(_STATUS_MAP.keys())}"
        )

    portfolio = PortfolioInfo(
        symbol=symbol,
        name=name,
        portfolio_type=portfolio_type,
        owner=msg.get("owner"),
        base_currency=msg.get("base_currency"),
        legal_entity_symbol=msg.get("legal_entity_symbol"),
        status=status,
        parent_portfolio_symbol=msg.get("parent_portfolio_symbol"),
    )
    return action, portfolio


def parse_book_message(msg: Dict[str, Any]) -> Tuple[str, BookInfo | None]:
    """Parse a book Kafka message.

    :param msg: Deserialised JSON message dict.
    :returns: A tuple of ``(action, book)``.  For ``DELETE`` actions
        the book is ``None``.
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

    book_type_str = msg.get("book_type", "")
    book_type = _BOOK_TYPE_MAP.get(book_type_str)
    if book_type is None:
        raise ValueError(
            f"Unknown book_type: {book_type_str!r}. "
            f"Expected one of: {list(_BOOK_TYPE_MAP.keys())}"
        )

    status_str = msg.get("status", "Active")
    status = _STATUS_MAP.get(status_str)
    if status is None:
        raise ValueError(
            f"Unknown status: {status_str!r}. "
            f"Expected one of: {list(_STATUS_MAP.keys())}"
        )

    book = BookInfo(
        symbol=symbol,
        name=name,
        book_type=book_type,
        owner=msg.get("owner"),
        base_currency=msg.get("base_currency"),
        legal_entity_symbol=msg.get("legal_entity_symbol"),
        portfolio_symbol=msg.get("portfolio_symbol"),
        status=status,
    )
    return action, book


# ---------------------------------------------------------------------------
# Subscriber service
# ---------------------------------------------------------------------------


class PortfolioKafkaSubscriber:
    """Subscribes to portfolio Kafka topics and persists updates to the portfolio store.

    :param db_path: Path to the portfolio SQLite database.
    :param bootstrap_servers: Kafka bootstrap servers.  Falls back to config.
    :param group_id: Kafka consumer group.  Falls back to config.
    :param portfolio_topic: Topic for portfolio messages.
    :param book_topic: Topic for book messages.
    """

    def __init__(
        self,
        db_path: str,
        *,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        portfolio_topic: str | None = None,
        book_topic: str | None = None,
    ):
        from secure_config import config

        self.db_path = db_path
        self.bootstrap_servers = bootstrap_servers or config["KAFKA_BOOTSTRAP_SERVERS"]
        self.group_id = group_id or config.get("KAFKA_CONSUMER_GROUP", "hgraph_platform")
        self.portfolio_topic = portfolio_topic or config.get(
            "PORTFOLIO_KAFKA_PORTFOLIO_TOPIC", "portfolio.portfolio"
        )
        self.book_topic = book_topic or config.get(
            "PORTFOLIO_KAFKA_BOOK_TOPIC", "portfolio.book"
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

        topics = [self.portfolio_topic, self.book_topic]
        self._receiver = KafkaReceiver(
            topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )

        self._running = True
        self._processed_count = 0
        logger.info(
            "Portfolio subscriber started: topics=%s, group=%s",
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
                                "Too many consecutive errors (%d), stopping portfolio subscriber",
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
        if topic == self.portfolio_topic:
            self._handle_portfolio_message(message)
        elif topic == self.book_topic:
            self._handle_book_message(message)
        else:
            logger.warning("Unknown topic: %s", topic)

    def _handle_portfolio_message(self, message: Dict[str, Any]) -> None:
        """Handle a portfolio message."""
        action, portfolio = parse_portfolio_message(message)
        symbol = message["symbol"]

        if action == "UPSERT":
            upsert_portfolio(self.db_path, portfolio)
            logger.info("Upserted portfolio: %s", symbol)
        elif action == "DELETE":
            deleted = delete_portfolio(self.db_path, symbol)
            if deleted:
                logger.info("Deleted portfolio: %s", symbol)
            else:
                logger.warning("Portfolio not found for deletion: %s", symbol)

    def _handle_book_message(self, message: Dict[str, Any]) -> None:
        """Handle a book message."""
        action, book = parse_book_message(message)
        symbol = message["symbol"]

        if action == "UPSERT":
            upsert_book(self.db_path, book)
            logger.info("Upserted book: %s", symbol)
        elif action == "DELETE":
            deleted = delete_book(self.db_path, symbol)
            if deleted:
                logger.info("Deleted book: %s", symbol)
            else:
                logger.warning("Book not found for deletion: %s", symbol)

    def stop(self) -> None:
        """Signal the subscriber to stop after the current poll cycle."""
        self._running = False
        logger.info("Portfolio subscriber stop requested")

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
