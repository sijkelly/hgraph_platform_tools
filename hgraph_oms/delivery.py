"""
Delivers booked trades to the downstream system (the ETRM), at least once.

The booking records are the outbox: a booked trade stays undelivered until the
downstream system acknowledges it.  :class:`TradePublisher` works through the
undelivered trades on a background thread, oldest first, so a slow or
unavailable broker never holds up the order engine.  A failed send is retried
with a growing delay, and because the outbox is on disk, trades still pending
when the process stops are delivered after the next start.

Delivery is at-least-once: a trade can be sent twice if the process stops
between the broker's acknowledgement and recording it.  Every message is keyed
by its trade ID so the consumer can discard a repeat.
"""

import json
import logging
import threading
from typing import Any, Callable

from hgraph_oms.booking import TradeBooker

__all__ = ("TradeSender", "TradePublisher", "kafka_trade_sender")

logger = logging.getLogger(__name__)

# (topic, trade_id, message) -> None; raises if the downstream system did not acknowledge
TradeSender = Callable[[str, str, dict[str, Any]], None]


class TradePublisher:
    """Sends each booked, undelivered trade to ``topic`` and records the outcome.

    :param booker: The booker whose records are the outbox.
    :param topic: Destination topic.
    :param send: Sends one trade message; must raise if it was not acknowledged.
    :param retry_backoff: Seconds to wait after a failed send; doubled after each further failure.
    :param max_backoff: Upper limit for that delay.
    :param poll_interval: How often to look for undelivered trades when not woken by :meth:`notify`.
    """

    def __init__(
        self,
        booker: TradeBooker,
        topic: str,
        send: TradeSender,
        *,
        retry_backoff: float = 1.0,
        max_backoff: float = 60.0,
        poll_interval: float = 30.0,
    ):
        self.booker = booker
        self.topic = topic
        self._send = send
        self._retry_backoff = retry_backoff
        self._max_backoff = max_backoff
        self._poll_interval = poll_interval
        self._wake = threading.Event()
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the delivery thread. Trades left undelivered by an earlier run are sent first."""
        if self._thread is not None:
            raise RuntimeError("Trade publisher already started")
        self._stopping.clear()
        self._wake.set()
        self._thread = threading.Thread(target=self._run, name="trade-publisher", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 10.0) -> None:
        """Stop the delivery thread. Undelivered trades stay in the outbox for the next start."""
        thread, self._thread = self._thread, None
        if thread is None:
            return
        self._stopping.set()
        self._wake.set()
        thread.join(timeout)

    def notify(self) -> None:
        """Tell the publisher there may be new trades to deliver. Safe to call from any thread."""
        self._wake.set()

    def deliver_pending(self) -> bool:
        """Send every undelivered trade, oldest first, stopping at the first failure.

        Stopping keeps trades in order and avoids hammering a broker that is down.

        :returns: ``True`` if nothing is left to deliver.
        """
        for booking in self.booker.bookings(undelivered=True):
            if self._stopping.is_set():
                return False
            try:
                with open(booking.detail, encoding="utf-8") as fh:
                    message = json.load(fh)
                self._send(self.topic, booking.trade_id, message)
            except Exception as exc:
                logger.error("Trade %s was not delivered to '%s': %s", booking.trade_id, self.topic, exc)
                self.booker.record_delivery(booking.trade_id, sent=False, detail=str(exc))
                return False
            self.booker.record_delivery(booking.trade_id, sent=True, detail=f"topic {self.topic}")
            logger.info("Delivered trade %s to '%s'", booking.trade_id, self.topic)
        return True

    def _run(self) -> None:
        backoff = self._retry_backoff
        wait = self._poll_interval
        while not self._stopping.is_set():
            self._wake.wait(wait)
            if self._stopping.is_set():
                break
            self._wake.clear()
            try:
                delivered_all = self.deliver_pending()
            except Exception:
                logger.exception("Trade publisher could not read its outbox")
                delivered_all = False
            if delivered_all:
                backoff, wait = self._retry_backoff, self._poll_interval
            else:
                wait, backoff = backoff, min(backoff * 2, self._max_backoff)


def kafka_trade_sender(bootstrap_servers: str | None = None) -> TradeSender:
    """Return a :data:`TradeSender` that publishes to Kafka.

    The connection is made on first use, so an unavailable broker shows up as a failed
    delivery to be retried rather than stopping the application from starting.

    :param bootstrap_servers: Comma-separated brokers; defaults to ``KAFKA_BOOTSTRAP_SERVERS``.
    """
    lock = threading.Lock()
    sender: list[Any] = []

    def send(topic: str, trade_id: str, message: dict[str, Any]) -> None:
        from hgraph_trade.hgraph_trade_booker.kafka_sender import KafkaSender

        with lock:
            try:
                if not sender:
                    # The publisher does the retrying, so each call makes a single attempt
                    sender.append(KafkaSender(bootstrap_servers=bootstrap_servers, max_retries=1))
                sender[0].send_to_kafka(topic, message, key=trade_id)
            except Exception as exc:
                # kafka-python's own messages rarely say which broker; make the stored error useful
                brokers = bootstrap_servers or "the configured brokers"
                raise ConnectionError(f"Kafka at {brokers}: {type(exc).__name__}: {exc}") from exc

    return send
