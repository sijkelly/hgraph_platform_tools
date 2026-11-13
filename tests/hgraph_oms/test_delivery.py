"""Tests for delivering booked trades downstream: the outbox, retries and recovery."""

import threading
from dataclasses import replace

import pytest

from hgraph_oms.booking import PENDING, SENT, TradeBooker
from hgraph_oms.delivery import TradePublisher, kafka_trade_sender
from tests.hgraph_oms.test_trades import TICKET

TOPIC = "trades.booked"


class _Downstream:
    """Stands in for the broker: records what it acknowledged, and can be switched off."""

    def __init__(self):
        self.up = True
        self.received: list[tuple[str, str, dict]] = []
        self.arrived = threading.Semaphore(0)

    def send(self, topic, trade_id, message):
        if not self.up:
            raise ConnectionError("broker unavailable")
        self.received.append((topic, trade_id, message))
        self.arrived.release()

    @property
    def trade_ids(self) -> list[str]:
        return [trade_id for _topic, trade_id, _message in self.received]


def _ticket(n: int):
    return replace(TICKET, trade_id=f"ORD-1-F{n}", fill_id=f"F{n}")


@pytest.fixture
def booker(oms_db, tmp_path) -> TradeBooker:
    return TradeBooker(oms_db, str(tmp_path / "booked"), internal_party="OURCO")


@pytest.fixture
def downstream() -> _Downstream:
    return _Downstream()


@pytest.fixture
def publisher(booker, downstream) -> TradePublisher:
    return TradePublisher(booker, TOPIC, downstream.send, retry_backoff=0.01, max_backoff=0.05)


def test_booked_trades_are_sent_in_order_keyed_by_trade_id(booker, downstream, publisher):
    booker.book([_ticket(1), _ticket(2), replace(_ticket(3), instrument="Unobtainium")])

    assert publisher.deliver_pending()

    assert downstream.trade_ids == ["ORD-1-F1", "ORD-1-F2"]  # the trade that failed booking is not sent
    topic, _trade_id, message = downstream.received[0]
    assert topic == TOPIC
    assert message["tradeHeader"]["tradeHeader"]["partyTradeIdentifier"]["tradeId"] == "ORD-1-F1"
    sent = booker.get("ORD-1-F1")
    assert (sent.delivery_status, sent.delivery_attempts, sent.delivery_detail) == (SENT, 1, f"topic {TOPIC}")
    assert booker.bookings(undelivered=True) == []


def test_sent_trades_are_not_sent_again(booker, downstream, publisher):
    booker.book([_ticket(1)])
    publisher.deliver_pending()
    booker.book([_ticket(2)])
    publisher.deliver_pending()

    assert downstream.trade_ids == ["ORD-1-F1", "ORD-1-F2"]


def test_failed_send_stays_pending_with_the_error_and_is_retried(booker, downstream, publisher):
    booker.book([_ticket(1), _ticket(2)])
    downstream.up = False

    assert not publisher.deliver_pending()
    assert not publisher.deliver_pending()

    first, second = booker.bookings()
    assert (first.delivery_status, first.delivery_attempts, first.delivery_detail) == (PENDING, 2, "broker unavailable")
    assert second.delivery_attempts == 0  # delivery stops at the first failure to keep trades in order

    downstream.up = True
    assert publisher.deliver_pending()
    assert downstream.trade_ids == ["ORD-1-F1", "ORD-1-F2"]
    assert booker.get("ORD-1-F1").delivery_attempts == 3


def test_missing_trade_file_is_reported_not_skipped(booker, downstream, publisher, tmp_path):
    booker.book([_ticket(1)])
    (tmp_path / "booked" / "ORD-1-F1.json").unlink()

    assert not publisher.deliver_pending()
    assert "No such file" in booker.get("ORD-1-F1").delivery_detail
    assert downstream.received == []


def test_background_thread_delivers_on_notify_and_retries_after_an_outage(booker, downstream, publisher):
    downstream.up = False
    publisher.start()
    try:
        booker.book([_ticket(1)])
        publisher.notify()
        assert not downstream.arrived.acquire(timeout=0.2)  # broker is down: nothing arrives

        downstream.up = True  # no notify: the publisher's own retry must deliver it
        assert downstream.arrived.acquire(timeout=5)
    finally:
        publisher.stop()

    assert downstream.trade_ids == ["ORD-1-F1"]
    assert booker.get("ORD-1-F1").delivery_status == SENT


def test_trades_left_pending_are_delivered_by_the_next_run(booker, downstream):
    booker.book([_ticket(1)])  # booked, then the process stops before delivery

    restarted = TradePublisher(booker, TOPIC, downstream.send)
    restarted.start()
    try:
        assert downstream.arrived.acquire(timeout=5)
    finally:
        restarted.stop()
    assert downstream.trade_ids == ["ORD-1-F1"]


def test_kafka_sender_connects_on_first_use_and_keys_by_trade_id(monkeypatch):
    created = []

    class _FakeKafkaSender:
        def __init__(self, bootstrap_servers=None, max_retries=3):
            created.append((bootstrap_servers, max_retries))
            self.sent = []

        def send_to_kafka(self, topic, message, key=None):
            self.sent.append((topic, message, key))

    monkeypatch.setattr("hgraph_trade.hgraph_trade_booker.kafka_sender.KafkaSender", _FakeKafkaSender)

    send = kafka_trade_sender("broker:9092")
    assert created == []  # creating the sender must not need a broker
    send(TOPIC, "ORD-1-F1", {"a": 1})
    send(TOPIC, "ORD-1-F2", {"a": 2})
    assert created == [("broker:9092", 1)]  # one connection, single attempt per call


def test_kafka_sender_error_names_the_broker(monkeypatch):
    def _no_broker(bootstrap_servers=None, max_retries=3):
        raise OSError("connection refused")

    monkeypatch.setattr("hgraph_trade.hgraph_trade_booker.kafka_sender.KafkaSender", _no_broker)

    with pytest.raises(ConnectionError, match="Kafka at broker:9092: OSError: connection refused"):
        kafka_trade_sender("broker:9092")(TOPIC, "ORD-1-F1", {})
