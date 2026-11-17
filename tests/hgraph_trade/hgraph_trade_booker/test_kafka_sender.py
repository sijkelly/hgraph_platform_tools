"""Tests for KafkaSender: acknowledgement, keys and retry, against a fake producer."""

import pytest
from kafka.errors import KafkaError, KafkaTimeoutError

from hgraph_trade.hgraph_trade_booker import kafka_sender
from hgraph_trade.hgraph_trade_booker.kafka_sender import KafkaSender


class _Future:
    def __init__(self, error=None):
        self.error = error

    def get(self, timeout=None):
        if self.error:
            raise self.error


class _Producer:
    """Fails the first ``failures`` sends at acknowledgement time, as an unreachable broker does."""

    def __init__(self, failures=0):
        self.failures = failures
        self.sent = []

    def send(self, topic, value, key=None):
        self.sent.append((topic, value, key))
        if len(self.sent) <= self.failures:
            return _Future(KafkaTimeoutError("no acknowledgement"))
        return _Future()


def _sender(monkeypatch, failures=0, **kwargs) -> tuple[KafkaSender, _Producer]:
    producer = _Producer(failures)
    monkeypatch.setattr(kafka_sender, "KafkaProducer", lambda bootstrap_servers: producer)
    monkeypatch.setattr(kafka_sender.time, "sleep", lambda seconds: None)
    return KafkaSender(bootstrap_servers="broker:9092", **kwargs), producer


def test_send_serialises_dict_and_encodes_key(monkeypatch):
    sender, producer = _sender(monkeypatch)
    sender.send_to_kafka("trades", {"tradeId": "T1"}, key="T1")
    assert producer.sent == [("trades", b'{"tradeId": "T1"}', b"T1")]


def test_send_without_key(monkeypatch):
    sender, producer = _sender(monkeypatch)
    sender.send_to_kafka("trades", "plain text")
    assert producer.sent == [("trades", b"plain text", None)]


def test_unacknowledged_send_is_retried(monkeypatch):
    sender, producer = _sender(monkeypatch, failures=2)
    sender.send_to_kafka("trades", {"a": 1})
    assert len(producer.sent) == 3


def test_send_raises_when_the_broker_never_acknowledges(monkeypatch):
    sender, producer = _sender(monkeypatch, failures=10, max_retries=2)
    with pytest.raises(KafkaError, match="after 2 attempts"):
        sender.send_to_kafka("trades", {"a": 1})
    assert len(producer.sent) == 2
