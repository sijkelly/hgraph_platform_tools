"""Tests for the generic Kafka consumer wrapper."""

from unittest.mock import MagicMock, patch

import pytest

from hgraph_trade.hgraph_trade_booker.kafka_consumer import KafkaReceiver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def mock_kafka_consumer():
    """Mock kafka.KafkaConsumer to avoid needing a live broker."""
    with patch("hgraph_trade.hgraph_trade_booker.kafka_consumer.KafkaConsumer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_cls, mock_instance


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def test_init_with_defaults(mock_kafka_consumer):
    mock_cls, mock_instance = mock_kafka_consumer
    receiver = KafkaReceiver(["topic.one"])

    mock_cls.assert_called_once()
    args, kwargs = mock_cls.call_args
    assert "topic.one" in args
    assert kwargs["auto_offset_reset"] == "earliest"
    assert kwargs["enable_auto_commit"] is False
    assert receiver.bootstrap_servers == "localhost:9092"


def test_init_with_custom_params(mock_kafka_consumer):
    mock_cls, _ = mock_kafka_consumer
    receiver = KafkaReceiver(
        ["topic.a", "topic.b"],
        bootstrap_servers="broker1:9092",
        group_id="custom-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    args, kwargs = mock_cls.call_args
    assert "topic.a" in args
    assert "topic.b" in args
    assert kwargs["bootstrap_servers"] == "broker1:9092"
    assert kwargs["group_id"] == "custom-group"
    assert kwargs["auto_offset_reset"] == "latest"
    assert kwargs["enable_auto_commit"] is True
    assert receiver.group_id == "custom-group"


def test_init_stores_topics(mock_kafka_consumer):
    receiver = KafkaReceiver(["t1", "t2"])
    assert receiver._topics == ["t1", "t2"]


# ---------------------------------------------------------------------------
# poll()
# ---------------------------------------------------------------------------


def test_poll_returns_empty_list(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer
    mock_instance.poll.return_value = {}
    receiver = KafkaReceiver(["topic"])

    result = receiver.poll(timeout_ms=500)
    assert result == []
    mock_instance.poll.assert_called_once_with(timeout_ms=500, max_records=100)


def test_poll_returns_deserialised_records(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer
    mock_record = MagicMock()
    mock_record.topic = "topic.one"
    mock_record.partition = 0
    mock_record.offset = 42
    mock_record.key = b"key1"
    mock_record.value = {"action": "UPSERT", "symbol": "ACME"}
    mock_record.timestamp = 1234567890

    mock_tp = MagicMock()
    mock_instance.poll.return_value = {mock_tp: [mock_record]}

    receiver = KafkaReceiver(["topic.one"])
    result = receiver.poll()

    assert len(result) == 1
    msg = result[0]
    assert msg["topic"] == "topic.one"
    assert msg["partition"] == 0
    assert msg["offset"] == 42
    assert msg["key"] == b"key1"
    assert msg["value"] == {"action": "UPSERT", "symbol": "ACME"}
    assert msg["timestamp"] == 1234567890


def test_poll_multiple_records(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer

    records = []
    for i in range(3):
        r = MagicMock()
        r.topic = "topic"
        r.partition = 0
        r.offset = i
        r.key = None
        r.value = {"i": i}
        r.timestamp = 1000 + i
        records.append(r)

    mock_tp = MagicMock()
    mock_instance.poll.return_value = {mock_tp: records}

    receiver = KafkaReceiver(["topic"])
    result = receiver.poll()
    assert len(result) == 3


def test_poll_custom_max_records(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer
    mock_instance.poll.return_value = {}

    receiver = KafkaReceiver(["topic"])
    receiver.poll(timeout_ms=2000, max_records=50)

    mock_instance.poll.assert_called_once_with(timeout_ms=2000, max_records=50)


# ---------------------------------------------------------------------------
# subscribe()
# ---------------------------------------------------------------------------


def test_subscribe_updates_topics(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer
    receiver = KafkaReceiver(["topic.one"])

    receiver.subscribe(["topic.one", "topic.two", "topic.three"])

    mock_instance.subscribe.assert_called_once_with(["topic.one", "topic.two", "topic.three"])
    assert receiver._topics == ["topic.one", "topic.two", "topic.three"]


# ---------------------------------------------------------------------------
# commit()
# ---------------------------------------------------------------------------


def test_commit(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer
    receiver = KafkaReceiver(["topic"])
    receiver.commit()
    mock_instance.commit.assert_called_once()


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


def test_close(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer
    receiver = KafkaReceiver(["topic"])
    receiver.close()
    mock_instance.close.assert_called_once()


def test_close_raises_on_error(mock_kafka_consumer):
    _, mock_instance = mock_kafka_consumer
    mock_instance.close.side_effect = Exception("Connection reset")

    receiver = KafkaReceiver(["topic"])
    with pytest.raises(RuntimeError, match="Error closing Kafka consumer"):
        receiver.close()
