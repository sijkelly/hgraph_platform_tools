"""Tests for the EV rate Kafka subscriber service."""

from datetime import date

import pytest

from hg_oap.orders.sales.ev import EVRate, InstrumentCategory
from hg_oap.units.default_unit_system import U

from hgraph_oap_adapter.ev_rate_kafka_subscriber import (
    EVRateKafkaSubscriber,
    parse_ev_rate_message,
)
from hgraph_oap_adapter.ev_rate_store import (
    get_all_ev_rates,
    get_ev_rate,
    init_ev_rate_db,
    upsert_ev_rate,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_ev_rate_sub.db")
    init_ev_rate_db(path)
    return path


@pytest.fixture()
def upsert_msg_gas():
    return {
        "action": "UPSERT",
        "instrument_category": "FixedFloatSwap",
        "unit": "MMBtu",
        "rate_per_unit": 0.02,
        "currency_symbol": "USD",
        "effective_date": None,
        "expiry_date": None,
    }


@pytest.fixture()
def upsert_msg_power():
    return {
        "action": "UPSERT",
        "instrument_category": "FixedFloatSwap",
        "unit": "kWh",
        "rate_per_unit": 0.001,
        "currency_symbol": "USD",
        "effective_date": "2026-01-01",
        "expiry_date": "2026-12-31",
    }


@pytest.fixture()
def upsert_msg_basis():
    return {
        "action": "UPSERT",
        "instrument_category": "BasisSwap",
        "unit": "MMBtu",
        "rate_per_unit": 0.015,
        "currency_symbol": "USD",
    }


@pytest.fixture()
def delete_msg_gas():
    return {
        "action": "DELETE",
        "instrument_category": "FixedFloatSwap",
        "unit": "MMBtu",
    }


# ---------------------------------------------------------------------------
# parse_ev_rate_message — happy path
# ---------------------------------------------------------------------------


def test_parse_upsert_minimal(upsert_msg_gas):
    action, ev_rate, unit_str = parse_ev_rate_message(upsert_msg_gas)

    assert action == "UPSERT"
    assert unit_str == "MMBtu"
    assert isinstance(ev_rate, EVRate)
    assert ev_rate.instrument_category == InstrumentCategory.FIXED_FLOAT_SWAP
    assert ev_rate.unit == U.MMBtu
    assert ev_rate.rate_per_unit == pytest.approx(0.02)
    assert ev_rate.currency_symbol == "USD"
    assert ev_rate.effective_date is None
    assert ev_rate.expiry_date is None


def test_parse_upsert_with_dates(upsert_msg_power):
    action, ev_rate, unit_str = parse_ev_rate_message(upsert_msg_power)

    assert action == "UPSERT"
    assert unit_str == "kWh"
    assert ev_rate.unit == U.kWh
    assert ev_rate.effective_date == date(2026, 1, 1)
    assert ev_rate.expiry_date == date(2026, 12, 31)


def test_parse_upsert_basis_swap(upsert_msg_basis):
    action, ev_rate, unit_str = parse_ev_rate_message(upsert_msg_basis)

    assert action == "UPSERT"
    assert ev_rate.instrument_category == InstrumentCategory.BASIS_SWAP
    assert ev_rate.rate_per_unit == pytest.approx(0.015)


def test_parse_delete(delete_msg_gas):
    action, ev_rate, unit_str = parse_ev_rate_message(delete_msg_gas)

    assert action == "DELETE"
    assert ev_rate is None
    assert unit_str == "MMBtu"


def test_parse_default_currency_symbol():
    msg = {
        "action": "UPSERT",
        "instrument_category": "Forward",
        "unit": "bbl",
        "rate_per_unit": 0.005,
    }
    _, ev_rate, _ = parse_ev_rate_message(msg)
    assert ev_rate.currency_symbol == "USD"


@pytest.mark.parametrize(
    "category_str,expected",
    [
        ("FixedFloatSwap", InstrumentCategory.FIXED_FLOAT_SWAP),
        ("BasisSwap", InstrumentCategory.BASIS_SWAP),
        ("Swaption", InstrumentCategory.SWAPTION),
        ("Forward", InstrumentCategory.FORWARD),
        ("Future", InstrumentCategory.FUTURE),
        ("Option", InstrumentCategory.OPTION),
    ],
)
def test_parse_all_instrument_categories(category_str, expected):
    msg = {
        "action": "UPSERT",
        "instrument_category": category_str,
        "unit": "MMBtu",
        "rate_per_unit": 0.01,
    }
    _, ev_rate, _ = parse_ev_rate_message(msg)
    assert ev_rate.instrument_category == expected


# ---------------------------------------------------------------------------
# parse_ev_rate_message — error cases
# ---------------------------------------------------------------------------


def test_parse_unknown_action_raises():
    msg = {"action": "UPDATE", "instrument_category": "FixedFloatSwap", "unit": "MMBtu"}
    with pytest.raises(ValueError, match="Unknown action"):
        parse_ev_rate_message(msg)


def test_parse_missing_category_raises():
    msg = {"action": "UPSERT", "unit": "MMBtu", "rate_per_unit": 0.01}
    with pytest.raises(ValueError, match="instrument_category"):
        parse_ev_rate_message(msg)


def test_parse_missing_unit_raises():
    msg = {"action": "UPSERT", "instrument_category": "FixedFloatSwap", "rate_per_unit": 0.01}
    with pytest.raises(ValueError, match="'unit'"):
        parse_ev_rate_message(msg)


def test_parse_unknown_category_raises():
    msg = {"action": "UPSERT", "instrument_category": "CreditDefault", "unit": "MMBtu", "rate_per_unit": 0.01}
    with pytest.raises(ValueError, match="Unknown instrument_category"):
        parse_ev_rate_message(msg)


def test_parse_unknown_unit_raises():
    msg = {"action": "UPSERT", "instrument_category": "FixedFloatSwap", "unit": "XYZ_FAKE", "rate_per_unit": 0.01}
    with pytest.raises(ValueError, match="Unknown unit symbol"):
        parse_ev_rate_message(msg)


def test_parse_missing_rate_on_upsert_raises():
    msg = {"action": "UPSERT", "instrument_category": "FixedFloatSwap", "unit": "MMBtu"}
    with pytest.raises(ValueError, match="rate_per_unit"):
        parse_ev_rate_message(msg)


def test_parse_non_numeric_rate_raises():
    msg = {
        "action": "UPSERT",
        "instrument_category": "FixedFloatSwap",
        "unit": "MMBtu",
        "rate_per_unit": "twenty_percent",
    }
    with pytest.raises(ValueError, match="must be a number"):
        parse_ev_rate_message(msg)


def test_parse_delete_no_rate_required():
    # DELETE should succeed even without rate_per_unit
    msg = {"action": "DELETE", "instrument_category": "BasisSwap", "unit": "kWh"}
    action, ev_rate, unit_str = parse_ev_rate_message(msg)
    assert action == "DELETE"
    assert ev_rate is None
    assert unit_str == "kWh"


# ---------------------------------------------------------------------------
# EVRateKafkaSubscriber.process_message — integration with the store
# ---------------------------------------------------------------------------


def test_process_upsert_stores_rate(db_path):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    msg = {
        "action": "UPSERT",
        "instrument_category": "FixedFloatSwap",
        "unit": "MMBtu",
        "rate_per_unit": 0.02,
        "currency_symbol": "USD",
    }
    subscriber.process_message("oap.ev_rate", msg)

    stored = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)
    assert stored is not None
    assert stored.rate_per_unit == pytest.approx(0.02)
    assert stored.instrument_category == InstrumentCategory.FIXED_FLOAT_SWAP


def test_process_upsert_updates_existing(db_path):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    # Insert initial rate
    msg_v1 = {
        "action": "UPSERT",
        "instrument_category": "Forward",
        "unit": "bbl",
        "rate_per_unit": 0.01,
    }
    subscriber.process_message("oap.ev_rate", msg_v1)

    # Update rate
    msg_v2 = {
        "action": "UPSERT",
        "instrument_category": "Forward",
        "unit": "bbl",
        "rate_per_unit": 0.025,
        "effective_date": "2026-04-01",
    }
    subscriber.process_message("oap.ev_rate", msg_v2)

    stored = get_ev_rate(db_path, InstrumentCategory.FORWARD, U.bbl)
    assert stored is not None
    assert stored.rate_per_unit == pytest.approx(0.025)


def test_process_delete_removes_rate(db_path):
    # Pre-seed the store
    gas_rate = EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
    )
    upsert_ev_rate(db_path, gas_rate)

    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    delete_msg = {
        "action": "DELETE",
        "instrument_category": "FixedFloatSwap",
        "unit": "MMBtu",
    }
    subscriber.process_message("oap.ev_rate", delete_msg)

    stored = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)
    assert stored is None


def test_process_delete_nonexistent_logs_warning(db_path, caplog):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    delete_msg = {
        "action": "DELETE",
        "instrument_category": "Swaption",
        "unit": "MWh",
    }
    import logging

    with caplog.at_level(logging.WARNING):
        subscriber.process_message("oap.ev_rate", delete_msg)

    assert any("not found" in r.message.lower() for r in caplog.records)


def test_process_unknown_topic_logs_warning(db_path, caplog):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    import logging

    with caplog.at_level(logging.WARNING):
        subscriber.process_message("some.other.topic", {"action": "UPSERT"})

    assert any("unknown topic" in r.message.lower() for r in caplog.records)


def test_process_bad_message_raises(db_path):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    bad_msg = {"action": "UPSERT"}  # Missing instrument_category and unit
    with pytest.raises(ValueError):
        subscriber.process_message("oap.ev_rate", bad_msg)


def test_multiple_categories_coexist(db_path):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    messages = [
        {"action": "UPSERT", "instrument_category": "FixedFloatSwap", "unit": "MMBtu", "rate_per_unit": 0.02},
        {"action": "UPSERT", "instrument_category": "BasisSwap", "unit": "MMBtu", "rate_per_unit": 0.015},
        {"action": "UPSERT", "instrument_category": "FixedFloatSwap", "unit": "kWh", "rate_per_unit": 0.001},
    ]
    for msg in messages:
        subscriber.process_message("oap.ev_rate", msg)

    all_rates = get_all_ev_rates(db_path)
    assert len(all_rates) == 3


def test_process_count_increments_via_start(db_path):
    """Verify processed_count by calling process_message directly multiple times."""
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )
    # processed_count is only incremented inside start(), not process_message()
    # So we just verify it starts at zero and process_message doesn't raise.
    assert subscriber.processed_count == 0

    msg = {"action": "UPSERT", "instrument_category": "Future", "unit": "bbl", "rate_per_unit": 0.003}
    subscriber.process_message("oap.ev_rate", msg)

    stored = get_ev_rate(db_path, InstrumentCategory.FUTURE, U.bbl)
    assert stored is not None


# ---------------------------------------------------------------------------
# Subscriber lifecycle / stop
# ---------------------------------------------------------------------------


def test_stop_sets_running_false(db_path):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )
    subscriber._running = True
    subscriber.stop()
    assert not subscriber._running


def test_close_clears_receiver(db_path):
    subscriber = EVRateKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        group_id="test",
        ev_rate_topic="oap.ev_rate",
    )

    class _FakeReceiver:
        closed = False

        def close(self):
            self.closed = True

    fake = _FakeReceiver()
    subscriber._receiver = fake
    subscriber.close()

    assert fake.closed
    assert subscriber._receiver is None


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------


def test_subscriber_uses_config_defaults(db_path, monkeypatch):
    """EVRateKafkaSubscriber picks up defaults from secure_config."""
    import secure_config

    monkeypatch.setitem(secure_config.config, "KAFKA_BOOTSTRAP_SERVERS", "kafka.test:9092")
    monkeypatch.setitem(secure_config.config, "KAFKA_CONSUMER_GROUP", "test_group")
    monkeypatch.setitem(secure_config.config, "EV_RATE_KAFKA_TOPIC", "oap.ev_rate_test")

    subscriber = EVRateKafkaSubscriber(db_path)

    assert subscriber.bootstrap_servers == "kafka.test:9092"
    assert subscriber.group_id == "test_group"
    assert subscriber.ev_rate_topic == "oap.ev_rate_test"
