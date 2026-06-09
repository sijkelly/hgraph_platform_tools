"""Tests for credit Kafka subscriber message parsing and processing."""

from datetime import date

import pytest

from hg_oap.credit.credit_limit import (
    CreditLimit,
    CreditLimitType,
    CreditStatus,
    CreditUtilization,
)

from hgraph_static_admin.credit_store import (
    get_all_credit_limits,
    get_credit_limit,
    get_credit_utilization,
    init_credit_db,
)
from hgraph_static_admin.credit_kafka_subscriber import (
    CreditKafkaSubscriber,
    parse_credit_limit_message,
    parse_credit_utilization_message,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_credit_subscriber.db")
    init_credit_db(path)
    return path


@pytest.fixture()
def limit_upsert_msg():
    return {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": 50_000_000.0,
        "limit_currency": "USD",
        "effective_date": "2025-01-01",
        "expiry_date": "2026-01-01",
        "status": "Active",
        "approved_by": "Credit Committee",
        "last_review_date": "2025-06-15",
    }


@pytest.fixture()
def limit_delete_msg():
    return {
        "action": "DELETE",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
    }


@pytest.fixture()
def utilization_upsert_msg():
    return {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "utilized_amount": 15_000_000.0,
        "utilization_currency": "USD",
        "limit_amount": 50_000_000.0,
        "available_amount": 35_000_000.0,
        "utilization_percentage": 30.0,
        "as_of_timestamp": "2025-07-01T14:30:00+00:00",
    }


@pytest.fixture()
def utilization_delete_msg():
    return {
        "action": "DELETE",
        "counterparty_symbol": "ACME",
    }


@pytest.fixture()
def subscriber(db_path):
    return CreditKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        limit_topic="credit.limit",
        utilization_topic="credit.utilization",
    )


# ---------------------------------------------------------------------------
# parse_credit_limit_message
# ---------------------------------------------------------------------------


def test_parse_limit_upsert(limit_upsert_msg):
    action, cl = parse_credit_limit_message(limit_upsert_msg)
    assert action == "UPSERT"
    assert isinstance(cl, CreditLimit)
    assert cl.counterparty_symbol == "ACME"
    assert cl.limit_type == CreditLimitType.BILATERAL
    assert cl.limit_amount == 50_000_000.0
    assert cl.limit_currency == "USD"
    assert cl.effective_date == date(2025, 1, 1)
    assert cl.expiry_date == date(2026, 1, 1)
    assert cl.status == CreditStatus.ACTIVE
    assert cl.approved_by == "Credit Committee"
    assert cl.last_review_date == date(2025, 6, 15)


def test_parse_limit_delete(limit_delete_msg):
    action, cl = parse_credit_limit_message(limit_delete_msg)
    assert action == "DELETE"
    assert cl is None


def test_parse_limit_missing_counterparty():
    msg = {"action": "UPSERT", "limit_type": "Bilateral", "limit_amount": 1000.0}
    with pytest.raises(ValueError, match="counterparty_symbol"):
        parse_credit_limit_message(msg)


def test_parse_limit_missing_limit_type():
    msg = {"action": "UPSERT", "counterparty_symbol": "ACME", "limit_amount": 1000.0}
    with pytest.raises(ValueError, match="limit_type"):
        parse_credit_limit_message(msg)


def test_parse_limit_missing_limit_amount():
    msg = {"action": "UPSERT", "counterparty_symbol": "ACME", "limit_type": "Bilateral"}
    with pytest.raises(ValueError, match="limit_amount"):
        parse_credit_limit_message(msg)


def test_parse_limit_unknown_action():
    msg = {"action": "REMOVE", "counterparty_symbol": "ACME", "limit_type": "Bilateral"}
    with pytest.raises(ValueError, match="Unknown action"):
        parse_credit_limit_message(msg)


def test_parse_limit_unknown_limit_type():
    msg = {"action": "UPSERT", "counterparty_symbol": "ACME", "limit_type": "Unknown", "limit_amount": 1000.0}
    with pytest.raises(ValueError, match="Unknown limit_type"):
        parse_credit_limit_message(msg)


def test_parse_limit_unknown_status():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": 1000.0,
        "status": "Deleted",
    }
    with pytest.raises(ValueError, match="Unknown status"):
        parse_credit_limit_message(msg)


def test_parse_limit_case_insensitive_action():
    msg = {
        "action": "upsert",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": 1000.0,
    }
    action, cl = parse_credit_limit_message(msg)
    assert action == "UPSERT"
    assert cl is not None


def test_parse_limit_with_dates():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "limit_type": "Total",
        "limit_amount": 100_000_000.0,
        "effective_date": "2025-03-01",
        "expiry_date": "2025-12-31",
        "last_review_date": "2025-06-01",
    }
    _, cl = parse_credit_limit_message(msg)
    assert cl.effective_date == date(2025, 3, 1)
    assert cl.expiry_date == date(2025, 12, 31)
    assert cl.last_review_date == date(2025, 6, 1)


def test_parse_limit_without_optional_fields():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": 1000.0,
    }
    _, cl = parse_credit_limit_message(msg)
    assert cl.limit_currency == "USD"
    assert cl.effective_date is None
    assert cl.expiry_date is None
    assert cl.status == CreditStatus.ACTIVE
    assert cl.approved_by is None
    assert cl.last_review_date is None


def test_parse_limit_default_status():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": 1000.0,
    }
    _, cl = parse_credit_limit_message(msg)
    assert cl.status == CreditStatus.ACTIVE


# ---------------------------------------------------------------------------
# parse_credit_utilization_message
# ---------------------------------------------------------------------------


def test_parse_utilization_upsert(utilization_upsert_msg):
    action, cu = parse_credit_utilization_message(utilization_upsert_msg)
    assert action == "UPSERT"
    assert isinstance(cu, CreditUtilization)
    assert cu.counterparty_symbol == "ACME"
    assert cu.utilized_amount == 15_000_000.0
    assert cu.utilization_currency == "USD"
    assert cu.limit_amount == 50_000_000.0
    assert cu.available_amount == 35_000_000.0
    assert cu.utilization_percentage == 30.0
    assert cu.as_of_timestamp == "2025-07-01T14:30:00+00:00"


def test_parse_utilization_delete(utilization_delete_msg):
    action, cu = parse_credit_utilization_message(utilization_delete_msg)
    assert action == "DELETE"
    assert cu is None


def test_parse_utilization_missing_counterparty():
    msg = {"action": "UPSERT", "utilized_amount": 1000.0}
    with pytest.raises(ValueError, match="counterparty_symbol"):
        parse_credit_utilization_message(msg)


def test_parse_utilization_missing_utilized_amount():
    msg = {"action": "UPSERT", "counterparty_symbol": "ACME"}
    with pytest.raises(ValueError, match="utilized_amount"):
        parse_credit_utilization_message(msg)


def test_parse_utilization_defaults():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "utilized_amount": 5000.0,
    }
    _, cu = parse_credit_utilization_message(msg)
    assert cu.utilization_currency == "USD"
    assert cu.limit_amount == 0.0
    assert cu.available_amount == 0.0
    assert cu.utilization_percentage == 0.0
    assert cu.as_of_timestamp is None


# ---------------------------------------------------------------------------
# process_message (via subscriber)
# ---------------------------------------------------------------------------


def test_process_limit_upsert(subscriber, db_path, limit_upsert_msg):
    subscriber.process_message("credit.limit", limit_upsert_msg)
    result = get_credit_limit(db_path, "ACME", "Bilateral")
    assert result is not None
    assert result.limit_amount == 50_000_000.0


def test_process_limit_delete(subscriber, db_path, limit_upsert_msg, limit_delete_msg):
    subscriber.process_message("credit.limit", limit_upsert_msg)
    assert get_credit_limit(db_path, "ACME", "Bilateral") is not None
    subscriber.process_message("credit.limit", limit_delete_msg)
    assert get_credit_limit(db_path, "ACME", "Bilateral") is None


def test_process_utilization_upsert(subscriber, db_path, utilization_upsert_msg):
    subscriber.process_message("credit.utilization", utilization_upsert_msg)
    result = get_credit_utilization(db_path, "ACME")
    assert result is not None
    assert result.utilized_amount == 15_000_000.0


def test_process_utilization_delete(subscriber, db_path, utilization_upsert_msg, utilization_delete_msg):
    subscriber.process_message("credit.utilization", utilization_upsert_msg)
    assert get_credit_utilization(db_path, "ACME") is not None
    subscriber.process_message("credit.utilization", utilization_delete_msg)
    assert get_credit_utilization(db_path, "ACME") is None


def test_process_unknown_topic(subscriber):
    subscriber.process_message("unknown.topic", {"action": "UPSERT", "counterparty_symbol": "X"})


# ---------------------------------------------------------------------------
# Full round-trips
# ---------------------------------------------------------------------------


def test_full_limit_round_trip(subscriber, db_path):
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "BIGCO",
        "limit_type": "Total",
        "limit_amount": 200_000_000.0,
        "limit_currency": "USD",
        "effective_date": "2025-01-01",
        "expiry_date": "2025-12-31",
        "status": "Active",
        "approved_by": "CRO",
        "last_review_date": "2025-06-01",
    }
    subscriber.process_message("credit.limit", msg)
    result = get_credit_limit(db_path, "BIGCO", "Total")
    assert result.counterparty_symbol == "BIGCO"
    assert result.limit_type == CreditLimitType.TOTAL
    assert result.limit_amount == 200_000_000.0
    assert result.effective_date == date(2025, 1, 1)
    assert result.approved_by == "CRO"


def test_full_utilization_round_trip(subscriber, db_path):
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "BIGCO",
        "utilized_amount": 80_000_000.0,
        "utilization_currency": "USD",
        "limit_amount": 200_000_000.0,
        "available_amount": 120_000_000.0,
        "utilization_percentage": 40.0,
        "as_of_timestamp": "2025-07-01T16:00:00+00:00",
    }
    subscriber.process_message("credit.utilization", msg)
    result = get_credit_utilization(db_path, "BIGCO")
    assert result.counterparty_symbol == "BIGCO"
    assert result.utilized_amount == 80_000_000.0
    assert result.available_amount == 120_000_000.0
    assert result.utilization_percentage == 40.0


def test_multiple_counterparties_round_trip(subscriber, db_path):
    messages = [
        {"action": "UPSERT", "counterparty_symbol": "ACME", "limit_type": "Bilateral", "limit_amount": 50_000_000.0},
        {"action": "UPSERT", "counterparty_symbol": "ACME", "limit_type": "Cleared", "limit_amount": 100_000_000.0},
        {"action": "UPSERT", "counterparty_symbol": "BIGCO", "limit_type": "Total", "limit_amount": 200_000_000.0},
    ]
    for msg in messages:
        subscriber.process_message("credit.limit", msg)

    results = get_all_credit_limits(db_path)
    assert len(results) == 3
    symbols = {(r.counterparty_symbol, r.limit_type) for r in results}
    assert symbols == {
        ("ACME", CreditLimitType.BILATERAL),
        ("ACME", CreditLimitType.CLEARED),
        ("BIGCO", CreditLimitType.TOTAL),
    }


# ---------------------------------------------------------------------------
# Numeric validation
# ---------------------------------------------------------------------------


def test_parse_limit_negative_amount():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": -1000.0,
    }
    with pytest.raises(ValueError, match="non-negative"):
        parse_credit_limit_message(msg)


def test_parse_limit_non_numeric_amount():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "limit_type": "Bilateral",
        "limit_amount": "not_a_number",
    }
    with pytest.raises(ValueError, match="numeric"):
        parse_credit_limit_message(msg)


def test_parse_utilization_negative_amount():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "utilized_amount": -500.0,
    }
    with pytest.raises(ValueError, match="non-negative"):
        parse_credit_utilization_message(msg)


def test_parse_utilization_non_numeric_amount():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "utilized_amount": "bad",
    }
    with pytest.raises(ValueError, match="numeric"):
        parse_credit_utilization_message(msg)


def test_parse_utilization_percentage_out_of_range():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "utilized_amount": 1000.0,
        "utilization_percentage": 150.0,
    }
    with pytest.raises(ValueError, match="0-100"):
        parse_credit_utilization_message(msg)


def test_parse_utilization_percentage_negative():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "utilized_amount": 1000.0,
        "utilization_percentage": -5.0,
    }
    with pytest.raises(ValueError, match="0-100"):
        parse_credit_utilization_message(msg)


def test_parse_utilization_non_numeric_optional_field():
    msg = {
        "action": "UPSERT",
        "counterparty_symbol": "ACME",
        "utilized_amount": 1000.0,
        "limit_amount": "bad",
    }
    with pytest.raises(ValueError, match="Numeric field"):
        parse_credit_utilization_message(msg)
