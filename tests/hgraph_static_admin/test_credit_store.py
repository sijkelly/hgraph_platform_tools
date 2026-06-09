"""Tests for credit limit and utilization SQLite store."""

import os
from datetime import date

import pytest

from hg_oap.credit.credit_limit import (
    CreditLimit,
    CreditLimitType,
    CreditStatus,
    CreditUtilization,
)

from hgraph_static_admin.credit_store import (
    delete_credit_limit,
    delete_credit_utilization,
    get_all_credit_limits,
    get_all_credit_utilizations,
    get_credit_limit,
    get_credit_limits_for_counterparty,
    get_credit_utilization,
    init_credit_db,
    upsert_credit_limit,
    upsert_credit_utilization,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_credit.db")
    init_credit_db(path)
    return path


@pytest.fixture()
def bilateral_limit():
    return CreditLimit(
        counterparty_symbol="ACME",
        limit_type=CreditLimitType.BILATERAL,
        limit_amount=50_000_000.0,
        limit_currency="USD",
        effective_date=date(2025, 1, 1),
        expiry_date=date(2026, 1, 1),
        status=CreditStatus.ACTIVE,
        approved_by="Credit Committee",
        last_review_date=date(2025, 6, 15),
    )


@pytest.fixture()
def cleared_limit():
    return CreditLimit(
        counterparty_symbol="ACME",
        limit_type=CreditLimitType.CLEARED,
        limit_amount=100_000_000.0,
        limit_currency="USD",
        status=CreditStatus.ACTIVE,
    )


@pytest.fixture()
def utilization():
    return CreditUtilization(
        counterparty_symbol="ACME",
        utilized_amount=15_000_000.0,
        utilization_currency="USD",
        limit_amount=50_000_000.0,
        available_amount=35_000_000.0,
        utilization_percentage=30.0,
        as_of_timestamp="2025-07-01T14:30:00+00:00",
    )


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------


def test_init_creates_db(tmp_path):
    path = str(tmp_path / "new.db")
    assert not os.path.exists(path)
    init_credit_db(path)
    assert os.path.exists(path)


def test_init_idempotent(db_path):
    init_credit_db(db_path)


# ---------------------------------------------------------------------------
# Credit Limit CRUD
# ---------------------------------------------------------------------------


def test_upsert_and_get_limit(db_path, bilateral_limit):
    upsert_credit_limit(db_path, bilateral_limit)
    result = get_credit_limit(db_path, "ACME", "Bilateral")
    assert result is not None
    assert result.counterparty_symbol == "ACME"
    assert result.limit_type == CreditLimitType.BILATERAL
    assert result.limit_amount == 50_000_000.0
    assert result.limit_currency == "USD"
    assert result.effective_date == date(2025, 1, 1)
    assert result.expiry_date == date(2026, 1, 1)
    assert result.status == CreditStatus.ACTIVE
    assert result.approved_by == "Credit Committee"
    assert result.last_review_date == date(2025, 6, 15)


def test_upsert_updates_existing(db_path, bilateral_limit):
    upsert_credit_limit(db_path, bilateral_limit)
    updated = CreditLimit(
        counterparty_symbol="ACME",
        limit_type=CreditLimitType.BILATERAL,
        limit_amount=75_000_000.0,
        limit_currency="USD",
        status=CreditStatus.ACTIVE,
        approved_by="Senior Credit Officer",
    )
    upsert_credit_limit(db_path, updated)
    result = get_credit_limit(db_path, "ACME", "Bilateral")
    assert result.limit_amount == 75_000_000.0
    assert result.approved_by == "Senior Credit Officer"
    assert len(get_all_credit_limits(db_path)) == 1


def test_get_missing_limit_returns_none(db_path):
    assert get_credit_limit(db_path, "NONEXISTENT", "Bilateral") is None


def test_get_limits_for_counterparty(db_path, bilateral_limit, cleared_limit):
    upsert_credit_limit(db_path, bilateral_limit)
    upsert_credit_limit(db_path, cleared_limit)
    results = get_credit_limits_for_counterparty(db_path, "ACME")
    assert len(results) == 2
    types = {r.limit_type for r in results}
    assert types == {CreditLimitType.BILATERAL, CreditLimitType.CLEARED}


def test_get_limits_for_counterparty_empty(db_path):
    assert get_credit_limits_for_counterparty(db_path, "NONEXISTENT") == []


def test_get_all_limits_empty(db_path):
    assert get_all_credit_limits(db_path) == []


def test_get_all_limits_multiple(db_path, bilateral_limit, cleared_limit):
    upsert_credit_limit(db_path, bilateral_limit)
    upsert_credit_limit(db_path, cleared_limit)
    # Add a limit for a different counterparty
    other = CreditLimit(
        counterparty_symbol="BIGCO",
        limit_type=CreditLimitType.TOTAL,
        limit_amount=200_000_000.0,
    )
    upsert_credit_limit(db_path, other)
    results = get_all_credit_limits(db_path)
    assert len(results) == 3


def test_delete_limit(db_path, bilateral_limit):
    upsert_credit_limit(db_path, bilateral_limit)
    assert delete_credit_limit(db_path, "ACME", "Bilateral") is True
    assert get_credit_limit(db_path, "ACME", "Bilateral") is None


def test_delete_missing_limit(db_path):
    assert delete_credit_limit(db_path, "NONEXISTENT", "Bilateral") is False


@pytest.mark.parametrize("ltype", list(CreditLimitType))
def test_limit_type_round_trip(db_path, ltype):
    cl = CreditLimit(
        counterparty_symbol="TEST",
        limit_type=ltype,
        limit_amount=10_000_000.0,
    )
    upsert_credit_limit(db_path, cl)
    result = get_credit_limit(db_path, "TEST", ltype.value)
    assert result.limit_type == ltype


@pytest.mark.parametrize("status", list(CreditStatus))
def test_status_round_trip(db_path, status):
    cl = CreditLimit(
        counterparty_symbol="TEST",
        limit_type=CreditLimitType.BILATERAL,
        limit_amount=10_000_000.0,
        status=status,
    )
    upsert_credit_limit(db_path, cl)
    result = get_credit_limit(db_path, "TEST", "Bilateral")
    assert result.status == status


def test_limit_with_none_optional_fields(db_path):
    cl = CreditLimit(
        counterparty_symbol="MIN",
        limit_type=CreditLimitType.BILATERAL,
        limit_amount=10_000_000.0,
    )
    upsert_credit_limit(db_path, cl)
    result = get_credit_limit(db_path, "MIN", "Bilateral")
    assert result.effective_date is None
    assert result.expiry_date is None
    assert result.approved_by is None
    assert result.last_review_date is None


# ---------------------------------------------------------------------------
# Credit Utilization CRUD
# ---------------------------------------------------------------------------


def test_upsert_and_get_utilization(db_path, utilization):
    upsert_credit_utilization(db_path, utilization)
    result = get_credit_utilization(db_path, "ACME")
    assert result is not None
    assert result.counterparty_symbol == "ACME"
    assert result.utilized_amount == 15_000_000.0
    assert result.utilization_currency == "USD"
    assert result.limit_amount == 50_000_000.0
    assert result.available_amount == 35_000_000.0
    assert result.utilization_percentage == 30.0
    assert result.as_of_timestamp == "2025-07-01T14:30:00+00:00"


def test_upsert_utilization_updates_existing(db_path, utilization):
    upsert_credit_utilization(db_path, utilization)
    updated = CreditUtilization(
        counterparty_symbol="ACME",
        utilized_amount=25_000_000.0,
        utilization_currency="USD",
        limit_amount=50_000_000.0,
        available_amount=25_000_000.0,
        utilization_percentage=50.0,
        as_of_timestamp="2025-07-01T15:00:00+00:00",
    )
    upsert_credit_utilization(db_path, updated)
    result = get_credit_utilization(db_path, "ACME")
    assert result.utilized_amount == 25_000_000.0
    assert result.utilization_percentage == 50.0
    assert len(get_all_credit_utilizations(db_path)) == 1


def test_get_missing_utilization_returns_none(db_path):
    assert get_credit_utilization(db_path, "NONEXISTENT") is None


def test_get_all_utilizations_empty(db_path):
    assert get_all_credit_utilizations(db_path) == []


def test_get_all_utilizations_multiple(db_path, utilization):
    upsert_credit_utilization(db_path, utilization)
    other = CreditUtilization(
        counterparty_symbol="BIGCO",
        utilized_amount=5_000_000.0,
        limit_amount=100_000_000.0,
        available_amount=95_000_000.0,
        utilization_percentage=5.0,
    )
    upsert_credit_utilization(db_path, other)
    results = get_all_credit_utilizations(db_path)
    assert len(results) == 2
    symbols = {r.counterparty_symbol for r in results}
    assert symbols == {"ACME", "BIGCO"}


def test_delete_utilization(db_path, utilization):
    upsert_credit_utilization(db_path, utilization)
    assert delete_credit_utilization(db_path, "ACME") is True
    assert get_credit_utilization(db_path, "ACME") is None


def test_delete_missing_utilization(db_path):
    assert delete_credit_utilization(db_path, "NONEXISTENT") is False
