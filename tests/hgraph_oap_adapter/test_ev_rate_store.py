"""Tests for the SQLite-backed EV rate store."""

import os
import tempfile
from datetime import date

import pytest

from hg_oap.orders.sales.ev import EVRate, InstrumentCategory
from hg_oap.units.default_unit_system import U
from hgraph_oap_adapter.ev_rate_store import (
    delete_ev_rate,
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
    """Provide a temporary SQLite database path and initialise the schema."""
    path = str(tmp_path / "test_ev_rates.db")
    init_ev_rate_db(path)
    return path


@pytest.fixture()
def gas_swap_rate():
    return EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
        currency_symbol="USD",
    )


@pytest.fixture()
def power_swap_rate():
    return EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.kWh,
        rate_per_unit=0.001,
        currency_symbol="USD",
    )


@pytest.fixture()
def basis_rate():
    return EVRate(
        instrument_category=InstrumentCategory.BASIS_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.015,
        currency_symbol="USD",
    )


# ---------------------------------------------------------------------------
# init_ev_rate_db
# ---------------------------------------------------------------------------


def test_init_creates_db_file(tmp_path):
    path = str(tmp_path / "new_ev.db")
    assert not os.path.exists(path)
    init_ev_rate_db(path)
    assert os.path.exists(path)


def test_init_idempotent(db_path):
    """Calling init twice should not raise."""
    init_ev_rate_db(db_path)


# ---------------------------------------------------------------------------
# upsert_ev_rate / get_ev_rate
# ---------------------------------------------------------------------------


def test_upsert_and_get(db_path, gas_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    result = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)

    assert result is not None
    assert result.instrument_category == InstrumentCategory.FIXED_FLOAT_SWAP
    assert result.unit == U.MMBtu
    assert result.rate_per_unit == pytest.approx(0.02)
    assert result.currency_symbol == "USD"


def test_upsert_updates_existing(db_path, gas_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)

    updated = EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.03,
        currency_symbol="EUR",
    )
    upsert_ev_rate(db_path, updated)

    result = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)
    assert result is not None
    assert result.rate_per_unit == pytest.approx(0.03)
    assert result.currency_symbol == "EUR"


def test_get_missing_returns_none(db_path):
    result = get_ev_rate(db_path, InstrumentCategory.SWAPTION, U.bbl)
    assert result is None


def test_get_different_category_returns_none(db_path, gas_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    result = get_ev_rate(db_path, InstrumentCategory.BASIS_SWAP, U.MMBtu)
    assert result is None


def test_get_different_unit_returns_none(db_path, gas_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    result = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.kWh)
    assert result is None


def test_multiple_rates_different_units(db_path, gas_swap_rate, power_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    upsert_ev_rate(db_path, power_swap_rate)

    gas = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)
    power = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.kWh)

    assert gas is not None
    assert power is not None
    assert gas.rate_per_unit == pytest.approx(0.02)
    assert power.rate_per_unit == pytest.approx(0.001)


def test_multiple_rates_different_categories(db_path, gas_swap_rate, basis_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    upsert_ev_rate(db_path, basis_rate)

    ff = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)
    bs = get_ev_rate(db_path, InstrumentCategory.BASIS_SWAP, U.MMBtu)

    assert ff is not None
    assert bs is not None
    assert ff.rate_per_unit == pytest.approx(0.02)
    assert bs.rate_per_unit == pytest.approx(0.015)


# ---------------------------------------------------------------------------
# as_of date filtering
# ---------------------------------------------------------------------------


def test_get_with_as_of_within_range(db_path):
    rate = EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
        effective_date=date(2026, 1, 1),
        expiry_date=date(2026, 12, 31),
    )
    upsert_ev_rate(db_path, rate)

    result = get_ev_rate(
        db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu, as_of=date(2026, 6, 15)
    )
    assert result is not None
    assert result.rate_per_unit == pytest.approx(0.02)


def test_get_with_as_of_before_effective(db_path):
    rate = EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
        effective_date=date(2026, 6, 1),
        expiry_date=date(2026, 12, 31),
    )
    upsert_ev_rate(db_path, rate)

    result = get_ev_rate(
        db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu, as_of=date(2026, 1, 15)
    )
    assert result is None


def test_get_with_as_of_after_expiry(db_path):
    rate = EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
        effective_date=date(2026, 1, 1),
        expiry_date=date(2026, 6, 30),
    )
    upsert_ev_rate(db_path, rate)

    result = get_ev_rate(
        db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu, as_of=date(2026, 7, 1)
    )
    assert result is None


def test_get_with_as_of_no_dates_always_matches(db_path, gas_swap_rate):
    """Rates with no effective/expiry should match any as_of date."""
    upsert_ev_rate(db_path, gas_swap_rate)

    result = get_ev_rate(
        db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu, as_of=date(2030, 1, 1)
    )
    assert result is not None


def test_get_with_as_of_on_effective_date(db_path):
    rate = EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
        effective_date=date(2026, 3, 1),
        expiry_date=date(2026, 3, 31),
    )
    upsert_ev_rate(db_path, rate)

    result = get_ev_rate(
        db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu, as_of=date(2026, 3, 1)
    )
    assert result is not None


def test_get_with_as_of_on_expiry_date(db_path):
    rate = EVRate(
        instrument_category=InstrumentCategory.FIXED_FLOAT_SWAP,
        unit=U.MMBtu,
        rate_per_unit=0.02,
        effective_date=date(2026, 3, 1),
        expiry_date=date(2026, 3, 31),
    )
    upsert_ev_rate(db_path, rate)

    result = get_ev_rate(
        db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu, as_of=date(2026, 3, 31)
    )
    assert result is not None


def test_upsert_preserves_dates(db_path):
    rate = EVRate(
        instrument_category=InstrumentCategory.FORWARD,
        unit=U.bbl,
        rate_per_unit=0.10,
        effective_date=date(2026, 1, 1),
        expiry_date=date(2026, 12, 31),
    )
    upsert_ev_rate(db_path, rate)

    result = get_ev_rate(db_path, InstrumentCategory.FORWARD, U.bbl)
    assert result is not None
    assert result.effective_date == date(2026, 1, 1)
    assert result.expiry_date == date(2026, 12, 31)


# ---------------------------------------------------------------------------
# get_all_ev_rates
# ---------------------------------------------------------------------------


def test_get_all_empty(db_path):
    result = get_all_ev_rates(db_path)
    assert result == []


def test_get_all_multiple(db_path, gas_swap_rate, power_swap_rate, basis_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    upsert_ev_rate(db_path, power_swap_rate)
    upsert_ev_rate(db_path, basis_rate)

    result = get_all_ev_rates(db_path)
    assert len(result) == 3


def test_get_all_returns_dicts(db_path, gas_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)

    result = get_all_ev_rates(db_path)
    assert len(result) == 1
    row = result[0]
    assert row["instrument_category"] == InstrumentCategory.FIXED_FLOAT_SWAP
    assert row["rate_per_unit"] == pytest.approx(0.02)
    assert "unit_str" in row


# ---------------------------------------------------------------------------
# delete_ev_rate
# ---------------------------------------------------------------------------


def test_delete_existing(db_path, gas_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    assert delete_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu) is True

    result = get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)
    assert result is None


def test_delete_nonexistent(db_path):
    assert delete_ev_rate(db_path, InstrumentCategory.OPTION, U.mt) is False


def test_delete_does_not_affect_others(db_path, gas_swap_rate, power_swap_rate):
    upsert_ev_rate(db_path, gas_swap_rate)
    upsert_ev_rate(db_path, power_swap_rate)

    delete_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu)

    assert get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.MMBtu) is None
    assert get_ev_rate(db_path, InstrumentCategory.FIXED_FLOAT_SWAP, U.kWh) is not None
