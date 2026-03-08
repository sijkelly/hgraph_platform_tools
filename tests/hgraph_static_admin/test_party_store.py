"""Tests for the SQLite party store (legal entities and trading relationships)."""

from datetime import date

import pytest

from hg_oap.parties.agreement import MasterAgreement, MasterAgreementType
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship
from hgraph_static_admin.party_store import (
    delete_legal_entity,
    delete_trading_relationship,
    get_all_legal_entities,
    get_all_trading_relationships,
    get_legal_entity,
    get_trading_relationship,
    init_party_db,
    upsert_legal_entity,
    upsert_trading_relationship,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_party.db")
    init_party_db(path)
    return path


@pytest.fixture()
def dealer():
    return LegalEntity(
        symbol="HGDEALER",
        name="HGraph Energy LLC",
        classification=PartyClassification.SWAP_DEALER,
        lei="529900HGRAPHDEALER0",
        jurisdiction="US",
        registration_id="NFA-99999",
        tax_id="99-9999999",
        address="100 Energy Plaza, Houston TX",
    )


@pytest.fixture()
def counterparty():
    return LegalEntity(
        symbol="ACME",
        name="ACME Energy Corp",
        classification=PartyClassification.NON_FINANCIAL_END_USER,
        lei="529900ACMEENERGY0LEI",
        jurisdiction="US",
    )


@pytest.fixture()
def msp():
    return LegalEntity(
        symbol="BIGSWAP",
        name="Big Swap Partners",
        classification=PartyClassification.MAJOR_SWAP_PARTICIPANT,
        lei="529900BIGSWAPPART0LE",
    )


@pytest.fixture()
def isda_agreement():
    return MasterAgreement(
        agreement_type=MasterAgreementType.ISDA,
        version="2002",
        agreement_date=date(2024, 6, 15),
        credit_support_annex=True,
        threshold_amount=5_000_000.0,
        governing_law="New York",
    )


@pytest.fixture()
def naesb_agreement():
    return MasterAgreement(
        agreement_type=MasterAgreementType.NAESB,
        version="2006",
        agreement_date=date(2023, 1, 10),
    )


@pytest.fixture()
def relationship(dealer, counterparty, isda_agreement):
    return TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.BILATERAL,
        isda=isda_agreement,
        dropcopy_enabled=True,
        internal_portfolio="GAS-BOOK-1",
        external_portfolio="EXT-001",
    )


# ---------------------------------------------------------------------------
# init_party_db
# ---------------------------------------------------------------------------


def test_init_creates_db(tmp_path):
    import os

    path = str(tmp_path / "new.db")
    assert not os.path.exists(path)
    init_party_db(path)
    assert os.path.exists(path)


def test_init_idempotent(db_path):
    init_party_db(db_path)  # second call should not raise


# ---------------------------------------------------------------------------
# Legal Entity CRUD
# ---------------------------------------------------------------------------


def test_upsert_and_get_entity(db_path, dealer):
    upsert_legal_entity(db_path, dealer)
    result = get_legal_entity(db_path, "HGDEALER")

    assert result is not None
    assert result.symbol == "HGDEALER"
    assert result.name == "HGraph Energy LLC"
    assert result.classification == PartyClassification.SWAP_DEALER
    assert result.lei == "529900HGRAPHDEALER0"
    assert result.jurisdiction == "US"
    assert result.registration_id == "NFA-99999"
    assert result.tax_id == "99-9999999"
    assert result.address == "100 Energy Plaza, Houston TX"


def test_upsert_updates_existing(db_path, dealer):
    upsert_legal_entity(db_path, dealer)

    updated = LegalEntity(
        symbol="HGDEALER",
        name="HGraph Energy LLC (Updated)",
        classification=PartyClassification.SWAP_DEALER,
        lei="529900HGRAPHDEALER0",
        jurisdiction="US",
        address="200 New Address, Houston TX",
    )
    upsert_legal_entity(db_path, updated)

    result = get_legal_entity(db_path, "HGDEALER")
    assert result is not None
    assert result.name == "HGraph Energy LLC (Updated)"
    assert result.address == "200 New Address, Houston TX"


def test_get_missing_entity_returns_none(db_path):
    assert get_legal_entity(db_path, "NONEXISTENT") is None


def test_get_all_entities_empty(db_path):
    assert get_all_legal_entities(db_path) == []


def test_get_all_entities_multiple(db_path, dealer, counterparty, msp):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    upsert_legal_entity(db_path, msp)

    result = get_all_legal_entities(db_path)
    assert len(result) == 3
    symbols = [e.symbol for e in result]
    assert "ACME" in symbols
    assert "BIGSWAP" in symbols
    assert "HGDEALER" in symbols


def test_delete_entity(db_path, dealer):
    upsert_legal_entity(db_path, dealer)
    assert delete_legal_entity(db_path, "HGDEALER") is True
    assert get_legal_entity(db_path, "HGDEALER") is None


def test_delete_missing_entity(db_path):
    assert delete_legal_entity(db_path, "NONEXISTENT") is False


def test_entity_classification_round_trip(db_path):
    """Each PartyClassification should survive a store/retrieve cycle."""
    for cls in PartyClassification:
        entity = LegalEntity(
            symbol=f"TEST_{cls.name}",
            name=f"Test {cls.value}",
            classification=cls,
        )
        upsert_legal_entity(db_path, entity)
        result = get_legal_entity(db_path, f"TEST_{cls.name}")
        assert result is not None
        assert result.classification == cls


def test_entity_with_none_optional_fields(db_path):
    entity = LegalEntity(
        symbol="MINIMAL",
        name="Minimal Corp",
        classification=PartyClassification.FINANCIAL_ENTITY,
    )
    upsert_legal_entity(db_path, entity)
    result = get_legal_entity(db_path, "MINIMAL")
    assert result is not None
    assert result.lei is None
    assert result.jurisdiction is None
    assert result.registration_id is None
    assert result.tax_id is None
    assert result.address is None


# ---------------------------------------------------------------------------
# Trading Relationship CRUD
# ---------------------------------------------------------------------------


def test_upsert_and_get_relationship(db_path, dealer, counterparty, relationship):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    upsert_trading_relationship(db_path, relationship)

    result = get_trading_relationship(db_path, "HGDEALER", "ACME")
    assert result is not None
    assert result.internal_party.symbol == "HGDEALER"
    assert result.external_party.symbol == "ACME"
    assert result.clearing_status == ClearingStatus.BILATERAL
    assert result.dropcopy_enabled is True
    assert result.internal_portfolio == "GAS-BOOK-1"
    assert result.external_portfolio == "EXT-001"


def test_relationship_isda_round_trip(db_path, dealer, counterparty, relationship):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    upsert_trading_relationship(db_path, relationship)

    result = get_trading_relationship(db_path, "HGDEALER", "ACME")
    assert result is not None
    assert result.isda is not None
    assert result.isda.agreement_type == MasterAgreementType.ISDA
    assert result.isda.version == "2002"
    assert result.isda.agreement_date == date(2024, 6, 15)
    assert result.isda.credit_support_annex is True
    assert result.isda.threshold_amount == 5_000_000.0
    assert result.isda.governing_law == "New York"


def test_relationship_naesb_round_trip(db_path, dealer, counterparty, naesb_agreement):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)

    rel = TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        naesb=naesb_agreement,
    )
    upsert_trading_relationship(db_path, rel)

    result = get_trading_relationship(db_path, "HGDEALER", "ACME")
    assert result is not None
    assert result.naesb is not None
    assert result.naesb.agreement_type == MasterAgreementType.NAESB
    assert result.naesb.version == "2006"
    assert result.isda is None


def test_relationship_no_agreements(db_path, dealer, counterparty):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)

    rel = TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.CLEARED,
    )
    upsert_trading_relationship(db_path, rel)

    result = get_trading_relationship(db_path, "HGDEALER", "ACME")
    assert result is not None
    assert result.clearing_status == ClearingStatus.CLEARED
    assert result.isda is None
    assert result.naesb is None
    assert result.eei is None


def test_get_missing_relationship(db_path):
    assert get_trading_relationship(db_path, "A", "B") is None


def test_get_all_relationships_empty(db_path):
    assert get_all_trading_relationships(db_path) == []


def test_get_all_relationships_multiple(db_path, dealer, counterparty, msp):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    upsert_legal_entity(db_path, msp)

    rel1 = TradingRelationship(internal_party=dealer, external_party=counterparty)
    rel2 = TradingRelationship(internal_party=dealer, external_party=msp)
    upsert_trading_relationship(db_path, rel1)
    upsert_trading_relationship(db_path, rel2)

    result = get_all_trading_relationships(db_path)
    assert len(result) == 2


def test_upsert_relationship_updates(db_path, dealer, counterparty):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)

    rel1 = TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.BILATERAL,
    )
    upsert_trading_relationship(db_path, rel1)

    rel2 = TradingRelationship(
        internal_party=dealer,
        external_party=counterparty,
        clearing_status=ClearingStatus.CLEARED,
        internal_portfolio="NEW-BOOK",
    )
    upsert_trading_relationship(db_path, rel2)

    result = get_trading_relationship(db_path, "HGDEALER", "ACME")
    assert result is not None
    assert result.clearing_status == ClearingStatus.CLEARED
    assert result.internal_portfolio == "NEW-BOOK"


def test_delete_relationship(db_path, dealer, counterparty):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)

    rel = TradingRelationship(internal_party=dealer, external_party=counterparty)
    upsert_trading_relationship(db_path, rel)

    assert delete_trading_relationship(db_path, "HGDEALER", "ACME") is True
    assert get_trading_relationship(db_path, "HGDEALER", "ACME") is None


def test_delete_missing_relationship(db_path):
    assert delete_trading_relationship(db_path, "A", "B") is False
