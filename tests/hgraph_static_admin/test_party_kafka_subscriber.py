"""Tests for the party Kafka subscriber service."""

import pytest

from hg_oap.parties.agreement import MasterAgreementType
from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.relationship import ClearingStatus
from hgraph_static_admin.party_kafka_subscriber import (
    PartyKafkaSubscriber,
    parse_legal_entity_message,
    parse_relationship_message,
)
from hgraph_static_admin.party_store import (
    get_all_legal_entities,
    get_legal_entity,
    get_trading_relationship,
    init_party_db,
    upsert_legal_entity,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "test_subscriber.db")
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
def entity_upsert_msg():
    return {
        "action": "UPSERT",
        "symbol": "ACME",
        "name": "ACME Energy Corp",
        "classification": "NFEU",
        "lei": "529900ACMEENERGY0LEI",
        "jurisdiction": "US",
        "registration_id": "NFA-12345",
        "tax_id": "12-3456789",
        "address": "123 Main St, Houston TX",
    }


@pytest.fixture()
def entity_delete_msg():
    return {"action": "DELETE", "symbol": "ACME"}


@pytest.fixture()
def relationship_upsert_msg():
    return {
        "action": "UPSERT",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "ACME",
        "clearing_status": "Bilateral",
        "isda": {
            "version": "2002",
            "credit_support_annex": True,
            "governing_law": "New York",
        },
        "dropcopy_enabled": False,
        "internal_portfolio": "GAS-BOOK-1",
        "external_portfolio": "EXT-001",
    }


@pytest.fixture()
def relationship_delete_msg():
    return {
        "action": "DELETE",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "ACME",
    }


@pytest.fixture()
def subscriber(db_path):
    return PartyKafkaSubscriber(
        db_path,
        bootstrap_servers="localhost:9092",
        entity_topic="party.legal_entity",
        relationship_topic="party.trading_relationship",
    )


# ---------------------------------------------------------------------------
# parse_legal_entity_message
# ---------------------------------------------------------------------------


def test_parse_entity_upsert(entity_upsert_msg):
    action, entity = parse_legal_entity_message(entity_upsert_msg)
    assert action == "UPSERT"
    assert entity is not None
    assert isinstance(entity, LegalEntity)
    assert entity.symbol == "ACME"
    assert entity.name == "ACME Energy Corp"
    assert entity.classification == PartyClassification.NON_FINANCIAL_END_USER
    assert entity.lei == "529900ACMEENERGY0LEI"
    assert entity.jurisdiction == "US"
    assert entity.registration_id == "NFA-12345"
    assert entity.tax_id == "12-3456789"
    assert entity.address == "123 Main St, Houston TX"


def test_parse_entity_delete(entity_delete_msg):
    action, entity = parse_legal_entity_message(entity_delete_msg)
    assert action == "DELETE"
    assert entity is None


def test_parse_entity_sd_classification():
    msg = {"action": "UPSERT", "symbol": "SD1", "name": "Dealer One", "classification": "SD"}
    action, entity = parse_legal_entity_message(msg)
    assert entity.classification == PartyClassification.SWAP_DEALER


def test_parse_entity_msp_classification():
    msg = {"action": "UPSERT", "symbol": "MSP1", "name": "MSP One", "classification": "MSP"}
    action, entity = parse_legal_entity_message(msg)
    assert entity.classification == PartyClassification.MAJOR_SWAP_PARTICIPANT


def test_parse_entity_fe_classification():
    msg = {"action": "UPSERT", "symbol": "FE1", "name": "FE One", "classification": "FE"}
    action, entity = parse_legal_entity_message(msg)
    assert entity.classification == PartyClassification.FINANCIAL_ENTITY


def test_parse_entity_missing_symbol():
    with pytest.raises(ValueError, match="symbol"):
        parse_legal_entity_message({"action": "UPSERT", "name": "No Symbol"})


def test_parse_entity_missing_name():
    with pytest.raises(ValueError, match="name"):
        parse_legal_entity_message({"action": "UPSERT", "symbol": "X"})


def test_parse_entity_unknown_action():
    with pytest.raises(ValueError, match="Unknown action"):
        parse_legal_entity_message({"action": "INVALID", "symbol": "X"})


def test_parse_entity_unknown_classification():
    with pytest.raises(ValueError, match="Unknown classification"):
        parse_legal_entity_message(
            {"action": "UPSERT", "symbol": "X", "name": "X", "classification": "INVALID"}
        )


def test_parse_entity_case_insensitive_action():
    msg = {"action": "upsert", "symbol": "X", "name": "X Corp", "classification": "SD"}
    action, entity = parse_legal_entity_message(msg)
    assert action == "UPSERT"
    assert entity is not None


# ---------------------------------------------------------------------------
# parse_relationship_message
# ---------------------------------------------------------------------------


def test_parse_relationship_upsert(db_path, dealer, counterparty, relationship_upsert_msg):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)

    action, rel = parse_relationship_message(relationship_upsert_msg, db_path)
    assert action == "UPSERT"
    assert rel is not None
    assert rel.internal_party.symbol == "HGDEALER"
    assert rel.external_party.symbol == "ACME"
    assert rel.clearing_status == ClearingStatus.BILATERAL
    assert rel.isda is not None
    assert rel.isda.agreement_type == MasterAgreementType.ISDA
    assert rel.isda.version == "2002"
    assert rel.isda.credit_support_annex is True
    assert rel.isda.governing_law == "New York"
    assert rel.dropcopy_enabled is False
    assert rel.internal_portfolio == "GAS-BOOK-1"


def test_parse_relationship_delete(db_path, relationship_delete_msg):
    action, rel = parse_relationship_message(relationship_delete_msg, db_path)
    assert action == "DELETE"
    assert rel is None


def test_parse_relationship_missing_parties(db_path):
    with pytest.raises(ValueError, match="internal_party_symbol"):
        parse_relationship_message({"action": "UPSERT"}, db_path)


def test_parse_relationship_party_not_found(db_path, dealer):
    upsert_legal_entity(db_path, dealer)
    msg = {
        "action": "UPSERT",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "NONEXISTENT",
    }
    with pytest.raises(ValueError, match="External party not found"):
        parse_relationship_message(msg, db_path)


def test_parse_relationship_unknown_clearing_status(db_path, dealer, counterparty):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    msg = {
        "action": "UPSERT",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "ACME",
        "clearing_status": "INVALID",
    }
    with pytest.raises(ValueError, match="Unknown clearing_status"):
        parse_relationship_message(msg, db_path)


def test_parse_relationship_no_agreements(db_path, dealer, counterparty):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    msg = {
        "action": "UPSERT",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "ACME",
        "clearing_status": "Cleared",
    }
    action, rel = parse_relationship_message(msg, db_path)
    assert rel.clearing_status == ClearingStatus.CLEARED
    assert rel.isda is None
    assert rel.naesb is None
    assert rel.eei is None


def test_parse_relationship_with_naesb(db_path, dealer, counterparty):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    msg = {
        "action": "UPSERT",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "ACME",
        "naesb": {"version": "2006", "agreement_date": "2023-01-10"},
    }
    action, rel = parse_relationship_message(msg, db_path)
    assert rel.naesb is not None
    assert rel.naesb.agreement_type == MasterAgreementType.NAESB
    assert rel.naesb.version == "2006"


# ---------------------------------------------------------------------------
# PartyKafkaSubscriber.process_message
# ---------------------------------------------------------------------------


def test_process_entity_upsert(db_path, subscriber, entity_upsert_msg):
    subscriber.process_message("party.legal_entity", entity_upsert_msg)

    result = get_legal_entity(db_path, "ACME")
    assert result is not None
    assert result.name == "ACME Energy Corp"
    assert result.classification == PartyClassification.NON_FINANCIAL_END_USER


def test_process_entity_delete(db_path, subscriber, dealer, entity_delete_msg):
    # First create then delete
    upsert_legal_entity(db_path, LegalEntity(
        symbol="ACME",
        name="ACME",
        classification=PartyClassification.NON_FINANCIAL_END_USER,
    ))
    subscriber.process_message("party.legal_entity", entity_delete_msg)
    assert get_legal_entity(db_path, "ACME") is None


def test_process_relationship_upsert(db_path, subscriber, dealer, counterparty, relationship_upsert_msg):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)

    subscriber.process_message("party.trading_relationship", relationship_upsert_msg)

    result = get_trading_relationship(db_path, "HGDEALER", "ACME")
    assert result is not None
    assert result.clearing_status == ClearingStatus.BILATERAL
    assert result.isda is not None


def test_process_unknown_topic(db_path, subscriber):
    """Unknown topics should be silently ignored (just logged)."""
    subscriber.process_message("unknown.topic", {"action": "UPSERT"})
    # Should not raise


# ---------------------------------------------------------------------------
# Full round-trip: message → parse → store → retrieve
# ---------------------------------------------------------------------------


def test_full_entity_round_trip(db_path, subscriber, entity_upsert_msg):
    subscriber.process_message("party.legal_entity", entity_upsert_msg)

    entity = get_legal_entity(db_path, "ACME")
    assert entity is not None
    assert entity.symbol == "ACME"
    assert entity.name == "ACME Energy Corp"
    assert entity.classification == PartyClassification.NON_FINANCIAL_END_USER
    assert entity.lei == "529900ACMEENERGY0LEI"
    assert entity.jurisdiction == "US"
    assert entity.registration_id == "NFA-12345"
    assert entity.tax_id == "12-3456789"
    assert entity.address == "123 Main St, Houston TX"


def test_full_relationship_round_trip(db_path, subscriber, dealer, counterparty, relationship_upsert_msg):
    # Pre-populate parties
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)

    subscriber.process_message("party.trading_relationship", relationship_upsert_msg)

    rel = get_trading_relationship(db_path, "HGDEALER", "ACME")
    assert rel is not None
    assert rel.internal_party.symbol == "HGDEALER"
    assert rel.internal_party.classification == PartyClassification.SWAP_DEALER
    assert rel.external_party.symbol == "ACME"
    assert rel.external_party.classification == PartyClassification.NON_FINANCIAL_END_USER
    assert rel.clearing_status == ClearingStatus.BILATERAL
    assert rel.isda is not None
    assert rel.isda.version == "2002"
    assert rel.isda.credit_support_annex is True
    assert rel.isda.governing_law == "New York"
    assert rel.internal_portfolio == "GAS-BOOK-1"
    assert rel.external_portfolio == "EXT-001"


def test_multiple_entities_round_trip(db_path, subscriber):
    """Process multiple entity messages and verify all are stored."""
    messages = [
        {"action": "UPSERT", "symbol": "A", "name": "Company A", "classification": "SD"},
        {"action": "UPSERT", "symbol": "B", "name": "Company B", "classification": "NFEU"},
        {"action": "UPSERT", "symbol": "C", "name": "Company C", "classification": "MSP"},
    ]
    for msg in messages:
        subscriber.process_message("party.legal_entity", msg)

    entities = get_all_legal_entities(db_path)
    assert len(entities) == 3
    symbols = {e.symbol for e in entities}
    assert symbols == {"A", "B", "C"}


# ---------------------------------------------------------------------------
# Numeric validation
# ---------------------------------------------------------------------------


def test_parse_agreement_non_numeric_threshold(db_path, dealer, counterparty):
    upsert_legal_entity(db_path, dealer)
    upsert_legal_entity(db_path, counterparty)
    msg = {
        "action": "UPSERT",
        "internal_party_symbol": "HGDEALER",
        "external_party_symbol": "ACME",
        "isda": {"version": "2002", "threshold_amount": "not_a_number"},
    }
    with pytest.raises(ValueError, match="threshold_amount must be numeric"):
        parse_relationship_message(msg, db_path)
