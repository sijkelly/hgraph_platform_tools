"""Tests for the party adapter — hg_oap party objects to platform-tools formats."""

import pytest

from hg_oap.parties.party import LegalEntity, PartyClassification
from hg_oap.parties.relationship import ClearingStatus, TradingRelationship

from hgraph_oap_adapter.party_adapter import (
    legal_entity_to_counterparty_row,
    relationship_to_trade_parties,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def dealer():
    return LegalEntity(
        symbol="DEALER",
        name="Acme Energy Trading LLC",
        classification=PartyClassification.SWAP_DEALER,
        lei="529900ACMEDEALER0LEI",
        jurisdiction="US",
        registration_id="NFA-0012345",
        tax_id="12-3456789",
    )


@pytest.fixture()
def utility():
    return LegalEntity(
        symbol="UTIL_CO",
        name="SunState Electric Co",
        classification=PartyClassification.NON_FINANCIAL_END_USER,
        lei="529900SUNSTATEUTL0LEI",
        jurisdiction="US",
    )


@pytest.fixture()
def relationship(dealer, utility):
    return TradingRelationship(
        internal_party=dealer,
        external_party=utility,
        clearing_status=ClearingStatus.BILATERAL,
        internal_portfolio="GAS_DESK",
        external_portfolio="HEDGE_BOOK",
    )


# ---------------------------------------------------------------------------
# legal_entity_to_counterparty_row
# ---------------------------------------------------------------------------


def test_counterparty_row_returns_dict(dealer):
    result = legal_entity_to_counterparty_row(dealer)
    assert isinstance(result, dict)


def test_counterparty_row_legal_name(dealer):
    result = legal_entity_to_counterparty_row(dealer)
    assert result["legal_name"] == "Acme Energy Trading LLC"


def test_counterparty_row_short_name(dealer):
    result = legal_entity_to_counterparty_row(dealer)
    assert result["short_name"] == "DEALER"


def test_counterparty_row_identifiers(dealer):
    result = legal_entity_to_counterparty_row(dealer)
    assert result["identifier1"] == "529900ACMEDEALER0LEI"  # LEI
    assert result["identifier2"] == "NFA-0012345"  # registration_id
    assert result["identifier3"] == "12-3456789"  # tax_id
    assert result["identifier4"] == "SD"  # classification value


def test_counterparty_row_empty_optional_fields(utility):
    """Optional fields that are None should become empty strings."""
    result = legal_entity_to_counterparty_row(utility)
    assert result["identifier2"] == ""  # no registration_id
    assert result["identifier3"] == ""  # no tax_id


def test_counterparty_row_classification_as_identifier4(utility):
    result = legal_entity_to_counterparty_row(utility)
    assert result["identifier4"] == "NFEU"


def test_counterparty_row_has_all_static_admin_columns(dealer):
    """Verify all columns from the static admin counterparties table."""
    result = legal_entity_to_counterparty_row(dealer)
    expected_columns = [
        "legal_name", "short_name",
        "identifier1", "identifier2", "identifier3", "identifier4",
        "cleared_otc", "dropcopy_enabled",
        "notification_preferences", "notification_contacts",
    ]
    for col in expected_columns:
        assert col in result, f"Missing column: {col}"


@pytest.mark.parametrize(
    "classification,expected",
    [
        (PartyClassification.SWAP_DEALER, "SD"),
        (PartyClassification.MAJOR_SWAP_PARTICIPANT, "MSP"),
        (PartyClassification.FINANCIAL_ENTITY, "FE"),
        (PartyClassification.NON_FINANCIAL_END_USER, "NFEU"),
    ],
)
def test_counterparty_row_all_classifications(classification, expected):
    entity = LegalEntity(
        symbol="TEST",
        name="Test Entity",
        classification=classification,
    )
    result = legal_entity_to_counterparty_row(entity)
    assert result["identifier4"] == expected


# ---------------------------------------------------------------------------
# relationship_to_trade_parties
# ---------------------------------------------------------------------------


def test_trade_parties_returns_dict(relationship):
    result = relationship_to_trade_parties(relationship)
    assert isinstance(result, dict)


def test_trade_parties_internal_external(relationship):
    result = relationship_to_trade_parties(relationship)
    assert result["internal_party"] == "DEALER"
    assert result["external_party"] == "UTIL_CO"


def test_trade_parties_counterparty_nested(relationship):
    result = relationship_to_trade_parties(relationship)
    assert result["counterparty"] == {"internal": "DEALER", "external": "UTIL_CO"}


def test_trade_parties_portfolio_nested(relationship):
    result = relationship_to_trade_parties(relationship)
    assert result["portfolio"] == {"internal": "GAS_DESK", "external": "HEDGE_BOOK"}


def test_trade_parties_traders_nested(relationship):
    result = relationship_to_trade_parties(
        relationship,
        internal_trader="JSmith",
        external_trader="JDoe",
    )
    assert result["traders"] == {"internal": "JSmith", "external": "JDoe"}


def test_trade_parties_flat_fields(relationship):
    result = relationship_to_trade_parties(
        relationship,
        internal_trader="JSmith",
        external_trader="JDoe",
    )
    assert result["internal_portfolio"] == "GAS_DESK"
    assert result["external_portfolio"] == "HEDGE_BOOK"
    assert result["internal_trader"] == "JSmith"
    assert result["external_trader"] == "JDoe"


def test_trade_parties_empty_portfolios():
    """Relationship with no portfolios should produce empty strings."""
    rel = TradingRelationship(
        internal_party=LegalEntity(
            symbol="A", name="A", classification=PartyClassification.SWAP_DEALER,
        ),
        external_party=LegalEntity(
            symbol="B", name="B", classification=PartyClassification.FINANCIAL_ENTITY,
        ),
    )
    result = relationship_to_trade_parties(rel)
    assert result["portfolio"] == {"internal": "", "external": ""}
    assert result["internal_portfolio"] == ""
    assert result["external_portfolio"] == ""


def test_trade_parties_default_traders(relationship):
    """Traders default to empty strings if not provided."""
    result = relationship_to_trade_parties(relationship)
    assert result["traders"] == {"internal": "", "external": ""}


def test_trade_parties_has_all_required_pipeline_keys(relationship):
    """Verify all party-related keys expected by the booking pipeline."""
    result = relationship_to_trade_parties(relationship)
    required_keys = [
        "internal_party", "external_party",
        "counterparty", "portfolio", "traders",
        "internal_portfolio", "external_portfolio",
        "internal_trader", "external_trader",
    ]
    for key in required_keys:
        assert key in result, f"Missing required key: {key}"
