"""
Tests for the entitlements module — checker, permissions, and pipeline integration.
"""

import sqlite3
import pytest

from hgraph_entitlements.example_code import (
    STATIC_ROLES,
    initialize_db,
    update_user_role,
    get_user_role,
)
from hgraph_entitlements.checker import (
    check_permission,
    require_permission,
    get_allowed_actions,
    PermissionDeniedError,
)


@pytest.fixture
def ent_conn():
    """In-memory SQLite database pre-initialised for entitlements."""
    conn = sqlite3.connect(":memory:")
    initialize_db(conn)
    return conn


@pytest.fixture
def trader_conn(ent_conn):
    """Entitlements DB with a Trader and a Read_only user."""
    update_user_role(ent_conn, "trader1", "Trader")
    update_user_role(ent_conn, "viewer1", "Read_only")
    update_user_role(ent_conn, "super1", "Super User")
    update_user_role(ent_conn, "midoffice1", "Middle Office")
    return ent_conn


# ---------- get_allowed_actions ----------

class TestGetAllowedActions:
    def test_trader_actions(self):
        actions = get_allowed_actions("Trader")
        assert "execute_trade" in actions
        assert "place_order" in actions

    def test_read_only_actions(self):
        actions = get_allowed_actions("Read_only")
        assert "view_data" in actions
        assert "execute_trade" not in actions

    def test_super_user_gets_all(self):
        actions = get_allowed_actions("Super User")
        # Should include actions from every role
        assert "execute_trade" in actions
        assert "view_data" in actions
        assert "settle_trade" in actions
        assert "view_financials" in actions
        # "all_permissions" itself should be excluded
        assert "all_permissions" not in actions

    def test_unknown_role_returns_empty(self):
        actions = get_allowed_actions("NonExistentRole")
        assert actions == set()


# ---------- check_permission ----------

class TestCheckPermission:
    def test_trader_can_execute_trade(self, trader_conn):
        assert check_permission("trader1", "execute_trade", conn=trader_conn) is True

    def test_trader_cannot_settle(self, trader_conn):
        assert check_permission("trader1", "settle_trade", conn=trader_conn) is False

    def test_viewer_cannot_execute_trade(self, trader_conn):
        assert check_permission("viewer1", "execute_trade", conn=trader_conn) is False

    def test_super_user_can_do_anything(self, trader_conn):
        assert check_permission("super1", "execute_trade", conn=trader_conn) is True
        assert check_permission("super1", "settle_trade", conn=trader_conn) is True
        assert check_permission("super1", "view_financials", conn=trader_conn) is True

    def test_unknown_user_returns_false(self, trader_conn):
        assert check_permission("nobody", "execute_trade", conn=trader_conn) is False

    def test_middle_office_can_settle(self, trader_conn):
        assert check_permission("midoffice1", "settle_trade", conn=trader_conn) is True


# ---------- require_permission ----------

class TestRequirePermission:
    def test_trader_passes(self, trader_conn):
        # Should not raise
        require_permission("trader1", "execute_trade", conn=trader_conn)

    def test_viewer_raises_for_trade(self, trader_conn):
        with pytest.raises(PermissionDeniedError) as exc_info:
            require_permission("viewer1", "execute_trade", conn=trader_conn)
        assert "viewer1" in str(exc_info.value)
        assert "execute_trade" in str(exc_info.value)

    def test_unknown_user_raises(self, trader_conn):
        with pytest.raises(PermissionDeniedError) as exc_info:
            require_permission("nobody", "execute_trade", conn=trader_conn)
        assert "not found" in str(exc_info.value)

    def test_error_attributes(self, trader_conn):
        with pytest.raises(PermissionDeniedError) as exc_info:
            require_permission("viewer1", "execute_trade", conn=trader_conn)
        err = exc_info.value
        assert err.user_id == "viewer1"
        assert err.action == "execute_trade"
        assert err.role == "Read_only"


# ---------- DB operations ----------

class TestEntitlementsDB:
    def test_update_and_get_role(self, ent_conn):
        update_user_role(ent_conn, "user_a", "Ops")
        assert get_user_role(ent_conn, "user_a") == "Ops"

    def test_update_role_overwrites(self, ent_conn):
        update_user_role(ent_conn, "user_b", "Ops")
        update_user_role(ent_conn, "user_b", "Trader")
        assert get_user_role(ent_conn, "user_b") == "Trader"

    def test_invalid_role_raises(self, ent_conn):
        with pytest.raises(ValueError, match="not a valid role"):
            update_user_role(ent_conn, "user_c", "MadeUpRole")

    def test_unknown_user_returns_none(self, ent_conn):
        assert get_user_role(ent_conn, "nonexistent") is None
