"""Tests for the entitlements module — checker, permissions, and DB operations."""

import sqlite3

import pytest
from hgraph_entitlements.checker import PermissionDeniedError, check_permission, get_allowed_actions, require_permission
from hgraph_entitlements.example_code import get_user_role, initialize_db, update_user_role


@pytest.fixture
def ent_conn():
    conn = sqlite3.connect(":memory:")
    initialize_db(conn)
    return conn


@pytest.fixture
def trader_conn(ent_conn):
    update_user_role(ent_conn, "trader1", "Trader")
    update_user_role(ent_conn, "viewer1", "Read_only")
    update_user_role(ent_conn, "super1", "Super User")
    update_user_role(ent_conn, "midoffice1", "Middle Office")
    return ent_conn


# ---------- get_allowed_actions ----------


@pytest.mark.parametrize(
    "role,expected_in,expected_not_in",
    [
        ("Trader", ["execute_trade", "place_order"], []),
        ("Read_only", ["view_data"], ["execute_trade"]),
        ("Super User", ["execute_trade", "view_data", "settle_trade", "view_financials"], ["all_permissions"]),
    ],
)
def test_allowed_actions(role, expected_in, expected_not_in):
    actions = get_allowed_actions(role)
    for action in expected_in:
        assert action in actions
    for action in expected_not_in:
        assert action not in actions


def test_unknown_role_returns_empty():
    assert get_allowed_actions("NonExistentRole") == set()


# ---------- check_permission ----------


@pytest.mark.parametrize(
    "user,action,expected",
    [
        ("trader1", "execute_trade", True),
        ("trader1", "settle_trade", False),
        ("viewer1", "execute_trade", False),
        ("super1", "execute_trade", True),
        ("super1", "settle_trade", True),
        ("super1", "view_financials", True),
        ("nobody", "execute_trade", False),
        ("midoffice1", "settle_trade", True),
    ],
)
def test_check_permission(user, action, expected, trader_conn):
    assert check_permission(user, action, conn=trader_conn) is expected


# ---------- require_permission ----------


def test_require_permission_passes(trader_conn):
    require_permission("trader1", "execute_trade", conn=trader_conn)


def test_require_permission_raises_for_viewer(trader_conn):
    with pytest.raises(PermissionDeniedError) as exc_info:
        require_permission("viewer1", "execute_trade", conn=trader_conn)
    assert "viewer1" in str(exc_info.value)
    assert "execute_trade" in str(exc_info.value)


def test_require_permission_raises_for_unknown_user(trader_conn):
    with pytest.raises(PermissionDeniedError) as exc_info:
        require_permission("nobody", "execute_trade", conn=trader_conn)
    assert "not found" in str(exc_info.value)


def test_require_permission_error_attributes(trader_conn):
    with pytest.raises(PermissionDeniedError) as exc_info:
        require_permission("viewer1", "execute_trade", conn=trader_conn)
    err = exc_info.value
    assert err.user_id == "viewer1"
    assert err.action == "execute_trade"
    assert err.role == "Read_only"


# ---------- DB operations ----------


def test_update_and_get_role(ent_conn):
    update_user_role(ent_conn, "user_a", "Ops")
    assert get_user_role(ent_conn, "user_a") == "Ops"


def test_update_role_overwrites(ent_conn):
    update_user_role(ent_conn, "user_b", "Ops")
    update_user_role(ent_conn, "user_b", "Trader")
    assert get_user_role(ent_conn, "user_b") == "Trader"


def test_invalid_role_raises(ent_conn):
    with pytest.raises(ValueError, match="not a valid role"):
        update_user_role(ent_conn, "user_c", "MadeUpRole")


def test_unknown_user_returns_none(ent_conn):
    assert get_user_role(ent_conn, "nonexistent") is None
