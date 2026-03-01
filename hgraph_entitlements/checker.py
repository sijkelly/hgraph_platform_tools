"""
checker.py

Permission checking interface for the trade pipeline.

Provides clean functions to verify whether a user is authorised to perform
a specific action, based on the entitlements database and the static role
definitions.

Usage:
    from hgraph_entitlements.checker import check_permission, require_permission

    # Returns True/False
    if check_permission(user_id="trader1", action="execute_trade"):
        ...

    # Raises PermissionError if denied
    require_permission(user_id="trader1", action="execute_trade")
"""

import logging
import sqlite3
from typing import Optional, Set

from hgraph_entitlements.example_code import (
    STATIC_ROLES,
    get_db_connection,
    get_user_role,
    initialize_db,
)

logger = logging.getLogger(__name__)


class PermissionDeniedError(PermissionError):
    """Raised when a user does not have the required permission."""

    def __init__(self, user_id: str, action: str, role: Optional[str] = None):
        self.user_id = user_id
        self.action = action
        self.role = role
        if role is None:
            msg = f"User '{user_id}' not found in entitlements database"
        else:
            msg = (
                f"User '{user_id}' (role='{role}') does not have "
                f"permission for action '{action}'"
            )
        super().__init__(msg)


def get_allowed_actions(role: str) -> Set[str]:
    """
    Return the set of actions allowed for a given role.

    The ``Super User`` role is treated as having all possible actions.

    :param role: Role name as defined in STATIC_ROLES.
    :return: Set of allowed action strings.
    """
    actions = STATIC_ROLES.get(role, set())
    if "all_permissions" in actions:
        # Collect every action from every role
        all_actions: Set[str] = set()
        for role_actions in STATIC_ROLES.values():
            all_actions.update(role_actions)
        all_actions.discard("all_permissions")
        return all_actions
    return actions


def check_permission(
    user_id: str,
    action: str,
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    """
    Check whether a user is allowed to perform an action.

    :param user_id: The user identifier.
    :param action: The action to check (e.g. "execute_trade").
    :param conn: Optional database connection. If None, a new connection is created
                 (and closed after the check).
    :return: True if the user has permission; False otherwise.
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        initialize_db(conn)
        close_conn = True

    try:
        role = get_user_role(conn, user_id)
        if role is None:
            logger.warning("Permission check: user '%s' not found", user_id)
            return False

        allowed = get_allowed_actions(role)
        has_permission = action in allowed

        if not has_permission:
            logger.info(
                "Permission denied: user '%s' (role='%s') attempted '%s'",
                user_id,
                role,
                action,
            )

        return has_permission
    finally:
        if close_conn:
            conn.close()


def require_permission(
    user_id: str,
    action: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Assert that a user has the required permission, or raise PermissionDeniedError.

    :param user_id: The user identifier.
    :param action: The required action (e.g. "execute_trade").
    :param conn: Optional database connection.
    :raises PermissionDeniedError: If the user does not have the required permission.
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        initialize_db(conn)
        close_conn = True

    try:
        role = get_user_role(conn, user_id)
        if role is None:
            raise PermissionDeniedError(user_id, action, role=None)

        allowed = get_allowed_actions(role)
        if action not in allowed:
            raise PermissionDeniedError(user_id, action, role=role)
    finally:
        if close_conn:
            conn.close()
