#!/usr/bin/env python3
"""
hgraph_entitlements module: Controls user permissions and handles permission events
using a forward propagated graph approach.
"""

import argparse
import logging
import re
import sqlite3
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from secure_config import config

__all__ = (
    "STATIC_ROLES",
    "EVENT_GRAPH",
    "register_event",
    "dispatch_event",
    "get_db_connection",
    "initialize_db",
    "get_user_role",
    "update_user_role",
    "permission_event_handler",
    "setup_cli",
    "main",
)

logger = logging.getLogger(__name__)

# Static mapping of roles to allowed actions.
# This mapping is hard-coded and changes require a release.
STATIC_ROLES: Dict[str, Set[str]] = {
    "Read_only": {"view_data"},
    "Trader": {"execute_trade", "place_order"},
    "Trading Manager": {"execute_trade", "place_order", "approve_trade"},
    "Ops": {"view_logs"},
    "Ops Manager": {"view_logs", "manage_ops"},
    "Middle Office": {"settle_trade"},
    "Middle Office Manager": {"settle_trade", "approve_settlement"},
    "FinCon": {"view_financials"},
    "FinCon Manager": {"view_financials", "approve_financials"},
    "Sales": {"access_sales_data"},
    "Sales Manager": {"access_sales_data", "approve_sales"},
    "Production Support": {"view_system_status"},
    "Production Support Manager": {"view_system_status", "manage_support"},
    "Dev": {"view_dev_logs"},
    "Dev Manager": {"view_dev_logs", "manage_dev"},
    "Super User": {"all_permissions"},
}

# Forward propagation graph: mapping event names to subscribers (functions)
EVENT_GRAPH: Dict[str, List[Callable[[Any], None]]] = {}


def register_event(event_name: str, handler: Callable[[Any], None]) -> None:
    """Register a handler function for a specific event.

    :param event_name: The name of the event.
    :param handler: Function to call when the event is dispatched.
    """
    EVENT_GRAPH.setdefault(event_name, []).append(handler)


def dispatch_event(event_name: str, payload: Any) -> None:
    """Dispatch an event and propagate it to all registered handlers using a FIFO queue.

    :param event_name: The name of the event.
    :param payload: Data associated with the event.
    """
    queue: deque[Tuple[str, Any]] = deque()
    queue.append((event_name, payload))
    while queue:
        current_event, data = queue.popleft()
        handlers = EVENT_GRAPH.get(current_event, [])
        for handler in handlers:
            handler(data)
            # For simplicity, assume handlers might dispatch further events.
            # In a real system, each handler would call dispatch_event as needed.


def get_db_connection(db_path: str = None) -> sqlite3.Connection:
    """Create and return a connection to the SQLite database.

    :param db_path: Path to the SQLite database file.
    :returns: Database connection object.
    """
    if db_path is None:
        db_path = config["ENTITLEMENTS_DB_PATH"]
    return sqlite3.connect(db_path)


def initialize_db(conn: sqlite3.Connection) -> None:
    """Initialise the entitlements database table if it does not exist.

    :param conn: The database connection.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_entitlements (
            user_id TEXT PRIMARY KEY,
            role TEXT NOT NULL
        )
        """)
    conn.commit()


def get_user_role(conn: sqlite3.Connection, user_id: str) -> Optional[str]:
    """Retrieve the role of a user from the entitlements database.

    :param conn: The database connection.
    :param user_id: The unique identifier of the user.
    :returns: The user's role if found; otherwise *None*.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT role FROM user_entitlements WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def update_user_role(conn: sqlite3.Connection, user_id: str, role: str) -> None:
    """Update or insert a user's role in the entitlements database and dispatch an event.

    :param conn: The database connection.
    :param user_id: The unique identifier of the user.
    :param role: The new role for the user.
    :raises ValueError: If *role* is not in ``STATIC_ROLES``.
    """
    cursor = conn.cursor()
    # Validate role using a regex to allow only known roles (for safety)
    if role not in STATIC_ROLES:
        raise ValueError(f"Role '{role}' is not a valid role.")
    cursor.execute(
        """
        INSERT INTO user_entitlements (user_id, role)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET role=excluded.role
        """,
        (user_id, role),
    )
    conn.commit()
    # Dispatch an event that the user's role has been updated.
    dispatch_event("user_role_updated", {"user_id": user_id, "role": role})


def permission_event_handler(payload: Dict[str, Any]) -> None:
    """Example event handler that reacts to user role updates.

    :param payload: Data containing ``user_id`` and new role.
    """
    user_id = payload.get("user_id")
    role = payload.get("role")
    # In a real system, additional propagation (like cache refresh or audit logging) would occur.
    logger.info("User '%s' role updated to '%s'. Allowed actions: %s", user_id, role, STATIC_ROLES.get(role))


def setup_cli() -> argparse.Namespace:
    """Set up the command-line interface for managing user entitlements.

    :returns: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Manage hgraph entitlements")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Command to update a user's role
    update_parser = subparsers.add_parser("update", help="Update a user's role")
    update_parser.add_argument("user_id", type=str, help="User identifier")
    update_parser.add_argument("role", type=str, help="New role for the user")

    # Command to query a user's role
    query_parser = subparsers.add_parser("query", help="Query a user's role")
    query_parser.add_argument("user_id", type=str, help="User identifier")

    return parser.parse_args()


def main() -> None:
    """
    Main function to run the CLI for hgraph entitlements.
    """
    # Register the event handler for user role updates.
    register_event("user_role_updated", permission_event_handler)

    args = setup_cli()
    conn = get_db_connection()
    initialize_db(conn)

    if args.command == "update":
        update_user_role(conn, args.user_id, args.role)
        logger.info("Updated user '%s' to role '%s'.", args.user_id, args.role)
    elif args.command == "query":
        role = get_user_role(conn, args.user_id)
        if role:
            logger.info("User '%s' has role '%s' with allowed actions: %s.", args.user_id, role, STATIC_ROLES.get(role))
        else:
            logger.warning("User '%s' not found.", args.user_id)
    conn.close()


if __name__ == "__main__":
    main()
