"""
hgraph_entitlements package initialization.

This package provides role-based access control and permission management
for the hgraph platform. It uses an event-driven architecture with forward
propagation to manage user entitlements and role assignments.

Modules:
- example_code.py: Core entitlements logic including role definitions,
  event dispatching, database operations, and CLI interface.
- checker.py: Permission checking interface for the trade pipeline.
"""

from .example_code import (
    STATIC_ROLES,
    register_event,
    dispatch_event,
    get_db_connection,
    initialize_db,
    get_user_role,
    update_user_role,
)

from .checker import (
    check_permission,
    require_permission,
    get_allowed_actions,
    PermissionDeniedError,
)

__all__ = [
    "STATIC_ROLES",
    "register_event",
    "dispatch_event",
    "get_db_connection",
    "initialize_db",
    "get_user_role",
    "update_user_role",
    "check_permission",
    "require_permission",
    "get_allowed_actions",
    "PermissionDeniedError",
]
