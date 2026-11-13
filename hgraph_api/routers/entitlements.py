"""Entitlements REST endpoints."""

from fastapi import APIRouter, HTTPException, Request

from hgraph_entitlements import (
    STATIC_ROLES,
    get_db_connection,
    get_user_role,
    initialize_db,
)
from hgraph_entitlements.checker import check_permission, get_allowed_actions

__all__ = ("router", "me_router")

router = APIRouter(prefix="/api/v1/entitlements", tags=["entitlements"])
me_router = APIRouter(prefix="/api/v1/me", tags=["entitlements"])


@me_router.get("")
def get_current_user(request: Request) -> dict:
    """Return who the API takes the caller to be, with their role and allowed actions."""
    user_id = getattr(request.state, "user_id", "")
    if not user_id:
        raise HTTPException(status_code=401, detail="The caller is not identified")
    conn = get_db_connection()
    initialize_db(conn)
    try:
        role = get_user_role(conn, user_id)
    finally:
        conn.close()
    return {
        "user_id": user_id,
        "role": role,
        "allowed_actions": sorted(get_allowed_actions(role)) if role else [],
        "auth_mode": getattr(request.app.state, "auth_mode", "header"),
    }


@router.get("/{user_id}")
def get_user_entitlements(user_id: str) -> dict:
    """Return a user's role and allowed actions."""
    conn = get_db_connection()
    initialize_db(conn)
    try:
        role = get_user_role(conn, user_id)
    finally:
        conn.close()

    if role is None:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")

    actions = sorted(get_allowed_actions(role))
    return {"user_id": user_id, "role": role, "allowed_actions": actions}


@router.get("/{user_id}/check/{action}")
def check_user_permission(user_id: str, action: str) -> dict:
    """Check if a user has permission for a specific action."""
    conn = get_db_connection()
    initialize_db(conn)
    try:
        allowed = check_permission(user_id, action, conn=conn)
    finally:
        conn.close()

    return {"user_id": user_id, "action": action, "allowed": allowed}


@router.get("/")
def list_roles() -> dict:
    """Return all available roles and their permissions."""
    return {
        "roles": {role: sorted(actions) for role, actions in STATIC_ROLES.items()},
    }
