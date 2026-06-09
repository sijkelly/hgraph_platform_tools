"""Health check endpoint."""

from fastapi import APIRouter

__all__ = ("router",)

router = APIRouter(prefix="/api/v1", tags=["health"])


@router.get("/health")
def health_check() -> dict:
    """Return service health status."""
    return {"status": "ok"}
