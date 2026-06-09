"""
API-specific configuration.

Extends the platform's ``secure_config`` with settings for the FastAPI
service (port, CORS origins, database directory).
"""

import os

from secure_config import config as platform_config

__all__ = ("api_config",)


def _load_api_config() -> dict[str, str]:
    """Build the API configuration dict."""
    return {
        **platform_config,
        "API_HOST": os.getenv("API_HOST", "0.0.0.0"),
        "API_PORT": os.getenv("API_PORT", "8000"),
        "API_CORS_ORIGINS": os.getenv("API_CORS_ORIGINS", "http://localhost:5173,http://localhost:3000"),
        "API_DB_DIR": os.getenv("API_DB_DIR", "."),
        "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY", ""),
    }


api_config = _load_api_config()
