"""
secure_config.py

Centralised configuration for the hgraph_platform_tools project.
All configurable values are loaded from environment variables with sensible defaults.
For local development, create a .env file in the project root (see .env.example).

Usage:
    from secure_config import config
    smtp_host = config["SMTP_HOST"]
"""

import os
from typing import Dict

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed; rely on OS-level environment variables

__all__ = (
    "load_secure_config",
    "config",
)


def load_secure_config() -> Dict[str, str]:
    """Load all configuration values from environment variables.

    :returns: Configuration dictionary with all platform settings.
    """
    return {
        # --- SMTP / Email ---
        "SMTP_HOST": os.getenv("SMTP_HOST", "smtp.example.com"),
        "SMTP_PORT": os.getenv("SMTP_PORT", "587"),
        "SMTP_USER": os.getenv("SMTP_USER", ""),
        "SMTP_PASS": os.getenv("SMTP_PASS", ""),
        "SENDER_EMAIL": os.getenv("SENDER_EMAIL", ""),
        "RECIPIENT_EMAIL": os.getenv("RECIPIENT_EMAIL", ""),
        # --- Database ---
        "ENTITLEMENTS_DB_PATH": os.getenv("ENTITLEMENTS_DB_PATH", "hgraph_entitlements.db"),
        "STATIC_DATA_DB_PATH": os.getenv("STATIC_DATA_DB_PATH", "static_data.db"),
        # --- API ---
        "STATIC_DATA_API_URL": os.getenv("STATIC_DATA_API_URL", "http://localhost:8080/api/counterparties"),
        # --- Kafka ---
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "KAFKA_CONSUMER_GROUP": os.getenv("KAFKA_CONSUMER_GROUP", "hgraph_platform"),
        # --- Party Kafka Subscriber ---
        "PARTY_KAFKA_ENTITY_TOPIC": os.getenv("PARTY_KAFKA_ENTITY_TOPIC", "party.legal_entity"),
        "PARTY_KAFKA_RELATIONSHIP_TOPIC": os.getenv("PARTY_KAFKA_RELATIONSHIP_TOPIC", "party.trading_relationship"),
        "PARTY_DB_PATH": os.getenv("PARTY_DB_PATH", "party_data.db"),
        # --- Portfolio Kafka Subscriber ---
        "PORTFOLIO_KAFKA_PORTFOLIO_TOPIC": os.getenv("PORTFOLIO_KAFKA_PORTFOLIO_TOPIC", "portfolio.portfolio"),
        "PORTFOLIO_KAFKA_BOOK_TOPIC": os.getenv("PORTFOLIO_KAFKA_BOOK_TOPIC", "portfolio.book"),
        "PORTFOLIO_DB_PATH": os.getenv("PORTFOLIO_DB_PATH", "portfolio_data.db"),
        # --- Trade messaging ---
        "MESSAGE_SENDER_ID": os.getenv("MESSAGE_SENDER_ID", "hgraph_platform"),
        "MESSAGE_TARGET_ID": os.getenv("MESSAGE_TARGET_ID", "booking_system"),
        "MESSAGE_VERSION": os.getenv("MESSAGE_VERSION", "1.0"),
        # --- Notification templates ---
        "NOTIFICATION_TEMPLATE_DIR": os.getenv("NOTIFICATION_TEMPLATE_DIR", "hgraph_notification/templates"),
    }


# Module-level config instance — import this directly
config = load_secure_config()
