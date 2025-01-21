#!/usr/bin/env python3
"""
secure_config.py

This script demonstrates how to handle sensitive configuration data (such as SMTP credentials)
securely using environment variables and, optionally, the python-dotenv package for local development.
"""

import os
from typing import Dict

from dotenv import load_dotenv
import os

load_dotenv()  # This will look for .env in the project root

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.example.com")
SMTP_PASS = os.getenv("SMTP_PASS", None)
...

# Optional: If python-dotenv is installed, we can load a local .env file.
# If python-dotenv isn't installed, this import will fail silently.
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


def load_secure_config() -> Dict[str, str]:
    """
    Load environment variables required for secure configuration.

    If python-dotenv is available, load values from a .env file (useful in local/dev).
    For production, it is best to set environment variables at the OS level
    or through a secret manager rather than relying on local files.

    Returns:
        Dict[str, str]: A dictionary containing keys such as SMTP_HOST, SMTP_USER, etc.
    """
    if DOTENV_AVAILABLE:
        load_dotenv()  # Load from .env if present

    config_data = {
        "SMTP_HOST": os.getenv("SMTP_HOST", "smtp.example.com"),
        "SMTP_PORT": os.getenv("SMTP_PORT", "587"),
        "SMTP_USER": os.getenv("SMTP_USER", "user@example.com"),
        "SMTP_PASS": os.getenv("SMTP_PASS", "password"),
        "SENDER_EMAIL": os.getenv("SENDER_EMAIL", "sender@example.com"),
        "RECIPIENT_EMAIL": os.getenv("RECIPIENT_EMAIL", "test_developer@example.com"),
    }

    return config_data


def main() -> None:
    """
    CLI entry point to demonstrate secure configuration loading.
    Run 'python secure_config.py' to print out config values for testing.
    """
    cfg = load_secure_config()
    print("Loaded configuration:")
    for key, value in cfg.items():
        print(f"  {key} = {value}")


if __name__ == "__main__":
    main()