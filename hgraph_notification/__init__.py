"""
hgraph_notification package initialization.

This package provides event-driven notification capabilities for the hgraph
platform. It handles trade confirmation emails and other notifications
using Jinja2 templates and SMTP delivery.

Sub-packages:
- scripts: Core notification logic including email sending and trade parsing.
- templates: Jinja2 email templates for trade confirmations.
"""

from .scripts.notification_email import (
    load_trade_data,
    prepare_client_side,
    event_driven_notification,
)
from .scripts.parse_trade import parse_trade_data

__all__ = [
    "load_trade_data",
    "prepare_client_side",
    "event_driven_notification",
    "parse_trade_data",
]
