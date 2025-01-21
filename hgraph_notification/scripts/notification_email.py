#!/usr/bin/env python3
"""
notification_email.py

This script is invoked by an event in the hgraph application (e.g., "trade executed").
It loads JSON trade data, inverts the buy/sell perspective, and renders a fixed-float
swap notification email using Jinja2. Finally, it sends the email via SMTP.

Example Usage (CLI):
    python notification_email.py --file /hgraph_platform_tools/hgraph_trade/test_trades/fixed_float_BM_swap_001.txt
"""

import argparse
import json
import os
import re
import smtplib
from typing import Dict, Any

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import jinja2


def load_trade_data(file_path: str) -> Dict[str, Any]:
    """
    Load trade data from a JSON file.

    Args:
        file_path (str): The path to the JSON file containing the trade details.

    Returns:
        Dict[str, Any]: Dictionary representation of the trade data.
    """
    with open(file_path, 'r', encoding='utf-8') as trade_file:
        return json.load(trade_file)


def prepare_client_side(buy_sell: str) -> str:
    """
    Invert the buy/sell perspective for the client.

    Args:
        buy_sell (str): Original perspective from the system (e.g., 'buy' or 'sell').

    Returns:
        str: Inverted perspective ('sell' if original was 'buy', otherwise 'buy').
    """
    side = buy_sell.lower().strip()
    if side == "buy":
        return "sell"
    elif side == "sell":
        return "buy"
    return "unknown"


def event_driven_notification(trade_data: Dict[str, Any]) -> None:
    """
    Main function to handle the notification event.
    It inverts buy/sell, renders the email template, and sends the email.

    Args:
        trade_data (Dict[str, Any]): Dictionary containing trade details.
    """
    # 1. Invert perspective for the client
    trade_data["client_side"] = prepare_client_side(trade_data.get("buy_sell", "unknown"))

    # 2. (Optional) Clean or reformat fields via regex if needed
    #    Example: ensure currency is uppercase
    if "currency" in trade_data and trade_data["currency"]:
        trade_data["currency"] = re.sub(r"[^A-Za-z]", "", trade_data["currency"]).upper()

    # 3. Set up Jinja2 environment (point to your template folder)
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("hgraph_notification/templates"),
        autoescape=jinja2.select_autoescape(["html", "xml"])
    )

    # 4. Load fixed-float swap email template
    template = env.get_template("swap/fixed_float_swap_notification_email.html")

    # 5. Render the template with the trade data
    html_content = template.render(**trade_data)

    # 6. Prepare SMTP credentials (from environment for security)
    smtp_host = os.getenv("SMTP_HOST", "smtp.example.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "user@example.com")
    smtp_pass = os.getenv("SMTP_PASS", "password")
    sender_email = os.getenv("SENDER_EMAIL", "sender@example.com")
    # For testing, you can store a developer email in RECIPIENT_EMAIL env var
    recipient_email = os.getenv("RECIPIENT_EMAIL", "test_developer@example.com")

    # 7. Construct the email
    message = MIMEMultipart("alternative")
    message["Subject"] = f"Fixed-Float Swap Notification - {trade_data.get('trade_id', '')}"
    message["From"] = sender_email
    message["To"] = recipient_email
    message.attach(MIMEText(html_content, "html"))

    # 8. Send the email
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(sender_email, recipient_email, message.as_string())
        print(f"Email sent to: {recipient_email}")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")


def main() -> None:
    """
    Main CLI entry point for manual testing or development.
    """
    parser = argparse.ArgumentParser(description="Send a fixed-float swap notification email.")
    parser.add_argument("--file", required=True, help="Path to the JSON trade file.")
    args = parser.parse_args()

    # Load trade data from file
    trade_info = load_trade_data(args.file)

    # Trigger the event-driven notification
    event_driven_notification(trade_info)


if __name__ == "__main__":
    main()