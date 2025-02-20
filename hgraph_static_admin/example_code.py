#!/usr/bin/env python
"""
hgraph Static Data Admin Module

This module manages static data required for processing trades.
It fetches data from an external API, processes the data, and stores it
in a local SQLite database. The data flow follows a forward propagation graph
(FPG) style.
"""

import sqlite3
import re
import argparse
from typing import List, Dict, Any
import requests


def init_db(db_path: str) -> None:
    """
    Initialize the SQLite database with the required static tables.

    Args:
        db_path (str): Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS counterparties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legal_name TEXT NOT NULL,
            short_name TEXT,
            identifier1 TEXT,
            identifier2 TEXT,
            identifier3 TEXT,
            identifier4 TEXT,
            cleared_otc TEXT,
            dropcopy_enabled TEXT,
            notification_preferences TEXT,
            notification_contacts TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def fetch_static_data(api_url: str) -> List[Dict[str, Any]]:
    """
    Fetch static data from an external API.

    Args:
        api_url (str): The URL of the API endpoint.

    Returns:
        List[Dict[str, Any]]: List of raw static data dictionaries.
    """
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()


def process_counterparty_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process and validate counterparty static data.

    This function cleans the data, applying regex where needed (e.g., to remove
    any invalid characters from identifiers), and maps external field names
    to the internal schema.

    Args:
        raw_data (List[Dict[str, Any]]): Raw data fetched from the API.

    Returns:
        List[Dict[str, Any]]: Processed counterparty data.
    """
    processed = []
    for entry in raw_data:
        legal_name = entry.get("Counterparty Legal Name", "").strip()
        short_name = entry.get("Counterparty Short Name", "").strip()
        identifier1 = re.sub(r"[^A-Za-z0-9]", "", entry.get("Counterparty Identifier 1", ""))
        identifier2 = re.sub(r"[^A-Za-z0-9]", "", entry.get("Counterparty Identifier 2", ""))
        identifier3 = re.sub(r"[^A-Za-z0-9]", "", entry.get("Counterparty Identifier 3", ""))
        identifier4 = re.sub(r"[^A-Za-z0-9]", "", entry.get("Counterparty Identifier 4", ""))
        cleared_otc = entry.get("Cleared/OTC", "OTC")
        dropcopy_enabled = entry.get("Dropcopy Enabled", "N")
        notification_preferences = entry.get("Notification Preferences", "")
        notification_contacts = entry.get("Notification Contacts", "")
        processed.append({
            "legal_name": legal_name,
            "short_name": short_name,
            "identifier1": identifier1,
            "identifier2": identifier2,
            "identifier3": identifier3,
            "identifier4": identifier4,
            "cleared_otc": cleared_otc,
            "dropcopy_enabled": dropcopy_enabled,
            "notification_preferences": notification_preferences,
            "notification_contacts": notification_contacts
        })
    return processed


def store_counterparty_data(db_path: str, data: List[Dict[str, Any]]) -> None:
    """
    Store processed counterparty data into the SQLite database.

    Args:
        db_path (str): Path to the SQLite database file.
        data (List[Dict[str, Any]]): Processed counterparty data.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for entry in data:
        cursor.execute(
            """
            INSERT INTO counterparties (
                legal_name, short_name, identifier1, identifier2,
                identifier3, identifier4, cleared_otc, dropcopy_enabled,
                notification_preferences, notification_contacts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry["legal_name"],
                entry["short_name"],
                entry["identifier1"],
                entry["identifier2"],
                entry["identifier3"],
                entry["identifier4"],
                entry["cleared_otc"],
                entry["dropcopy_enabled"],
                entry["notification_preferences"],
                entry["notification_contacts"]
            )
        )
    conn.commit()
    conn.close()


def run_pipeline(api_url: str, db_path: str) -> None:
    """
    Run the forward propagation graph pipeline to fetch, process, and store static data.

    Args:
        api_url (str): API URL to fetch static data.
        db_path (str): Path to the SQLite database.
    """
    # Stage 1: Fetch
    raw_data = fetch_static_data(api_url)
    # Stage 2: Process/Transform
    processed_data = process_counterparty_data(raw_data)
    # Stage 3: Store
    store_counterparty_data(db_path, processed_data)
    print("Static data pipeline completed successfully.")


def parse_cli_args() -> argparse.Namespace:
    """
    Parse command line arguments for the static data admin module.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="hgraph Static Data Admin Module")
    parser.add_argument(
        "--init-db", action="store_true", help="Initialize the database schema"
    )
    parser.add_argument(
        "--fetch", action="store_true", help="Fetch and update static data"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="static_data.db",
        help="Path to the SQLite database file",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default="http://example.com/api/counterparties",
        help="API URL for fetching static data",
    )
    return parser.parse_args()


def main() -> None:
    """
    Main function to run the static data admin CLI.
    """
    args = parse_cli_args()
    if args.init_db:
        init_db(args.db_path)
        print(f"Database initialized at {args.db_path}")
    if args.fetch:
        run_pipeline(args.api_url, args.db_path)


if __name__ == "__main__":
    main()