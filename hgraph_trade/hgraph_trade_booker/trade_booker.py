"""
trade_booker.py

This module provides functionality to finalise (or "book") a trade by writing the
trade data to a specified output directory. The resulting file can then be used
by downstream systems for further processing or confirmation.

Includes optional dead-letter quarantine for trades that fail to book.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

__all__ = (
    "DEFAULT_QUARANTINE_DIR",
    "book_trade",
    "book_trades_batch",
)

logger = logging.getLogger(__name__)

# Default quarantine directory for failed trades
DEFAULT_QUARANTINE_DIR = "quarantine"


def book_trade(
    trade_data: Dict[str, Any],
    output_file: str,
    output_dir: str,
) -> None:
    """
    Book the trade by saving it to a specified output directory in JSON format.

    :param trade_data: The fully mapped and validated trade data dictionary.
    :param output_file: The name of the output file (e.g., "booked_trade.json").
    :param output_dir: The directory where the file will be saved. Created if absent.
    :raises IOError: If an error occurs while writing to the output path.
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_file)

    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(trade_data, fh, indent=4)
        logger.info("Trade booked successfully: %s", output_path)
    except IOError as exc:
        logger.error("Failed to book trade to %s: %s", output_path, exc)
        raise IOError(f"Error writing to {output_path}: {exc}") from exc


def book_trades_batch(
    messages: List[Dict[str, Any]],
    output_dir: str,
    quarantine_dir: Optional[str] = None,
) -> Dict[str, List]:
    """
    Book a batch of trade messages, quarantining any that fail.

    Each message is written to its own file named after the trade_id (or index).
    Trades that fail to write are saved to the quarantine directory so they can
    be inspected and retried later.

    :param messages: List of fully assembled trade message dictionaries.
    :param output_dir: Directory for successfully booked trades.
    :param quarantine_dir: Directory for failed trades. Defaults to ``output_dir/quarantine``.
    :return: A dict with ``"booked"`` and ``"quarantined"`` lists of file paths.
    """
    if quarantine_dir is None:
        quarantine_dir = os.path.join(output_dir, DEFAULT_QUARANTINE_DIR)

    booked: List[str] = []
    quarantined: List[str] = []

    for idx, message in enumerate(messages):
        # Try to extract a meaningful trade ID for the filename
        trade_id = message.get("tradeHeader", {}).get("partyTradeIdentifier", {}).get("tradeId", f"trade_{idx}")
        filename = f"{trade_id}.json"

        try:
            book_trade(message, filename, output_dir)
            booked.append(os.path.join(output_dir, filename))
        except (IOError, OSError) as exc:
            logger.error("Trade %s failed to book, quarantining: %s", trade_id, exc)
            try:
                os.makedirs(quarantine_dir, exist_ok=True)
                quarantine_path = os.path.join(quarantine_dir, filename)
                with open(quarantine_path, "w", encoding="utf-8") as fh:
                    json.dump(
                        {"original_message": message, "error": str(exc)},
                        fh,
                        indent=4,
                    )
                quarantined.append(quarantine_path)
                logger.info("Quarantined trade %s to %s", trade_id, quarantine_path)
            except (IOError, OSError) as q_exc:
                logger.critical("Failed to quarantine trade %s: %s", trade_id, q_exc)

    logger.info(
        "Batch booking complete: %d booked, %d quarantined",
        len(booked),
        len(quarantined),
    )
    return {"booked": booked, "quarantined": quarantined}
