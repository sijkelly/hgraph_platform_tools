"""
NLP order parser using Claude API.

Parses free-text order descriptions (natural language or pasted chat messages)
into structured order fields for review before submission.
"""

import json
import logging
from typing import Any

from hgraph_api.config import api_config

__all__ = ("parse_order_text",)

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are an order parsing assistant for a US power and gas commodity trading desk.
Your job is to extract structured order fields from free text — either natural language instructions
or pasted client chat/email messages.

Extract the following fields when present:
- instrument: The commodity/product (e.g., "Henry Hub Natural Gas", "PJM West 7x24 Power")
- side: "Buy" or "Sell"
- quantity: The numeric quantity (e.g., "50000", "10000")
- unit: The unit of measurement (e.g., "MMBtu", "MWh", "lots")
- price: The price per unit (e.g., "3.45", "market")
- counterparty: The counterparty name or symbol (e.g., "ACME", "ACME Energy Corp")
- delivery_period: The delivery month/period (e.g., "January 2026", "Q1 2026", "Cal 2026")
- order_type: "Limit" or "Market"
- portfolio: Portfolio name if mentioned
- book: Book name if mentioned
- notes: Any additional context that doesn't fit the above fields

For each field you extract, provide a confidence score from 0.0 to 1.0.
If a field is ambiguous, set confidence lower. If not mentioned, omit the field.
If the price is "market" or not specified, set order_type to "Market".

Respond ONLY with valid JSON in this exact format (omit fields not found):
{
  "instrument": {"value": "...", "confidence": 0.95},
  "side": {"value": "Buy", "confidence": 1.0},
  "quantity": {"value": "50000", "confidence": 0.9},
  "unit": {"value": "MMBtu", "confidence": 0.9},
  "price": {"value": "3.45", "confidence": 0.95},
  "counterparty": {"value": "ACME", "confidence": 0.8},
  "delivery_period": {"value": "January 2026", "confidence": 0.9},
  "order_type": {"value": "Limit", "confidence": 1.0},
  "notes": "any extra context"
}"""


def _get_client():
    """Lazily create an Anthropic client."""
    try:
        import anthropic
    except ImportError:
        raise RuntimeError(
            "The 'anthropic' package is required for NLP order parsing. "
            "Install it with: uv pip install anthropic"
        )

    api_key = api_config.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY environment variable must be set for NLP order parsing"
        )
    return anthropic.Anthropic(api_key=api_key)


def parse_order_text(text: str, context: str | None = None) -> dict[str, Any]:
    """Parse free text into structured order fields using Claude.

    :param text: The raw order text (natural language or pasted chat).
    :param context: Optional additional context (e.g. current counterparty selected).
    :returns: Dict with parsed fields, each containing 'value' and 'confidence'.
    :raises RuntimeError: If the Anthropic client cannot be created.
    """
    client = _get_client()

    user_message = text
    if context:
        user_message = f"Context: {context}\n\nOrder text: {text}"

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )

    response_text = response.content[0].text.strip()

    # Strip markdown code fences if present
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        response_text = "\n".join(lines)

    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError:
        logger.error("Failed to parse Claude response as JSON: %s", response_text[:200])
        parsed = {"notes": response_text}

    parsed["raw_text"] = text
    return parsed
