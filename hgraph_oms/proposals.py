"""
Order proposals: orders drafted by an AI agent (or anyone else) for a human to confirm.

An agent never submits an order itself.  It records a proposal, which changes
nothing in the order book; a person then confirms it, which submits the order
under that person's name, or dismisses it.  The proposal keeps who drafted it,
the text it was drafted from and the agent's reasoning, so every order that
began with an agent can be traced back to what the agent saw.
"""

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

__all__ = ("PROPOSED", "CONFIRMED", "DISMISSED", "OrderProposal", "ProposalStore")

PROPOSED = "Proposed"
CONFIRMED = "Confirmed"
DISMISSED = "Dismissed"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS order_proposals (
    proposal_id  TEXT PRIMARY KEY,
    status       TEXT NOT NULL,
    fields       TEXT NOT NULL,
    proposed_by  TEXT NOT NULL,
    source_text  TEXT NOT NULL DEFAULT '',
    rationale    TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    decided_by   TEXT NOT NULL DEFAULT '',
    decided_at   TEXT NOT NULL DEFAULT '',
    order_id     TEXT NOT NULL DEFAULT '',
    reason       TEXT NOT NULL DEFAULT ''
)
"""
_COLUMNS = (
    "proposal_id, status, fields, proposed_by, source_text, rationale, created_at, "
    "decided_by, decided_at, order_id, reason"
)


@dataclass(frozen=True, kw_only=True)
class OrderProposal:
    """A drafted order awaiting a human decision.

    :param fields: The order terms as the API's order fields (instrument, side, quantity, ...).
    :param proposed_by: The agent or user that drafted it.
    :param source_text: The message the order was drafted from, if any.
    :param rationale: The drafter's explanation of how it read that message.
    :param decided_by: The person who confirmed or dismissed it.
    :param order_id: The order created on confirmation.
    :param reason: Why it was dismissed.
    """

    proposal_id: str
    status: str
    fields: dict[str, Any]
    proposed_by: str
    source_text: str = ""
    rationale: str = ""
    created_at: str = ""
    decided_by: str = ""
    decided_at: str = ""
    order_id: str = ""
    reason: str = ""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProposalStore:
    """SQLite store of order proposals.

    :param db_path: SQLite file; the table is created if it does not exist.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(_CREATE_TABLE_SQL)
            conn.commit()
        finally:
            conn.close()

    def create(
        self, fields: dict[str, Any], *, proposed_by: str, source_text: str = "", rationale: str = ""
    ) -> OrderProposal:
        """Record a new proposal. Nothing is submitted to the order book."""
        proposal = OrderProposal(
            proposal_id=f"PRP-{uuid.uuid4().hex[:8].upper()}",
            status=PROPOSED,
            fields=dict(fields),
            proposed_by=proposed_by,
            source_text=source_text,
            rationale=rationale,
            created_at=_now(),
        )
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                f"INSERT INTO order_proposals ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, '', '', '', '')",
                (
                    proposal.proposal_id,
                    proposal.status,
                    json.dumps(proposal.fields),
                    proposal.proposed_by,
                    proposal.source_text,
                    proposal.rationale,
                    proposal.created_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return proposal

    def get(self, proposal_id: str) -> OrderProposal | None:
        rows = self._query("WHERE proposal_id = ?", (proposal_id,))
        return rows[0] if rows else None

    def proposals(self, *, status: str | None = None) -> list[OrderProposal]:
        """Return proposals, newest first, optionally only those with ``status``."""
        if status is None:
            return self._query("", ())
        return self._query("WHERE status = ?", (status,))

    def decide(
        self, proposal_id: str, status: str, *, decided_by: str, order_id: str = "", reason: str = ""
    ) -> OrderProposal | None:
        """Confirm or dismiss a proposal that is still open.

        The change is atomic, so of two people deciding at once only one succeeds.

        :returns: The decided proposal, or ``None`` if it does not exist or was already decided.
        """
        if status not in (CONFIRMED, DISMISSED):
            raise ValueError(f"A proposal can only be decided as {CONFIRMED} or {DISMISSED}")
        return self._transition(
            proposal_id, PROPOSED, status, decided_by=decided_by, decided_at=_now(), order_id=order_id, reason=reason
        )

    def reopen(self, proposal_id: str) -> OrderProposal | None:
        """Put a confirmed proposal back to open, for when its order could not be submitted at all."""
        return self._transition(proposal_id, CONFIRMED, PROPOSED, decided_by="", decided_at="", order_id="", reason="")

    def _transition(self, proposal_id: str, from_status: str, to_status: str, **values: str) -> OrderProposal | None:
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(
                "UPDATE order_proposals SET status = ?, decided_by = ?, decided_at = ?, order_id = ?, reason = ? "
                "WHERE proposal_id = ? AND status = ?",
                (
                    to_status,
                    values["decided_by"],
                    values["decided_at"],
                    values["order_id"],
                    values["reason"],
                    proposal_id,
                    from_status,
                ),
            )
            conn.commit()
            changed = cursor.rowcount == 1
        finally:
            conn.close()
        return self.get(proposal_id) if changed else None

    def _query(self, where: str, params: tuple) -> list[OrderProposal]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                f"SELECT {_COLUMNS} FROM order_proposals {where} ORDER BY rowid DESC", params
            ).fetchall()
        finally:
            conn.close()
        return [OrderProposal(**{**dict(row), "fields": json.loads(row["fields"])}) for row in rows]
