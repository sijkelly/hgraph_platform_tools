"""
pipeline_result.py

Structured result objects for the trade processing pipeline.
These provide a consistent way to report success/failure at both the
individual trade level and the overall pipeline level.
"""

import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class TradeStatus(Enum):
    """Status of an individual trade through the pipeline."""
    SUCCESS = "success"
    VALIDATION_FAILED = "validation_failed"
    MAPPING_FAILED = "mapping_failed"
    BOOKING_FAILED = "booking_failed"
    SEND_FAILED = "send_failed"


@dataclass
class TradeResult:
    """
    Result of processing a single trade through the pipeline.

    Attributes:
        trade_id: The trade identifier (if available).
        status: The final status of the trade.
        message: A summary of what happened.
        error: The exception that caused failure, if any.
        data: The processed trade data on success, or the original data on failure.
        stage: The pipeline stage where processing stopped.
        timestamp: When this result was created.
    """
    trade_id: str
    status: TradeStatus
    message: str = ""
    error: Optional[Exception] = None
    data: Optional[Dict[str, Any]] = None
    stage: str = ""
    timestamp: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )

    @property
    def succeeded(self) -> bool:
        """Whether this trade was processed successfully."""
        return self.status == TradeStatus.SUCCESS

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the result to a dictionary (for logging/reporting)."""
        return {
            "trade_id": self.trade_id,
            "status": self.status.value,
            "message": self.message,
            "error": str(self.error) if self.error else None,
            "stage": self.stage,
            "timestamp": self.timestamp,
        }


@dataclass
class PipelineResult:
    """
    Aggregated result for an entire pipeline run across one or more trades.

    Attributes:
        results: Individual TradeResult objects.
        started_at: When the pipeline run started.
        finished_at: When the pipeline run finished.
    """
    results: List[TradeResult] = field(default_factory=list)
    started_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    finished_at: Optional[str] = None

    def add(self, result: TradeResult) -> None:
        """Add a trade result to this pipeline run."""
        self.results.append(result)

    def finalise(self) -> None:
        """Mark the pipeline run as complete."""
        self.finished_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    @property
    def succeeded(self) -> List[TradeResult]:
        """All trades that were processed successfully."""
        return [r for r in self.results if r.succeeded]

    @property
    def failed(self) -> List[TradeResult]:
        """All trades that failed at some stage."""
        return [r for r in self.results if not r.succeeded]

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def success_count(self) -> int:
        return len(self.succeeded)

    @property
    def failure_count(self) -> int:
        return len(self.failed)

    def summary(self) -> str:
        """Human-readable summary of the pipeline run."""
        lines = [
            f"Pipeline run: {self.total} trade(s) processed",
            f"  Succeeded: {self.success_count}",
            f"  Failed:    {self.failure_count}",
            f"  Started:   {self.started_at}",
            f"  Finished:  {self.finished_at or 'in progress'}",
        ]
        if self.failed:
            lines.append("  Failures:")
            for r in self.failed:
                lines.append(f"    - {r.trade_id}: [{r.status.value}] {r.message}")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the full pipeline result for logging/reporting."""
        return {
            "total": self.total,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "results": [r.to_dict() for r in self.results],
        }
