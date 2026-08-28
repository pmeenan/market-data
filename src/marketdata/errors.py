"""Cross-layer operational exceptions used by ingestion and scheduling."""

from __future__ import annotations

from typing import Any


class BudgetExhausted(RuntimeError):
    """A clean pre-request quota stop with any durably published partial work."""

    def __init__(self, reason: str):
        self.reason = reason
        self.partial_ingest: Any | None = None
        super().__init__(f"Tiingo budget stop: {reason}")
