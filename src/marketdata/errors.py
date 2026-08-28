"""Cross-layer operational exceptions used by ingestion and scheduling."""

from __future__ import annotations

from pathlib import Path
from typing import Any


class BudgetExhausted(RuntimeError):
    """A clean pre-request quota stop with any durably published partial work."""

    def __init__(self, reason: str):
        self.reason = reason
        self.partial_ingest: Any | None = None
        super().__init__(f"Tiingo budget stop: {reason}")


class DataDirectoryBusyError(RuntimeError):
    """Raised when another coordinator owns the warehouse mutation lock."""

    def __init__(self, lock_path: Path, holder: dict[str, object] | None = None):
        detail = ""
        if holder:
            fields = [
                f"pid={holder['pid']}" if isinstance(holder.get("pid"), int) else "",
                (
                    f"operation={holder['operation']}"
                    if isinstance(holder.get("operation"), str)
                    else ""
                ),
                (
                    f"acquired_at={holder['acquired_at']}"
                    if isinstance(holder.get("acquired_at"), str)
                    else ""
                ),
            ]
            rendered = ", ".join(field for field in fields if field)
            if rendered:
                detail = f"; holder {rendered}"
        super().__init__(
            f"data directory is busy: another mutation coordinator holds "
            f"{lock_path}{detail}"
        )
        self.lock_path = lock_path
        self.holder = holder
