"""Identity-resolution value objects and exact dataset-key validation.

The registry itself lives in SQLite via :mod:`marketdata.store.meta`.  These
types keep resolution outcomes explicit: callers must handle missing evidence
and conflicts instead of receiving a guessed instrument or identifier.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

ACTIVE_ALIAS_END = date.max

DatasetKey = Literal["eod", "intraday_1hour", "intraday_5min"]
ResolutionStatus = Literal["resolved", "zero_matches", "multiple_matches"]
ValidationState = Literal["unvalidated", "validated", "rejected", "conflict"]

DATASET_KEYS: tuple[DatasetKey, ...] = (
    "eod",
    "intraday_1hour",
    "intraday_5min",
)
VALIDATION_STATES: tuple[ValidationState, ...] = (
    "unvalidated",
    "validated",
    "rejected",
    "conflict",
)


def require_dataset_key(dataset_key: str) -> DatasetKey:
    """Return a typed exact dataset key, rejecting endpoint-family aliases."""
    if dataset_key not in DATASET_KEYS:
        allowed = ", ".join(DATASET_KEYS)
        raise ValueError(
            f"invalid dataset_key {dataset_key!r}; expected one of {allowed}"
        )
    return dataset_key  # type: ignore[return-value]


def require_validation_state(validation_state: str) -> ValidationState:
    if validation_state not in VALIDATION_STATES:
        allowed = ", ".join(VALIDATION_STATES)
        raise ValueError(
            f"invalid validation_state {validation_state!r}; expected one of {allowed}"
        )
    return validation_state  # type: ignore[return-value]


@dataclass(frozen=True)
class ResolutionSegment:
    """One date segment with a constant set of active alias evidence."""

    start: date
    end: date
    status: ResolutionStatus
    instrument_ids: tuple[str, ...]
    alias_ids: tuple[int, ...]

    @property
    def instrument_id(self) -> str | None:
        return self.instrument_ids[0] if self.status == "resolved" else None


@dataclass(frozen=True)
class AliasResolutionReport:
    ticker: str
    start: date
    end: date
    segments: tuple[ResolutionSegment, ...]

    @property
    def resolved(self) -> bool:
        return all(segment.status == "resolved" for segment in self.segments)


@dataclass(frozen=True)
class IdentifierResolution:
    """Dataset-specific identifier outcome and its supporting evidence rows.

    ``vendor_identifier_ids`` contains only rows for the requested instrument
    and candidate identifier. ``vendor_identifier_id`` is populated only when
    one of those rows individually covers the entire request; it is ``None``
    when a resolved identifier depends on the union of abutting envelopes.
    """

    instrument_id: str
    dataset_key: DatasetKey
    start: date
    end: date
    status: ResolutionStatus
    vendor_identifier_ids: tuple[int, ...]
    vendor_identifier_id: int | None
    identifier_type: str | None
    identifier_value: str | None
    conflicting_instrument_ids: tuple[str, ...]


@dataclass(frozen=True)
class UniverseResolution:
    year: int
    ticker: str
    status: ResolutionStatus
    instrument_ids: tuple[str, ...]

    @property
    def instrument_id(self) -> str | None:
        return self.instrument_ids[0] if self.status == "resolved" else None
