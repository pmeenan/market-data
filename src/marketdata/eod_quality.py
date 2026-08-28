"""One shared definition of publishable EOD OHLC invariants."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

TIINGO_EOD_OHLC_GROUPS = (
    ("open", "high", "low", "close"),
    ("adjOpen", "adjHigh", "adjLow", "adjClose"),
)
CANONICAL_EOD_OHLC_GROUPS = (
    ("open", "high", "low", "close"),
    ("adj_open", "adj_high", "adj_low", "adj_close"),
)


def eod_ohlc_invalid_reason(
    row: Mapping[str, Any],
    groups: Sequence[tuple[str, str, str, str]] = TIINGO_EOD_OHLC_GROUPS,
) -> str | None:
    """Return a stable rejection reason, or ``None`` when both OHLC sets pass."""
    for open_name, high_name, low_name, close_name in groups:
        try:
            open_, high, low, close = (
                float(row[column])
                for column in (open_name, high_name, low_name, close_name)
            )
        except (KeyError, TypeError, ValueError):
            return "missing or invalid EOD OHLC values"
        if not all(math.isfinite(value) for value in (open_, high, low, close)):
            return "non-finite EOD OHLC values"
        if high < max(open_, low, close) or low > min(open_, high, close):
            return (
                "EOD OHLC ordering violation for "
                f"{open_name}/{high_name}/{low_name}/{close_name}"
            )
    return None


def eod_ohlc_invalid_sql(
    groups: Sequence[tuple[str, str, str, str]] = CANONICAL_EOD_OHLC_GROUPS,
) -> str:
    """Generate the DuckDB predicate from the same ordered field groups."""
    predicates: list[str] = []
    for open_, high, low, close in groups:
        non_finite = " OR ".join(
            f"{column} IS NULL OR NOT isfinite({column})"
            for column in (open_, high, low, close)
        )
        predicates.append(
            f"(({non_finite}) OR {high} < greatest({open_}, {low}, {close}) "
            f"OR {low} > least({open_}, {high}, {close}))"
        )
    return " OR ".join(predicates)
