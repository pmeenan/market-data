"""Tiingo request-budget calendar helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

# Tiingo documents daily and monthly resets in EST rather than a
# daylight-saving-aware Eastern timezone.
TIINGO_BILLING_TIMEZONE = timezone(timedelta(hours=-5), name="EST")


def tiingo_billing_date(now: datetime) -> date:
    """Return the Tiingo billing-calendar date containing ``now``."""
    if now.tzinfo is None:
        raise ValueError("budget timestamps must be timezone-aware")
    return now.astimezone(TIINGO_BILLING_TIMEZONE).date()


def tiingo_billing_month_start(now: datetime) -> datetime:
    """Return the UTC instant at which Tiingo's current month began."""
    if now.tzinfo is None:
        raise ValueError("budget timestamps must be timezone-aware")
    local = now.astimezone(TIINGO_BILLING_TIMEZONE)
    return local.replace(day=1, hour=0, minute=0, second=0, microsecond=0).astimezone(
        UTC
    )
