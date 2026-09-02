"""US exchange-session semantics for IEX planning and research.

Canonical intraday bars retain Tiingo's timestamps exactly as received.  This
module supplies the separate XNYS calendar projection used to bound requests
and to select/label regular-session bars for research.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import polars as pl

from marketdata.store.bars import require_intraday_freq

IEX_ROW_CAP = 10_000
_ROWS_PER_WEEKDAY = {"1hour": 6, "5min": 78}
_BAR_MINUTES = {"1hour": 60, "5min": 5}
_BAR_LABEL_SEMANTICS = {
    "1hour": "clock_hour_start",
    "5min": "session_5min_start",
}
_NORMAL_SESSION_MINUTES = 390
_EASTERN = ZoneInfo("America/New_York")
_ONGOING_NOT_BEFORE_UTC = (23, 30)


def weekend_only(start: date, end: date) -> bool:
    """Whether an inclusive interval contains no possible weekday session."""
    return all(
        date.fromordinal(ordinal).weekday() >= 5
        for ordinal in range(start.toordinal(), end.toordinal() + 1)
    )


@dataclass(frozen=True)
class IntradayRequestChunk:
    """One target range plus the next-session request lookahead.

    ``start`` and ``end`` partition the caller's requested dates.  ``fetch_end``
    reaches the first exchange session after ``end`` so Tiingo finalizes the
    last target session; rows after ``end`` are validation-only lookahead and
    must be discarded.  ``max_response_rows`` is a weekday-grid upper bound
    below Tiingo's silent cap.
    """

    start: date
    end: date
    fetch_end: date
    max_response_rows: int


@dataclass(frozen=True)
class OvernightCollectionWindow:
    """Post-close archive window ending before the next decision morning."""

    session_date: date
    opened_at: datetime
    closes_at: datetime


def latest_completed_session(now: datetime) -> date:
    """Return the most recent XNYS session whose regular close has passed."""
    if now.tzinfo is None:
        raise ValueError("completed-session timestamp must be timezone-aware")
    now = now.astimezone(UTC)
    schedule = session_schedule(now.date() - timedelta(days=10), now.date())
    completed = schedule.filter(pl.col("session_close") <= now)
    if completed.is_empty():
        raise ValueError("no completed XNYS session is available")
    return completed.row(-1, named=True)["session_date"]


def overnight_collection_window(
    now: datetime, *, morning_hour: int = 8
) -> OvernightCollectionWindow | None:
    """Return the active post-market window, or ``None`` during daytime.

    The window opens no earlier than 23:30 UTC after the latest completed XNYS
    session.  That preserves the deployed EOD publication buffer in both US
    daylight- and standard-time seasons.  It ends at ``morning_hour`` New York
    time on the next XNYS session date.  Weekend and exchange-holiday nights
    therefore remain one continuous safe window while early post-close and
    regular-session/daytime invocations fail closed.
    """
    if now.tzinfo is None:
        raise ValueError("overnight collection timestamp must be timezone-aware")
    if not 0 <= morning_hour <= 23:
        raise ValueError("morning decision hour must be between 0 and 23")
    now = now.astimezone(UTC)
    try:
        session_date = latest_completed_session(now)
    except ValueError:
        return None
    session_close = session_schedule(session_date, session_date).row(0, named=True)[
        "session_close"
    ]
    not_before_hour, not_before_minute = _ONGOING_NOT_BEFORE_UTC
    publication_buffer_end = datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        not_before_hour,
        not_before_minute,
        tzinfo=UTC,
    )
    opened_at = max(session_close, publication_buffer_end)
    next_session = next_session_after(session_date)
    closes_at = datetime(
        next_session.year,
        next_session.month,
        next_session.day,
        morning_hour,
        tzinfo=_EASTERN,
    ).astimezone(UTC)
    if not opened_at <= now < closes_at:
        return None
    return OvernightCollectionWindow(session_date, opened_at, closes_at)


@lru_cache(maxsize=16)
def _xnys_calendar(start_year: int, end_year: int):
    return xcals.get_calendar(
        "XNYS",
        start=f"{start_year}-01-01",
        end=f"{end_year}-12-31",
    )


def _calendar_for(start: date, end: date):
    # Include adjacent years so pre-first-session January dates and Dec. 31
    # lookaheads both lie within the instantiated calendar bounds.
    start_year = start.year - 1 if start.year > 1 else start.year
    end_year = end.year + 1 if end.year < 9999 else end.year
    return _xnys_calendar(start_year, end_year)


@lru_cache(maxsize=4096)
def next_session_after(day: date) -> date:
    """Return the first XNYS session strictly after ``day``."""
    calendar = _calendar_for(day, day)
    session = calendar.date_to_session(day.isoformat(), direction="next")
    if session.date() <= day:
        session = calendar.next_session(session)
    return session.date()


def session_schedule(start: date, end: date) -> pl.DataFrame:
    """Return XNYS session dates and regular open/close timestamps in UTC."""
    if start > end:
        raise ValueError("calendar start must not be after end")
    schedule = _calendar_for(start, end).schedule.loc[
        start.isoformat() : end.isoformat(), ["open", "close"]
    ]
    schema = {
        "session_date": pl.Date,
        "session_open": pl.Datetime("us", "UTC"),
        "session_close": pl.Datetime("us", "UTC"),
        "is_early_close": pl.Boolean,
    }
    if schedule.empty:
        return pl.DataFrame(schema=schema)
    opens = [value.to_pydatetime() for value in schedule["open"]]
    closes = [value.to_pydatetime() for value in schedule["close"]]
    return pl.DataFrame(
        {
            "session_date": [value.date() for value in schedule.index],
            "session_open": opens,
            "session_close": closes,
            "is_early_close": [
                int((close - open_).total_seconds() // 60) < _NORMAL_SESSION_MINUTES
                for open_, close in zip(opens, closes, strict=True)
            ],
        },
        schema=schema,
    )


def label_intraday_sessions(frame: pl.DataFrame, *, freq: str) -> pl.DataFrame:
    """Filter to valid XNYS regular-session labels and add calendar fields.

    Five-minute rows are start-labelled relative to the 09:30 session open.
    Tiingo's direct hourly rows are fixed clock-hour bins and therefore begin
    at 10:00 rather than at the open.  A full bin must end no later than the
    scheduled close, including on half-days.
    """
    require_intraday_freq(freq)
    if "ts" not in frame.columns:
        raise ValueError("intraday frame must contain ts")
    additions = {
        "session_date": pl.Date,
        "session_open": pl.Datetime("us", "UTC"),
        "session_close": pl.Datetime("us", "UTC"),
        "is_early_close": pl.Boolean,
        "minutes_from_open": pl.Int32,
        "bar_label_semantics": pl.Utf8,
    }
    conflicts = set(additions) & set(frame.columns)
    if conflicts:
        raise ValueError(
            f"intraday frame already contains calendar fields: {sorted(conflicts)}"
        )
    if frame.is_empty():
        return pl.DataFrame(schema=frame.schema | additions)

    ts_dtype = frame.schema["ts"]
    if not isinstance(ts_dtype, pl.Datetime) or ts_dtype.time_zone != "UTC":
        raise ValueError("intraday ts must be a UTC datetime")
    first = frame["ts"].min()
    last = frame["ts"].max()
    assert isinstance(first, datetime) and isinstance(last, datetime)
    schedule = session_schedule(first.date(), last.date())
    bar_minutes = _BAR_MINUTES[freq]
    labelled = (
        frame.with_columns(pl.col("ts").dt.date().alias("session_date"))
        .join(
            schedule,
            on="session_date",
            how="inner",
            maintain_order="left",
        )
        .with_columns(
            (
                (pl.col("ts") - pl.col("session_open"))
                .dt.total_minutes()
                .cast(pl.Int32)
            ).alias("minutes_from_open")
        )
    )
    valid = (pl.col("minutes_from_open") >= 0) & (
        pl.col("ts") + pl.duration(minutes=bar_minutes) <= pl.col("session_close")
    )
    if freq == "1hour":
        # Direct hourly labels are whole Eastern clock hours (10:00, 11:00,
        # ...), not 60-minute offsets from the 09:30 open.
        valid &= pl.col("ts").dt.convert_time_zone("America/New_York").dt.minute() == 0
    else:
        valid &= pl.col("minutes_from_open") % bar_minutes == 0
    return labelled.filter(valid).with_columns(
        pl.lit(_BAR_LABEL_SEMANTICS[freq]).alias("bar_label_semantics")
    )


def expected_intraday_labels(
    start: datetime, end: datetime, *, freq: str
) -> pl.DataFrame:
    """Enumerate exact regular-session labels in one inclusive UTC span."""
    require_intraday_freq(freq)
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("intraday label bounds must be timezone-aware")
    start = start.astimezone(UTC)
    end = end.astimezone(UTC)
    if start > end:
        raise ValueError("intraday label start must not be after end")
    schedule = session_schedule(start.date(), end.date())
    schema = {"ts": pl.Datetime("us", "UTC")}
    if schedule.is_empty():
        return pl.DataFrame(schema=schema)

    bar_minutes = _BAR_MINUTES[freq]
    if freq == "1hour":
        first_label = pl.datetime(
            pl.col("session_date").dt.year(),
            pl.col("session_date").dt.month(),
            pl.col("session_date").dt.day(),
            hour=10,
            time_zone="America/New_York",
        ).dt.convert_time_zone("UTC")
    else:
        first_label = pl.col("session_open")
    labels = (
        schedule.with_columns(first_label.alias("first_label"))
        .select(
            pl.datetime_ranges(
                "first_label",
                pl.col("session_close") - pl.duration(minutes=bar_minutes),
                interval=f"{bar_minutes}m",
                closed="both",
            ).alias("ts")
        )
        .explode("ts", empty_as_null=True)
        .filter(pl.col("ts").is_between(start, end, closed="both"))
        .sort("ts")
    )
    return labels.cast(schema)


def plan_intraday_requests(
    start: date,
    end: date,
    *,
    freq: str,
    reverse: bool = False,
) -> list[IntradayRequestChunk]:
    """Partition a target range into lookahead-safe sub-10,000-row requests."""
    require_intraday_freq(freq)
    if start > end:
        raise ValueError("intraday plan start must not be after end")
    chunks = _plan_intraday_requests(start, end, freq)
    return list(reversed(chunks)) if reverse else list(chunks)


def max_intraday_probe_sessions(freq: str) -> int:
    """Return the absolute session-count ceiling for one lookahead-safe chunk.

    Exchange holidays can make the realizable limit smaller for a particular
    date span, so callers must still confirm that their concrete span produces
    exactly one planner chunk before issuing any requests.
    """
    require_intraday_freq(freq)
    max_weekdays = (IEX_ROW_CAP - 1) // _ROWS_PER_WEEKDAY[freq]
    return max_weekdays - 1


@lru_cache(maxsize=1024)
def _plan_intraday_requests(
    start: date, end: date, freq: str
) -> tuple[IntradayRequestChunk, ...]:
    max_weekdays = (IEX_ROW_CAP - 1) // _ROWS_PER_WEEKDAY[freq]
    chunks: list[IntradayRequestChunk] = []
    cursor = start
    while cursor <= end:
        low = cursor
        high = end
        chosen_end: date | None = None
        chosen_fetch_end: date | None = None
        chosen_weekdays = 0
        while low <= high:
            candidate = low + (high - low) // 2
            fetch_end = next_session_after(candidate)
            weekdays = _weekday_count(cursor, fetch_end)
            if weekdays <= max_weekdays:
                chosen_end = candidate
                chosen_fetch_end = fetch_end
                chosen_weekdays = weekdays
                low = candidate + timedelta(days=1)
            else:
                high = candidate - timedelta(days=1)
        if chosen_end is None or chosen_fetch_end is None:
            raise ValueError(
                "one intraday request day cannot fit below Tiingo's row cap"
            )
        chunks.append(
            IntradayRequestChunk(
                start=cursor,
                end=chosen_end,
                fetch_end=chosen_fetch_end,
                max_response_rows=chosen_weekdays * _ROWS_PER_WEEKDAY[freq],
            )
        )
        cursor = chosen_end + timedelta(days=1)
    return tuple(chunks)


def _weekday_count(start: date, end: date) -> int:
    days = (end - start).days + 1
    weeks, remainder = divmod(days, 7)
    return weeks * 5 + sum(
        (start.weekday() + offset) % 7 < 5 for offset in range(remainder)
    )
