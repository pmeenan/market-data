"""Exchange-calendar planning and bar-label semantics."""

from datetime import date, datetime, timedelta

import polars as pl

import marketdata.calendar as market_calendar
from marketdata.calendar import (
    IEX_ROW_CAP,
    expected_intraday_labels,
    label_intraday_sessions,
    next_session_after,
    plan_intraday_requests,
    session_schedule,
)


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _frame(*timestamps: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "instrument_id": ["stable-id"] * len(timestamps),
            "ts": [_utc(value) for value in timestamps],
            "close": list(range(len(timestamps))),
        },
        schema_overrides={"ts": pl.Datetime("us", "UTC")},
    )


def test_xnys_schedule_is_utc_and_handles_dst_holidays_and_half_days():
    dst = session_schedule(date(2024, 3, 8), date(2024, 3, 11))

    assert dst["session_date"].to_list() == [date(2024, 3, 8), date(2024, 3, 11)]
    assert dst["session_open"].to_list() == [
        _utc("2024-03-08T14:30:00Z"),
        _utc("2024-03-11T13:30:00Z"),
    ]
    assert dst["session_close"].to_list() == [
        _utc("2024-03-08T21:00:00Z"),
        _utc("2024-03-11T20:00:00Z"),
    ]

    thanksgiving = session_schedule(date(2025, 11, 27), date(2025, 11, 28))
    assert thanksgiving.to_dicts() == [
        {
            "session_date": date(2025, 11, 28),
            "session_open": _utc("2025-11-28T14:30:00Z"),
            "session_close": _utc("2025-11-28T18:00:00Z"),
            "is_early_close": True,
        }
    ]


def test_next_session_handles_dates_before_the_years_first_session():
    assert next_session_after(date(2024, 1, 1)) == date(2024, 1, 2)
    assert next_session_after(date(2022, 1, 2)) == date(2022, 1, 3)

    chunks = plan_intraday_requests(date(2024, 1, 1), date(2024, 1, 1), freq="1hour")
    assert [(chunk.start, chunk.end, chunk.fetch_end) for chunk in chunks] == [
        (date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2))
    ]


def test_five_minute_labels_filter_holidays_and_after_close_rows():
    raw = _frame(
        "2024-03-11T13:30:00Z",
        "2024-03-11T13:35:00Z",
        "2024-03-11T20:00:00Z",  # close label is outside the session
        "2024-07-04T13:30:00Z",  # holiday force-fill
        "2025-11-28T17:55:00Z",
        "2025-11-28T18:00:00Z",  # after the half-day close
    )

    labelled = label_intraday_sessions(raw, freq="5min")

    assert labelled["ts"].to_list() == [
        _utc("2024-03-11T13:30:00Z"),
        _utc("2024-03-11T13:35:00Z"),
        _utc("2025-11-28T17:55:00Z"),
    ]
    assert labelled["minutes_from_open"].to_list() == [0, 5, 205]
    assert labelled["session_date"].to_list() == [
        date(2024, 3, 11),
        date(2024, 3, 11),
        date(2025, 11, 28),
    ]
    assert labelled["is_early_close"].to_list() == [False, False, True]
    assert labelled["bar_label_semantics"].unique().to_list() == ["session_5min_start"]
    assert labelled.schema == label_intraday_sessions(raw.clear(), freq="5min").schema


def test_direct_hourly_labels_are_whole_clock_hours_not_open_anchored():
    raw = _frame(
        "2024-03-11T13:30:00Z",  # 09:30 Eastern: no direct hourly bin
        "2024-03-11T19:00:00Z",  # 15:00 Eastern
        "2024-03-11T14:00:00Z",  # 10:00 Eastern; retain input order
        "2025-11-28T17:00:00Z",  # 12:00 Eastern, ends at half-day close
        "2025-11-28T18:00:00Z",  # close label
    )

    labelled = label_intraday_sessions(raw, freq="1hour")

    assert labelled["ts"].to_list() == [
        _utc("2024-03-11T19:00:00Z"),
        _utc("2024-03-11T14:00:00Z"),
        _utc("2025-11-28T17:00:00Z"),
    ]
    assert labelled["minutes_from_open"].to_list() == [330, 30, 150]
    assert labelled["bar_label_semantics"].unique().to_list() == ["clock_hour_start"]


def test_expected_intraday_labels_share_frequency_and_half_day_semantics():
    hourly = expected_intraday_labels(
        _utc("2024-03-11T13:30:00Z"),
        _utc("2024-03-11T20:00:00Z"),
        freq="1hour",
    )
    half_day = expected_intraday_labels(
        _utc("2025-11-28T14:30:00Z"),
        _utc("2025-11-28T18:00:00Z"),
        freq="5min",
    )

    assert hourly["ts"].to_list() == [
        _utc(f"2024-03-11T{hour:02d}:00:00Z") for hour in range(14, 20)
    ]
    assert half_day.height == 42
    assert half_day["ts"].min() == _utc("2025-11-28T14:30:00Z")
    assert half_day["ts"].max() == _utc("2025-11-28T17:55:00Z")


def test_both_intraday_plans_tile_ranges_with_next_session_lookahead_under_cap():
    start, end = date(2016, 12, 12), date(2026, 8, 25)
    for freq in ("1hour", "5min"):
        chunks = plan_intraday_requests(start, end, freq=freq)

        assert len(chunks) > 1
        assert chunks[0].start == start
        assert chunks[-1].end == end
        for previous, current in zip(chunks, chunks[1:], strict=False):
            assert current.start == previous.end + timedelta(days=1)
        for chunk in chunks:
            assert chunk.fetch_end == next_session_after(chunk.end)
            assert chunk.max_response_rows < IEX_ROW_CAP

        assert plan_intraday_requests(start, end, freq=freq, reverse=True) == list(
            reversed(chunks)
        )


def test_identical_intraday_ranges_reuse_the_cached_plan(monkeypatch):
    start, end = date(2034, 1, 1), date(2035, 12, 31)
    calls = 0
    original = market_calendar._weekday_count

    def recording_weekday_count(first, last):
        nonlocal calls
        calls += 1
        return original(first, last)

    market_calendar._plan_intraday_requests.cache_clear()
    monkeypatch.setattr(market_calendar, "_weekday_count", recording_weekday_count)

    first = plan_intraday_requests(start, end, freq="5min")
    first_call_count = calls
    second = plan_intraday_requests(start, end, freq="5min")

    assert first_call_count > 0
    assert calls == first_call_count
    assert first == second
    assert first is not second
