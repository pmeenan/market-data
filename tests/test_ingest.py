"""Ingestion semantics tests against a fake Tiingo client (fully offline)."""

from datetime import date, timedelta

from marketdata.ingest import (
    REFRESH_WINDOW_DAYS,
    backfill_eod,
    backfill_intraday,
    reconcile,
    update_eod,
)
from marketdata.store import BarStore, MetaStore
from marketdata.tiingo import TiingoError


def eod_row(
    d: date, close: float = 100.0, div: float = 0.0, split: float = 1.0
) -> dict:
    return {
        "date": f"{d.isoformat()}T00:00:00.000Z",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000,
        "adjOpen": close,
        "adjHigh": close,
        "adjLow": close,
        "adjClose": close,
        "adjVolume": 1000,
        "divCash": div,
        "splitFactor": split,
    }


def weekdays(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


class FakeTiingo:
    """Serves a fixed per-ticker EOD history; records every request."""

    def __init__(self, history: dict[str, list[dict]], fail: set[str] = frozenset()):
        self.history = {t.upper(): rows for t, rows in history.items()}
        self.fail = {t.upper() for t in fail}
        self.eod_calls: list[tuple[str, date, date]] = []
        self.intraday_calls: list[tuple[str, date, date, str]] = []

    def eod(self, ticker, start=None, end=None):
        ticker = ticker.upper()
        if ticker in self.fail:
            raise TiingoError("simulated failure")
        start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
        self.eod_calls.append((ticker, start, end))
        return [
            r
            for r in self.history.get(ticker, [])
            if start <= date.fromisoformat(r["date"][:10]) <= end
        ]

    def intraday(self, ticker, start, end=None, freq="1hour"):
        start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
        self.intraday_calls.append((ticker.upper(), start, end, freq))
        rows = []
        for d in weekdays(start, end):
            if d.year >= 2024:  # simulate bounded IEX history
                rows.append(
                    {
                        "date": f"{d.isoformat()}T15:00:00.000Z",
                        "open": 10.0,
                        "high": 10.5,
                        "low": 9.5,
                        "close": 10.2,
                        "volume": 100,
                    }
                )
        return rows


def stores(tmp_path):
    return BarStore(tmp_path), MetaStore(tmp_path / "meta.db")


def test_backfill_fetches_missing_leading_history(tmp_path):
    """The reviewer's blocker: rank-year fetch first, then full history."""
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(1995, 1, 2), date(2025, 12, 31))]
    }
    client = FakeTiingo(history)

    # Step 1: fetch the ranking year only
    backfill_eod(client, bars, meta, ["AAPL"], date(2025, 1, 1), date(2025, 12, 31))
    assert meta.get_coverage("AAPL", "eod") == (date(2025, 1, 1), date(2025, 12, 31))

    # Step 2: full history from 1995 must fetch the LEADING gap, not start in 2026
    backfill_eod(client, bars, meta, ["AAPL"], date(1995, 1, 1), date(2025, 12, 31))
    assert (("AAPL", date(1995, 1, 1), date(2024, 12, 31))) in client.eod_calls
    assert meta.get_coverage("AAPL", "eod") == (date(1995, 1, 1), date(2025, 12, 31))
    df = bars.read_eod("AAPL")
    assert df["date"].min() == date(1995, 1, 2)

    # Fully covered now: a rerun makes no requests
    n = len(client.eod_calls)
    result = backfill_eod(
        client, bars, meta, ["AAPL"], date(1995, 1, 1), date(2025, 12, 31)
    )
    assert len(client.eod_calls) == n and result.skipped == ["AAPL"]


def test_empty_recent_response_not_marked_covered(tmp_path):
    bars, meta = stores(tmp_path)
    client = FakeTiingo({"NEWCO": []})  # nothing published yet
    today = date.today()

    backfill_eod(client, bars, meta, ["NEWCO"], today - timedelta(days=2), today)
    # publication lag: the range ends now, so it must NOT be marked covered
    assert meta.get_coverage("NEWCO", "eod") is None

    # a rerun tries again rather than skipping
    backfill_eod(client, bars, meta, ["NEWCO"], today - timedelta(days=2), today)
    assert len(client.eod_calls) == 2


def test_empty_historical_response_is_covered(tmp_path):
    bars, meta = stores(tmp_path)
    client = FakeTiingo({"GONE": []})  # e.g. delisted before the range
    backfill_eod(client, bars, meta, ["GONE"], date(2010, 1, 1), date(2010, 12, 31))
    assert meta.get_coverage("GONE", "eod") == (date(2010, 1, 1), date(2010, 12, 31))
    n = len(client.eod_calls)
    backfill_eod(client, bars, meta, ["GONE"], date(2010, 1, 1), date(2010, 12, 31))
    assert len(client.eod_calls) == n  # no refetch


def test_update_refetches_rolling_overlap(tmp_path):
    bars, meta = stores(tmp_path)
    today = date.today()
    last = today - timedelta(days=2)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)
    cov_last = meta.get_coverage("AAPL", "eod")[1]

    # correction lands inside the refresh window
    corrected = cov_last - timedelta(days=3)
    for r in client.history["AAPL"]:
        if r["date"][:10] == corrected.isoformat():
            r["close"] = 555.0
    update_eod(client, bars, meta, ["AAPL"])

    _, req_start, _ = client.eod_calls[-1]
    assert req_start == cov_last - timedelta(days=REFRESH_WINDOW_DAYS)
    df = bars.read_eod("AAPL")
    import polars as pl

    if corrected.weekday() < 5:
        assert df.filter(pl.col("date") == corrected)["close"][0] == 555.0
    assert meta.get_coverage("AAPL", "eod")[0] == date(2024, 1, 1)


def test_new_dividend_triggers_full_refresh(tmp_path):
    bars, meta = stores(tmp_path)
    today = date.today()
    last = today - timedelta(days=10)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)

    # a dividend appears after the old coverage edge
    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    result = update_eod(client, bars, meta, ["AAPL"])

    assert result.refreshed == ["AAPL"]
    # the refresh refetched from the start of coverage, not just the window
    assert client.eod_calls[-1][1] == date(2024, 1, 1)


def test_prefix_truncated_full_refresh_keeps_history(tmp_path):
    """A 'full snapshot' containing only the newest rows must not replace
    the file — that would silently erase stored history."""

    class PrefixTruncatingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.truncate:
                # simulate a vendor response with only the newest row
                return rows[-1:]
            return rows

    today = date.today()
    last = today - timedelta(days=10)
    bars, meta = stores(tmp_path)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = PrefixTruncatingTiingo(history)
    client.truncate = False
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)
    rows_before = bars.read_eod("AAPL").height

    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    client.truncate = True
    result = update_eod(client, bars, meta, ["AAPL"])

    # the truncated snapshot passes the max-date check (it contains the
    # dividend row) but must still be rejected
    assert result.refreshed == [] and "AAPL" in result.failed
    assert bars.read_eod("AAPL").height == rows_before  # history intact
    assert meta.get_coverage("AAPL", "eod")[1] == last


def _dividend_refresh_setup(tmp_path, client_cls):
    """Backfill history, then append a dividend that will trigger a full
    refresh on the next update. Returns (bars, meta, client, last)."""
    today = date.today()
    last = today - timedelta(days=10)
    bars, meta = stores(tmp_path)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = client_cls(history)
    client.sabotage = False
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)
    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    client.sabotage = True
    return bars, meta, client, last


def test_full_refresh_must_contain_trigger_dates(tmp_path):
    """A snapshot with all old dates but missing the dividend row that
    triggered the refresh must fail, not report success."""

    class OmittingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.sabotage and str(start) == "2024-01-01":
                return [r for r in rows if r["divCash"] == 0.0]
            return rows

    bars, meta, client, last = _dividend_refresh_setup(tmp_path, OmittingTiingo)
    result = update_eod(client, bars, meta, ["AAPL"])
    assert result.refreshed == [] and "AAPL" in result.failed
    assert meta.get_coverage("AAPL", "eod")[1] == last  # retried next run


def test_full_refresh_must_agree_on_corp_action_values(tmp_path):
    """A snapshot containing the trigger date but with the dividend zeroed
    out must fail."""

    class ZeroingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.sabotage and str(start) == "2024-01-01":
                return [
                    {**r, "divCash": 0.0} if r["divCash"] != 0.0 else r for r in rows
                ]
            return rows

    bars, meta, client, last = _dividend_refresh_setup(tmp_path, ZeroingTiingo)
    result = update_eod(client, bars, meta, ["AAPL"])
    assert result.refreshed == [] and "AAPL" in result.failed
    assert meta.get_coverage("AAPL", "eod")[1] == last


def test_backfill_failed_full_refresh_keeps_file_untouched(tmp_path):
    """Unlike update, backfill fetches missing segments. Those frames must
    remain staged if the subsequent full-refresh validation fails."""

    class OmittingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.sabotage and str(start) == "2024-01-01":
                return [r for r in rows if r["divCash"] == 0.0]
            return rows

    bars, meta, client, last = _dividend_refresh_setup(tmp_path, OmittingTiingo)
    before = bars.read_eod("AAPL")

    result = backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), date.today())

    assert result.refreshed == [] and "AAPL" in result.failed
    assert bars.read_eod("AAPL").equals(before)
    assert meta.get_coverage("AAPL", "eod")[1] == last


def test_backfill_reports_failures(tmp_path):
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
    }
    client = FakeTiingo(history, fail={"BADCO"})
    result = backfill_eod(
        client, bars, meta, ["AAPL", "BADCO"], date(2024, 1, 1), date(2024, 6, 28)
    )
    assert result.fetched == ["AAPL"]
    assert "BADCO" in result.failed and not result.ok


def test_intraday_leading_backfill_and_freq_validation(tmp_path):
    bars, meta = stores(tmp_path)
    client = FakeTiingo({})
    import pytest

    for unsupported in ("1min", "2min", "15min", "30min"):
        with pytest.raises(ValueError):
            backfill_intraday(
                client, bars, meta, ["AAPL"], date(2024, 1, 1), freq=unsupported
            )

    backfill_intraday(
        client, bars, meta, ["AAPL"], date(2024, 6, 1), date(2024, 7, 31), freq="1hour"
    )
    assert meta.get_coverage("AAPL", "intraday_1hour") == (
        date(2024, 6, 1),
        date(2024, 7, 31),
    )

    # extending the range backwards fetches the leading gap
    backfill_intraday(
        client, bars, meta, ["AAPL"], date(2024, 4, 1), date(2024, 7, 31), freq="1hour"
    )
    assert meta.get_coverage("AAPL", "intraday_1hour") == (
        date(2024, 4, 1),
        date(2024, 7, 31),
    )
    assert all(c[3] == "1hour" for c in client.intraday_calls)
    df = bars.read_intraday("AAPL", freq="1hour")
    assert df["ts"].dt.date().min() == date(2024, 4, 1)  # a Monday: bars exist


def test_reconcile_rebuilds_coverage_from_parquet(tmp_path):
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
    }
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), date(2024, 6, 28))
    meta.clear_coverage()
    assert meta.get_coverage("AAPL", "eod") is None

    counts = reconcile(bars, meta)
    assert counts["eod"] == 1
    first, last = meta.get_coverage("AAPL", "eod")
    assert first == date(2024, 1, 1) and last == date(2024, 6, 28)


def test_reconcile_removes_stale_coverage(tmp_path):
    """Coverage for a ticker with no Parquet file must not survive reconcile
    — a ghost entry would make later backfills skip real fetches."""
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
    }
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), date(2024, 6, 28))
    meta.set_coverage("GHOST", "eod", date(2020, 1, 1), date(2024, 12, 31))

    reconcile(bars, meta)
    assert meta.get_coverage("GHOST", "eod") is None
    assert meta.get_coverage("AAPL", "eod") is not None

    # and a backfill for GHOST now actually fetches
    backfill_eod(client, bars, meta, ["GHOST"], date(2024, 1, 1), date(2024, 6, 28))
    assert ("GHOST", date(2024, 1, 1), date(2024, 6, 28)) in client.eod_calls


def test_intraday_today_stays_refreshable(tmp_path):
    """A partial current day is written but not marked covered, so it is
    refetched on the next run."""
    bars, meta = stores(tmp_path)
    client = FakeTiingo({})
    today = date.today()
    start = today - timedelta(days=3)

    backfill_intraday(client, bars, meta, ["AAPL"], start, today, freq="1hour")
    cov = meta.get_coverage("AAPL", "intraday_1hour")
    if cov is not None:
        assert cov[1] <= today - timedelta(days=1)

    n = len(client.intraday_calls)
    backfill_intraday(client, bars, meta, ["AAPL"], start, today, freq="1hour")
    assert len(client.intraday_calls) > n  # today still gets refetched


def test_reconcile_caps_intraday_coverage_at_yesterday(tmp_path):
    """Rebuilding coverage from Parquet must not re-mark today's partial
    session as complete — and a ticker with only today's bars gets no
    coverage entry at all."""
    from marketdata.store.bars import intraday_frame

    bars, meta = stores(tmp_path)
    today = date.today()
    past = today - timedelta(days=5)

    def bar(d: date) -> dict:
        return {
            "date": f"{d.isoformat()}T15:00:00.000Z",
            "open": 10.0,
            "high": 10.5,
            "low": 9.5,
            "close": 10.2,
            "volume": 100,
        }

    bars.write_intraday(
        "AAPL", intraday_frame("AAPL", [bar(past), bar(today)]), freq="1hour"
    )
    bars.write_intraday(
        "ONLYTODAY", intraday_frame("ONLYTODAY", [bar(today)]), freq="1hour"
    )

    reconcile(bars, meta)
    first, last = meta.get_coverage("AAPL", "intraday_1hour")
    assert first == past and last <= today - timedelta(days=1)
    assert meta.get_coverage("ONLYTODAY", "intraday_1hour") is None


def test_empty_full_refresh_is_a_failure(tmp_path):
    """A corporate action whose full-history refetch comes back empty must
    not be reported as refreshed with the dividend silently missing."""

    class TruncatingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            # full-history requests (from the coverage start) come back empty
            if str(start) == "2024-01-01" and self.history_truncated:
                return []
            return rows

    today = date.today()
    last = today - timedelta(days=10)
    bars, meta = stores(tmp_path)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = TruncatingTiingo(history)
    client.history_truncated = False
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)

    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    client.history_truncated = True
    result = update_eod(client, bars, meta, ["AAPL"])

    assert result.refreshed == []
    assert "AAPL" in result.failed and not result.ok
    # coverage untouched: the refresh will be retried next run
    assert meta.get_coverage("AAPL", "eod")[1] == last
