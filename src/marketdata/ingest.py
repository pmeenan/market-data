"""Backfill and incremental-update orchestration.

Correctness model:

- Coverage is a per-(ticker, dataset) *interval* [first, last]. A backfill
  fetches the missing leading segment (before `first`) and/or trailing
  segment (after `last`) of the requested range, so "rank on 2025, then
  backfill from 1995" works.
- An empty response only marks a range covered when the range ends at least
  PUBLICATION_LAG_DAYS in the past — running before Tiingo publishes a
  session cannot permanently skip it.
- Nightly updates refetch a rolling REFRESH_WINDOW_DAYS overlap so
  late-evening corrections and restated adjusted values are picked up
  (Parquet writes are merge-upserts keyed on date).
- A new split or dividend observed past the old coverage edge triggers a
  full-history refresh for that ticker, so one file never mixes adjustment
  vintages.
- Coverage is reconcilable from the canonical Parquet files (`reconcile`).

Everything is idempotent and resumable: interrupt any operation and rerun it
and it converges to the same dataset.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl

from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import INTRADAY_FREQS, eod_frame, intraday_frame
from marketdata.tiingo import TiingoClient, TiingoError

log = logging.getLogger(__name__)

# Tiingo's IEX endpoint limits how much intraday history one request may
# span; fetch in conservative chunks.
INTRADAY_CHUNK_DAYS = 30

# Nightly updates refetch this many days before the coverage edge to pick up
# corrections and restated adjustments.
REFRESH_WINDOW_DAYS = 7

# An empty response marks a range covered only if the range ends at least
# this many days in the past (EOD corrections can land through the evening;
# weekends/holidays add slack).
PUBLICATION_LAG_DAYS = 5
INTRADAY_PUBLICATION_LAG_DAYS = 1

DEFAULT_INTRADAY_FREQ = "1hour"


@dataclass
class IngestResult:
    fetched: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    refreshed: list[str] = field(default_factory=list)  # full corp-action refreshes
    failed: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.failed

    def summary(self) -> str:
        parts = [
            f"{len(self.fetched)} fetched",
            f"{len(self.skipped)} up-to-date",
            f"{len(self.failed)} failed",
        ]
        if self.refreshed:
            parts.insert(2, f"{len(self.refreshed)} fully refreshed (corp action)")
        return ", ".join(parts)

    def to_dict(self) -> dict:
        return {
            "fetched": sorted(self.fetched),
            "skipped": sorted(self.skipped),
            "refreshed": sorted(self.refreshed),
            "failed": dict(sorted(self.failed.items())),
            "ok": self.ok,
        }


def _missing_segments(
    requested: tuple[date, date], covered: tuple[date, date] | None
) -> list[tuple[date, date]]:
    """Leading and trailing sub-ranges of `requested` not in `covered`.

    Coverage is one contiguous interval (merge-upsert writes make interior
    gaps impossible via this module), so at most two segments result.
    """
    start, end = requested
    if covered is None:
        return [(start, end)]
    first, last = covered
    segments = []
    if start < first:
        segments.append((start, min(end, first - timedelta(days=1))))
    if end > last:
        segments.append((max(start, last + timedelta(days=1)), end))
    return segments


def _covered_through(
    max_received: date | None, seg_end: date, today: date, lag_days: int
) -> date | None:
    """How far a fetch of [.., seg_end] proves coverage. Historical ranges
    are complete regardless of content; recent ranges only through the data
    actually received."""
    if seg_end <= today - timedelta(days=lag_days):
        return seg_end
    return max_received


def _has_new_corp_action(df: pl.DataFrame, after: date) -> bool:
    new = df.filter(pl.col("date") > after)
    if new.is_empty():
        return False
    return bool(
        new.select(
            ((pl.col("split_factor") != 1.0) | (pl.col("div_cash") != 0.0)).any()
        ).item()
    )


def _full_refresh_eod(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    ticker: str,
    first: date,
    prev_last: date,
    end: date,
    today: date,
    trigger: pl.DataFrame | None = None,
) -> None:
    """Refetch a ticker's entire history so adjusted columns are one
    consistent vintage, validate the snapshot, and atomically replace the
    file (merge-upsert cannot remove stale dates a new snapshot omits).

    `trigger` is the frame from the fetch that revealed the corporate
    action; it has NOT been written yet, so the snapshot must be validated
    against it too — otherwise a snapshot omitting the very dividend/split
    that triggered the refresh would pass.

    Raises TiingoError if the snapshot is empty or demonstrably incomplete —
    prefix-truncated history, missing trigger dates, or corporate-action
    values that disagree with the trigger — so callers report the ticker as
    failed rather than refreshed and the existing file is kept.
    """
    rows = client.eod(ticker, first, end)
    if not rows:
        raise TiingoError(
            f"full refresh of {ticker} returned no rows for {first}..{end}"
        )
    df = eod_frame(ticker, rows)
    if df["date"].max() < prev_last:
        raise TiingoError(
            f"full refresh of {ticker} incomplete: snapshot ends {df['date'].max()}, "
            f"previous coverage reached {prev_last}"
        )
    existing = bars.read_eod(ticker)
    if existing is not None:
        # Every date already stored through prev_last must survive the
        # replacement; a vendor deleting history is an explicit manual
        # operation, never an automated overwrite.
        missing = existing.filter(pl.col("date") <= prev_last).join(
            df.select("date"), on="date", how="anti"
        )
        if missing.height:
            raise TiingoError(
                f"full refresh of {ticker} would drop {missing.height} previously "
                f"stored dates (e.g. {missing['date'].min()}); keeping existing file"
            )
    if trigger is not None:
        missing_trigger = trigger.select("date").join(
            df.select("date"), on="date", how="anti"
        )
        if missing_trigger.height:
            raise TiingoError(
                f"full refresh of {ticker} omits {missing_trigger.height} dates from "
                f"the triggering fetch (e.g. {missing_trigger['date'].min()})"
            )
        actions = trigger.filter(
            (pl.col("split_factor") != 1.0) | (pl.col("div_cash") != 0.0)
        )
        mismatched = (
            actions.select("date", "div_cash", "split_factor")
            .join(
                df.select("date", "div_cash", "split_factor"), on="date", suffix="_snap"
            )
            .filter(
                (pl.col("div_cash") != pl.col("div_cash_snap"))
                | (pl.col("split_factor") != pl.col("split_factor_snap"))
            )
        )
        if mismatched.height:
            raise TiingoError(
                f"full refresh of {ticker} disagrees with the triggering fetch on "
                f"corporate-action values for {mismatched.height} dates "
                f"(e.g. {mismatched['date'].min()})"
            )
    bars.replace_eod(ticker, df)
    covered = _covered_through(df["date"].max(), end, today, PUBLICATION_LAG_DAYS)
    meta.set_ticker_coverage_v1(
        ticker, "eod", min(first, df["date"].min()), covered or df["date"].max()
    )


def backfill_eod(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    tickers: list[str],
    start: date,
    end: date | None = None,
    *,
    force: bool = False,
) -> IngestResult:
    """Fetch daily history to cover [start, end] for each ticker — including
    missing leading history before existing coverage."""
    today = date.today()
    end = end or today
    result = IngestResult()
    for i, ticker in enumerate(tickers, 1):
        try:
            covered = None if force else meta.get_ticker_coverage_v1(ticker, "eod")
            segments = _missing_segments((start, end), covered)
            if not segments:
                result.skipped.append(ticker)
                continue
            got_rows = False
            new_first = covered[0] if covered else None
            new_last = covered[1] if covered else None
            pending_frames: list[pl.DataFrame] = []
            trigger_frames: list[pl.DataFrame] = []
            for seg_start, seg_end in segments:
                rows = client.eod(ticker, seg_start, seg_end)
                max_received = None
                if rows:
                    df = eod_frame(ticker, rows)
                    # Stage segment writes until corporate-action detection is
                    # complete. If a validated full refresh is required, it
                    # must succeed before canonical Parquet changes at all.
                    pending_frames.append(df)
                    got_rows = True
                    max_received = df["date"].max()
                    if covered is not None and _has_new_corp_action(df, covered[1]):
                        trigger_frames.append(df)
                covered_to = _covered_through(
                    max_received, seg_end, today, PUBLICATION_LAG_DAYS
                )
                if covered_to is not None and covered_to >= seg_start:
                    new_first = (
                        seg_start if new_first is None else min(new_first, seg_start)
                    )
                    new_last = (
                        covered_to if new_last is None else max(new_last, covered_to)
                    )
            if trigger_frames:
                trigger_df = pl.concat(trigger_frames)
                _full_refresh_eod(
                    client,
                    bars,
                    meta,
                    ticker,
                    min(new_first, start),
                    covered[1],
                    end,
                    today,
                    trigger=trigger_df,
                )
                result.refreshed.append(ticker)
            elif new_first is not None and new_last is not None:
                for df in pending_frames:
                    bars.write_eod(ticker, df)
                meta.set_ticker_coverage_v1(ticker, "eod", new_first, new_last)
                (result.fetched if got_rows else result.skipped).append(ticker)
            else:
                result.skipped.append(ticker)
            if i % 25 == 0 or i == len(tickers):
                log.info("eod backfill: %d/%d (%s)", i, len(tickers), result.summary())
        except TiingoError as e:
            log.warning("eod backfill failed for %s: %s", ticker, e)
            result.failed[ticker] = str(e)
    return result


def update_eod(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    tickers: list[str],
    *,
    default_start: date = date(2000, 1, 3),
) -> IngestResult:
    """Nightly incremental update.

    Refetches a REFRESH_WINDOW_DAYS overlap before each coverage edge (to
    absorb corrections/restatements), and falls back to a full backfill for
    tickers with no coverage yet. A newly observed split/dividend triggers a
    full-history refresh for that ticker.
    """
    today = date.today()
    result = IngestResult()
    for i, ticker in enumerate(tickers, 1):
        try:
            covered = meta.get_ticker_coverage_v1(ticker, "eod")
            if covered is None:
                sub = backfill_eod(client, bars, meta, [ticker], default_start)
                result.fetched += sub.fetched
                result.skipped += sub.skipped
                result.refreshed += sub.refreshed
                result.failed.update(sub.failed)
                continue
            first, last = covered
            fetch_start = max(first, last - timedelta(days=REFRESH_WINDOW_DAYS))
            rows = client.eod(ticker, fetch_start, today)
            if rows:
                df = eod_frame(ticker, rows)
                if _has_new_corp_action(df, last):
                    _full_refresh_eod(
                        client,
                        bars,
                        meta,
                        ticker,
                        first,
                        last,
                        today,
                        today,
                        trigger=df,
                    )
                    result.refreshed.append(ticker)
                else:
                    bars.write_eod(ticker, df)
                    meta.set_ticker_coverage_v1(
                        ticker, "eod", first, max(last, df["date"].max())
                    )
                    result.fetched.append(ticker)
            else:
                result.skipped.append(ticker)
            if i % 25 == 0 or i == len(tickers):
                log.info("eod update: %d/%d (%s)", i, len(tickers), result.summary())
        except TiingoError as e:
            log.warning("eod update failed for %s: %s", ticker, e)
            result.failed[ticker] = str(e)
    return result


def backfill_intraday(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    tickers: list[str],
    start: date,
    end: date | None = None,
    *,
    freq: str = DEFAULT_INTRADAY_FREQ,
) -> IngestResult:
    """Fetch intraday bars in chunks to cover [start, end], leading segments
    included. Note: Tiingo's IEX feed reaches back a bounded number of years,
    is unadjusted, and reports IEX-only volume."""
    if freq not in INTRADAY_FREQS:
        raise ValueError(f"freq must be one of {INTRADAY_FREQS}, got {freq!r}")
    today = date.today()
    end = end or today
    dataset = f"intraday_{freq}"
    result = IngestResult()
    for i, ticker in enumerate(tickers, 1):
        try:
            covered = meta.get_ticker_coverage_v1(ticker, dataset)
            segments = _missing_segments((start, end), covered)
            if not segments:
                result.skipped.append(ticker)
                continue
            wrote_any = False
            for seg_start, seg_end in segments:
                # Leading segments are fetched newest-chunk-first so the
                # coverage interval stays contiguous if interrupted.
                leading = covered is not None and seg_end < covered[0]
                for chunk_start, chunk_end in _chunks(
                    seg_start, seg_end, reverse=leading
                ):
                    rows = client.intraday(ticker, chunk_start, chunk_end, freq=freq)
                    max_received = None
                    if rows:
                        df = intraday_frame(ticker, rows)
                        bars.write_intraday(ticker, df, freq=freq)
                        wrote_any = True
                        max_received = df["ts"].dt.date().max()
                    covered_to = _covered_through(
                        max_received, chunk_end, today, INTRADAY_PUBLICATION_LAG_DAYS
                    )
                    if covered_to is not None:
                        # Today's partial session is written but never marked
                        # covered — it stays refreshable until the day ends.
                        covered_to = min(covered_to, today - timedelta(days=1))
                    if covered_to is not None and covered_to >= chunk_start:
                        meta.extend_ticker_coverage_v1(
                            ticker, dataset, chunk_start, covered_to
                        )
            (result.fetched if wrote_any else result.skipped).append(ticker)
            if i % 10 == 0 or i == len(tickers):
                log.info(
                    "%s backfill: %d/%d (%s)",
                    dataset,
                    i,
                    len(tickers),
                    result.summary(),
                )
        except TiingoError as e:
            log.warning("%s backfill failed for %s: %s", dataset, ticker, e)
            result.failed[ticker] = str(e)
    return result


def _chunks(
    start: date, end: date, *, reverse: bool = False
) -> list[tuple[date, date]]:
    out = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=INTRADAY_CHUNK_DAYS - 1), end)
        out.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return list(reversed(out)) if reverse else out


def reconcile(bars: BarStore, meta: MetaStore) -> dict[str, int]:
    """Rebuild coverage metadata from the canonical Parquet files.

    The complete replacement map is built first and swapped in atomically,
    so stale entries for files that no longer exist do not survive.
    """
    entries: dict[tuple[str, str], tuple[date, date]] = {}
    counts = {"eod": 0}
    for ticker in bars.eod_tickers():
        df = (
            pl.scan_parquet(bars.eod_path(ticker))
            .select(pl.col("date").min().alias("lo"), pl.col("date").max().alias("hi"))
            .collect()
        )
        lo, hi = df["lo"][0], df["hi"][0]
        if lo is not None:
            entries[(ticker, "eod")] = (lo, hi)
            counts["eod"] += 1
    intraday_root = bars.data_dir / "intraday"
    if intraday_root.exists():
        # Same rule as ingestion: today's partial session is never covered.
        cap = date.today() - timedelta(days=1)
        for freq_dir in sorted(p for p in intraday_root.iterdir() if p.is_dir()):
            dataset = f"intraday_{freq_dir.name}"
            counts[dataset] = 0
            for ticker_dir in sorted(p for p in freq_dir.iterdir() if p.is_dir()):
                files = sorted(ticker_dir.glob("*.parquet"))
                if not files:
                    continue
                df = (
                    pl.scan_parquet(files)
                    .select(
                        pl.col("ts").dt.date().min().alias("lo"),
                        pl.col("ts").dt.date().max().alias("hi"),
                    )
                    .collect()
                )
                lo, hi = df["lo"][0], df["hi"][0]
                if lo is not None and lo <= cap:
                    entries[(ticker_dir.name, dataset)] = (lo, min(hi, cap))
                    counts[dataset] += 1
    meta.replace_ticker_coverage_v1(entries)
    return counts
