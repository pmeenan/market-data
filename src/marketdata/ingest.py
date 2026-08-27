"""Backfill and incremental-update orchestration.

Correctness model:

- Coverage is a per-(instrument_id, dataset_key) *interval* [first, last]. A backfill
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
  full-history refresh for that instrument, so one slice never mixes adjustment
  vintages.
- Coverage is reconcilable from the canonical Parquet files (`reconcile`).

Everything is idempotent and resumable: interrupt any operation and rerun it
and it converges to the same dataset.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl

from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import (
    eod_frame,
    instrument_bucket,
    intraday_frame,
    require_canonical_generation,
    require_intraday_freq,
)
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


@dataclass(frozen=True)
class IngestTarget:
    """One stable bar owner plus the Tiingo identifier used for transport.

    The identifier is deliberately separate from ``instrument_id``.  Callers
    must obtain it from exact-dataset identity evidence; the next M1 step owns
    that request segmentation and response-validation orchestration.
    """

    instrument_id: str
    identifier: str

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("instrument_id must not be empty")
        if not self.identifier.strip():
            raise ValueError("Tiingo identifier must not be empty")


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


def _require_registered_targets(meta: MetaStore, targets: list[IngestTarget]) -> None:
    instrument_ids = [target.instrument_id for target in targets]
    if len(instrument_ids) != len(set(instrument_ids)):
        raise ValueError("ingestion targets must have unique instrument_ids")
    unknown = set(instrument_ids) - meta.instrument_ids()
    if unknown:
        raise ValueError(
            f"ingestion targets contain unknown instruments: {sorted(unknown)}"
        )


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


def _prepare_full_refresh_eod(
    client: TiingoClient,
    bars: BarStore,
    target: IngestTarget,
    first: date,
    prev_last: date,
    end: date,
    today: date,
    trigger: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, tuple[date, date]]:
    """Refetch an instrument's entire history so adjusted columns are one
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
    rows = client.eod(target.identifier, first, end)
    if not rows:
        raise TiingoError(
            f"full refresh of {target.instrument_id} returned no rows for "
            f"{first}..{end}"
        )
    df = eod_frame(target.identifier, rows)
    if df["date"].max() < prev_last:
        raise TiingoError(
            f"full refresh of {target.instrument_id} incomplete: snapshot ends "
            f"{df['date'].max()}, previous coverage reached {prev_last}"
        )
    existing = bars.read_canonical_eod(target.instrument_id)
    if existing is not None:
        # Every date already stored through prev_last must survive the
        # replacement; a vendor deleting history is an explicit manual
        # operation, never an automated overwrite.
        missing = existing.filter(pl.col("date") <= prev_last).join(
            df.select("date"), on="date", how="anti"
        )
        if missing.height:
            raise TiingoError(
                f"full refresh of {target.instrument_id} would drop "
                f"{missing.height} previously stored dates "
                f"(e.g. {missing['date'].min()}); keeping existing slice"
            )
    if trigger is not None:
        missing_trigger = trigger.select("date").join(
            df.select("date"), on="date", how="anti"
        )
        if missing_trigger.height:
            raise TiingoError(
                f"full refresh of {target.instrument_id} omits "
                f"{missing_trigger.height} dates from the triggering fetch "
                f"(e.g. {missing_trigger['date'].min()})"
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
                f"full refresh of {target.instrument_id} disagrees with the "
                f"triggering fetch on corporate-action values for "
                f"{mismatched.height} dates (e.g. {mismatched['date'].min()})"
            )
    covered = _covered_through(df["date"].max(), end, today, PUBLICATION_LAG_DAYS)
    return df, (
        min(first, df["date"].min()),
        covered or df["date"].max(),
    )


def _bucket_groups(targets: list[IngestTarget]) -> Iterator[list[IngestTarget]]:
    grouped: dict[str, list[IngestTarget]] = {}
    for target in targets:
        grouped.setdefault(instrument_bucket(target.instrument_id), []).append(target)
    yield from (grouped[bucket] for bucket in sorted(grouped))


def backfill_eod(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    targets: list[IngestTarget],
    start: date,
    end: date | None = None,
    *,
    force: bool = False,
) -> IngestResult:
    """Fetch daily history for stable instruments, including
    missing leading history before existing coverage."""
    require_canonical_generation(bars, meta.storage_generation())
    _require_registered_targets(meta, targets)
    today = date.today()
    end = end or today
    result = IngestResult()
    processed = 0
    for group in _bucket_groups(targets):
        frames: dict[str, pl.DataFrame] = {}
        replacements: set[str] = set()
        ready: dict[str, tuple[date, date, str]] = {}
        for target in group:
            instrument_id = target.instrument_id
            try:
                covered = None if force else meta.get_coverage(instrument_id, "eod")
                segments = _missing_segments((start, end), covered)
                if not segments:
                    result.skipped.append(instrument_id)
                    continue
                got_rows = False
                new_first = covered[0] if covered else None
                new_last = covered[1] if covered else None
                pending_frames: list[pl.DataFrame] = []
                trigger_frames: list[pl.DataFrame] = []
                for seg_start, seg_end in segments:
                    rows = client.eod(target.identifier, seg_start, seg_end)
                    max_received = None
                    if rows:
                        frame = eod_frame(target.identifier, rows)
                        pending_frames.append(frame)
                        got_rows = True
                        max_received = frame["date"].max()
                        if covered is not None and _has_new_corp_action(
                            frame, covered[1]
                        ):
                            trigger_frames.append(frame)
                    covered_to = _covered_through(
                        max_received, seg_end, today, PUBLICATION_LAG_DAYS
                    )
                    if covered_to is not None and covered_to >= seg_start:
                        new_first = (
                            seg_start
                            if new_first is None
                            else min(new_first, seg_start)
                        )
                        new_last = (
                            covered_to
                            if new_last is None
                            else max(new_last, covered_to)
                        )
                if trigger_frames:
                    snapshot, coverage = _prepare_full_refresh_eod(
                        client,
                        bars,
                        target,
                        min(new_first, start),
                        covered[1],
                        end,
                        today,
                        trigger=pl.concat(trigger_frames),
                    )
                    frames[instrument_id] = bars.canonicalize_eod(
                        instrument_id, snapshot
                    )
                    replacements.add(instrument_id)
                    ready[instrument_id] = (*coverage, "refreshed")
                elif new_first is not None and new_last is not None:
                    if pending_frames:
                        frames[instrument_id] = bars.canonicalize_eod(
                            instrument_id, pl.concat(pending_frames)
                        )
                    ready[instrument_id] = (
                        new_first,
                        new_last,
                        "fetched" if got_rows else "skipped",
                    )
                else:
                    result.skipped.append(instrument_id)
            except (TiingoError, ValueError, pl.exceptions.PolarsError) as exc:
                log.warning("eod backfill failed for %s: %s", instrument_id, exc)
                result.failed[instrument_id] = str(exc)

        if frames:
            try:
                bars.publish_eod(frames, replace_instruments=frozenset(replacements))
            except ValueError as exc:
                for instrument_id in frames:
                    result.failed[instrument_id] = str(exc)
                    ready.pop(instrument_id, None)
        for instrument_id, (new_first, new_last, outcome) in ready.items():
            meta.set_coverage(instrument_id, "eod", new_first, new_last)
            getattr(result, outcome).append(instrument_id)

        processed += len(group)
        if processed % 25 < len(group) or processed == len(targets):
            log.info(
                "eod backfill: %d/%d (%s)", processed, len(targets), result.summary()
            )
    return result


def update_eod(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    targets: list[IngestTarget],
    *,
    default_start: date = date(2000, 1, 3),
) -> IngestResult:
    """Nightly incremental update.

    Refetches a REFRESH_WINDOW_DAYS overlap before each coverage edge (to
    absorb corrections/restatements), and falls back to a full backfill for
    instruments with no coverage yet. A newly observed split/dividend triggers
    a full-history refresh for that instrument.
    """
    require_canonical_generation(bars, meta.storage_generation())
    _require_registered_targets(meta, targets)
    today = date.today()
    result = IngestResult()
    processed = 0
    for group in _bucket_groups(targets):
        frames: dict[str, pl.DataFrame] = {}
        replacements: set[str] = set()
        ready: dict[str, tuple[date, date, str]] = {}
        uncovered: list[IngestTarget] = []
        for target in group:
            instrument_id = target.instrument_id
            try:
                covered = meta.get_coverage(instrument_id, "eod")
                if covered is None:
                    uncovered.append(target)
                    continue
                first, last = covered
                fetch_start = max(first, last - timedelta(days=REFRESH_WINDOW_DAYS))
                rows = client.eod(target.identifier, fetch_start, today)
                if not rows:
                    result.skipped.append(instrument_id)
                    continue
                frame = eod_frame(target.identifier, rows)
                if _has_new_corp_action(frame, last):
                    snapshot, coverage = _prepare_full_refresh_eod(
                        client,
                        bars,
                        target,
                        first,
                        last,
                        today,
                        today,
                        trigger=frame,
                    )
                    frames[instrument_id] = bars.canonicalize_eod(
                        instrument_id, snapshot
                    )
                    replacements.add(instrument_id)
                    ready[instrument_id] = (*coverage, "refreshed")
                else:
                    frames[instrument_id] = bars.canonicalize_eod(instrument_id, frame)
                    ready[instrument_id] = (
                        first,
                        max(last, frame["date"].max()),
                        "fetched",
                    )
            except (TiingoError, ValueError, pl.exceptions.PolarsError) as exc:
                log.warning("eod update failed for %s: %s", instrument_id, exc)
                result.failed[instrument_id] = str(exc)

        if frames:
            try:
                bars.publish_eod(frames, replace_instruments=frozenset(replacements))
            except ValueError as exc:
                for instrument_id in frames:
                    result.failed[instrument_id] = str(exc)
                    ready.pop(instrument_id, None)
        for instrument_id, (first, last, outcome) in ready.items():
            meta.set_coverage(instrument_id, "eod", first, last)
            getattr(result, outcome).append(instrument_id)
        if uncovered:
            sub = backfill_eod(client, bars, meta, uncovered, default_start)
            result.fetched += sub.fetched
            result.skipped += sub.skipped
            result.refreshed += sub.refreshed
            result.failed.update(sub.failed)

        processed += len(group)
        if processed % 25 < len(group) or processed == len(targets):
            log.info(
                "eod update: %d/%d (%s)", processed, len(targets), result.summary()
            )
    return result


def backfill_intraday(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    targets: list[IngestTarget],
    start: date,
    end: date | None = None,
    *,
    freq: str = DEFAULT_INTRADAY_FREQ,
) -> IngestResult:
    """Fetch intraday bars in chunks to cover [start, end], leading segments
    included. Note: Tiingo's IEX feed reaches back a bounded number of years,
    is unadjusted, and reports IEX-only volume."""
    require_intraday_freq(freq)
    require_canonical_generation(bars, meta.storage_generation())
    _require_registered_targets(meta, targets)
    today = date.today()
    end = end or today
    dataset = f"intraday_{freq}"
    result = IngestResult()
    processed = 0
    for group in _bucket_groups(targets):
        plans: dict[str, list[tuple[date, date]]] = {}
        targets_by_id = {target.instrument_id: target for target in group}
        wrote_any = dict.fromkeys(targets_by_id, False)
        failed: set[str] = set()
        planned: set[str] = set()
        for target in group:
            covered = meta.get_coverage(target.instrument_id, dataset)
            segments = _missing_segments((start, end), covered)
            plan: list[tuple[date, date]] = []
            for seg_start, seg_end in segments:
                leading = covered is not None and seg_end < covered[0]
                plan.extend(_chunks(seg_start, seg_end, reverse=leading))
            plans[target.instrument_id] = plan
            if plan:
                planned.add(target.instrument_id)
            else:
                result.skipped.append(target.instrument_id)

        while any(plans.values()):
            frames: dict[str, pl.DataFrame] = {}
            coverage_ready: dict[str, tuple[date, date]] = {}
            for instrument_id, plan in plans.items():
                if not plan or instrument_id in failed:
                    continue
                chunk_start, chunk_end = plan.pop(0)
                target = targets_by_id[instrument_id]
                try:
                    rows = client.intraday(
                        target.identifier, chunk_start, chunk_end, freq=freq
                    )
                    max_received = None
                    if rows:
                        frame = intraday_frame(target.identifier, rows)
                        frames[instrument_id] = bars.canonicalize_intraday(
                            instrument_id, frame
                        )
                        max_received = frame["ts"].dt.date().max()
                    covered_to = _covered_through(
                        max_received,
                        chunk_end,
                        today,
                        INTRADAY_PUBLICATION_LAG_DAYS,
                    )
                    if covered_to is not None:
                        covered_to = min(covered_to, today - timedelta(days=1))
                    if covered_to is not None and covered_to >= chunk_start:
                        coverage_ready[instrument_id] = (chunk_start, covered_to)
                except (TiingoError, ValueError, pl.exceptions.PolarsError) as exc:
                    log.warning(
                        "%s backfill failed for %s: %s", dataset, instrument_id, exc
                    )
                    result.failed[instrument_id] = str(exc)
                    failed.add(instrument_id)
                    plan.clear()

            if frames:
                try:
                    bars.publish_intraday(frames, freq=freq)
                    for instrument_id in frames:
                        wrote_any[instrument_id] = True
                except ValueError as exc:
                    for instrument_id in frames:
                        result.failed[instrument_id] = str(exc)
                        failed.add(instrument_id)
                        plans[instrument_id].clear()
                        coverage_ready.pop(instrument_id, None)
            for instrument_id, (chunk_start, covered_to) in coverage_ready.items():
                meta.extend_coverage(instrument_id, dataset, chunk_start, covered_to)

        for instrument_id in sorted(planned - failed):
            (result.fetched if wrote_any[instrument_id] else result.skipped).append(
                instrument_id
            )
        processed += len(group)
        if processed % 10 < len(group) or processed == len(targets):
            log.info(
                "%s backfill: %d/%d (%s)",
                dataset,
                processed,
                len(targets),
                result.summary(),
            )
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
    """Rebuild canonical instrument coverage from active v2 Parquet only."""
    require_canonical_generation(bars, meta.storage_generation())
    from marketdata.reconcile import reconcile_canonical

    return reconcile_canonical(bars, meta).counts
