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
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Any

import polars as pl

from marketdata.calendar import (
    IEX_ROW_CAP,
    IntradayRequestChunk,
    next_session_after,
    plan_intraday_requests,
    weekend_only,
)
from marketdata.eod_quality import eod_ohlc_invalid_reason
from marketdata.errors import BudgetExhausted
from marketdata.locking import data_directory_locked
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import (
    eod_frame,
    instrument_bucket,
    intraday_frame,
    require_canonical_generation,
    require_intraday_freq,
)
from marketdata.tiingo import (
    ResponseReservationExceeded,
    TiingoClient,
    TiingoError,
    TiingoNotFoundError,
)

log = logging.getLogger(__name__)

# Nightly updates refetch this many days before the coverage edge to pick up
# corrections and restated adjustments.
REFRESH_WINDOW_DAYS = 7

# An empty response marks a range covered only if the range ends at least
# this many days in the past (EOD corrections can land through the evening;
# weekends/holidays add slack).
PUBLICATION_LAG_DAYS = 5
INTRADAY_PUBLICATION_LAG_DAYS = 1

DEFAULT_INTRADAY_FREQ = "1hour"
DEFAULT_EOD_HISTORY_START = date(2000, 1, 3)
IEX_HISTORY_START = date(2016, 12, 12)


@dataclass(frozen=True)
class IngestTarget:
    """One stable bar owner plus the Tiingo identifier used for transport.

    The identifier is deliberately separate from ``instrument_id``.  Callers
    must obtain it from exact-dataset identity evidence. Operator-facing paths
    do so through the validated request-segment orchestration in this module.
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
    blocked: dict[str, str] = field(default_factory=dict)  # terminal exclusions
    segments: list[dict[str, Any]] = field(default_factory=list)

    @property
    def partial(self) -> bool:
        """Whether at least one requested segment was excluded or failed."""
        return bool(self.failed or self.blocked)

    @property
    def ok(self) -> bool:
        return not self.partial

    def summary(self) -> str:
        parts = [
            f"{len(self.fetched)} fetched",
            f"{len(self.skipped)} up-to-date",
            f"{len(self.failed)} failed",
            f"{len(self.blocked)} blocked",
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
            "blocked": dict(sorted(self.blocked.items())),
            "segments": self.segments,
            "ok": self.ok,
        }

    def record_instrument_outcome(self, instrument_id: str, outcome: str) -> None:
        """Record one best successful outcome per instrument.

        Detailed per-segment outcomes remain in ``segments``.  The top-level
        lists are instrument summaries with precedence refreshed > fetched >
        skipped, so multi-evidence instruments are never double-counted.
        """
        ranks = {"skipped": 0, "fetched": 1, "refreshed": 2}
        current = next(
            (
                name
                for name in ("refreshed", "fetched", "skipped")
                if instrument_id in getattr(self, name)
            ),
            None,
        )
        if current is not None and ranks[current] >= ranks[outcome]:
            return
        for name in ranks:
            values = getattr(self, name)
            while instrument_id in values:
                values.remove(instrument_id)
        getattr(self, outcome).append(instrument_id)


@dataclass(frozen=True)
class ValidatedRequestSegment:
    """One request unit authorized by alias and exact-dataset evidence."""

    ticker: str
    dataset_key: str
    start: date
    end: date
    status: str
    instrument_ids: tuple[str, ...] = ()
    alias_ids: tuple[int, ...] = ()
    instrument_id: str | None = None
    identifier_type: str | None = None
    identifier_value: str | None = None
    vendor_identifier_ids: tuple[int, ...] = ()
    conflicting_instrument_ids: tuple[str, ...] = ()
    detail: str = ""

    @property
    def key(self) -> str:
        return f"{self.ticker}:{self.start.isoformat()}..{self.end.isoformat()}"

    def to_dict(self, *, outcome: str | None = None) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "dataset_key": self.dataset_key,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "status": outcome or self.status,
            "instrument_ids": list(self.instrument_ids),
            "alias_ids": list(self.alias_ids),
            "instrument_id": self.instrument_id,
            "identifier_type": self.identifier_type,
            "identifier_value": self.identifier_value,
            "vendor_identifier_ids": list(self.vendor_identifier_ids),
            "conflicting_instrument_ids": list(self.conflicting_instrument_ids),
            "detail": self.detail,
        }


def plan_validated_segments(
    meta: MetaStore,
    tickers: Sequence[str],
    dataset_key: str,
    start: date,
    end: date,
) -> list[ValidatedRequestSegment]:
    """Resolve ticker requests into explicit ready and blocked date segments."""
    from marketdata.identity import require_dataset_key

    dataset_key = require_dataset_key(dataset_key)
    if start > end:
        raise ValueError("ingestion start must not be after end")
    planned: list[ValidatedRequestSegment] = []
    for ticker in dict.fromkeys(value.strip().upper() for value in tickers):
        if not ticker:
            raise ValueError("ticker must not be empty")
        alias_report = meta.resolve_alias_range(ticker, start, end)
        for alias_segment in alias_report.segments:
            if alias_segment.status != "resolved":
                weekend_gap = alias_segment.status == "zero_matches" and weekend_only(
                    alias_segment.start, alias_segment.end
                )
                planned.append(
                    ValidatedRequestSegment(
                        ticker=ticker,
                        dataset_key=dataset_key,
                        start=alias_segment.start,
                        end=alias_segment.end,
                        status=(
                            "non_session_gap"
                            if weekend_gap
                            else f"alias_{alias_segment.status}"
                        ),
                        instrument_ids=alias_segment.instrument_ids,
                        alias_ids=alias_segment.alias_ids,
                        detail=(
                            "weekend-only interval has no possible market bars"
                            if weekend_gap
                            else "ticker/date range does not resolve to exactly one instrument"
                        ),
                    )
                )
                continue
            instrument_id = alias_segment.instrument_id
            assert instrument_id is not None
            planned.extend(
                _plan_identifier_segments(
                    meta,
                    ticker=ticker,
                    dataset_key=dataset_key,
                    instrument_id=instrument_id,
                    alias_ids=alias_segment.alias_ids,
                    start=alias_segment.start,
                    end=alias_segment.end,
                )
            )
    return planned


def _plan_identifier_segments(
    meta: MetaStore,
    *,
    ticker: str,
    dataset_key: str,
    instrument_id: str,
    alias_ids: tuple[int, ...],
    start: date,
    end: date,
) -> list[ValidatedRequestSegment]:
    planned: list[ValidatedRequestSegment] = []
    for identifier in meta.resolve_vendor_identifier_range(
        instrument_id, dataset_key, start, end
    ):
        status = (
            "ready"
            if identifier.status == "resolved"
            else f"identifier_{identifier.status}"
        )
        detail = ""
        if status == "ready" and identifier.identifier_type.lower() == "ticker":
            if identifier.identifier_value.upper() != ticker:
                status = "identifier_alias_mismatch"
                detail = "bare-ticker identifier does not match the requested alias"
        if status != "ready" and not detail:
            detail = (
                "no unique validated identifier covers this exact dataset/date range"
            )
        if status == "identifier_zero_matches" and weekend_only(
            identifier.start, identifier.end
        ):
            status = "non_session_gap"
            detail = "weekend-only interval has no possible market bars"
        planned.append(
            ValidatedRequestSegment(
                ticker=ticker,
                dataset_key=dataset_key,
                start=identifier.start,
                end=identifier.end,
                status=status,
                instrument_ids=(instrument_id,),
                alias_ids=alias_ids,
                instrument_id=instrument_id,
                identifier_type=identifier.identifier_type,
                identifier_value=identifier.identifier_value,
                vendor_identifier_ids=identifier.vendor_identifier_ids,
                conflicting_instrument_ids=identifier.conflicting_instrument_ids,
                detail=detail,
            )
        )
    return planned


class _ValidatedSegmentsClient:
    """Narrow a Tiingo client to a batch of pre-authorized request segments."""

    def __init__(
        self,
        client: TiingoClient,
        bars: BarStore,
        meta: MetaStore,
        segments: Sequence[ValidatedRequestSegment],
    ):
        if any(
            segment.status != "ready" or segment.identifier_value is None
            for segment in segments
        ):
            raise ValueError("only ready identity segments may be fetched")
        by_identifier = {segment.identifier_value: segment for segment in segments}
        if len(by_identifier) != len(segments):
            raise ValueError("validated batch contains duplicate request identifiers")
        self._client = client
        self._bars = bars
        self._meta = meta
        self._segments = by_identifier

    def _validate_request(
        self, identifier: str, start: date | str, end: date | str
    ) -> tuple[ValidatedRequestSegment, date, date]:
        request_start = date.fromisoformat(str(start))
        request_end = date.fromisoformat(str(end))
        segment = self._segments.get(identifier)
        if segment is None:
            raise TiingoError("request identifier differs from validated evidence")
        if (
            request_start < segment.start
            or request_end > segment.end
            or request_start > request_end
        ):
            raise TiingoError(
                "request falls outside validated identity segment "
                f"{segment.start}..{segment.end}"
            )
        return segment, request_start, request_end

    def _validate_rows(
        self,
        segment: ValidatedRequestSegment,
        rows: list[dict[str, Any]],
        request_start: date,
        request_end: date,
        *,
        context_end: date | None = None,
    ) -> list[dict[str, Any]]:
        for index, row in enumerate(rows):
            raw_timestamp = row.get("date")
            try:
                row_date = date.fromisoformat(str(raw_timestamp)[:10])
            except (TypeError, ValueError) as exc:
                raise TiingoError(
                    f"response row {index} has an invalid timestamp"
                ) from exc
            if not request_start <= row_date <= request_end:
                raise TiingoError(
                    f"response row {index} timestamp {row_date} falls outside "
                    f"request {request_start}..{request_end}"
                )
            in_identity_envelope = segment.start <= row_date <= segment.end
            in_context_lookahead = (
                context_end is not None and segment.end < row_date <= context_end
            )
            if not in_identity_envelope and not in_context_lookahead:
                raise TiingoError(
                    f"response row {index} timestamp {row_date} falls outside "
                    "the validated instrument envelope"
                )
            metadata_key = segment.identifier_type
            metadata_matches = True
            if metadata_key in row:
                actual = str(row[metadata_key])
                expected = str(segment.identifier_value)
                metadata_matches = (
                    actual.upper() == expected.upper()
                    if metadata_key.lower() == "ticker"
                    else actual == expected
                )
            if not metadata_matches:
                raise TiingoError(
                    f"response row {index} {metadata_key} conflicts with "
                    "validated identity evidence"
                )
        if segment.dataset_key != "eod":
            return rows
        accepted: list[dict[str, Any]] = []
        rejected: list[tuple[Mapping[str, Any], str]] = []
        for row in rows:
            reason = eod_ohlc_invalid_reason(row)
            if reason is None:
                accepted.append(row)
            else:
                rejected.append((row, reason))
        quarantine_path = self._bars.quarantine_eod_response_rows(
            segment.instrument_id or "",
            segment.ticker,
            rejected,
        )
        if quarantine_path is not None:
            log.warning(
                "quarantined %d invalid EOD response row(s) for %s at %s",
                len(rejected),
                segment.instrument_id,
                quarantine_path,
            )
        return accepted

    def eod(
        self,
        identifier: str,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> list[dict[str, Any]]:
        if start is None or end is None:
            raise TiingoError("EOD request does not match the validated dataset/span")
        segment, request_start, request_end = self._validate_request(
            identifier, start, end
        )
        if segment.dataset_key != "eod":
            raise TiingoError("EOD request does not match the validated dataset/span")
        rows = self._client.eod(identifier, request_start, request_end)
        return self._validate_rows(segment, rows, request_start, request_end)

    def full_refresh_eod(
        self,
        identifier: str,
        start: date | str,
        end: date | str,
    ) -> list[dict[str, Any]]:
        """Authorize a wider snapshot only for a stable non-ticker identifier."""
        segment = self._segments.get(identifier)
        if segment is None or segment.dataset_key != "eod":
            raise TiingoError("full refresh identifier is not authorized for EOD")
        request_start = date.fromisoformat(str(start))
        request_end = date.fromisoformat(str(end))
        if request_start > request_end:
            raise TiingoError("full refresh start is after its end")
        resolution = self._meta.resolve_vendor_identifier(
            segment.instrument_id or "", "eod", request_start, request_end
        )
        alias_authorized = self._meta.instrument_aliases_cover_range(
            segment.instrument_id or "", request_start, request_end
        )
        if segment.identifier_type.lower() == "ticker":
            alias_report = self._meta.resolve_alias_range(
                identifier, request_start, request_end
            )
            alias_authorized = alias_authorized and all(
                alias_segment.status == "resolved"
                and alias_segment.instrument_id == segment.instrument_id
                for alias_segment in alias_report.segments
            )
        if (
            resolution.status != "resolved"
            or resolution.identifier_type != segment.identifier_type
            or resolution.identifier_value != identifier
            or not alias_authorized
        ):
            raise TiingoError(
                "full-history refresh lacks identifier and alias evidence for "
                f"{request_start}..{request_end}"
            )
        rows = self._client.eod(identifier, request_start, request_end)
        expanded = replace(segment, start=request_start, end=request_end)
        return self._validate_rows(expanded, rows, request_start, request_end)

    def intraday(
        self,
        identifier: str,
        start: date | str,
        end: date | str | None = None,
        freq: str = "1hour",
    ) -> list[dict[str, Any]]:
        if end is None:
            raise TiingoError(
                "intraday request does not match the validated exact dataset key"
            )
        request_start = date.fromisoformat(str(start))
        request_end = date.fromisoformat(str(end))
        segment = self._segments.get(identifier)
        if (
            segment is None
            or request_start < segment.start
            or request_start > request_end
        ):
            raise TiingoError(
                "intraday request falls outside validated identity evidence"
            )
        if segment.dataset_key != f"intraday_{freq}":
            raise TiingoError(
                "intraday request does not match the validated exact dataset key"
            )
        context_end = None
        if request_end > segment.end:
            allowed_context_end = next_session_after(segment.end)
            if request_end > allowed_context_end:
                raise TiingoError(
                    "intraday request extends beyond the one-session "
                    f"context lookahead ending {allowed_context_end}"
                )
            context_end = request_end
        rows = self._client.intraday(identifier, request_start, request_end, freq=freq)
        return self._validate_rows(
            segment,
            rows,
            request_start,
            request_end,
            context_end=context_end,
        )


def _require_registered_targets(meta: MetaStore, targets: list[IngestTarget]) -> None:
    instrument_ids = [target.instrument_id for target in targets]
    if len(instrument_ids) != len(set(instrument_ids)):
        raise ValueError("ingestion targets must have unique instrument_ids")
    unknown = set(instrument_ids) - meta.instrument_ids()
    if unknown:
        raise ValueError(
            f"ingestion targets contain unknown instruments: {sorted(unknown)}"
        )


def _validated_target_ranges(
    targets: Sequence[IngestTarget],
    start: date,
    end: date,
    ranges: Mapping[str, tuple[date, date]] | None,
) -> dict[str, tuple[date, date]]:
    instrument_ids = {target.instrument_id for target in targets}
    if ranges is not None:
        unknown = set(ranges) - instrument_ids
        if unknown:
            raise ValueError(f"ranges contain unknown targets: {sorted(unknown)}")
    resolved = {
        target.instrument_id: (
            ranges[target.instrument_id]
            if ranges is not None and target.instrument_id in ranges
            else (start, end)
        )
        for target in targets
    }
    invalid = {
        instrument_id: requested
        for instrument_id, requested in resolved.items()
        if requested[0] > requested[1]
    }
    if invalid:
        raise ValueError(f"ingestion start must not be after end: {invalid}")
    return resolved


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
    refresh = getattr(client, "full_refresh_eod", client.eod)
    rows = refresh(target.identifier, first, end)
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


@data_directory_locked("ingest:eod-backfill")
def backfill_eod(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    targets: list[IngestTarget],
    start: date,
    end: date | None = None,
    *,
    force: bool = False,
    ranges: Mapping[str, tuple[date, date]] | None = None,
) -> IngestResult:
    """Fetch daily history for stable instruments, including
    missing leading history before existing coverage."""
    require_canonical_generation(bars, meta.storage_generation())
    _require_registered_targets(meta, targets)
    today = date.today()
    end = end or today
    requested_ranges = _validated_target_ranges(targets, start, end, ranges)
    result = IngestResult()
    processed = 0
    for group in _bucket_groups(targets):
        frames: dict[str, pl.DataFrame] = {}
        replacements: set[str] = set()
        ready: dict[str, tuple[date, date, str]] = {}
        interrupted: BudgetExhausted | ResponseReservationExceeded | None = None
        for target in group:
            instrument_id = target.instrument_id
            target_start, target_end = requested_ranges[instrument_id]
            try:
                existing_coverage = meta.get_coverage(instrument_id, "eod")
                covered = None if force else existing_coverage
                segments = _missing_segments((target_start, target_end), covered)
                if not segments:
                    result.skipped.append(instrument_id)
                    continue
                got_rows = False
                new_first = existing_coverage[0] if existing_coverage else None
                new_last = existing_coverage[1] if existing_coverage else None
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
                        if existing_coverage is not None and _has_new_corp_action(
                            frame, existing_coverage[1]
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
                        min(new_first, target_start),
                        existing_coverage[1],
                        target_end,
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
            except (BudgetExhausted, ResponseReservationExceeded) as exc:
                interrupted = exc
                break
            except TiingoNotFoundError as exc:
                log.warning("eod backfill blocked for %s: %s", instrument_id, exc)
                result.blocked[instrument_id] = str(exc)
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

        if interrupted is not None:
            interrupted.partial_ingest = result  # type: ignore[attr-defined]
            raise interrupted

        processed += len(group)
        if processed % 25 < len(group) or processed == len(targets):
            log.info(
                "eod backfill: %d/%d (%s)", processed, len(targets), result.summary()
            )
    return result


@data_directory_locked("ingest:eod-update")
def update_eod(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    targets: list[IngestTarget],
    *,
    default_start: date = DEFAULT_EOD_HISTORY_START,
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
            except TiingoNotFoundError as exc:
                log.warning("eod update blocked for %s: %s", instrument_id, exc)
                result.blocked[instrument_id] = str(exc)
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
            result.blocked.update(sub.blocked)

        processed += len(group)
        if processed % 25 < len(group) or processed == len(targets):
            log.info(
                "eod update: %d/%d (%s)", processed, len(targets), result.summary()
            )
    return result


@data_directory_locked("ingest:intraday-backfill")
def backfill_intraday(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    targets: list[IngestTarget],
    start: date,
    end: date | None = None,
    *,
    freq: str = DEFAULT_INTRADAY_FREQ,
    ranges: Mapping[str, tuple[date, date]] | None = None,
    force: bool = False,
) -> IngestResult:
    """Fetch intraday bars in chunks to cover [start, end], leading segments
    included. Note: Tiingo's IEX feed reaches back a bounded number of years,
    is unadjusted, and reports IEX-only volume."""
    require_intraday_freq(freq)
    require_canonical_generation(bars, meta.storage_generation())
    _require_registered_targets(meta, targets)
    today = date.today()
    end = end or today
    requested_ranges = _validated_target_ranges(targets, start, end, ranges)
    dataset = f"intraday_{freq}"
    result = IngestResult()
    processed = 0
    for group in _bucket_groups(targets):
        plans: dict[str, list[IntradayRequestChunk]] = {}
        targets_by_id = {target.instrument_id: target for target in group}
        wrote_any = dict.fromkeys(targets_by_id, False)
        failed: set[str] = set()
        planned: set[str] = set()
        interrupted: BudgetExhausted | ResponseReservationExceeded | None = None
        interrupted_instrument: str | None = None
        completed_calls: set[str] = set()
        for target in group:
            target_start, target_end = requested_ranges[target.instrument_id]
            existing_coverage = meta.get_coverage(target.instrument_id, dataset)
            covered = None if force else existing_coverage
            segments = _missing_segments((target_start, target_end), covered)
            plan: list[IntradayRequestChunk] = []
            for seg_start, seg_end in segments:
                leading = covered is not None and seg_end < covered[0]
                plan.extend(
                    plan_intraday_requests(
                        seg_start, seg_end, freq=freq, reverse=leading
                    )
                )
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
                chunk = plan.pop(0)
                target = targets_by_id[instrument_id]
                try:
                    rows = client.intraday(
                        target.identifier, chunk.start, chunk.fetch_end, freq=freq
                    )
                    rows = intraday_target_rows(rows, chunk)
                    max_received = None
                    if rows:
                        frame = intraday_frame(target.identifier, rows)
                        frames[instrument_id] = bars.canonicalize_intraday(
                            instrument_id, frame
                        )
                        max_received = frame["ts"].dt.date().max()
                    covered_to = _covered_through(
                        max_received,
                        chunk.end,
                        today,
                        INTRADAY_PUBLICATION_LAG_DAYS,
                    )
                    if covered_to is not None:
                        covered_to = min(covered_to, today - timedelta(days=1))
                    if covered_to is not None and covered_to >= chunk.start:
                        coverage_ready[instrument_id] = (chunk.start, covered_to)
                    completed_calls.add(instrument_id)
                except (BudgetExhausted, ResponseReservationExceeded) as exc:
                    interrupted = exc
                    interrupted_instrument = instrument_id
                    break
                except TiingoNotFoundError as exc:
                    log.warning(
                        "%s backfill blocked for %s: %s",
                        dataset,
                        instrument_id,
                        exc,
                    )
                    result.blocked[instrument_id] = str(exc)
                    failed.add(instrument_id)
                    plan.clear()
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

            if interrupted is not None:
                for instrument_id in sorted(completed_calls - failed):
                    if instrument_id == interrupted_instrument:
                        continue
                    if not plans[instrument_id]:
                        (
                            result.fetched
                            if wrote_any[instrument_id]
                            else result.skipped
                        ).append(instrument_id)
                interrupted.partial_ingest = result  # type: ignore[attr-defined]
                raise interrupted

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


def intraday_target_rows(
    rows: list[dict[str, Any]], chunk: IntradayRequestChunk
) -> list[dict[str, Any]]:
    """Validate one IEX response envelope and return only target rows."""
    if len(rows) >= IEX_ROW_CAP:
        raise ValueError(
            f"IEX response has {len(rows):,} rows and may be silently truncated"
        )
    if len(rows) > chunk.max_response_rows:
        raise ValueError(
            f"IEX response has {len(rows):,} rows, exceeding the planner envelope "
            f"of {chunk.max_response_rows:,}"
        )
    target: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        try:
            row_date = date.fromisoformat(str(row.get("date"))[:10])
        except ValueError as exc:
            raise ValueError(
                f"IEX response row {index} has an invalid timestamp"
            ) from exc
        if not chunk.start <= row_date <= chunk.fetch_end:
            raise ValueError(
                f"IEX response row {index} timestamp {row_date} falls outside "
                f"request {chunk.start}..{chunk.fetch_end}"
            )
        if row_date <= chunk.end:
            target.append(row)
    return target


def _record_segment_result(
    result: IngestResult,
    segment: ValidatedRequestSegment,
    sub_result: IngestResult,
) -> None:
    """Merge one low-level operation without losing its request identity."""
    instrument_id = segment.instrument_id
    assert instrument_id is not None
    if instrument_id in sub_result.blocked:
        detail = sub_result.blocked[instrument_id]
        _record_blocked_segment(result, segment, detail)
        return
    if instrument_id in sub_result.failed:
        detail = sub_result.failed[instrument_id]
        result.failed[segment.key] = detail
        result.segments.append({**segment.to_dict(outcome="failed"), "detail": detail})
        return
    if instrument_id in sub_result.refreshed:
        outcome = "refreshed"
    elif instrument_id in sub_result.fetched:
        outcome = "fetched"
    else:
        outcome = "skipped"
    result.record_instrument_outcome(instrument_id, outcome)
    result.segments.append(segment.to_dict(outcome=outcome))


def _record_blocked_segment(
    result: IngestResult, segment: ValidatedRequestSegment, detail: str | None = None
) -> None:
    reason = detail or segment.detail or segment.status
    result.blocked[segment.key] = reason
    result.segments.append({**segment.to_dict(outcome="blocked"), "detail": reason})


def _record_unattempted_failure(
    result: IngestResult, segment: ValidatedRequestSegment, detail: str
) -> None:
    result.failed[segment.key] = detail
    result.segments.append({**segment.to_dict(outcome="failed"), "detail": detail})


def _record_skipped_segment(
    result: IngestResult, segment: ValidatedRequestSegment
) -> None:
    if segment.instrument_id is not None:
        result.record_instrument_outcome(segment.instrument_id, "skipped")
    result.segments.append(segment.to_dict(outcome="skipped"))


def _adjacent_to_coverage(
    segment: ValidatedRequestSegment, covered: tuple[date, date]
) -> bool:
    first, last = covered
    if segment.end >= first and segment.start <= last:
        return True
    if segment.start > last:
        return weekend_only(last + timedelta(days=1), segment.start - timedelta(days=1))
    return weekend_only(segment.end + timedelta(days=1), first - timedelta(days=1))


def _execute_validated_segments(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    segments: Sequence[ValidatedRequestSegment],
    *,
    force: bool = False,
    update: bool = False,
) -> IngestResult:
    """Execute ready segments without ever bridging a blocked coverage gap."""
    result = IngestResult()
    pending: dict[tuple[str, str], list[ValidatedRequestSegment]] = {}
    for segment in segments:
        if segment.status == "ready":
            owner = (segment.instrument_id or "", segment.dataset_key)
            pending.setdefault(owner, []).append(segment)
        elif segment.status in {"inactive", "non_session_gap"}:
            _record_skipped_segment(result, segment)
        else:
            _record_blocked_segment(result, segment)

    # Work newest-to-oldest when an instrument has no edge yet.  Thereafter
    # only an overlapping or adjacent unit is eligible, preserving the single
    # contiguous coverage interval even when identity evidence has a gap.
    for owned in pending.values():
        owned.sort(key=lambda item: (item.end, item.start), reverse=True)
    coverage = {owner: meta.get_coverage(*owner) for owner in pending}
    while pending:
        selected: list[ValidatedRequestSegment] = []
        for owner in sorted(pending):
            owned = pending[owner]
            covered = coverage[owner]
            selected_index = (
                0
                if covered is None
                else next(
                    (
                        index
                        for index, segment in enumerate(owned)
                        if _adjacent_to_coverage(segment, covered)
                    ),
                    None,
                )
            )
            if selected_index is None:
                continue
            selected.append(owned.pop(selected_index))

        if not selected:
            break
        grouped: dict[str, list[ValidatedRequestSegment]] = {}
        for segment in selected:
            grouped.setdefault(segment.dataset_key, []).append(segment)
        failed_owners: set[tuple[str, str]] = set()
        blocked_owners: set[tuple[str, str]] = set()
        for dataset_key, dataset_batch in sorted(grouped.items()):
            # The transport request carries only the identifier value.  Keep
            # same-value cross-type collisions in separate batches so lookup
            # remains unambiguous without sacrificing normal bucket batching.
            batches: list[list[ValidatedRequestSegment]] = []
            identifier_occurrences: dict[str, int] = {}
            for segment in sorted(
                dataset_batch,
                key=lambda item: (
                    item.identifier_value or "",
                    item.identifier_type or "",
                    item.instrument_id or "",
                ),
            ):
                identifier = segment.identifier_value or ""
                lane = identifier_occurrences.get(identifier, 0)
                identifier_occurrences[identifier] = lane + 1
                if lane == len(batches):
                    batches.append([])
                batches[lane].append(segment)
            for batch in batches:
                segment_start = min(segment.start for segment in batch)
                segment_end = max(segment.end for segment in batch)
                ranges = {
                    segment.instrument_id or "": (segment.start, segment.end)
                    for segment in batch
                }
                targets = [
                    IngestTarget(
                        segment.instrument_id or "", segment.identifier_value or ""
                    )
                    for segment in batch
                ]
                validated_client = _ValidatedSegmentsClient(client, bars, meta, batch)
                try:
                    if dataset_key == "eod":
                        sub_result = backfill_eod(
                            validated_client,  # type: ignore[arg-type]
                            bars,
                            meta,
                            targets,
                            segment_start,
                            segment_end,
                            force=True if update else force,
                            ranges=ranges,
                        )
                    else:
                        freq = dataset_key.removeprefix("intraday_")
                        sub_result = backfill_intraday(
                            validated_client,  # type: ignore[arg-type]
                            bars,
                            meta,
                            targets,
                            segment_start,
                            segment_end,
                            freq=freq,
                            ranges=ranges,
                            force=True if update else force,
                        )
                except (BudgetExhausted, ResponseReservationExceeded) as exc:
                    sub_result = getattr(exc, "partial_ingest", None) or IngestResult()
                    successful = set(
                        sub_result.fetched
                        + sub_result.skipped
                        + sub_result.refreshed
                        + list(sub_result.failed)
                        + list(sub_result.blocked)
                    )
                    for segment in batch:
                        if segment.instrument_id in successful:
                            _record_segment_result(result, segment, sub_result)
                    exc.partial_ingest = result  # type: ignore[attr-defined]
                    raise
                for segment in batch:
                    _record_segment_result(result, segment, sub_result)
                    owner = (segment.instrument_id or "", segment.dataset_key)
                    if segment.instrument_id in sub_result.failed:
                        failed_owners.add(owner)
                    elif segment.instrument_id in sub_result.blocked:
                        blocked_owners.add(owner)
                    else:
                        coverage[owner] = meta.get_coverage(*owner)

        for owner in failed_owners:
            detail = "not attempted after another segment for this instrument failed"
            for segment in pending.pop(owner, []):
                _record_unattempted_failure(result, segment, detail)
            coverage.pop(owner, None)
        for owner in blocked_owners:
            detail = (
                "not attempted after another segment for this instrument was "
                "terminally blocked"
            )
            for segment in pending.pop(owner, []):
                _record_blocked_segment(result, segment, detail)
            coverage.pop(owner, None)
        for owner in [owner for owner, owned in pending.items() if not owned]:
            pending.pop(owner)
            coverage.pop(owner, None)

    for owned in pending.values():
        for segment in owned:
            _record_blocked_segment(
                result,
                segment,
                "validated segment is not adjacent to stored coverage; an identity "
                "gap must be resolved before coverage can advance across it",
            )
    return result


@data_directory_locked("ingest:validated-eod-backfill")
def backfill_eod_validated(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    tickers: Sequence[str],
    start: date,
    end: date | None = None,
    *,
    force: bool = False,
) -> IngestResult:
    """Identity-plan and ingest EOD ticker ranges, reporting blocked slices."""
    require_canonical_generation(bars, meta.storage_generation())
    end = end or date.today()
    segments = plan_validated_segments(meta, tickers, "eod", start, end)
    return _execute_validated_segments(client, bars, meta, segments, force=force)


@data_directory_locked("ingest:validated-intraday-backfill")
def backfill_intraday_validated(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    tickers: Sequence[str],
    start: date,
    end: date | None = None,
    *,
    freq: str = DEFAULT_INTRADAY_FREQ,
    force: bool = False,
) -> IngestResult:
    """Identity-plan and ingest one exact IEX frequency."""
    require_intraday_freq(freq)
    require_canonical_generation(bars, meta.storage_generation())
    end = end or date.today()
    segments = plan_validated_segments(meta, tickers, f"intraday_{freq}", start, end)
    return _execute_validated_segments(client, bars, meta, segments, force=force)


@data_directory_locked("ingest:validated-intraday-update")
def update_intraday_validated(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    tickers: Sequence[str],
    *,
    freq: str = DEFAULT_INTRADAY_FREQ,
    default_start: date = IEX_HISTORY_START,
) -> IngestResult:
    """Refresh current exact-frequency IEX data through identity evidence."""
    require_intraday_freq(freq)
    require_canonical_generation(bars, meta.storage_generation())
    dataset_key = f"intraday_{freq}"
    segments = _plan_current_update_segments(
        meta, tickers, dataset_key, default_start=default_start
    )
    return _execute_validated_segments(client, bars, meta, segments, update=True)


@data_directory_locked("ingest:validated-eod-update")
def update_eod_validated(
    client: TiingoClient,
    bars: BarStore,
    meta: MetaStore,
    tickers: Sequence[str],
    *,
    default_start: date = DEFAULT_EOD_HISTORY_START,
) -> IngestResult:
    """Update each ticker's current alias through an exact EOD identity plan."""
    require_canonical_generation(bars, meta.storage_generation())
    segments = _plan_current_update_segments(
        meta, tickers, "eod", default_start=default_start
    )
    return _execute_validated_segments(client, bars, meta, segments, update=True)


def _plan_current_update_segments(
    meta: MetaStore,
    tickers: Sequence[str],
    dataset_key: str,
    *,
    default_start: date,
) -> list[ValidatedRequestSegment]:
    """Plan current aliases once, then validate only their exact identifiers."""
    today = date.today()
    segments: list[ValidatedRequestSegment] = []
    for ticker in tickers:
        normalized = ticker.strip().upper()
        if not normalized:
            raise ValueError("ticker must not be empty")
        alias_report = meta.resolve_alias_range(normalized, date.min, today)
        current = alias_report.segments[-1]
        if current.instrument_id is None:
            latest_resolved = next(
                (
                    segment
                    for segment in reversed(alias_report.segments[:-1])
                    if segment.instrument_id is not None
                ),
                None,
            )
            if current.status == "zero_matches" and latest_resolved is not None:
                segments.append(
                    ValidatedRequestSegment(
                        ticker=normalized,
                        dataset_key=dataset_key,
                        start=today,
                        end=today,
                        status="inactive",
                        instrument_ids=latest_resolved.instrument_ids,
                        alias_ids=latest_resolved.alias_ids,
                        instrument_id=latest_resolved.instrument_id,
                        detail="ticker has no alias active today",
                    )
                )
                continue
            weekend_gap = current.status == "zero_matches" and weekend_only(
                today, today
            )
            segments.append(
                ValidatedRequestSegment(
                    ticker=normalized,
                    dataset_key=dataset_key,
                    start=today,
                    end=today,
                    status=(
                        "non_session_gap" if weekend_gap else f"alias_{current.status}"
                    ),
                    instrument_ids=current.instrument_ids,
                    alias_ids=current.alias_ids,
                    detail=(
                        "weekend-only interval has no possible market bars"
                        if weekend_gap
                        else "ticker/date range does not resolve to exactly one instrument"
                    ),
                )
            )
            continue
        covered = meta.get_coverage(current.instrument_id, dataset_key)
        request_start = (
            max(current.start, covered[1] - timedelta(days=REFRESH_WINDOW_DAYS))
            if covered is not None
            else max(default_start, current.start)
        )
        segments.extend(
            _plan_identifier_segments(
                meta,
                ticker=normalized,
                dataset_key=dataset_key,
                instrument_id=current.instrument_id,
                alias_ids=current.alias_ids,
                start=request_start,
                end=today,
            )
        )
    return segments


@data_directory_locked("reconcile:canonical")
def reconcile(bars: BarStore, meta: MetaStore) -> dict[str, int]:
    """Rebuild canonical instrument coverage from active v2 Parquet only."""
    require_canonical_generation(bars, meta.storage_generation())
    from marketdata.reconcile import reconcile_canonical

    return reconcile_canonical(bars, meta).counts
