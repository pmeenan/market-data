"""Durable, budget-aware breadth-first historical ingestion.

The canonical coverage interval remains the source of truth for published
bars.  This module persists only operational facts that coverage cannot
reconstruct: the immutable cohort, one-turn-per-instrument sweep cursor,
attempt outcomes, and transport request/byte usage.
"""

from __future__ import annotations

import hashlib
from calendar import monthrange
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

from marketdata.budget import tiingo_billing_date, tiingo_billing_month_start
from marketdata.calendar import (
    latest_completed_session,
    plan_intraday_requests,
    weekend_only,
)
from marketdata.errors import QUOTA_STOP_REASONS, BudgetExhausted
from marketdata.identity import DATASET_KEYS, merge_closed_date_ranges
from marketdata.ingest import (
    IngestResult,
    ValidatedRequestSegment,
    _execute_validated_segments,
    plan_validated_segments,
    update_eod_validated,
    update_intraday_validated,
)
from marketdata.jsonutil import canonical_json
from marketdata.locking import (
    DataDirectoryLock,
    coordinated_data_directory,
    data_directory_locked,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import instrument_bucket
from marketdata.tiingo import (
    RequestAttemptObserver,
    ResponseReservationExceeded,
    TiingoClient,
)

HOURLY_REQUEST_LIMIT = 10_000
DAILY_REQUEST_LIMIT = 100_000
TOTAL_BYTE_LIMIT = 40_000_000_000
HISTORICAL_BYTE_LIMIT = 30_000_000_000
HISTORICAL_BYTE_LIMIT_MAX = 39_000_000_000
HISTORICAL_LIMIT_RAMP_DAYS = 7

# Tiingo documents its monthly bandwidth reset as midnight EST on the first of
# each month. Its ledger still does not define whether encoded or decoded bytes
# are charged (RE-006), but current bar responses are uncompressed so the two
# observable sizes match. A reservation far above measured maximum payloads
# protects the remaining uncertainty.
RESPONSE_RESERVATION_BYTES = 64_000_000

# Current responses can lag or fail transiently after the regular session.  The
# production driver runs one sweep every six minutes, so forty unsuccessful
# retry turns retain a target for roughly four hours without allowing one
# unavailable instrument to pin the immutable cycle forever.
CURRENT_RETRY_ATTEMPTS = 40


class TiingoLike(Protocol):
    def eod(self, ticker: str, start: date, end: date) -> list[dict[str, Any]]: ...

    def intraday(
        self, ticker: str, start: date, end: date, freq: str = "1hour"
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class CurrentJobMember:
    """One intended stable owner in a frozen current-collection cohort."""

    ticker: str
    instrument_id: str | None = None


@dataclass(frozen=True)
class BudgetPolicy:
    hourly_request_limit: int = HOURLY_REQUEST_LIMIT
    daily_request_limit: int = DAILY_REQUEST_LIMIT
    total_byte_limit: int = TOTAL_BYTE_LIMIT
    historical_byte_limit: int = HISTORICAL_BYTE_LIMIT
    historical_byte_limit_max: int | None = None
    historical_limit_ramp_days: int = HISTORICAL_LIMIT_RAMP_DAYS
    response_reservation_bytes: int = RESPONSE_RESERVATION_BYTES

    def __post_init__(self) -> None:
        limits = (
            self.hourly_request_limit,
            self.daily_request_limit,
            self.total_byte_limit,
            self.historical_byte_limit,
            self.historical_limit_ramp_days,
        )
        if any(value <= 0 for value in limits):
            raise ValueError("budget policy limits must be positive")
        if self.response_reservation_bytes < 0:
            raise ValueError("response reservation must not be negative")
        historical_max = self.effective_historical_byte_limit_max
        if historical_max < self.historical_byte_limit:
            raise ValueError(
                "late-month historical limit must not be below its base limit"
            )
        if historical_max > self.total_byte_limit:
            raise ValueError(
                "late-month historical limit must not exceed the total byte limit"
            )
        if self.historical_limit_ramp_days < 2:
            raise ValueError("historical limit ramp must span at least two days")

    @property
    def effective_historical_byte_limit_max(self) -> int:
        """Late-month maximum, defaulting to the caller's base fixture limit."""
        return (
            self.historical_byte_limit
            if self.historical_byte_limit_max is None
            else self.historical_byte_limit_max
        )

    def historical_total_byte_limit(self, now: datetime) -> int:
        """Total billing-month usage at which new historical work must stop.

        The base limit applies until the final seven Tiingo billing dates. The
        limit then rises in equal daily steps and reaches its maximum on the
        final day. Current work is not subject to this admission limit and may
        continue to the separate total ceiling.
        """
        if now.tzinfo is None:
            raise ValueError("budget timestamps must be timezone-aware")
        today = tiingo_billing_date(now)
        final_day = monthrange(today.year, today.month)[1]
        days_remaining = final_day - today.day
        if days_remaining >= self.historical_limit_ramp_days:
            return self.historical_byte_limit
        step = self.historical_limit_ramp_days - 1 - days_remaining
        increase = self.effective_historical_byte_limit_max - self.historical_byte_limit
        return self.historical_byte_limit + (
            increase * step // (self.historical_limit_ramp_days - 1)
        )

    def billing_month_start(self, now: datetime) -> datetime:
        """Return the UTC instant at which Tiingo's current month began."""
        return tiingo_billing_month_start(now)


DEFAULT_BUDGET_POLICY = BudgetPolicy(
    historical_byte_limit_max=HISTORICAL_BYTE_LIMIT_MAX
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class SchedulerRunResult:
    job_id: str
    job_status: str
    sweep_started: int
    sweep_ended: int
    attempted_units: int = 0
    advanced_units: int = 0
    successful_units: int = 0
    stop_reason: str | None = None
    ingest: IngestResult = field(default_factory=IngestResult)

    @property
    def quota_stopped(self) -> bool:
        return self.stop_reason in QUOTA_STOP_REASONS

    def to_dict(self) -> dict[str, Any]:
        return self.ingest.to_dict() | {
            "scheduler": {
                "job_id": self.job_id,
                "job_status": self.job_status,
                "sweep_started": self.sweep_started,
                "sweep_ended": self.sweep_ended,
                "attempted_units": self.attempted_units,
                "advanced_units": self.advanced_units,
                "successful_units": self.successful_units,
                "stop_reason": self.stop_reason,
                "stopped": self.stop_reason is not None,
                "quota_stopped": self.quota_stopped,
            }
        }


@dataclass
class IngestionCycleResult:
    """Current-first work plus the optional historical sweep that followed."""

    current: IngestResult = field(default_factory=IngestResult)
    history: SchedulerRunResult | None = None
    stop_reason: str | None = None

    @property
    def partial(self) -> bool:
        return self.current.partial or (
            self.history is not None and self.history.ingest.partial
        )

    @property
    def ok(self) -> bool:
        return not self.partial

    def to_dict(self) -> dict[str, Any]:
        return {
            "current": self.current.to_dict(),
            "history": self.history.to_dict() if self.history is not None else None,
            "stop_reason": self.stop_reason,
            "ok": self.ok,
        }


class PersistentAttemptObserver(RequestAttemptObserver):
    """Reserve before transport and settle each attempt immediately after it."""

    def __init__(
        self,
        meta: MetaStore,
        *,
        work_kind: str,
        operation: str,
        policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
        clock: Callable[[], datetime] = _utc_now,
    ):
        self._meta = meta
        self._work_kind = work_kind
        self._operation = operation
        self._policy = policy
        self._clock = clock

    def before_attempt(
        self, path: str = "", params: dict[str, Any] | None = None
    ) -> int:
        now = self._clock()
        attempt_id, reason = self._meta.reserve_request_attempt(
            now=now,
            work_kind=self._work_kind,
            operation=_operation_label(self._operation, path),
            reserved_bytes=self._policy.response_reservation_bytes,
            hourly_request_limit=self._policy.hourly_request_limit,
            daily_request_limit=self._policy.daily_request_limit,
            total_byte_limit=self._policy.total_byte_limit,
            historical_total_byte_limit=(self._policy.historical_total_byte_limit(now)),
            billing_month_start=self._policy.billing_month_start(now),
        )
        if reason is not None:
            raise BudgetExhausted(reason)
        assert attempt_id is not None
        return attempt_id

    def response_byte_limit(self, reservation: Any) -> int:
        """Maximum encoded body covered by the pre-request reservation."""
        return self._policy.response_reservation_bytes

    def can_start_batch(self, attempts: int) -> bool:
        now = self._clock()
        return self._meta.can_start_request_batch(
            now=now,
            work_kind=self._work_kind,
            attempts=attempts,
            reserved_bytes=self._policy.response_reservation_bytes,
            hourly_request_limit=self._policy.hourly_request_limit,
            daily_request_limit=self._policy.daily_request_limit,
            total_byte_limit=self._policy.total_byte_limit,
            historical_total_byte_limit=(self._policy.historical_total_byte_limit(now)),
            billing_month_start=self._policy.billing_month_start(now),
        )

    def after_attempt(
        self,
        reservation: Any,
        observed_bytes: int,
        *,
        complete: bool,
        bytes_known: bool = True,
    ) -> None:
        self._meta.settle_request_attempt(
            int(reservation),
            observed_bytes,
            complete=complete,
            bytes_known=bytes_known,
        )


class _ObservedClientProxy:
    """Make simple offline fakes obey the same per-call accounting contract."""

    def __init__(self, client: TiingoLike, observer: RequestAttemptObserver):
        self._client = client
        self._observer = observer

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        owner = str(args[0]) if args else ""
        reservation = self._observer.before_attempt(f"{method}:{owner}", kwargs)
        before = int(getattr(self._client, "response_bytes", 0))
        complete = False
        try:
            result = getattr(self._client, method)(*args, **kwargs)
            complete = True
            return result
        finally:
            after = int(getattr(self._client, "response_bytes", before))
            self._observer.after_attempt(
                reservation,
                max(0, after - before),
                complete=complete,
                bytes_known=complete,
            )

    def eod(self, ticker: str, start: date, end: date) -> list[dict[str, Any]]:
        return self._call("eod", ticker, start, end)

    def intraday(
        self, ticker: str, start: date, end: date, freq: str = "1hour"
    ) -> list[dict[str, Any]]:
        return self._call("intraday", ticker, start, end, freq=freq)


@contextmanager
def observed_client(
    client: TiingoLike,
    observer: RequestAttemptObserver,
) -> Iterator[TiingoLike]:
    """Attach attempt hooks to the real client, or wrap an offline fake."""
    if isinstance(client, TiingoClient):
        previous = client.set_attempt_observer(observer)
        try:
            yield client
        finally:
            client.set_attempt_observer(previous)
    else:
        yield _ObservedClientProxy(client, observer)


def history_job_id(
    dataset_key: str,
    tickers: Sequence[str],
    start: date,
    end: date | None,
    *,
    phase: int | None = None,
    force: bool = False,
) -> str:
    payload = _history_request_payload(
        dataset_key, tickers, start, end, phase=phase, force=force
    )
    digest = hashlib.sha256(canonical_json(payload).encode()).hexdigest()[:20]
    return f"history-{digest}"


@data_directory_locked("ingest:history-job-resolve")
def resolve_history_job(
    meta: MetaStore,
    *,
    dataset_key: str,
    tickers: Sequence[str],
    start: date,
    end: date | None,
    phase: int | None = None,
    force: bool = False,
    job_id: str | None = None,
    retry_blocked: bool = False,
) -> str:
    """Resolve one CLI/scheduled request to a frozen, initialized job.

    Active jobs resume normally. Terminal ranges remain dormant unless the
    operator explicitly requests a retry after repairing their evidence.
    """
    durable_job_id = job_id or history_job_id(
        dataset_key, tickers, start, end, phase=phase, force=force
    )
    if force and job_id is None:
        # A force invocation is a new operation. Its printed id remains usable
        # with --job-id to resume that exact operation after interruption.
        durable_job_id = f"{durable_job_id}-{uuid4().hex[:12]}"
    existing = meta.history_job(durable_job_id)
    frozen_end = (
        end
        if end is not None
        else date.fromisoformat(existing["range_end"])
        if existing is not None
        else date.today()
    )
    if phase is not None and phase > 1:
        request_payload = _history_request_payload(
            dataset_key,
            tickers,
            start,
            frozen_end,
            phase=phase,
            force=force,
        )
        request_hash = hashlib.sha256(
            canonical_json(request_payload).encode()
        ).hexdigest()
        stop_reason = meta.backfill_program_prerequisite_stop_reason(
            durable_job_id, phase, request_hash=request_hash
        )
        if stop_reason is not None:
            raise ValueError(f"history phase {phase} is not admitted: {stop_reason}")
    initialize_history_job(
        meta,
        job_id=durable_job_id,
        dataset_key=dataset_key,
        tickers=tickers,
        start=start,
        end=frozen_end,
        phase=phase,
        force=force,
        retry_blocked=retry_blocked,
    )
    return durable_job_id


def run_history_request(
    client: TiingoLike,
    bars: BarStore,
    meta: MetaStore,
    *,
    dataset_key: str,
    tickers: Sequence[str],
    start: date,
    end: date | None,
    phase: int | None = None,
    force: bool = False,
    job_id: str | None = None,
    retry_blocked: bool = False,
    policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
    max_units: int | None = None,
    clock: Callable[[], datetime] = _utc_now,
) -> SchedulerRunResult:
    """Resolve a history request, then run it in lock-yielding turns."""
    durable_job_id = resolve_history_job(
        meta,
        dataset_key=dataset_key,
        tickers=tickers,
        start=start,
        end=end,
        phase=phase,
        force=force,
        job_id=job_id,
        retry_blocked=retry_blocked,
    )
    return run_history_sweep(
        client,
        bars,
        meta,
        durable_job_id,
        policy=policy,
        max_units=max_units,
        clock=clock,
    )


def cancel_history_job(meta: MetaStore, job_id: str) -> None:
    """Signal cancellation without waiting for a running history turn.

    This SQLite-only control operation deliberately bypasses the Parquet
    mutation lock. A running sweep observes it after its current durable turn.
    """
    meta.cancel_history_job(job_id)


@data_directory_locked("ingest:history-job-initialize")
def initialize_history_job(
    meta: MetaStore,
    *,
    job_id: str,
    dataset_key: str,
    tickers: Sequence[str],
    start: date,
    end: date,
    phase: int | None = None,
    force: bool = False,
    retry_blocked: bool = False,
) -> None:
    """Snapshot stable owners and their date-ranged aliases deterministically."""
    _require_phase_dataset(phase, dataset_key)
    request_payload = _history_request_payload(
        dataset_key, tickers, start, end, phase=phase, force=force
    )
    request_hash = hashlib.sha256(canonical_json(request_payload).encode()).hexdigest()
    existing = meta.history_job(job_id)
    if existing is not None:
        expected = (
            phase,
            dataset_key,
            start.isoformat(),
            end.isoformat(),
            request_hash,
            int(force),
        )
        actual = (
            existing["phase"],
            existing["dataset_key"],
            existing["range_start"],
            existing["range_end"],
            existing["request_hash"],
            existing["force"],
        )
        if actual != expected:
            raise ValueError(f"history job {job_id!r} already has a different request")
        if retry_blocked:
            if existing["status"] != "blocked":
                raise ValueError(
                    f"history job {job_id!r} is {existing['status']}, not blocked"
                )
            if not meta.reactivate_history_job(job_id):
                reason = (
                    "the job is cancelled"
                    if existing["cancelled"]
                    else "it has no retryable terminal ranges"
                )
                raise ValueError(
                    f"history job {job_id!r} was not reactivated because {reason}"
                )
        return
    ranges_by_instrument: dict[str, list[dict[str, Any]]] = {}
    blocked: list[dict[str, Any]] = []
    for ticker in sorted({value.strip().upper() for value in tickers}):
        if not ticker:
            raise ValueError("history cohort ticker must not be empty")
        report = meta.resolve_alias_range(ticker, start, end)
        for segment in report.segments:
            if segment.status == "resolved":
                assert segment.instrument_id is not None
                ranges_by_instrument.setdefault(segment.instrument_id, []).append(
                    {
                        "ticker": ticker,
                        "start": segment.start,
                        "end": segment.end,
                    }
                )
            elif segment.status == "zero_matches" and weekend_only(
                segment.start, segment.end
            ):
                continue
            else:
                blocked.append(
                    {
                        "ticker": ticker,
                        "start": segment.start,
                        "end": segment.end,
                        "status": f"alias_{segment.status}",
                        "detail": (
                            "ticker/date range does not resolve to exactly one "
                            "stable instrument"
                        ),
                    }
                )

    targets = [
        {
            "instrument_id": instrument_id,
            "ranges": _merge_alias_ranges(ranges_by_instrument[instrument_id]),
        }
        for instrument_id in sorted(
            ranges_by_instrument,
            key=lambda value: (instrument_bucket(value), value),
        )
    ]
    snapshot = {
        "targets": [
            {
                "instrument_id": target["instrument_id"],
                "ranges": [
                    {
                        "ticker": item["ticker"],
                        "start": item["start"].isoformat(),
                        "end": item["end"].isoformat(),
                    }
                    for item in target["ranges"]
                ],
            }
            for target in targets
        ],
        "blocked": [
            item | {"start": item["start"].isoformat(), "end": item["end"].isoformat()}
            for item in blocked
        ],
    }
    cohort_hash = hashlib.sha256(canonical_json(snapshot).encode()).hexdigest()
    meta.create_history_job(
        job_id=job_id,
        phase=phase,
        dataset_key=dataset_key,
        start=start,
        end=end,
        request_hash=request_hash,
        cohort_hash=cohort_hash,
        force=force,
        targets=targets,
        blocked_ranges=blocked,
    )


@data_directory_locked("ingest:current-job-initialize")
def initialize_current_job(
    meta: MetaStore,
    *,
    job_id: str,
    dataset_key: str,
    members: Sequence[CurrentJobMember],
    end: date,
    default_start: date,
    refresh_overlap_days: int,
) -> None:
    """Freeze one overnight dataset sweep at stable-instrument granularity.

    Existing owners start at their durable trailing coverage edge, including
    the correction overlap. This bridges every missed session since the
    historical or prior current run. Owners without coverage start at the
    caller's forward-only boundary (or their later alias start).
    """
    if dataset_key not in DATASET_KEYS:
        raise ValueError(f"invalid current dataset {dataset_key!r}")
    if default_start > end:
        raise ValueError("current default start must not be after end")
    if refresh_overlap_days < 0:
        raise ValueError("current refresh overlap must not be negative")
    normalized = sorted(
        {
            (member.ticker.strip().upper(), member.instrument_id)
            for member in members
            if member.ticker.strip()
        },
        key=lambda item: (item[0], item[1] or ""),
    )
    if not normalized:
        raise ValueError("current job requires a non-empty cohort")
    expected_ids = [instrument_id for _, instrument_id in normalized if instrument_id]
    if len(expected_ids) != len(set(expected_ids)):
        raise ValueError("current job stable instrument ids must be unique")

    targets_by_instrument: dict[str, dict[str, Any]] = {}
    blocked: list[dict[str, Any]] = []
    for preferred_ticker, expected_instrument_id in normalized:
        ticker = preferred_ticker
        if expected_instrument_id is not None:
            active_aliases = [
                row
                for row in meta.instrument_alias_records(expected_instrument_id)
                if date.fromisoformat(str(row["start_date"]))
                <= end
                <= date.fromisoformat(str(row["end_date"]))
            ]
            active_tickers = sorted({str(row["ticker"]) for row in active_aliases})
            if len(active_tickers) == 1:
                ticker = active_tickers[0]
        report = meta.resolve_alias_range(ticker, end, end)
        segment = report.segments[0]
        instrument_id = segment.instrument_id
        if (
            segment.status != "resolved"
            or instrument_id is None
            or (
                expected_instrument_id is not None
                and instrument_id != expected_instrument_id
            )
        ):
            blocked.append(
                {
                    "ticker": ticker,
                    "start": end,
                    "end": end,
                    "status": f"alias_{segment.status}",
                    "detail": (
                        "ticker is not a unique active alias for the frozen stable "
                        "instrument"
                    ),
                }
            )
            continue
        aliases = [
            row
            for row in meta.instrument_alias_records(instrument_id)
            if str(row["ticker"]) == ticker
            and date.fromisoformat(str(row["start_date"]))
            <= end
            <= date.fromisoformat(str(row["end_date"]))
        ]
        if not aliases:
            raise RuntimeError("resolved current alias has no persisted envelope")
        alias_start = min(date.fromisoformat(str(row["start_date"])) for row in aliases)
        covered = meta.get_coverage(instrument_id, dataset_key)
        if covered is not None and covered[0] <= end <= covered[1]:
            # Retain the stable owner in the frozen job/ranking cohort while
            # letting the planner retire this already-covered target without a
            # request. This matters for intentional recovery of an older cycle.
            request_start = end
        elif covered is not None:
            overlap_start = covered[1] - timedelta(days=refresh_overlap_days)
            if dataset_key.startswith("intraday_") and covered[0] >= default_start:
                # A member that began forward-only at cohort entry must not
                # creep backward by one correction window on every later cycle.
                overlap_start = max(default_start, overlap_start)
            request_start = max(alias_start, overlap_start)
        else:
            request_start = max(alias_start, default_start)
        if request_start > end:
            blocked.append(
                {
                    "ticker": ticker,
                    "start": end,
                    "end": end,
                    "status": "coverage_after_cycle",
                    "detail": (
                        "stored coverage begins after the requested cycle and does "
                        "not contain its session"
                    ),
                }
            )
            continue
        targets_by_instrument[instrument_id] = {
            "instrument_id": instrument_id,
            "ranges": [{"ticker": ticker, "start": request_start, "end": end}],
        }

    targets = [
        targets_by_instrument[instrument_id]
        for instrument_id in sorted(
            targets_by_instrument,
            key=lambda value: (instrument_bucket(value), value),
        )
    ]
    blocked.sort(key=lambda row: (row["ticker"], row["start"], row["end"]))
    starts = [item["start"] for target in targets for item in target["ranges"]] or [
        default_start
    ]
    range_start = min(starts)
    request_payload = {
        "work_kind": "current",
        "dataset_key": dataset_key,
        "members": [
            {"ticker": ticker, "instrument_id": instrument_id}
            for ticker, instrument_id in normalized
        ],
        "default_start": default_start.isoformat(),
        "end": end.isoformat(),
        "refresh_overlap_days": refresh_overlap_days,
    }
    request_hash = hashlib.sha256(canonical_json(request_payload).encode()).hexdigest()
    snapshot = {
        "targets": [
            {
                "instrument_id": target["instrument_id"],
                "ranges": [
                    item
                    | {
                        "start": item["start"].isoformat(),
                        "end": item["end"].isoformat(),
                    }
                    for item in target["ranges"]
                ],
            }
            for target in targets
        ],
        "blocked": [
            item | {"start": item["start"].isoformat(), "end": item["end"].isoformat()}
            for item in blocked
        ],
    }
    cohort_hash = hashlib.sha256(canonical_json(snapshot).encode()).hexdigest()
    meta.create_history_job(
        job_id=job_id,
        phase=None,
        dataset_key=dataset_key,
        start=range_start,
        end=end,
        request_hash=request_hash,
        cohort_hash=cohort_hash,
        force=False,
        targets=targets,
        blocked_ranges=blocked,
        work_kind="current",
        refresh_overlap_days=refresh_overlap_days,
    )


def run_ingestion_cycle(
    client: TiingoLike,
    bars: BarStore,
    meta: MetaStore,
    *,
    current_tickers: Sequence[str],
    current_datasets: Sequence[str],
    history_job_id: str | None,
    policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
    max_history_units: int | None = None,
    clock: Callable[[], datetime] = _utc_now,
) -> IngestionCycleResult:
    """Run declared current collection before permitting historical work."""
    if current_datasets and not current_tickers:
        raise ValueError("current-first cycle requires a non-empty current cohort")
    datasets = tuple(dict.fromkeys(current_datasets))
    invalid = [
        dataset_key for dataset_key in datasets if dataset_key not in DATASET_KEYS
    ]
    if invalid:
        raise ValueError(f"invalid current dataset {invalid[0]!r}")
    data_dir = coordinated_data_directory(bars=bars, meta=meta)
    cycle = IngestionCycleResult()
    if datasets:
        with DataDirectoryLock(data_dir, operation="ingest:current-first-cycle"):
            for dataset_key in datasets:
                observer = PersistentAttemptObserver(
                    meta,
                    work_kind="current",
                    operation=f"current:{dataset_key}",
                    policy=policy,
                    clock=clock,
                )
                try:
                    with observed_client(client, observer) as metered:
                        if dataset_key == "eod":
                            result = update_eod_validated(
                                metered,  # type: ignore[arg-type]
                                bars,
                                meta,
                                current_tickers,
                            )
                        else:
                            result = update_intraday_validated(
                                metered,  # type: ignore[arg-type]
                                bars,
                                meta,
                                current_tickers,
                                freq=dataset_key.removeprefix("intraday_"),
                            )
                except BudgetExhausted as exc:
                    if exc.partial_ingest is not None:
                        _merge_ingest(cycle.current, exc.partial_ingest)
                    cycle.stop_reason = exc.reason
                    return cycle
                _merge_ingest(cycle.current, result)
                if not result.ok:
                    cycle.stop_reason = "current_work_incomplete"
                    return cycle

    if history_job_id is not None:
        cycle.history = run_history_sweep(
            client,
            bars,
            meta,
            history_job_id,
            policy=policy,
            max_units=max_history_units,
            clock=clock,
        )
        cycle.stop_reason = cycle.history.stop_reason
    return cycle


def run_history_sweep(
    client: TiingoLike,
    bars: BarStore,
    meta: MetaStore,
    job_id: str,
    *,
    policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
    max_units: int | None = None,
    clock: Callable[[], datetime] = _utc_now,
) -> SchedulerRunResult:
    """Resume one breadth-first sweep, yielding the lock between turns."""
    if max_units is not None and max_units <= 0:
        raise ValueError("max_units must be positive")
    data_dir = coordinated_data_directory(bars=bars, meta=meta)
    with DataDirectoryLock(data_dir, operation=f"ingest:history-sweep-setup:{job_id}"):
        job = meta.history_job(job_id)
        if job is None:
            raise ValueError(f"unknown history job {job_id!r}")
        started_sweep = int(job["sweep"])
        report = SchedulerRunResult(
            job_id=job_id,
            job_status=_job_status(job),
            sweep_started=started_sweep,
            sweep_ended=started_sweep,
        )
        if job["phase"] is not None and int(job["phase"]) > 1:
            report.stop_reason = meta.backfill_program_prerequisite_stop_reason(
                job_id, int(job["phase"])
            )
            if report.stop_reason is not None:
                return report
        if job["status"] != "active":
            _add_static_blockers(meta, job_id, report.ingest)
            return report
        targets = meta.history_targets(job_id)
        if not targets:
            return report
    observer = PersistentAttemptObserver(
        meta,
        work_kind=str(job["work_kind"]),
        operation=f"{job['work_kind']}:{job_id}",
        policy=policy,
        clock=clock,
    )
    completed_through = (
        min(
            date.fromisoformat(str(job["range_end"])),
            latest_completed_session(clock()),
        )
        if str(job["work_kind"]) == "current"
        else None
    )
    with observed_client(client, observer) as metered:
        while report.sweep_ended == started_sweep:
            with DataDirectoryLock(data_dir, operation=f"ingest:history-turn:{job_id}"):
                keep_running = _run_history_turn(
                    client,
                    metered,
                    bars,
                    meta,
                    job_id,
                    targets,
                    observer,
                    report,
                    max_units,
                    completed_through,
                )
            if not keep_running:
                break

    _add_static_blockers(meta, job_id, report.ingest)
    current = meta.history_job(job_id)
    assert current is not None
    report.sweep_ended = int(current["sweep"])
    report.job_status = _job_status(current)
    return report


def _run_history_turn(
    client: TiingoLike,
    metered: TiingoLike,
    bars: BarStore,
    meta: MetaStore,
    job_id: str,
    targets: list[Any],
    observer: PersistentAttemptObserver,
    report: SchedulerRunResult,
    max_units: int | None,
    completed_through: date | None,
) -> bool:
    """Plan, publish, and checkpoint one lock-bounded durable turn."""
    job = meta.history_job(job_id)
    if job is None:
        raise RuntimeError(f"history job {job_id!r} disappeared")
    report.job_status = _job_status(job)
    report.sweep_ended = int(job["sweep"])
    if report.job_status != "active":
        return False
    if job["phase"] is not None and int(job["phase"]) > 1:
        report.stop_reason = meta.backfill_program_prerequisite_stop_reason(
            job_id, int(job["phase"])
        )
        if report.stop_reason is not None:
            return False

    cursor = int(job["cursor"])
    target = targets[cursor]
    active_range = _active_range(meta, job_id, int(target["target_ordinal"]))
    if active_range is None:
        _checkpoint(
            meta,
            job_id,
            target,
            _last_range(meta, job_id, int(target["target_ordinal"])),
            attempt_status="already_complete",
            detail="",
            attempted=False,
            successful=False,
        )
        return _history_turn_continues(meta, job_id, report, max_units)

    planned = _plan_target_unit(meta, job, target, active_range)
    if planned is None:
        _checkpoint(
            meta,
            job_id,
            target,
            active_range,
            attempt_status="already_covered",
            detail="",
            attempted=False,
            successful=False,
            range_status="complete",
        )
        return _history_turn_continues(meta, job_id, report, max_units)

    segment, direction = planned
    if segment.status == "non_session_gap":
        _checkpoint_success_without_request(
            meta, job_id, target, active_range, segment, direction
        )
        report.advanced_units += 1
    elif segment.status != "ready":
        detail = segment.detail or segment.status
        report.ingest.blocked[segment.key] = detail
        report.ingest.segments.append(
            {**segment.to_dict(outcome="blocked"), "detail": detail}
        )
        _checkpoint(
            meta,
            job_id,
            target,
            active_range,
            attempt_status="identity_blocked",
            detail=detail,
            attempted=True,
            successful=False,
            terminal_blocked=True,
        )
        report.attempted_units += 1
    else:
        batch = [
            (
                target,
                active_range,
                segment,
                direction,
                meta.get_coverage(str(target["instrument_id"]), segment.dataset_key),
            )
        ]
        remaining = (
            len(targets) if max_units is None else max_units - report.attempted_units
        )
        bucket = instrument_bucket(str(target["instrument_id"]))
        for candidate_target in targets[cursor + 1 :]:
            if (
                len(batch) >= remaining
                or instrument_bucket(str(candidate_target["instrument_id"])) != bucket
            ):
                break
            candidate_range = _active_range(
                meta, job_id, int(candidate_target["target_ordinal"])
            )
            if candidate_range is None:
                break
            candidate_plan = _plan_target_unit(
                meta, job, candidate_target, candidate_range
            )
            if candidate_plan is None or candidate_plan[0].status != "ready":
                break
            batch.append(
                (
                    candidate_target,
                    candidate_range,
                    candidate_plan[0],
                    candidate_plan[1],
                    meta.get_coverage(
                        str(candidate_target["instrument_id"]),
                        candidate_plan[0].dataset_key,
                    ),
                )
            )
        attempts_per_unit = int(getattr(client, "max_attempts", 1))
        if len(batch) > 1 and not observer.can_start_batch(
            len(batch) * attempts_per_unit
        ):
            batch = batch[:1]
        try:
            unit_result = _execute_validated_segments(
                metered,
                bars,
                meta,
                [item[2] for item in batch],
                force=bool(job["force"]),
                update=str(job["work_kind"]) == "current",
                completed_through=completed_through,
            )
        except BudgetExhausted as exc:
            attempted, advanced = _checkpoint_partial_batch(
                meta, job_id, batch, exc.partial_ingest, report.ingest
            )
            report.attempted_units += attempted
            report.successful_units += advanced
            report.advanced_units += advanced
            report.stop_reason = exc.reason
            return False
        except ResponseReservationExceeded as exc:
            processed, advanced = _checkpoint_partial_batch(
                meta,
                job_id,
                batch,
                getattr(exc, "partial_ingest", None),
                report.ingest,
            )
            report.successful_units += advanced
            report.advanced_units += advanced
            report.attempted_units += processed + 1
            failed_target, failed_range, failed_segment, _, _ = batch[processed]
            detail = str(exc)
            report.ingest.failed[failed_segment.key] = detail
            report.ingest.segments.append(
                {**failed_segment.to_dict(outcome="failed"), "detail": detail}
            )
            _checkpoint(
                meta,
                job_id,
                failed_target,
                failed_range,
                attempt_status="response_reservation_exceeded",
                detail=detail,
                attempted=True,
                successful=False,
                terminal_blocked=True,
            )
            report.stop_reason = "response_reservation_exceeded"
            return False
        report.attempted_units += len(batch)
        for (
            batch_target,
            batch_range,
            batch_segment,
            batch_direction,
            coverage_before,
        ) in batch:
            successful = _checkpoint_after_ingest(
                meta,
                job_id,
                batch_target,
                batch_range,
                batch_segment,
                batch_direction,
                unit_result,
                coverage_before=coverage_before,
                force=bool(job["force"]),
            )
            if successful:
                report.successful_units += 1
                report.advanced_units += 1
        _merge_ingest(report.ingest, unit_result)

    return _history_turn_continues(meta, job_id, report, max_units)


def _history_turn_continues(
    meta: MetaStore,
    job_id: str,
    report: SchedulerRunResult,
    max_units: int | None,
) -> bool:
    current = meta.history_job(job_id)
    if current is None:
        raise RuntimeError(f"history job {job_id!r} disappeared")
    report.sweep_ended = int(current["sweep"])
    report.job_status = _job_status(current)
    if report.job_status != "active":
        return False
    if max_units is not None and report.attempted_units >= max_units:
        report.stop_reason = "max_units"
        return False
    return report.sweep_ended == report.sweep_started


def _plan_target_unit(meta, job, target, range_row):
    dataset_key = str(job["dataset_key"])
    instrument_id = str(target["instrument_id"])
    range_start = date.fromisoformat(range_row["range_start"])
    range_end = date.fromisoformat(range_row["range_end"])
    frontier_end = date.fromisoformat(range_row["frontier_end"])
    covered = meta.get_coverage(instrument_id, dataset_key)
    force = bool(job["force"])
    direction = "leading"
    unit_start = range_start
    unit_end = min(range_end, frontier_end)
    if str(job["work_kind"]) == "current":
        direction = "trailing"
        overlap_days = int(job["refresh_overlap_days"])
        if covered is not None:
            if covered[0] <= range_start and covered[1] >= range_end:
                return None
            if covered[0] > range_end:
                direction = "leading"
                unit_end = min(range_end, covered[0] - timedelta(days=1))
            else:
                unit_start = max(range_start, covered[1] - timedelta(days=overlap_days))
                unit_end = range_end
        else:
            unit_end = range_end
    elif covered is not None and not force:
        first, last = covered
        if first <= range_start and last >= range_end:
            return None
        if last < range_end:
            direction = "trailing"
            # Bridge from the canonical coverage edge even when it precedes
            # the job range; the lower ingestion layer must never bridge an
            # unfetched weekday gap merely by extending metadata.
            unit_start = last + timedelta(days=1)
            unit_end = range_end
        elif first <= unit_end <= last:
            unit_end = first - timedelta(days=1)
    if unit_end < unit_start:
        return None

    if dataset_key.startswith("intraday_"):
        freq = dataset_key.removeprefix("intraday_")
        chunk = plan_intraday_requests(
            unit_start, unit_end, freq=freq, reverse=direction == "leading"
        )[0]
        unit_start, unit_end = chunk.start, chunk.end

    planned = plan_validated_segments(
        meta, [str(range_row["ticker"])], dataset_key, unit_start, unit_end
    )
    anchor = unit_end if direction == "leading" else unit_start
    candidate = next(
        (segment for segment in planned if segment.start <= anchor <= segment.end),
        None,
    )
    if candidate is None:
        raise RuntimeError("identity planner omitted the scheduler unit anchor")
    if candidate.instrument_id not in {None, instrument_id}:
        candidate = ValidatedRequestSegment(
            ticker=candidate.ticker,
            dataset_key=candidate.dataset_key,
            start=candidate.start,
            end=candidate.end,
            status="alias_owner_changed",
            instrument_ids=candidate.instrument_ids,
            alias_ids=candidate.alias_ids,
            detail="alias no longer resolves to the snapshotted stable instrument",
        )
    if direction == "leading":
        candidate = _replace_segment(candidate, start=max(unit_start, candidate.start))
    else:
        candidate = _replace_segment(candidate, end=min(unit_end, candidate.end))
    if candidate.status == "ready" and dataset_key.startswith("intraday_"):
        freq = dataset_key.removeprefix("intraday_")
        chunk = plan_intraday_requests(
            candidate.start,
            candidate.end,
            freq=freq,
            reverse=direction == "leading",
        )[0]
        candidate = _replace_segment(candidate, start=chunk.start, end=chunk.end)
    return candidate, direction


def _checkpoint_after_ingest(
    meta,
    job_id,
    target,
    range_row,
    segment,
    direction,
    result,
    *,
    coverage_before,
    force,
):
    job = meta.history_job(job_id)
    if job is None:
        raise RuntimeError(f"history job {job_id!r} disappeared")
    current_work = str(job["work_kind"]) == "current"
    current_retry_attempt = (
        int(target["attempted_turns"]) - int(target["successful_depth"]) + 1
    )
    instrument_id = str(target["instrument_id"])
    failed = instrument_id in result.failed or segment.key in result.failed
    blocked = segment.key in result.blocked
    if failed or blocked:
        detail = result.failed.get(instrument_id) or result.failed.get(segment.key)
        detail = detail or result.blocked.get(segment.key, "ingestion did not advance")
        retry_exhausted = (
            current_work and failed and current_retry_attempt >= CURRENT_RETRY_ATTEMPTS
        )
        if current_work and failed:
            if retry_exhausted:
                detail = (
                    f"{detail}; exhausted {CURRENT_RETRY_ATTEMPTS} current-cycle "
                    "attempts and excluded this range"
                )
            else:
                detail = (
                    f"{detail}; current-cycle attempt {current_retry_attempt}/"
                    f"{CURRENT_RETRY_ATTEMPTS} remains retryable"
                )
            result.failed.pop(instrument_id, None)
            result.failed.pop(segment.key, None)
            if retry_exhausted:
                result.blocked[segment.key] = detail
                for item in result.segments:
                    if (
                        item["ticker"] == segment.ticker
                        and item["dataset_key"] == segment.dataset_key
                        and item["start"] == segment.start.isoformat()
                        and item["end"] == segment.end.isoformat()
                    ):
                        item["status"] = "blocked"
                        item["detail"] = detail
            else:
                result.failed[segment.key] = detail
        attempt_status = "failed" if failed else "terminal_blocked"
        if current_work and failed:
            attempt_status = (
                "current_retry_exhausted"
                if retry_exhausted
                else "current_retry_pending"
            )
        _checkpoint(
            meta,
            job_id,
            target,
            range_row,
            attempt_status=attempt_status,
            detail=detail,
            attempted=True,
            successful=False,
            # A lower-layer validated-ingest blocker is just as terminal for
            # this immutable job range as a blocker produced by the planner.
            # Leaving it active retries the same fail-closed segment forever.
            terminal_blocked=blocked or retry_exhausted,
        )
        return False

    range_start = date.fromisoformat(range_row["range_start"])
    range_end = date.fromisoformat(range_row["range_end"])
    old_frontier = date.fromisoformat(range_row["frontier_end"])
    coverage = meta.get_coverage(instrument_id, segment.dataset_key)
    range_status = "active"
    frontier = old_frontier
    if force:
        frontier = segment.start - timedelta(days=1)
        if frontier < range_start:
            range_status = "complete"
            frontier = range_start
    elif (
        coverage is not None and coverage[0] <= range_start and coverage[1] >= range_end
    ):
        range_status = "complete"
        frontier = range_start
    elif direction == "leading" and coverage is not None:
        frontier = min(old_frontier, coverage[0] - timedelta(days=1))
        if frontier < range_start:
            range_status = "complete"
            frontier = range_start
    if current_work:
        coverage_advanced = coverage is not None and (
            coverage_before is None
            or coverage[0] < coverage_before[0]
            or coverage[1] > coverage_before[1]
        )
        advanced = range_status == "complete" or coverage_advanced
    else:
        advanced = (
            range_status == "complete"
            or frontier < old_frontier
            or (
                direction == "trailing"
                and coverage is not None
                and coverage[1] >= segment.end
            )
        )
    if current_work and not advanced:
        retry_exhausted = current_retry_attempt >= CURRENT_RETRY_ATTEMPTS
        detail = (
            "completed current request did not establish coverage through the "
            f"cycle session; attempt {current_retry_attempt}/"
            f"{CURRENT_RETRY_ATTEMPTS} "
            + (
                "was exhausted and this range is excluded from the cycle"
                if retry_exhausted
                else "remains retryable"
            )
        )
        outcome = "blocked" if retry_exhausted else "failed"
        getattr(result, outcome)[segment.key] = detail
        result.segments.append({**segment.to_dict(outcome=outcome), "detail": detail})
        _checkpoint(
            meta,
            job_id,
            target,
            range_row,
            attempt_status=(
                "current_retry_exhausted"
                if retry_exhausted
                else "current_retry_pending"
            ),
            detail=detail,
            attempted=True,
            successful=False,
            frontier_end=frontier,
            range_status=range_status,
            terminal_blocked=retry_exhausted,
        )
        return False
    _checkpoint(
        meta,
        job_id,
        target,
        range_row,
        attempt_status="advanced" if advanced else "deferred",
        detail="" if advanced else "coverage did not advance",
        attempted=True,
        successful=advanced,
        frontier_end=frontier,
        range_status=range_status,
    )
    return advanced


def _checkpoint_partial_batch(
    meta: MetaStore,
    job_id: str,
    batch: list[
        tuple[
            Any,
            Any,
            ValidatedRequestSegment,
            str,
            tuple[date, date] | None,
        ]
    ],
    partial: IngestResult | None,
    report: IngestResult,
) -> tuple[int, int]:
    """Checkpoint the prefix durably published before an interrupted batch."""
    if partial is None:
        return 0, 0
    outcomes = set(partial.fetched + partial.skipped + partial.refreshed)
    processed = 0
    advanced = 0
    job = meta.history_job(job_id)
    assert job is not None
    for target, range_row, segment, direction, coverage_before in batch:
        instrument_id = str(target["instrument_id"])
        represented = (
            instrument_id in outcomes
            or instrument_id in partial.failed
            or segment.key in partial.failed
            or segment.key in partial.blocked
        )
        if not represented:
            break
        if _checkpoint_after_ingest(
            meta,
            job_id,
            target,
            range_row,
            segment,
            direction,
            partial,
            coverage_before=coverage_before,
            force=bool(job["force"]),
        ):
            advanced += 1
        processed += 1
    _merge_ingest(report, partial)
    return processed, advanced


def _checkpoint_success_without_request(
    meta, job_id, target, range_row, segment, direction
):
    range_start = date.fromisoformat(range_row["range_start"])
    frontier = date.fromisoformat(range_row["frontier_end"])
    if direction == "leading":
        frontier = segment.start - timedelta(days=1)
        complete = frontier < range_start
    else:
        job = meta.history_job(job_id)
        coverage = meta.get_coverage(
            str(target["instrument_id"]), str(job["dataset_key"])
        )
        if coverage is not None:
            meta.extend_coverage(
                str(target["instrument_id"]),
                str(job["dataset_key"]),
                coverage[0],
                segment.end,
            )
        complete = segment.end >= date.fromisoformat(range_row["range_end"])
    _checkpoint(
        meta,
        job_id,
        target,
        range_row,
        attempt_status="non_session_gap",
        detail=segment.detail,
        attempted=False,
        successful=False,
        frontier_end=range_start if complete else frontier,
        range_status="complete" if complete else "active",
    )


def _checkpoint(
    meta,
    job_id,
    target,
    range_row,
    *,
    attempt_status,
    detail,
    attempted,
    successful,
    frontier_end=None,
    range_status=None,
    terminal_blocked=None,
):
    ordinal = int(target["target_ordinal"])
    next_cursor = ordinal + 1
    job = meta.history_job(job_id)
    sweep = int(job["sweep"])
    if next_cursor >= meta.history_target_count(job_id):
        next_cursor = 0
        sweep += 1
    current_key = (ordinal, int(range_row["range_ordinal"]))
    current_status = range_status or str(range_row["status"])
    current_terminal = (
        bool(range_row["terminal_blocked"])
        if terminal_blocked is None
        else bool(terminal_blocked)
    )
    active_after = (
        current_status == "active" and not current_terminal
    ) or meta.history_has_active_range(job_id, excluding=current_key)
    blocked = current_terminal or meta.history_has_blockers(job_id)
    job_status = "active" if active_after else "blocked" if blocked else "complete"
    meta.checkpoint_history_turn(
        job_id=job_id,
        target_ordinal=ordinal,
        range_ordinal=int(range_row["range_ordinal"]),
        frontier_end=frontier_end or date.fromisoformat(range_row["frontier_end"]),
        range_status=range_status or str(range_row["status"]),
        attempt_status=attempt_status,
        detail=detail,
        attempted=attempted,
        successful=successful,
        terminal_blocked=current_terminal,
        cursor=next_cursor,
        sweep=sweep,
        job_status=job_status,
    )


def _active_range(meta: MetaStore, job_id: str, ordinal: int):
    return next(
        (
            row
            for row in meta.history_ranges(job_id, ordinal)
            if row["status"] == "active" and not row["terminal_blocked"]
        ),
        None,
    )


def _last_range(meta: MetaStore, job_id: str, ordinal: int):
    ranges = meta.history_ranges(job_id, ordinal)
    if not ranges:
        raise RuntimeError("history target has no ranges")
    return ranges[-1]


def _replace_segment(segment: ValidatedRequestSegment, **changes):
    values = segment.__dict__ | changes
    return ValidatedRequestSegment(**values)


def _job_status(job: Any) -> str:
    return "cancelled" if bool(job["cancelled"]) else str(job["status"])


def _merge_ingest(target: IngestResult, source: IngestResult) -> None:
    for instrument_id in source.skipped:
        target.record_instrument_outcome(instrument_id, "skipped")
    for instrument_id in source.fetched:
        target.record_instrument_outcome(instrument_id, "fetched")
    for instrument_id in source.refreshed:
        target.record_instrument_outcome(instrument_id, "refreshed")
    target.failed.update(source.failed)
    target.blocked.update(source.blocked)
    target.segments.extend(source.segments)


def _add_static_blockers(meta: MetaStore, job_id: str, result: IngestResult) -> None:
    for row in meta.history_blocked_ranges(job_id):
        key = f"{row['ticker']}:{row['range_start']}..{row['range_end']}"
        result.blocked[key] = str(row["detail"])
        result.segments.append(
            {
                "ticker": row["ticker"],
                "start": row["range_start"],
                "end": row["range_end"],
                "status": row["status"],
                "outcome": "blocked",
                "detail": row["detail"],
            }
        )


def _merge_alias_ranges(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker: dict[str, list[tuple[date, date]]] = {}
    for item in items:
        by_ticker.setdefault(item["ticker"], []).append((item["start"], item["end"]))
    merged = [
        {"ticker": ticker, "start": start, "end": end}
        for ticker, ranges in sorted(by_ticker.items())
        for start, end in merge_closed_date_ranges(ranges)
    ]
    return sorted(
        merged, key=lambda value: (value["end"], value["start"]), reverse=True
    )


def _require_phase_dataset(phase: int | None, dataset_key: str) -> None:
    allowed = {
        None: {"eod", "intraday_1hour", "intraday_5min"},
        1: {"eod", "intraday_1hour"},
        2: {"eod"},
        3: {"intraday_5min"},
    }
    if phase not in allowed or dataset_key not in allowed[phase]:
        raise ValueError(f"dataset {dataset_key!r} is not valid for phase {phase}")


def _history_request_payload(
    dataset_key: str,
    tickers: Sequence[str],
    start: date,
    end: date | None,
    *,
    phase: int | None,
    force: bool,
) -> dict[str, Any]:
    return {
        "dataset_key": dataset_key,
        "tickers": sorted({ticker.strip().upper() for ticker in tickers}),
        "start": start.isoformat(),
        "end": end.isoformat() if end is not None else None,
        "phase": phase,
        "force": force,
    }


def _operation_label(operation: str, path: str) -> str:
    label = f"{operation}:{path}" if path else operation
    if len(label) <= 512:
        return label
    digest = hashlib.sha256(label.encode()).hexdigest()[:16]
    return f"{label[:495]}:{digest}"
