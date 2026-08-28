"""Durable, budget-aware breadth-first historical ingestion.

The canonical coverage interval remains the source of truth for published
bars.  This module persists only operational facts that coverage cannot
reconstruct: the immutable cohort, one-turn-per-instrument sweep cursor,
attempt outcomes, and transport request/byte usage.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

from marketdata.calendar import plan_intraday_requests, weekend_only
from marketdata.errors import BudgetExhausted
from marketdata.identity import DATASET_KEYS
from marketdata.ingest import (
    IngestResult,
    ValidatedRequestSegment,
    _execute_validated_segments,
    plan_validated_segments,
    update_eod_validated,
    update_intraday_validated,
)
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

# Tiingo does not document whether its monthly ledger counts encoded or
# decoded bodies, nor the account's precise reset boundary (RE-006). Current
# bar responses are uncompressed, so observable encoded/decoded sizes match.
# A rolling window longer than any calendar month plus a reservation far above
# measured maximum bar payloads is the deliberately conservative enforcement
# basis until the vendor exposes authoritative usage semantics.
ROLLING_BUDGET_DAYS = 32
RESPONSE_RESERVATION_BYTES = 64_000_000


class TiingoLike(Protocol):
    def eod(self, ticker: str, start: date, end: date) -> list[dict[str, Any]]: ...

    def intraday(
        self, ticker: str, start: date, end: date, freq: str = "1hour"
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class BudgetPolicy:
    hourly_request_limit: int = HOURLY_REQUEST_LIMIT
    daily_request_limit: int = DAILY_REQUEST_LIMIT
    total_byte_limit: int = TOTAL_BYTE_LIMIT
    historical_byte_limit: int = HISTORICAL_BYTE_LIMIT
    rolling_days: int = ROLLING_BUDGET_DAYS
    response_reservation_bytes: int = RESPONSE_RESERVATION_BYTES


DEFAULT_BUDGET_POLICY = BudgetPolicy()


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
        return self.stop_reason in {
            "hourly_request_limit",
            "daily_request_limit",
            "rolling_total_byte_limit",
            "rolling_historical_byte_limit",
        }

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
    def ok(self) -> bool:
        return self.current.ok and (self.history is None or self.history.ingest.ok)

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
        attempt_id, reason = self._meta.reserve_request_attempt(
            now=self._clock(),
            work_kind=self._work_kind,
            operation=_operation_label(self._operation, path),
            reserved_bytes=self._policy.response_reservation_bytes,
            hourly_request_limit=self._policy.hourly_request_limit,
            daily_request_limit=self._policy.daily_request_limit,
            total_byte_limit=self._policy.total_byte_limit,
            historical_byte_limit=self._policy.historical_byte_limit,
            rolling_days=self._policy.rolling_days,
        )
        if reason is not None:
            raise BudgetExhausted(reason)
        assert attempt_id is not None
        return attempt_id

    def response_byte_limit(self, reservation: Any) -> int:
        """Maximum encoded body covered by the pre-request reservation."""
        return self._policy.response_reservation_bytes

    def can_start_batch(self, attempts: int) -> bool:
        return self._meta.can_start_request_batch(
            now=self._clock(),
            work_kind=self._work_kind,
            attempts=attempts,
            reserved_bytes=self._policy.response_reservation_bytes,
            hourly_request_limit=self._policy.hourly_request_limit,
            daily_request_limit=self._policy.daily_request_limit,
            total_byte_limit=self._policy.total_byte_limit,
            historical_byte_limit=self._policy.historical_byte_limit,
            rolling_days=self._policy.rolling_days,
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
    digest = hashlib.sha256(_canonical_json(payload).encode()).hexdigest()[:20]
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
) -> str:
    """Resolve one CLI/scheduled request to a frozen, initialized job."""
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
    initialize_history_job(
        meta,
        job_id=durable_job_id,
        dataset_key=dataset_key,
        tickers=tickers,
        start=start,
        end=frozen_end,
        phase=phase,
        force=force,
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
) -> None:
    """Snapshot stable owners and their date-ranged aliases deterministically."""
    _require_phase_dataset(phase, dataset_key)
    request_payload = _history_request_payload(
        dataset_key, tickers, start, end, phase=phase, force=force
    )
    request_hash = hashlib.sha256(_canonical_json(request_payload).encode()).hexdigest()
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
        if existing["status"] == "blocked":
            meta.reactivate_history_job(job_id)
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
    cohort_hash = hashlib.sha256(_canonical_json(snapshot).encode()).hexdigest()
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
        if job["phase"] is not None:
            predecessors = meta.active_lower_phase_jobs(int(job["phase"]))
            if predecessors:
                report.stop_reason = "phase_predecessor_active"
                return report
        if job["status"] != "active":
            _add_static_blockers(meta, job_id, report.ingest)
            return report
        targets = meta.history_targets(job_id)
        if not targets:
            return report
    observer = PersistentAttemptObserver(
        meta,
        work_kind="historical",
        operation=f"history:{job_id}",
        policy=policy,
        clock=clock,
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
) -> bool:
    """Plan, publish, and checkpoint one lock-bounded durable turn."""
    job = meta.history_job(job_id)
    if job is None:
        raise RuntimeError(f"history job {job_id!r} disappeared")
    report.job_status = _job_status(job)
    report.sweep_ended = int(job["sweep"])
    if report.job_status != "active":
        return False
    if job["phase"] is not None and meta.active_lower_phase_jobs(int(job["phase"])):
        report.stop_reason = "phase_predecessor_active"
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
        batch = [(target, active_range, segment, direction)]
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
            failed_target, failed_range, failed_segment, _ = batch[processed]
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
        _merge_ingest(report.ingest, unit_result)
        report.attempted_units += len(batch)
        for batch_target, batch_range, batch_segment, batch_direction in batch:
            successful = _checkpoint_after_ingest(
                meta,
                job_id,
                batch_target,
                batch_range,
                batch_segment,
                batch_direction,
                unit_result,
                force=bool(job["force"]),
            )
            if successful:
                report.successful_units += 1
                report.advanced_units += 1

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
    if covered is not None and not force:
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
    force,
):
    instrument_id = str(target["instrument_id"])
    failed = instrument_id in result.failed or segment.key in result.failed
    blocked = segment.key in result.blocked
    if failed or blocked:
        detail = result.failed.get(instrument_id) or result.failed.get(segment.key)
        detail = detail or result.blocked.get(segment.key, "ingestion did not advance")
        _checkpoint(
            meta,
            job_id,
            target,
            range_row,
            attempt_status="failed" if failed else "identity_blocked",
            detail=detail,
            attempted=True,
            successful=False,
            # A lower-layer validated-ingest blocker is just as terminal for
            # this immutable job range as a blocker produced by the planner.
            # Leaving it active retries the same fail-closed segment forever.
            terminal_blocked=blocked,
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
    advanced = (
        range_status == "complete"
        or frontier < old_frontier
        or (
            direction == "trailing"
            and coverage is not None
            and coverage[1] >= segment.end
        )
    )
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
    batch: list[tuple[Any, Any, ValidatedRequestSegment, str]],
    partial: IngestResult | None,
    report: IngestResult,
) -> tuple[int, int]:
    """Checkpoint the prefix durably published before an interrupted batch."""
    if partial is None:
        return 0, 0
    _merge_ingest(report, partial)
    outcomes = set(partial.fetched + partial.skipped + partial.refreshed)
    processed = 0
    advanced = 0
    job = meta.history_job(job_id)
    assert job is not None
    for target, range_row, segment, direction in batch:
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
            force=bool(job["force"]),
        ):
            advanced += 1
        processed += 1
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
    merged: list[dict[str, Any]] = []
    for item in sorted(
        items, key=lambda value: (value["ticker"], value["start"], value["end"])
    ):
        if (
            merged
            and merged[-1]["ticker"] == item["ticker"]
            and item["start"] <= merged[-1]["end"] + timedelta(days=1)
        ):
            merged[-1]["end"] = max(merged[-1]["end"], item["end"])
        else:
            merged.append(dict(item))
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


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


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
