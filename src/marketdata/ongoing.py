"""Durable post-market EOD and rolling-liquidity intraday collection."""

from __future__ import annotations

import hashlib
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Protocol

import duckdb
import polars as pl

from marketdata.calendar import session_schedule
from marketdata.identity_bootstrap import (
    IdentityBootstrapResult,
    IntradayIdentityBootstrapResult,
    bootstrap_eod_identities,
    bootstrap_intraday_identities,
    supported_us_stock_etf_records,
)
from marketdata.ingest import (
    DEFAULT_EOD_HISTORY_START,
    IEX_HISTORY_START,
    REFRESH_WINDOW_DAYS,
)
from marketdata.jsonutil import canonical_json
from marketdata.locking import data_directory_locked
from marketdata.scheduler import (
    DEFAULT_BUDGET_POLICY,
    BudgetPolicy,
    CurrentJobMember,
    SchedulerRunResult,
    initialize_current_job,
    run_history_sweep,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import create_canonical_parquet_view

DEFAULT_ONGOING_PROGRAM_ID = "ongoing-main-v1"
DEFAULT_COHORT_SIZE = 5_000
DEFAULT_LOOKBACK_SESSIONS = 20
DEFAULT_MIN_OBSERVATIONS = 15

_TERMINAL_CYCLE_STATES = {"complete", "complete_with_exclusions"}


class OngoingProgramClient(Protocol):
    response_bytes: int

    def supported_tickers(
        self, tickers: Collection[str] | None = None
    ) -> list[dict[str, str]]: ...

    def ticker_metadata(self, ticker: str) -> dict[str, Any]: ...

    def eod(self, ticker: str, start: date, end: date) -> list[dict[str, Any]]: ...

    def intraday(
        self, ticker: str, start: date, end: date, freq: str = "1hour"
    ) -> list[dict[str, Any]]: ...


@dataclass
class OngoingProgramStepResult:
    program_id: str
    session_date: date
    cycle_state: str
    action: str
    dataset_key: str | None = None
    target_count: int | None = None
    identity_cursor: int | None = None
    cohort_snapshot_id: str | None = None
    identity: IdentityBootstrapResult | IntradayIdentityBootstrapResult | None = None
    sweep: SchedulerRunResult | None = None
    stop_reason: str | None = None
    jobs: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def terminal(self) -> bool:
        return self.cycle_state in _TERMINAL_CYCLE_STATES or self.action == "up_to_date"

    @property
    def partial(self) -> bool:
        return bool(
            self.cycle_state == "complete_with_exclusions"
            or any(bool(job.get("has_exclusions")) for job in self.jobs.values())
            or (self.identity is not None and self.identity.partial)
            or (self.sweep is not None and self.sweep.ingest.partial)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "program": {
                "program_id": self.program_id,
                "session_date": self.session_date.isoformat(),
                "cycle_state": self.cycle_state,
                "action": self.action,
                "dataset_key": self.dataset_key,
                "target_count": self.target_count,
                "identity_cursor": self.identity_cursor,
                "cohort_snapshot_id": self.cohort_snapshot_id,
                "stop_reason": self.stop_reason,
                "jobs": self.jobs,
            },
            "identity": self.identity.to_dict() if self.identity is not None else None,
            "sweep": self.sweep.to_dict() if self.sweep is not None else None,
            "partial": self.partial,
            "terminal": self.terminal,
            "ok": not self.partial,
        }


class _SnapshotMetadataClient:
    """Serve frozen supported records while delegating authenticated metadata."""

    def __init__(
        self,
        client: OngoingProgramClient,
        records: Sequence[Mapping[str, str]],
    ):
        self._client = client
        self._records = [dict(row) for row in records]

    @property
    def response_bytes(self) -> int:
        return int(getattr(self._client, "response_bytes", 0))

    def supported_tickers(
        self, tickers: Collection[str] | None = None
    ) -> list[dict[str, str]]:
        if tickers is None:
            return list(self._records)
        requested = {ticker.strip().upper() for ticker in tickers}
        return [row for row in self._records if row["ticker"] in requested]

    def ticker_metadata(self, ticker: str) -> dict[str, Any]:
        return self._client.ticker_metadata(ticker)


@data_directory_locked("ingest:ongoing-program-initialize")
def initialize_ongoing_program(
    meta: MetaStore,
    *,
    program_id: str = DEFAULT_ONGOING_PROGRAM_ID,
    initial_session: date,
    cohort_size: int = DEFAULT_COHORT_SIZE,
    lookback_sessions: int = DEFAULT_LOOKBACK_SESSIONS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> None:
    """Create the immutable D-030/D-031 ongoing program definition."""
    definition = {
        "program_id": program_id,
        "initial_session": initial_session.isoformat(),
        "cohort_size": cohort_size,
        "lookback_sessions": lookback_sessions,
        "min_observations": min_observations,
        "eod_default_start": DEFAULT_EOD_HISTORY_START.isoformat(),
        "intraday_new_member_policy": "forward_from_cohort_as_of",
        "refresh_overlap_days": REFRESH_WINDOW_DAYS,
        "datasets": ["eod", "intraday_1hour", "intraday_5min"],
    }
    definition_hash = hashlib.sha256(canonical_json(definition).encode()).hexdigest()
    meta.create_ongoing_program(
        program_id=program_id,
        definition_hash=definition_hash,
        initial_session=initial_session,
        cohort_size=cohort_size,
        lookback_sessions=lookback_sessions,
        min_observations=min_observations,
    )


def run_ongoing_program_step(
    client: OngoingProgramClient,
    bars: BarStore,
    meta: MetaStore,
    *,
    session_date: date,
    program_id: str = DEFAULT_ONGOING_PROGRAM_ID,
    identity_batch_size: int = 1_000,
    max_units: int = 1_000,
    policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
) -> OngoingProgramStepResult:
    """Perform one bounded overnight scope, identity, ranking, or data step."""
    if identity_batch_size <= 0 or max_units <= 0:
        raise ValueError("ongoing batch sizes must be positive")
    program = meta.ongoing_program(program_id)
    if program is None:
        raise ValueError(f"unknown ongoing program {program_id!r}; initialize it first")
    initial_session = date.fromisoformat(str(program["initial_session"]))
    if session_date < initial_session:
        raise ValueError("ongoing session precedes the program's initial session")

    cycle = _selected_cycle(meta, program_id, session_date)
    if cycle is None:
        records = _active_supported_records(client.supported_tickers(), session_date)
        if not records:
            raise ValueError("Tiingo supported-tickers snapshot has no active US scope")
        cycle = _freeze_cycle(meta, program_id, session_date, records)
        return _result(
            meta,
            cycle,
            action="scope_frozen",
            target_count=_ticker_count(meta, cycle),
        )
    cycle_session = date.fromisoformat(str(cycle["session_date"]))
    if str(cycle["state"]) in _TERMINAL_CYCLE_STATES:
        return _result(meta, cycle, action="up_to_date")

    state = str(cycle["state"])
    if state == "eod_identity":
        return _run_eod_identity_batch(
            client,
            meta,
            cycle,
            identity_batch_size=identity_batch_size,
            policy=policy,
        )
    if state == "eod":
        return _run_dataset_step(
            client,
            bars,
            meta,
            cycle,
            dataset_key="eod",
            members=[
                CurrentJobMember(ticker)
                for ticker in meta.ongoing_supported_tickers(
                    str(cycle["supported_snapshot_id"])
                )
            ],
            default_start=DEFAULT_EOD_HISTORY_START,
            next_state="cohort",
            max_units=max_units,
            policy=policy,
        )
    if state == "cohort":
        snapshot = _select_or_create_cohort(meta, bars, program, cycle)
        _advance_cycle(
            meta,
            cycle,
            state="hourly_identity",
            cohort_snapshot_id=str(snapshot["snapshot_id"]),
            last_stop_reason=None,
        )
        refreshed = _cycle(meta, cycle)
        return _result(
            meta,
            refreshed,
            action="cohort_selected",
            target_count=int(snapshot["member_count"]),
            cohort_snapshot_id=str(snapshot["snapshot_id"]),
        )
    if state in {"hourly_identity", "five_min_identity"}:
        dataset_key = (
            "intraday_1hour" if state == "hourly_identity" else "intraday_5min"
        )
        return _run_intraday_identity_batch(
            client,
            meta,
            cycle,
            dataset_key=dataset_key,
            identity_batch_size=identity_batch_size,
            policy=policy,
        )
    if state in {"hourly", "five_min"}:
        dataset_key = "intraday_1hour" if state == "hourly" else "intraday_5min"
        snapshot_id = str(cycle["cohort_snapshot_id"])
        snapshot = meta.ongoing_cohort_snapshot(snapshot_id)
        if snapshot is None:
            raise RuntimeError("ongoing cycle lost its cohort snapshot")
        members = [
            CurrentJobMember(str(row["ticker"]), str(row["instrument_id"]))
            for row in meta.ongoing_cohort_members(snapshot_id)
        ]
        return _run_dataset_step(
            client,
            bars,
            meta,
            cycle,
            dataset_key=dataset_key,
            members=members,
            default_start=date.fromisoformat(str(snapshot["as_of_session"])),
            next_state="five_min_identity" if state == "hourly" else "complete",
            max_units=max_units,
            policy=policy,
        )
    raise RuntimeError(f"unsupported ongoing cycle state {state!r} for {cycle_session}")


def _selected_cycle(meta: MetaStore, program_id: str, requested: date):
    cycles = meta.ongoing_cycles(program_id)
    unfinished = [
        row for row in cycles if str(row["state"]) not in _TERMINAL_CYCLE_STATES
    ]
    if unfinished:
        return unfinished[0]
    latest = cycles[-1] if cycles else None
    if (
        latest is not None
        and date.fromisoformat(str(latest["session_date"])) >= requested
    ):
        return latest
    return None


def _active_supported_records(
    rows: Sequence[Mapping[str, str]], as_of_session: date
) -> list[dict[str, str]]:
    return [
        row
        for row in supported_us_stock_etf_records(rows)
        if date.fromisoformat(row["startDate"])
        <= as_of_session
        <= date.fromisoformat(row["endDate"])
    ]


@data_directory_locked("ingest:ongoing-cycle-freeze")
def _freeze_cycle(
    meta: MetaStore,
    program_id: str,
    session_date: date,
    records: Sequence[Mapping[str, str]],
):
    snapshot = meta.create_ongoing_supported_snapshot(
        as_of_session=session_date, records=records
    )
    prefix = f"ongoing-{program_id}-{session_date:%Y%m%d}"
    return meta.create_ongoing_cycle(
        program_id=program_id,
        session_date=session_date,
        supported_snapshot_id=str(snapshot["snapshot_id"]),
        eod_job_id=f"{prefix}-eod",
        hourly_job_id=f"{prefix}-hourly",
        five_min_job_id=f"{prefix}-5min",
    )


def _ticker_count(meta: MetaStore, cycle: Any) -> int:
    snapshot = meta.ongoing_supported_snapshot(str(cycle["supported_snapshot_id"]))
    if snapshot is None:
        raise RuntimeError("ongoing cycle lost its supported snapshot")
    return int(snapshot["ticker_count"])


def _run_eod_identity_batch(
    client: OngoingProgramClient,
    meta: MetaStore,
    cycle: Any,
    *,
    identity_batch_size: int,
    policy: BudgetPolicy,
) -> OngoingProgramStepResult:
    snapshot_id = str(cycle["supported_snapshot_id"])
    tickers = meta.ongoing_supported_tickers(snapshot_id)
    cursor = int(cycle["eod_identity_cursor"])
    batch = tickers[cursor : cursor + identity_batch_size]
    if not batch:
        _advance_cycle(meta, cycle, state="eod", last_stop_reason=None)
        return _result(
            meta,
            _cycle(meta, cycle),
            action="eod_identity_prepared",
            target_count=len(tickers),
            identity_cursor=cursor,
        )
    records = meta.ongoing_supported_records(snapshot_id, batch)
    identity = bootstrap_eod_identities(
        _SnapshotMetadataClient(client, records),
        meta,
        batch,
        policy=policy,
    )
    next_cursor = cursor if identity.stop_reason else cursor + len(batch)
    state = "eod" if next_cursor == len(tickers) else "eod_identity"
    _advance_cycle(
        meta,
        cycle,
        state=state,
        eod_identity_cursor=next_cursor,
        last_stop_reason=identity.stop_reason,
    )
    return _result(
        meta,
        _cycle(meta, cycle),
        action="eod_identity_prepared" if state == "eod" else "eod_identity_batch",
        dataset_key="eod",
        target_count=len(tickers),
        identity_cursor=next_cursor,
        identity=identity,
        stop_reason=identity.stop_reason,
    )


def _run_intraday_identity_batch(
    client: OngoingProgramClient,
    meta: MetaStore,
    cycle: Any,
    *,
    dataset_key: str,
    identity_batch_size: int,
    policy: BudgetPolicy,
) -> OngoingProgramStepResult:
    snapshot_id = str(cycle["cohort_snapshot_id"])
    members = meta.ongoing_cohort_members(snapshot_id)
    cursor_field = (
        "hourly_identity_cursor"
        if dataset_key == "intraday_1hour"
        else "five_min_identity_cursor"
    )
    cursor = int(cycle[cursor_field])
    batch = members[cursor : cursor + identity_batch_size]
    next_state = "hourly" if dataset_key == "intraday_1hour" else "five_min"
    if not batch:
        _advance_cycle(meta, cycle, state=next_state, last_stop_reason=None)
        return _result(
            meta,
            _cycle(meta, cycle),
            action=f"{dataset_key}_identity_prepared",
            dataset_key=dataset_key,
            target_count=len(members),
            identity_cursor=cursor,
            cohort_snapshot_id=snapshot_id,
        )
    session = date.fromisoformat(str(cycle["session_date"]))
    tickers = [_current_member_ticker(meta, row, session) for row in batch]
    start = _identity_bridge_start(meta, batch, dataset_key)
    end = session
    identity = bootstrap_intraday_identities(
        client,
        meta,
        tickers,
        start=start,
        end=end,
        freq=dataset_key.removeprefix("intraday_"),
        policy=policy,
    )
    next_cursor = cursor if identity.stop_reason else cursor + len(batch)
    state = next_state if next_cursor == len(members) else str(cycle["state"])
    _advance_cycle(
        meta,
        cycle,
        state=state,
        **{cursor_field: next_cursor},
        last_stop_reason=identity.stop_reason,
    )
    return _result(
        meta,
        _cycle(meta, cycle),
        action=(
            f"{dataset_key}_identity_prepared"
            if state == next_state
            else f"{dataset_key}_identity_batch"
        ),
        dataset_key=dataset_key,
        target_count=len(members),
        identity_cursor=next_cursor,
        cohort_snapshot_id=snapshot_id,
        identity=identity,
        stop_reason=identity.stop_reason,
    )


def _identity_bridge_start(
    meta: MetaStore, members: Sequence[Any], dataset_key: str
) -> date:
    if not members:
        raise ValueError("intraday identity bridge requires cohort members")
    snapshot = meta.ongoing_cohort_snapshot(str(members[0]["snapshot_id"]))
    if snapshot is None:
        raise RuntimeError("ongoing cohort snapshot disappeared")
    starts = [date.fromisoformat(str(snapshot["as_of_session"]))]
    cohort_start = starts[0]
    for row in members:
        covered = meta.get_coverage(str(row["instrument_id"]), dataset_key)
        if covered is not None:
            overlap_start = covered[1] - timedelta(days=REFRESH_WINDOW_DAYS)
            if covered[0] >= cohort_start:
                overlap_start = max(cohort_start, overlap_start)
            starts.append(overlap_start)
    return max(IEX_HISTORY_START, min(starts))


def _current_member_ticker(meta: MetaStore, member: Any, session: date) -> str:
    instrument_id = str(member["instrument_id"])
    active = [
        str(row["ticker"])
        for row in meta.instrument_alias_records(instrument_id)
        if date.fromisoformat(str(row["start_date"]))
        <= session
        <= date.fromisoformat(str(row["end_date"]))
    ]
    tickers = sorted(set(active))
    if len(tickers) != 1:
        return str(member["ticker"])
    report = meta.resolve_alias_range(tickers[0], session, session)
    segment = report.segments[0]
    return (
        tickers[0]
        if segment.status == "resolved" and segment.instrument_id == instrument_id
        else str(member["ticker"])
    )


def _run_dataset_step(
    client: OngoingProgramClient,
    bars: BarStore,
    meta: MetaStore,
    cycle: Any,
    *,
    dataset_key: str,
    members: Sequence[CurrentJobMember],
    default_start: date,
    next_state: str,
    max_units: int,
    policy: BudgetPolicy,
) -> OngoingProgramStepResult:
    job_field = {
        "eod": "eod_job_id",
        "intraday_1hour": "hourly_job_id",
        "intraday_5min": "five_min_job_id",
    }[dataset_key]
    job_id = str(cycle[job_field])
    session = date.fromisoformat(str(cycle["session_date"]))
    if meta.history_job(job_id) is None:
        initialize_current_job(
            meta,
            job_id=job_id,
            dataset_key=dataset_key,
            members=members,
            end=session,
            default_start=default_start,
            refresh_overlap_days=REFRESH_WINDOW_DAYS,
        )
        job = meta.history_job(job_id)
        assert job is not None
        if str(job["status"]) in {"complete", "blocked"}:
            terminal_state = next_state
            if next_state == "complete":
                terminal_state = (
                    "complete_with_exclusions"
                    if _cycle_has_exclusions(meta, cycle)
                    else "complete"
                )
            _advance_cycle(meta, cycle, state=terminal_state, last_stop_reason=None)
        return _result(
            meta,
            _cycle(meta, cycle),
            action="dataset_job_initialized",
            dataset_key=dataset_key,
            target_count=len(members),
            cohort_snapshot_id=cycle["cohort_snapshot_id"],
        )
    sweep = run_history_sweep(
        client,
        bars,
        meta,
        job_id,
        policy=policy,
        max_units=max_units,
    )
    if sweep.job_status in {"complete", "blocked", "cancelled"}:
        if next_state == "complete":
            terminal_state = (
                "complete_with_exclusions"
                if _cycle_has_exclusions(meta, cycle)
                else "complete"
            )
            _advance_cycle(
                meta,
                cycle,
                state=terminal_state,
                last_stop_reason=sweep.stop_reason,
            )
        else:
            _advance_cycle(
                meta,
                cycle,
                state=next_state,
                last_stop_reason=sweep.stop_reason,
            )
    else:
        _advance_cycle(meta, cycle, last_stop_reason=sweep.stop_reason)
    return _result(
        meta,
        _cycle(meta, cycle),
        action="dataset_sweep",
        dataset_key=dataset_key,
        target_count=len(members),
        cohort_snapshot_id=cycle["cohort_snapshot_id"],
        sweep=sweep,
        stop_reason=sweep.stop_reason,
    )


def _cycle_has_exclusions(meta: MetaStore, cycle: Any) -> bool:
    return any(
        (job := meta.history_job(str(cycle[field]))) is not None
        and (bool(job["cancelled"]) or str(job["status"]) == "blocked")
        for field in ("eod_job_id", "hourly_job_id", "five_min_job_id")
    )


@data_directory_locked("ingest:ongoing-cohort-rank")
def _select_or_create_cohort(meta: MetaStore, bars: BarStore, program: Any, cycle: Any):
    session = date.fromisoformat(str(cycle["session_date"]))
    latest = meta.latest_ongoing_cohort_snapshot(
        str(program["program_id"]), through=session
    )
    if latest is not None:
        latest_session = date.fromisoformat(str(latest["as_of_session"]))
        if (latest_session.year, latest_session.month) == (session.year, session.month):
            return latest

    schedule = session_schedule(session - timedelta(days=90), session)
    sessions = schedule["session_date"].to_list()
    lookback_count = int(program["lookback_sessions"])
    if len(sessions) < lookback_count:
        raise ValueError("not enough XNYS sessions for the ongoing liquidity rank")
    selected_sessions = sessions[-lookback_count:]
    lookback_start, lookback_end = selected_sessions[0], selected_sessions[-1]
    job_id = str(cycle["eod_job_id"])
    ticker_by_instrument: dict[str, str] = {}
    for target in meta.history_targets(job_id):
        ranges = meta.history_ranges(job_id, int(target["target_ordinal"]))
        if ranges:
            ticker_by_instrument[str(target["instrument_id"])] = str(
                ranges[-1]["ticker"]
            )
    if not ticker_by_instrument:
        raise ValueError("completed all-active EOD job has no rankable instruments")
    if not bars.canonical_eod_files():
        raise ValueError("canonical EOD bars are required for the ongoing rank")

    con = duckdb.connect()
    try:
        create_canonical_parquet_view(con, "eod", bars.canonical_eod_glob())
        con.register(
            "eligible_instruments",
            pl.DataFrame(
                {"instrument_id": sorted(ticker_by_instrument)},
                schema={"instrument_id": pl.Utf8},
            ),
        )
        con.register(
            "ranking_sessions",
            pl.DataFrame({"date": selected_sessions}, schema={"date": pl.Date}),
        )
        rows = con.execute(
            """SELECT bars.instrument_id,
                      avg(CAST(bars.close AS DOUBLE) * bars.volume) AS adv,
                      count(*) AS observations
                 FROM eod AS bars
                 JOIN eligible_instruments USING (instrument_id)
                 JOIN ranking_sessions USING (date)
                WHERE bars.close >= 0 AND bars.volume >= 0
                GROUP BY bars.instrument_id
               HAVING count(*) >= ?
                ORDER BY adv DESC, bars.instrument_id
                LIMIT ?""",
            [int(program["min_observations"]), int(program["cohort_size"])],
        ).fetchall()
    finally:
        con.close()
    members = [
        {
            "rank": rank,
            "instrument_id": str(instrument_id),
            "ticker": ticker_by_instrument[str(instrument_id)],
            "avg_dollar_volume": float(adv),
            "observation_count": int(observations),
        }
        for rank, (instrument_id, adv, observations) in enumerate(rows, start=1)
    ]
    return meta.create_ongoing_cohort_snapshot(
        program_id=str(program["program_id"]),
        as_of_session=session,
        lookback_start=lookback_start,
        lookback_end=lookback_end,
        cohort_size=int(program["cohort_size"]),
        min_observations=int(program["min_observations"]),
        members=members,
    )


@data_directory_locked("ingest:ongoing-cycle-checkpoint")
def _advance_cycle(meta: MetaStore, cycle: Any, **changes: Any) -> None:
    meta.update_ongoing_cycle(
        str(cycle["program_id"]),
        date.fromisoformat(str(cycle["session_date"])),
        **changes,
    )


def _cycle(meta: MetaStore, cycle: Any):
    refreshed = meta.ongoing_cycle(
        str(cycle["program_id"]), date.fromisoformat(str(cycle["session_date"]))
    )
    if refreshed is None:
        raise RuntimeError("ongoing cycle disappeared")
    return refreshed


def _result(
    meta: MetaStore, cycle: Any, *, action: str, **values: Any
) -> OngoingProgramStepResult:
    jobs: dict[str, dict[str, Any]] = {}
    for dataset_key, field_name in (
        ("eod", "eod_job_id"),
        ("intraday_1hour", "hourly_job_id"),
        ("intraday_5min", "five_min_job_id"),
    ):
        job = meta.history_job(str(cycle[field_name]))
        if job is not None:
            jobs[dataset_key] = {
                "job_id": str(job["job_id"]),
                "status": (
                    "cancelled" if bool(job["cancelled"]) else str(job["status"])
                ),
                "target_count": meta.history_target_count(str(job["job_id"])),
                "cursor": int(job["cursor"]),
                "sweep": int(job["sweep"]),
                "has_exclusions": bool(job["cancelled"])
                or meta.history_has_blockers(str(job["job_id"])),
            }
    return OngoingProgramStepResult(
        program_id=str(cycle["program_id"]),
        session_date=date.fromisoformat(str(cycle["session_date"])),
        cycle_state=str(cycle["state"]),
        action=action,
        jobs=jobs,
        **values,
    )
