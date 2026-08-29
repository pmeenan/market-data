"""Durable orchestration for the ordered D-011 historical program."""

from __future__ import annotations

import hashlib
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from marketdata.identity_bootstrap import (
    IdentityBootstrapResult,
    IntradayIdentityBootstrapResult,
    bootstrap_eod_identities,
    bootstrap_intraday_identities,
    supported_us_stock_etf_records,
)
from marketdata.jsonutil import canonical_json
from marketdata.locking import data_directory_locked
from marketdata.scheduler import (
    DEFAULT_BUDGET_POLICY,
    BudgetPolicy,
    SchedulerRunResult,
    initialize_history_job,
    run_history_request,
)
from marketdata.store import BarStore, MetaStore

SEED_SCOPE = "seed-universes-v1"
SUPPORTED_US_SCOPE = "tiingo-supported-us-v1"
DEFAULT_PROGRAM_ID = "main-v1"
DEFAULT_PHASE1_EOD_JOB_ID = "phase1-seed-eod-episodes-v3-20060828-20260827"
DEFAULT_PHASE1_HOURLY_JOB_ID = "phase1-seed-hourly-v1-20161212-20260827"


class BackfillProgramClient(Protocol):
    response_bytes: int

    def supported_tickers(
        self, tickers: Collection[str] | None = None
    ) -> list[dict[str, str]]: ...

    def ticker_metadata(self, ticker: str) -> dict[str, Any]: ...

    def eod(self, ticker: str, start: date, end: date) -> list[dict[str, Any]]: ...

    def intraday(
        self, ticker: str, start: date, end: date, freq: str = "1hour"
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class BackfillProgramComponent:
    component_key: str
    component_ordinal: int
    phase: int
    dataset_key: str
    scope_key: str
    start: date
    end: date
    job_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_key": self.component_key,
            "component_ordinal": self.component_ordinal,
            "phase": self.phase,
            "dataset_key": self.dataset_key,
            "scope_key": self.scope_key,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "job_id": self.job_id,
        }


@dataclass
class BackfillProgramStepResult:
    program_id: str
    program_status: str
    action: str
    component_key: str | None = None
    phase: int | None = None
    dataset_key: str | None = None
    component_state: str | None = None
    cohort_count: int | None = None
    identity_cursor: int | None = None
    identity: IdentityBootstrapResult | IntradayIdentityBootstrapResult | None = None
    history: SchedulerRunResult | None = None
    stop_reason: str | None = None

    @property
    def partial(self) -> bool:
        return bool(
            self.component_state == "blocked"
            or (self.identity is not None and self.identity.partial)
            or (self.history is not None and self.history.ingest.partial)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "program": {
                "program_id": self.program_id,
                "status": self.program_status,
                "action": self.action,
                "component_key": self.component_key,
                "phase": self.phase,
                "dataset_key": self.dataset_key,
                "component_state": self.component_state,
                "cohort_count": self.cohort_count,
                "identity_cursor": self.identity_cursor,
                "stop_reason": self.stop_reason,
            },
            "identity": self.identity.to_dict() if self.identity is not None else None,
            "history": self.history.to_dict() if self.history is not None else None,
            "partial": self.partial,
            "ok": not self.partial,
        }


class _SnapshotMetadataClient:
    """Serve the frozen archive while delegating authenticated metadata."""

    def __init__(
        self,
        client: BackfillProgramClient,
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


def default_program_components(
    *,
    eod_start: date,
    intraday_start: date,
    end: date,
    phase1_eod_job_id: str,
    phase1_hourly_job_id: str,
) -> tuple[BackfillProgramComponent, ...]:
    """Declare the one supported production program in D-011 order."""
    return (
        BackfillProgramComponent(
            "phase1_seed_eod",
            10,
            1,
            "eod",
            SEED_SCOPE,
            eod_start,
            end,
            phase1_eod_job_id,
        ),
        BackfillProgramComponent(
            "phase1_seed_hourly",
            20,
            1,
            "intraday_1hour",
            SEED_SCOPE,
            intraday_start,
            end,
            phase1_hourly_job_id,
        ),
        BackfillProgramComponent(
            "phase2_all_eod",
            30,
            2,
            "eod",
            SUPPORTED_US_SCOPE,
            eod_start,
            end,
            f"phase2-all-eod-v1-{eod_start:%Y%m%d}-{end:%Y%m%d}",
        ),
        BackfillProgramComponent(
            "phase3_seed_5min",
            40,
            3,
            "intraday_5min",
            SEED_SCOPE,
            intraday_start,
            end,
            f"phase3-seed-5min-v1-{intraday_start:%Y%m%d}-{end:%Y%m%d}",
        ),
    )


@data_directory_locked("ingest:backfill-program-initialize")
def initialize_default_backfill_program(
    meta: MetaStore,
    *,
    program_id: str,
    phase1_eod_job_id: str,
    phase1_hourly_job_id: str,
) -> None:
    """Adopt the completed phase-1 jobs and freeze the shared seed scope."""
    seed_tickers = meta.all_universe_tickers()
    if not seed_tickers:
        raise ValueError("the default backfill program requires stored seed universes")
    eod_job = _required_adopted_job(meta, phase1_eod_job_id, "eod")
    hourly_job = _required_adopted_job(meta, phase1_hourly_job_id, "intraday_1hour")
    if str(eod_job["range_end"]) != str(hourly_job["range_end"]):
        raise ValueError("phase-1 EOD and hourly jobs must share one frozen end date")
    end = date.fromisoformat(str(eod_job["range_end"]))
    eod_start = date.fromisoformat(str(eod_job["range_start"]))
    intraday_start = date.fromisoformat(str(hourly_job["range_start"]))
    initialize_history_job(
        meta,
        job_id=phase1_eod_job_id,
        phase=1,
        dataset_key="eod",
        tickers=seed_tickers,
        start=eod_start,
        end=end,
    )
    initialize_history_job(
        meta,
        job_id=phase1_hourly_job_id,
        phase=1,
        dataset_key="intraday_1hour",
        tickers=seed_tickers,
        start=intraday_start,
        end=end,
    )
    components = default_program_components(
        eod_start=eod_start,
        intraday_start=intraday_start,
        end=end,
        phase1_eod_job_id=phase1_eod_job_id,
        phase1_hourly_job_id=phase1_hourly_job_id,
    )
    definition_hash = hashlib.sha256(
        canonical_json([component.to_dict() for component in components]).encode()
    ).hexdigest()
    meta.create_backfill_program(
        program_id=program_id,
        definition_hash=definition_hash,
        components=[
            {
                "component_key": component.component_key,
                "component_ordinal": component.component_ordinal,
                "phase": component.phase,
                "dataset_key": component.dataset_key,
                "scope_key": component.scope_key,
                "start": component.start,
                "end": component.end,
                "job_id": component.job_id,
            }
            for component in components
        ],
    )
    scope = meta.freeze_backfill_program_scope(
        program_id=program_id,
        scope_key=SEED_SCOPE,
        source_kind="seed_universes",
        tickers=seed_tickers,
    )
    for component_key in ("phase1_seed_eod", "phase1_seed_hourly"):
        meta.advance_backfill_program_identity(
            program_id=program_id,
            component_key=component_key,
            cursor=int(scope["ticker_count"]),
            prepared=True,
            stop_reason=None,
        )
    sync_backfill_program(meta, program_id)


def _required_adopted_job(meta: MetaStore, job_id: str, dataset_key: str) -> Any:
    job = meta.history_job(job_id)
    if job is None:
        raise ValueError(f"required phase-1 history job {job_id!r} does not exist")
    if int(job["phase"]) != 1 or str(job["dataset_key"]) != dataset_key:
        raise ValueError(f"phase-1 history job {job_id!r} has the wrong definition")
    if bool(job["cancelled"]):
        raise ValueError(f"phase-1 history job {job_id!r} is cancelled")
    return job


@data_directory_locked("ingest:backfill-program-sync")
def sync_backfill_program(meta: MetaStore, program_id: str) -> str:
    """Mirror designated job state and derive the overall program state."""
    if meta.backfill_program(program_id) is None:
        raise ValueError(f"unknown backfill program {program_id!r}")
    for component in meta.backfill_program_components(program_id):
        job = meta.history_job(str(component["job_id"]))
        if job is None:
            continue
        if bool(job["cancelled"]):
            raise ValueError(
                f"designated backfill job {job['job_id']!r} is cancelled; "
                "register its replacement before advancing the program"
            )
        expected = (
            int(component["phase"]),
            str(component["dataset_key"]),
            str(component["range_start"]),
            str(component["range_end"]),
        )
        actual = (
            int(job["phase"]),
            str(job["dataset_key"]),
            str(job["range_start"]),
            str(job["range_end"]),
        )
        if actual != expected:
            raise ValueError(
                f"designated backfill job {job['job_id']!r} does not match its "
                "program component"
            )
        meta.set_backfill_program_component_state(
            program_id=program_id,
            component_key=str(component["component_key"]),
            state=str(job["status"]),
        )
    states = [str(row["state"]) for row in meta.backfill_program_components(program_id)]
    if all(state in {"complete", "blocked"} for state in states):
        status = "complete_with_exclusions" if "blocked" in states else "complete"
    else:
        status = "active"
    meta.set_backfill_program_status(program_id, status)
    return status


@data_directory_locked("ingest:backfill-program-freeze-scope")
def _freeze_supported_scope(
    meta: MetaStore,
    *,
    program_id: str,
    rows: Sequence[Mapping[str, str]],
) -> Any:
    tickers = sorted({str(row["ticker"]) for row in rows})
    return meta.freeze_backfill_program_scope(
        program_id=program_id,
        scope_key=SUPPORTED_US_SCOPE,
        source_kind="tiingo_supported_us",
        tickers=tickers,
        supported_records=rows,
    )


@data_directory_locked("ingest:backfill-program-identity-checkpoint")
def _checkpoint_identity(
    meta: MetaStore,
    *,
    program_id: str,
    component_key: str,
    cursor: int,
    prepared: bool,
    stop_reason: str | None,
) -> None:
    meta.advance_backfill_program_identity(
        program_id=program_id,
        component_key=component_key,
        cursor=cursor,
        prepared=prepared,
        stop_reason=stop_reason,
    )


def run_backfill_program_step(
    client: BackfillProgramClient,
    bars: BarStore,
    meta: MetaStore,
    *,
    program_id: str,
    identity_batch_size: int = 250,
    max_history_units: int | None = 500,
    policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
) -> BackfillProgramStepResult:
    """Perform one bounded preparation batch or one historical sweep prefix."""
    if identity_batch_size <= 0:
        raise ValueError("identity_batch_size must be positive")
    status = sync_backfill_program(meta, program_id)
    components = meta.backfill_program_components(program_id)
    component = next(
        (row for row in components if str(row["state"]) not in {"complete", "blocked"}),
        None,
    )
    if component is None:
        return BackfillProgramStepResult(program_id, status, "program_complete")

    component_key = str(component["component_key"])
    phase = int(component["phase"])
    dataset_key = str(component["dataset_key"])
    scope_key = str(component["scope_key"])
    base = {
        "program_id": program_id,
        "program_status": status,
        "component_key": component_key,
        "phase": phase,
        "dataset_key": dataset_key,
        "component_state": str(component["state"]),
    }

    scope = meta.backfill_program_scope(program_id, scope_key)
    if scope is None:
        if scope_key != SUPPORTED_US_SCOPE:
            raise ValueError(
                f"backfill program scope {scope_key!r} was not frozen at initialization"
            )
        records = supported_us_stock_etf_records(client.supported_tickers())
        if not records:
            raise ValueError("Tiingo supported-tickers snapshot has no in-scope rows")
        scope = _freeze_supported_scope(meta, program_id=program_id, rows=records)
        return BackfillProgramStepResult(
            **base,
            action="scope_frozen",
            cohort_count=int(scope["ticker_count"]),
        )

    tickers = meta.backfill_program_tickers(program_id, scope_key)
    cohort_count = len(tickers)
    if str(component["identity_status"]) != "prepared":
        cursor = int(component["identity_cursor"])
        batch = tickers[cursor : cursor + identity_batch_size]
        if not batch:
            _checkpoint_identity(
                meta,
                program_id=program_id,
                component_key=component_key,
                cursor=cohort_count,
                prepared=True,
                stop_reason=None,
            )
            return BackfillProgramStepResult(
                **base,
                action="identity_prepared",
                cohort_count=cohort_count,
                identity_cursor=cohort_count,
            )
        if dataset_key == "eod":
            if str(scope["source_kind"]) == "tiingo_supported_us":
                archive_rows = meta.backfill_program_supported_records(
                    program_id, scope_key, batch
                )
                identity_client: Any = _SnapshotMetadataClient(client, archive_rows)
            else:
                identity_client = client
            identity: IdentityBootstrapResult | IntradayIdentityBootstrapResult = (
                bootstrap_eod_identities(
                    identity_client,
                    meta,
                    batch,
                    policy=policy,
                )
            )
        else:
            identity = bootstrap_intraday_identities(
                client,
                meta,
                batch,
                start=date.fromisoformat(str(component["range_start"])),
                end=date.fromisoformat(str(component["range_end"])),
                freq=dataset_key.removeprefix("intraday_"),
                policy=policy,
            )
        stopped = identity.stop_reason is not None
        next_cursor = cursor if stopped else cursor + len(batch)
        prepared = next_cursor == cohort_count
        _checkpoint_identity(
            meta,
            program_id=program_id,
            component_key=component_key,
            cursor=next_cursor,
            prepared=prepared,
            stop_reason=identity.stop_reason,
        )
        result_base = dict(base)
        result_base["component_state"] = "pending" if prepared else "preparing"
        return BackfillProgramStepResult(
            **result_base,
            action="identity_prepared" if prepared else "identity_batch",
            cohort_count=cohort_count,
            identity_cursor=next_cursor,
            identity=identity,
            stop_reason=identity.stop_reason,
        )

    prerequisite = meta.backfill_program_prerequisite_stop_reason(
        str(component["job_id"]), phase
    )
    if prerequisite is not None:
        return BackfillProgramStepResult(
            **base,
            action="waiting_for_predecessor",
            cohort_count=cohort_count,
            identity_cursor=int(component["identity_cursor"]),
            stop_reason=prerequisite,
        )
    history = run_history_request(
        client,
        bars,
        meta,
        dataset_key=dataset_key,
        tickers=tickers,
        start=date.fromisoformat(str(component["range_start"])),
        end=date.fromisoformat(str(component["range_end"])),
        phase=phase,
        job_id=str(component["job_id"]),
        policy=policy,
        max_units=max_history_units,
    )
    status = sync_backfill_program(meta, program_id)
    refreshed = meta.backfill_program_component(program_id, component_key)
    assert refreshed is not None
    return BackfillProgramStepResult(
        program_id=program_id,
        program_status=status,
        action="history_sweep",
        component_key=component_key,
        phase=phase,
        dataset_key=dataset_key,
        component_state=str(refreshed["state"]),
        cohort_count=cohort_count,
        identity_cursor=int(refreshed["identity_cursor"]),
        history=history,
        stop_reason=history.stop_reason,
    )
