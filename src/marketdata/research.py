"""Failure-safe publication primitives for cataloged research results."""

from __future__ import annotations

import glob
import hashlib
import json
import re
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import duckdb
import polars as pl

from marketdata.calendar import expected_intraday_labels, session_schedule
from marketdata.config import Config
from marketdata.identity import DatasetKey, require_dataset_key
from marketdata.jsonutil import canonical_json
from marketdata.locking import DataDirectoryLock
from marketdata.quality import (
    DEFAULT_ZERO_VOLUME_RUN_LENGTH,
    MIN_ZERO_VOLUME_RUN_LENGTH,
    NONLOCAL_EVENT_GATE_CHECKS,
    QUALITY_CHECKS,
    QUALITY_DUCKDB_MEMORY_LIMIT,
    QualityCheck,
    QualityGateResult,
    QualityReport,
    check_quality,
    evaluate_quality,
    require_memory_limit,
)
from marketdata.research_layout import (
    ResearchRunLayout,
    normalize_relative_data_path,
    research_run_layout,
    resolve_data_path,
)
from marketdata.store.bars import (
    BarStore,
    atomic_write_parquet,
    canonical_dataset_glob,
    canonical_dataset_root,
    require_canonical_generation,
)
from marketdata.store.meta import MetaStore

_STUDY_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EVENT_KEY = ("instrument_id", "event_date")
_EVENT_AUDIT_METRIC_PREFIX = "event_audit."
_EVENT_RUNNER_PARAMETER = "_event_runner"
_OUTCOME_STATUSES = frozenset({"evaluable", "missing_outcome"})
INPUT_MANIFEST_SCHEMA = {
    "run_id": pl.Utf8,
    "input_patterns_json": pl.Utf8,
    "input_metadata_json": pl.Utf8,
    "relative_path": pl.Utf8,
    "content_sha256": pl.Utf8,
    "size_bytes": pl.Int64,
    "first_date": pl.Date,
    "last_date": pl.Date,
}


@dataclass(frozen=True)
class ResearchMetric:
    """One tidy numeric metric, optionally sliced by canonical dimensions."""

    name: str
    value: int | float
    dimensions: Mapping[str, Any] = field(default_factory=dict)
    unit: str | None = None


@dataclass(frozen=True)
class ResearchOutput:
    """Study-specific observations plus shared catalog metrics."""

    observations: pl.DataFrame
    metrics: Sequence[ResearchMetric] = ()


@dataclass(frozen=True)
class ResearchContext:
    """The immutable input selection handed to a study evaluator."""

    run_id: str
    input_patterns: tuple[str, ...]
    input_files: tuple[Path, ...]
    input_fingerprint: str


@dataclass(frozen=True)
class EventLookback:
    """One local, contiguous bar window required to decide an event."""

    dataset_key: DatasetKey
    start_column: str
    end_column: str


@dataclass(frozen=True)
class EventQualityPolicy:
    """The bounded stored-data checks that must pass before event selection."""

    dataset_keys: tuple[DatasetKey, ...]
    blocking_checks: tuple[QualityCheck, ...]
    start: date
    end: date
    zero_volume_run_length: int = DEFAULT_ZERO_VOLUME_RUN_LENGTH
    memory_limit: str | None = None


@dataclass(frozen=True)
class EventStudyContext:
    """Explicit Parquet views available during one event-study phase.

    The DuckDB connection is valid only for the duration of the callback.  It
    intentionally has no attached metadata database, so universe membership is
    not part of the supported candidate-selection surface.
    """

    run_id: str
    connection: duckdb.DuckDBPyConnection
    dataset_keys: tuple[DatasetKey, ...]
    input_files: Mapping[DatasetKey, tuple[Path, ...]]


@dataclass(frozen=True)
class EventEligibilityAudit:
    """Candidate rows annotated by the reusable D-026 eligibility checks."""

    candidates: pl.DataFrame
    eligible: pl.DataFrame
    counts: Mapping[str, int]


class EventStudyGateError(ValueError):
    """A declared quality policy blocked an event-study publication."""

    def __init__(self, report: QualityReport, gate: QualityGateResult):
        self.report = report
        self.gate = gate
        super().__init__(str(self))

    def __str__(self) -> str:
        checks = sorted(
            {finding.check for finding in self.gate.blocking_findings}
            | set(self.gate.checks_not_run)
        )
        detail = ", ".join(checks) or "unknown checks"
        return f"declared event-study quality gates failed: {detail}"


@dataclass(frozen=True)
class PublishedResearchRun:
    run_id: str
    study_name: str
    study_schema_version: int
    input_fingerprint: str
    observation_count: int
    observation_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class InputFingerprintStatus:
    run_id: str
    expected_fingerprint: str
    current_fingerprint: str | None
    matches: bool
    missing_files: tuple[str, ...]
    added_files: tuple[str, ...]
    changed_files: tuple[str, ...]
    metadata_changed: bool


@dataclass(frozen=True)
class ResearchReconciliationReport:
    """Stale catalog rows and unowned artifact directories under the lock."""

    applied: bool
    stale_running_run_ids: tuple[str, ...]
    orphan_directories: tuple[str, ...]
    failed_run_ids: tuple[str, ...]
    removed_directories: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "stale_running_run_ids": list(self.stale_running_run_ids),
            "orphan_directories": list(self.orphan_directories),
            "failed_run_ids": list(self.failed_run_ids),
            "removed_directories": list(self.removed_directories),
        }


ResearchEvaluator = Callable[[ResearchContext], ResearchOutput]
ResearchInputMetadataBuilder = Callable[
    [tuple[Path, ...], MetaStore], Mapping[str, Any]
]
EventCandidateBuilder = Callable[[EventStudyContext], pl.DataFrame]
EventSelector = Callable[[EventStudyContext, pl.DataFrame], pl.DataFrame]
EventObserver = Callable[[EventStudyContext, pl.DataFrame], ResearchOutput]
RegisteredEventStudy = Callable[[Config, Mapping[str, Any]], PublishedResearchRun]
_EVENT_STUDIES: dict[str, RegisteredEventStudy] = {}


def run_research_publication(
    config: Config,
    *,
    study_name: str,
    study_schema_version: int,
    parameters: Mapping[str, Any],
    input_globs: Sequence[str | Path],
    evaluate: ResearchEvaluator,
    source_revision: str | None = None,
    input_metadata_builder: ResearchInputMetadataBuilder | None = None,
) -> PublishedResearchRun:
    """Run an evaluator against one locked input vintage and publish its result.

    Input patterns are relative to ``config.data_dir``. They are expanded once
    while the shared warehouse lock is held, and the evaluator must read only
    the explicit paths supplied in :class:`ResearchContext`.
    """
    study_name = _require_study_name(study_name)
    if study_schema_version < 1:
        raise ValueError("study_schema_version must be at least 1")
    patterns = _validate_input_globs(input_globs)
    if not callable(evaluate):
        raise TypeError("evaluate must be callable")
    if input_metadata_builder is not None and not callable(input_metadata_builder):
        raise TypeError("input_metadata_builder must be callable")

    run_id = uuid4().hex
    layout = research_run_layout(config.data_dir, study_name, run_id)
    with DataDirectoryLock(
        config.data_dir, operation=f"research publication {study_name}"
    ):
        bars = BarStore(config.data_dir)
        with MetaStore(config.meta_path) as meta:
            require_canonical_generation(bars, meta.storage_generation())
            meta.create_research_run(
                run_id=run_id,
                study_name=study_name,
                study_schema_version=study_schema_version,
                parameters=parameters,
                source_revision=source_revision,
            )
            try:
                input_files = _expand_input_globs(config.data_dir, patterns)
                input_metadata = (
                    {}
                    if input_metadata_builder is None
                    else _normalize_input_metadata(
                        dict(input_metadata_builder(input_files, meta))
                    )
                )
                manifest, fingerprint = _build_input_manifest(
                    config.data_dir,
                    run_id,
                    patterns,
                    input_files,
                    input_metadata,
                )
                output = evaluate(
                    ResearchContext(
                        run_id=run_id,
                        input_patterns=patterns,
                        input_files=input_files,
                        input_fingerprint=fingerprint,
                    )
                )
                if not isinstance(output, ResearchOutput):
                    raise TypeError("evaluate must return ResearchOutput")
                observations = _prepare_observations(meta, run_id, output.observations)
                observation_path, manifest_path = _publish_artifacts(
                    layout, observations, manifest
                )
                relative_observations = observation_path.relative_to(
                    config.data_dir
                ).as_posix()
                relative_manifest = manifest_path.relative_to(
                    config.data_dir
                ).as_posix()
                meta.succeed_research_run(
                    run_id=run_id,
                    input_fingerprint=fingerprint,
                    observation_path=relative_observations,
                    manifest_path=relative_manifest,
                    observation_count=observations.height,
                    metrics=[
                        {
                            "name": metric.name,
                            "value": metric.value,
                            "dimensions": dict(metric.dimensions),
                            "unit": metric.unit,
                        }
                        for metric in output.metrics
                    ],
                )
            except Exception as exc:
                cleanup_error = _cleanup_run_directory(layout.directory)
                summary = f"{type(exc).__name__}: {exc}"
                if cleanup_error is not None:
                    summary += f"; cleanup failed: {cleanup_error}"
                try:
                    meta.fail_research_run(run_id, summary)
                except Exception as catalog_exc:
                    exc.add_note(
                        "could not record the failed research run: "
                        f"{type(catalog_exc).__name__}: {catalog_exc}"
                    )
                raise

    return PublishedResearchRun(
        run_id=run_id,
        study_name=study_name,
        study_schema_version=study_schema_version,
        input_fingerprint=fingerprint,
        observation_count=observations.height,
        observation_path=observation_path,
        manifest_path=manifest_path,
    )


def run_event_study(
    config: Config,
    *,
    study_name: str,
    study_schema_version: int,
    parameters: Mapping[str, Any],
    selection_dataset_keys: Sequence[str],
    outcome_dataset_keys: Sequence[str] = (),
    lookbacks: Sequence[EventLookback],
    quality_policy: EventQualityPolicy,
    build_candidates: EventCandidateBuilder,
    select_events: EventSelector,
    observe_events: EventObserver,
    source_revision: str | None = None,
) -> PublishedResearchRun:
    """Select, audit, evaluate, and publish one vectorized event study.

    Candidate construction and selection can see only ``selection_dataset_keys``.
    Outcome datasets are exposed only after the eligible selected event frame is
    materialized.  This is an event-study boundary, not a portfolio or order
    simulator.

    Selection views contain the full declared datasets, not an as-of sandbox.
    Callbacks must enforce feature availability at each decision timestamp;
    the eligibility audit checks declared lookbacks, not feature provenance.
    Studies must test that changing future rows cannot change earlier signals.
    """
    selection_keys = _normalize_event_dataset_keys(
        selection_dataset_keys, "selection_dataset_keys"
    )
    outcome_keys = _normalize_event_dataset_keys(
        outcome_dataset_keys, "outcome_dataset_keys", allow_empty=True
    )
    all_keys = tuple(dict.fromkeys((*selection_keys, *outcome_keys)))
    normalized_lookbacks = _normalize_event_lookbacks(lookbacks, selection_keys)
    normalized_quality = _normalize_event_quality_policy(quality_policy, all_keys)
    for callback, label in (
        (build_candidates, "build_candidates"),
        (select_events, "select_events"),
        (observe_events, "observe_events"),
    ):
        if not callable(callback):
            raise TypeError(f"{label} must be callable")

    effective_parameters = dict(parameters)
    if _EVENT_RUNNER_PARAMETER in effective_parameters:
        raise ValueError(
            f"{_EVENT_RUNNER_PARAMETER!r} is reserved for the event runner"
        )
    effective_parameters[_EVENT_RUNNER_PARAMETER] = {
        "kind": "vectorized_event_study",
        "selection_dataset_keys": list(selection_keys),
        "outcome_dataset_keys": list(outcome_keys),
        "lookbacks": [
            {
                "dataset_key": requirement.dataset_key,
                "start_column": requirement.start_column,
                "end_column": requirement.end_column,
            }
            for requirement in normalized_lookbacks
        ],
        "quality_policy": {
            "dataset_keys": list(normalized_quality.dataset_keys),
            "blocking_checks": list(normalized_quality.blocking_checks),
            "start": normalized_quality.start.isoformat(),
            "end": normalized_quality.end.isoformat(),
            "zero_volume_run_length": normalized_quality.zero_volume_run_length,
            "memory_limit": normalized_quality.memory_limit
            or QUALITY_DUCKDB_MEMORY_LIMIT,
            "empty_row_checks": "vacuously_checked",
        },
        "semantics": "event_study_without_portfolio_or_order_simulation",
    }
    input_patterns = tuple(
        Path(canonical_dataset_glob(config.data_dir, key))
        .relative_to(config.data_dir)
        .as_posix()
        for key in all_keys
    )

    def build_input_metadata(
        input_files: tuple[Path, ...], meta: MetaStore
    ) -> Mapping[str, Any]:
        files_by_dataset = _partition_event_input_files(
            config.data_dir, input_files, all_keys
        )
        instrument_ids = _event_input_instrument_ids(files_by_dataset, selection_keys)
        return _identity_input_metadata(meta, instrument_ids)

    def evaluate(publication: ResearchContext) -> ResearchOutput:
        files_by_dataset = _partition_event_input_files(
            config.data_dir, publication.input_files, all_keys
        )
        selection_connection = _connect_event_inputs(files_by_dataset, selection_keys)
        try:
            selection_context = EventStudyContext(
                run_id=publication.run_id,
                connection=selection_connection,
                dataset_keys=selection_keys,
                input_files={key: files_by_dataset[key] for key in selection_keys},
            )
            candidates = build_candidates(selection_context)
            audit = audit_event_eligibility(
                config,
                selection_context,
                candidates,
                normalized_lookbacks,
            )
            quality_report = check_quality(
                config,
                dataset_keys=normalized_quality.dataset_keys,
                instrument_ids=tuple(
                    audit.candidates["instrument_id"].unique().sort().to_list()
                ),
                start=normalized_quality.start,
                end=normalized_quality.end,
                zero_volume_run_length=normalized_quality.zero_volume_run_length,
                empty_row_checks_are_run=True,
                memory_limit=normalized_quality.memory_limit,
            )
            quality_gate = evaluate_quality(
                quality_report, normalized_quality.blocking_checks
            )
            if not quality_gate.passed:
                raise EventStudyGateError(quality_report, quality_gate)
            selected = select_events(selection_context, audit.eligible)
            selected = _validate_selected_events(audit.eligible, selected)
        finally:
            selection_connection.close()

        outcome_connection = _connect_event_inputs(files_by_dataset, all_keys)
        try:
            outcome_context = EventStudyContext(
                run_id=publication.run_id,
                connection=outcome_connection,
                dataset_keys=all_keys,
                input_files=files_by_dataset,
            )
            output = observe_events(outcome_context, selected)
        finally:
            outcome_connection.close()
        return _prepare_event_output(
            output,
            selected=selected,
            audit=audit,
            quality_report=quality_report,
        )

    return run_research_publication(
        config,
        study_name=study_name,
        study_schema_version=study_schema_version,
        parameters=effective_parameters,
        input_globs=input_patterns,
        evaluate=evaluate,
        source_revision=source_revision,
        input_metadata_builder=build_input_metadata,
    )


def audit_event_eligibility(
    config: Config,
    context: EventStudyContext,
    candidates: pl.DataFrame,
    lookbacks: Sequence[EventLookback],
) -> EventEligibilityAudit:
    """Apply local identity, calendar, and contiguous-lookback checks.

    Only the declared windows through ``decision_ts`` are inspected.  Coverage
    rows, terminal backfill ranges, remote history, and outcome datasets are not
    inputs to this audit.
    """
    requirements = _normalize_event_lookbacks(lookbacks, context.dataset_keys)
    frame = _validate_event_candidates(candidates, requirements)
    if frame.is_empty():
        annotated = frame.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("eligibility_status")
        )
        return EventEligibilityAudit(
            candidates=annotated,
            eligible=frame,
            counts=_empty_eligibility_counts(),
        )

    working = frame.with_row_index("_event_row_id")
    con = context.connection
    con.register("_event_candidates", working)
    event_start = cast(date, working["event_date"].min())
    event_end = cast(date, working["event_date"].max())
    schedule = session_schedule(event_start, event_end)
    con.register("_event_sessions", schedule)
    calendar_status = con.execute(
        """SELECT candidates._event_row_id,
                  count(sessions.session_date) = 1
                  AND min(candidates.decision_ts) >= min(sessions.session_open)
                  AND max(candidates.decision_ts) <= max(sessions.session_close)
                     AS _calendar_complete
             FROM _event_candidates AS candidates
             LEFT JOIN _event_sessions AS sessions
               ON sessions.session_date = candidates.event_date
            GROUP BY candidates._event_row_id
            ORDER BY candidates._event_row_id"""
    ).pl()

    instrument_ids = tuple(working["instrument_id"].unique().sort().to_list())
    with MetaStore(config.meta_path) as meta:
        alias_rows = meta.instrument_aliases_for_instruments(instrument_ids)
    aliases = pl.DataFrame(
        [
            {
                "instrument_id": str(row["instrument_id"]),
                "ticker": str(row["ticker"]),
                "start_date": date.fromisoformat(str(row["start_date"])),
                "end_date": date.fromisoformat(str(row["end_date"])),
            }
            for row in alias_rows
        ],
        schema={
            "instrument_id": pl.Utf8,
            "ticker": pl.Utf8,
            "start_date": pl.Date,
            "end_date": pl.Date,
        },
    )
    con.register("_event_aliases", aliases)
    identity_status = con.execute(
        """SELECT candidates._event_row_id,
                  count(DISTINCT aliases.ticker) = 1 AS _identity_complete
             FROM _event_candidates AS candidates
             LEFT JOIN _event_aliases AS aliases
               ON aliases.instrument_id = candidates.instrument_id
              AND candidates.event_date
                  BETWEEN aliases.start_date AND aliases.end_date
            GROUP BY candidates._event_row_id
            ORDER BY candidates._event_row_id"""
    ).pl()

    status = (
        working.select("_event_row_id")
        .join(calendar_status, on="_event_row_id", how="left")
        .join(identity_status, on="_event_row_id", how="left")
    )
    lookback_columns: list[str] = []
    for index, requirement in enumerate(requirements):
        complete_column = f"_lookback_{index}_complete"
        lookback_columns.append(complete_column)
        result = _audit_one_lookback(con, working, requirement, complete_column)
        invalid_windows = result.filter(pl.col("_expected_count") == 0)
        if invalid_windows.height:
            sample = (
                invalid_windows.join(working, on="_event_row_id", how="left")
                .select(*_EVENT_KEY)
                .head(10)
                .rows()
            )
            raise ValueError(
                f"event lookback {requirement.dataset_key!r} contains no expected "
                f"bar labels for candidates: {sample}"
            )
        status = status.join(
            result.select("_event_row_id", complete_column),
            on="_event_row_id",
            how="left",
        )

    complete_lookback = pl.all_horizontal(
        pl.col(column).fill_null(False) for column in lookback_columns
    )
    status = status.with_columns(
        pl.when(~pl.col("_identity_complete").fill_null(False))
        .then(pl.lit("identity_excluded"))
        .when(~pl.col("_calendar_complete").fill_null(False))
        .then(pl.lit("calendar_excluded"))
        .when(~complete_lookback)
        .then(pl.lit("lookback_incomplete"))
        .otherwise(pl.lit("eligible"))
        .alias("eligibility_status")
    )
    annotated = working.join(
        status.select("_event_row_id", "eligibility_status"),
        on="_event_row_id",
        how="left",
    ).drop("_event_row_id")
    eligible = annotated.filter(pl.col("eligibility_status") == "eligible").drop(
        "eligibility_status"
    )
    counts = _empty_eligibility_counts()
    counts["candidates"] = annotated.height
    for value, count in annotated.group_by("eligibility_status").len().iter_rows():
        counts[str(value)] = int(count)
    return EventEligibilityAudit(annotated, eligible, counts)


def register_event_study(
    study_name: str, runner: RegisteredEventStudy, *, replace: bool = False
) -> None:
    """Register one focused built-in study for the common CLI entry point."""
    normalized = _require_study_name(study_name)
    if not callable(runner):
        raise TypeError("event study runner must be callable")
    if normalized in _EVENT_STUDIES and not replace:
        raise ValueError(f"event study is already registered: {normalized}")
    _EVENT_STUDIES[normalized] = runner


def registered_event_studies() -> tuple[str, ...]:
    """Return CLI-visible built-in study names in deterministic order."""
    return tuple(sorted(_EVENT_STUDIES))


def run_registered_event_study(
    config: Config, study_name: str, parameters: Mapping[str, Any]
) -> PublishedResearchRun:
    """Dispatch a registered study without permitting an alternate CLI path."""
    normalized = _require_study_name(study_name)
    try:
        runner = _EVENT_STUDIES[normalized]
    except KeyError as exc:
        available = ", ".join(registered_event_studies()) or "none"
        raise ValueError(
            f"unknown event study {normalized!r}; registered studies: {available}"
        ) from exc
    return runner(config, parameters)


def _normalize_event_dataset_keys(
    values: Sequence[str], label: str, *, allow_empty: bool = False
) -> tuple[DatasetKey, ...]:
    normalized = tuple(
        dict.fromkeys(cast(DatasetKey, require_dataset_key(value)) for value in values)
    )
    if not normalized and not allow_empty:
        raise ValueError(f"{label} must contain at least one dataset key")
    return normalized


def _normalize_event_lookbacks(
    lookbacks: Sequence[EventLookback], selection_keys: Sequence[DatasetKey]
) -> tuple[EventLookback, ...]:
    requirements: list[EventLookback] = []
    for requirement in lookbacks:
        if not isinstance(requirement, EventLookback):
            raise TypeError("lookbacks must contain EventLookback values")
        dataset_key = cast(DatasetKey, require_dataset_key(requirement.dataset_key))
        if dataset_key not in selection_keys:
            raise ValueError(
                f"lookback dataset {dataset_key!r} is not a selection dataset"
            )
        start_column = _require_column_name(
            requirement.start_column, "lookback start_column"
        )
        end_column = _require_column_name(requirement.end_column, "lookback end_column")
        requirements.append(EventLookback(dataset_key, start_column, end_column))
    if not requirements:
        raise ValueError("at least one local event lookback is required")
    declared_datasets = {requirement.dataset_key for requirement in requirements}
    unused = sorted(set(selection_keys) - declared_datasets)
    if unused:
        raise ValueError(f"every selection dataset needs a declared lookback: {unused}")
    return tuple(requirements)


def _normalize_event_quality_policy(
    policy: EventQualityPolicy, available_keys: Sequence[DatasetKey]
) -> EventQualityPolicy:
    if not isinstance(policy, EventQualityPolicy):
        raise TypeError("quality_policy must be an EventQualityPolicy")
    dataset_keys = _normalize_event_dataset_keys(
        policy.dataset_keys, "quality_policy.dataset_keys"
    )
    unavailable = sorted(set(dataset_keys) - set(available_keys))
    if unavailable:
        raise ValueError(
            f"quality policy references undeclared input datasets: {unavailable}"
        )
    blocking: list[QualityCheck] = []
    for check in dict.fromkeys(policy.blocking_checks):
        if check not in QUALITY_CHECKS:
            raise ValueError(f"unknown quality check {check!r}")
        blocking.append(cast(QualityCheck, check))
    if not blocking:
        raise ValueError("event studies must declare at least one blocking check")
    disallowed = sorted(set(blocking) & set(NONLOCAL_EVENT_GATE_CHECKS))
    if disallowed:
        raise ValueError(
            "full-history coverage checks cannot gate local event eligibility: "
            f"{disallowed}"
        )
    if policy.start > policy.end:
        raise ValueError("quality policy start must not be after end")
    if policy.zero_volume_run_length < MIN_ZERO_VOLUME_RUN_LENGTH:
        raise ValueError(
            "quality policy zero_volume_run_length must be at least "
            f"{MIN_ZERO_VOLUME_RUN_LENGTH}"
        )
    return EventQualityPolicy(
        dataset_keys=dataset_keys,
        blocking_checks=tuple(blocking),
        start=policy.start,
        end=policy.end,
        zero_volume_run_length=policy.zero_volume_run_length,
        memory_limit=(
            require_memory_limit(policy.memory_limit)
            if policy.memory_limit is not None
            else None
        ),
    )


def _partition_event_input_files(
    data_dir: Path,
    input_files: Sequence[Path],
    dataset_keys: Sequence[DatasetKey],
) -> dict[DatasetKey, tuple[Path, ...]]:
    root = data_dir.resolve()
    prefixes = {
        key: canonical_dataset_root(root, key).relative_to(root).as_posix() + "/"
        for key in dataset_keys
    }
    grouped: dict[DatasetKey, list[Path]] = {key: [] for key in dataset_keys}
    for path in input_files:
        relative = path.relative_to(root).as_posix()
        matches = [key for key in dataset_keys if relative.startswith(prefixes[key])]
        if len(matches) != 1:
            raise RuntimeError(
                f"event-study input file has no unique dataset owner: {relative}"
            )
        grouped[matches[0]].append(path)
    missing = [key for key, paths in grouped.items() if not paths]
    if missing:
        raise RuntimeError(f"event-study datasets have no explicit files: {missing}")
    return {key: tuple(paths) for key, paths in grouped.items()}


def _connect_event_inputs(
    files_by_dataset: Mapping[DatasetKey, tuple[Path, ...]],
    dataset_keys: Sequence[DatasetKey],
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        for dataset_key in dataset_keys:
            paths = [str(path) for path in files_by_dataset[dataset_key]]
            relation = con.from_parquet(
                paths, hive_partitioning=False, union_by_name=False
            )
            relation.create_view(dataset_key)
        return con
    except Exception:
        con.close()
        raise


def _event_input_instrument_ids(
    files_by_dataset: Mapping[DatasetKey, tuple[Path, ...]],
    dataset_keys: Sequence[DatasetKey],
) -> tuple[str, ...]:
    scans = [
        pl.scan_parquet(files_by_dataset[dataset_key], glob=False).select(
            "instrument_id"
        )
        for dataset_key in dataset_keys
    ]
    return tuple(
        pl.concat(scans)
        .select("instrument_id")
        .unique()
        .sort("instrument_id")
        .collect()["instrument_id"]
        .to_list()
    )


def _identity_input_metadata(
    meta: MetaStore, instrument_ids: Sequence[str]
) -> dict[str, Any]:
    selected = tuple(sorted(dict.fromkeys(instrument_ids)))
    rows = meta.instrument_aliases_for_instruments(selected)
    return {
        "identity_aliases": {
            "instrument_ids": list(selected),
            "rows": [
                {
                    "instrument_id": str(row["instrument_id"]),
                    "ticker": str(row["ticker"]),
                    "start_date": str(row["start_date"]),
                    "end_date": str(row["end_date"]),
                }
                for row in rows
            ],
        }
    }


def _validate_event_candidates(
    candidates: pl.DataFrame, requirements: Sequence[EventLookback]
) -> pl.DataFrame:
    if not isinstance(candidates, pl.DataFrame):
        raise TypeError("build_candidates must return a polars DataFrame")
    required = {*_EVENT_KEY, "decision_ts"}
    missing = sorted(required - set(candidates.columns))
    if missing:
        raise ValueError(f"event candidates lack required columns: {missing}")
    if (
        "eligibility_status" in candidates.columns
        or "_event_row_id" in candidates.columns
    ):
        raise ValueError("event candidates use reserved audit columns")
    if candidates.schema["instrument_id"] != pl.Utf8:
        raise ValueError("event candidate instrument_id must be a string")
    if candidates.schema["event_date"] != pl.Date:
        raise ValueError("event candidate event_date must be a date")
    decision_dtype = candidates.schema["decision_ts"]
    if not isinstance(decision_dtype, pl.Datetime) or decision_dtype.time_zone != "UTC":
        raise ValueError("event candidate decision_ts must be a UTC datetime")
    for column in required:
        if candidates[column].null_count():
            raise ValueError(f"event candidate {column} must not be null")
    if candidates.filter(pl.col("instrument_id").str.strip_chars() == "").height:
        raise ValueError("event candidate instrument_id must not be empty")
    if candidates.filter(
        pl.col("instrument_id") != pl.col("instrument_id").str.strip_chars()
    ).height:
        raise ValueError("event candidate instrument_id must not contain whitespace")
    if candidates.select(_EVENT_KEY).is_duplicated().any():
        raise ValueError("event candidates must be unique by instrument_id/event_date")

    for requirement in requirements:
        for column in (requirement.start_column, requirement.end_column):
            if column not in candidates.columns:
                raise ValueError(f"event candidates lack lookback column {column!r}")
            if candidates[column].null_count():
                raise ValueError(f"event lookback column {column!r} must not be null")
        start = pl.col(requirement.start_column)
        end = pl.col(requirement.end_column)
        if requirement.dataset_key == "eod":
            if (
                candidates.schema[requirement.start_column] != pl.Date
                or candidates.schema[requirement.end_column] != pl.Date
            ):
                raise ValueError("EOD lookback bounds must be date columns")
            invalid = (start > end) | (end >= pl.col("event_date"))
        else:
            start_dtype = candidates.schema[requirement.start_column]
            end_dtype = candidates.schema[requirement.end_column]
            if (
                not isinstance(start_dtype, pl.Datetime)
                or start_dtype.time_zone != "UTC"
                or not isinstance(end_dtype, pl.Datetime)
                or end_dtype.time_zone != "UTC"
            ):
                raise ValueError("intraday lookback bounds must be UTC datetimes")
            minutes = 60 if requirement.dataset_key == "intraday_1hour" else 5
            invalid = (start > end) | (
                end + pl.duration(minutes=minutes) > pl.col("decision_ts")
            )
        if candidates.filter(invalid).height:
            raise ValueError(
                f"event lookback {requirement.dataset_key!r} is invalid or noncausal"
            )
    return candidates


def _audit_one_lookback(
    con: duckdb.DuckDBPyConnection,
    candidates: pl.DataFrame,
    requirement: EventLookback,
    complete_column: str,
) -> pl.DataFrame:
    start_value = candidates[requirement.start_column].min()
    end_value = candidates[requirement.end_column].max()
    assert isinstance(start_value, (date, datetime))
    assert isinstance(end_value, (date, datetime))
    if requirement.dataset_key == "eod":
        assert isinstance(start_value, date) and not isinstance(start_value, datetime)
        assert isinstance(end_value, date) and not isinstance(end_value, datetime)
        expected = session_schedule(start_value, end_value).select(
            pl.col("session_date").alias("_expected_time")
        )
        time_column = "date"
    else:
        assert isinstance(start_value, datetime) and isinstance(end_value, datetime)
        freq = requirement.dataset_key.removeprefix("intraday_")
        expected = expected_intraday_labels(start_value, end_value, freq=freq).rename(
            {"ts": "_expected_time"}
        )
        time_column = "ts"
    expected_name = f"_event_expected_{complete_column}"
    con.register(expected_name, expected)
    start_column = _quoted_column(requirement.start_column)
    end_column = _quoted_column(requirement.end_column)
    return con.execute(
        f"""SELECT candidates._event_row_id,
                   count(DISTINCT expected._expected_time) AS _expected_count,
                   count(DISTINCT expected._expected_time) > 0
                   AND count(DISTINCT CASE
                         WHEN bars.instrument_id IS NOT NULL
                         THEN expected._expected_time END)
                       = count(DISTINCT expected._expected_time)
                      AS {complete_column}
              FROM _event_candidates AS candidates
              LEFT JOIN {expected_name} AS expected
                ON expected._expected_time
                   BETWEEN candidates.{start_column} AND candidates.{end_column}
              LEFT JOIN {requirement.dataset_key} AS bars
                ON bars.instrument_id = candidates.instrument_id
               AND bars.{time_column} = expected._expected_time
             GROUP BY candidates._event_row_id
             ORDER BY candidates._event_row_id"""
    ).pl()


def _validate_selected_events(
    eligible: pl.DataFrame, selected: pl.DataFrame
) -> pl.DataFrame:
    selected = _validate_event_candidates(selected, ())
    if selected.schema != eligible.schema:
        raise ValueError("event selectors must preserve the audited candidate schema")
    selected_keys = selected.select(_EVENT_KEY)
    extra = selected_keys.join(
        eligible.select(_EVENT_KEY), on=list(_EVENT_KEY), how="anti"
    ).head(10)
    if extra.height:
        raise ValueError(
            f"selected events were not eligible candidates: {extra.rows()}"
        )
    canonical = eligible.join(
        selected_keys, on=list(_EVENT_KEY), how="semi", maintain_order="left"
    )
    if not selected.sort(list(_EVENT_KEY)).equals(
        canonical.sort(list(_EVENT_KEY)), null_equal=True
    ):
        raise ValueError("event selectors must not mutate audited candidate values")
    return canonical


def _prepare_event_output(
    output: ResearchOutput,
    *,
    selected: pl.DataFrame,
    audit: EventEligibilityAudit,
    quality_report: QualityReport,
) -> ResearchOutput:
    if not isinstance(output, ResearchOutput):
        raise TypeError("observe_events must return ResearchOutput")
    observations = output.observations
    if not isinstance(observations, pl.DataFrame):
        raise TypeError("event observations must be a polars DataFrame")
    required = {*_EVENT_KEY, "observation_label", "outcome_status"}
    missing = sorted(required - set(observations.columns))
    if missing:
        raise ValueError(f"event observations lack required columns: {missing}")
    expected_types = {
        "instrument_id": pl.Utf8,
        "event_date": pl.Date,
        "observation_label": pl.Utf8,
        "outcome_status": pl.Utf8,
    }
    for column, dtype in expected_types.items():
        if observations.schema[column] != dtype:
            raise ValueError(f"event observation {column} has an invalid type")
        if observations[column].null_count():
            raise ValueError(f"event observation {column} must not be null")
    if observations.filter(
        (pl.col("instrument_id").str.strip_chars() == "")
        | (pl.col("observation_label").str.strip_chars() == "")
    ).height:
        raise ValueError("event observation identifiers and labels must not be empty")
    invalid_statuses = sorted(
        set(observations["outcome_status"].unique().to_list()) - _OUTCOME_STATUSES
    )
    if invalid_statuses:
        raise ValueError(f"invalid event outcome statuses: {invalid_statuses}")
    if observations.select(*_EVENT_KEY, "observation_label").is_duplicated().any():
        raise ValueError("event observations must be unique by event and label")
    selected_keys = selected.select(_EVENT_KEY)
    observation_keys = observations.select(_EVENT_KEY).unique()
    missing_events = selected_keys.join(
        observation_keys, on=list(_EVENT_KEY), how="anti"
    ).head(10)
    extra_events = observation_keys.join(
        selected_keys, on=list(_EVENT_KEY), how="anti"
    ).head(10)
    if missing_events.height or extra_events.height:
        raise ValueError(
            "event observations must retain every selected event exactly in scope; "
            f"missing={missing_events.rows()}, extra={extra_events.rows()}"
        )
    reserved_metrics = [
        metric.name
        for metric in output.metrics
        if metric.name.startswith(_EVENT_AUDIT_METRIC_PREFIX)
    ]
    if reserved_metrics:
        raise ValueError(
            f"study metrics use reserved event-audit names: {sorted(reserved_metrics)}"
        )

    evaluable = observations.filter(pl.col("outcome_status") == "evaluable")
    missing_outcomes = observations.filter(
        pl.col("outcome_status") == "missing_outcome"
    )
    event_outcomes = observations.group_by(*_EVENT_KEY).agg(
        (pl.col("outcome_status") == "missing_outcome")
        .any()
        .alias("has_missing_outcome")
    )
    shared_counts = {
        **audit.counts,
        "selected": selected.height,
        "evaluable": event_outcomes.filter(~pl.col("has_missing_outcome")).height,
        "missing_outcome": event_outcomes.filter(pl.col("has_missing_outcome")).height,
    }
    shared_metrics = [
        ResearchMetric(f"{_EVENT_AUDIT_METRIC_PREFIX}{name}", value, unit="events")
        for name, value in shared_counts.items()
    ]
    shared_metrics.extend(
        [
            ResearchMetric(
                f"{_EVENT_AUDIT_METRIC_PREFIX}evaluable_observations",
                evaluable.height,
                unit="observations",
            ),
            ResearchMetric(
                f"{_EVENT_AUDIT_METRIC_PREFIX}missing_outcome_observations",
                missing_outcomes.height,
                unit="observations",
            ),
        ]
    )
    for severity, count in quality_report.finding_counts().items():
        shared_metrics.append(
            ResearchMetric(
                f"{_EVENT_AUDIT_METRIC_PREFIX}quality_findings",
                count,
                dimensions={"severity": severity},
                unit="findings",
            )
        )
    return ResearchOutput(
        observations=observations,
        metrics=(*output.metrics, *shared_metrics),
    )


def _empty_eligibility_counts() -> dict[str, int]:
    return {
        "candidates": 0,
        "eligible": 0,
        "identity_excluded": 0,
        "calendar_excluded": 0,
        "lookback_incomplete": 0,
    }


def _require_column_name(value: str, label: str) -> str:
    if not isinstance(value, str) or not _COLUMN_NAME.fullmatch(value):
        raise ValueError(f"{label} must be a simple column name")
    return value


def _quoted_column(value: str) -> str:
    return f'"{_require_column_name(value, "column")}"'


def reconcile_research_state(
    config: Config, *, apply: bool = False
) -> ResearchReconciliationReport:
    """Report or explicitly clean state left by interrupted research runs.

    Every supported publisher holds the shared data-directory lock for its
    complete lifetime. Once this function owns that lock, any catalog row still
    marked ``running`` is necessarily abandoned rather than concurrently live.
    Dry-run is the default; cleanup and failure transitions require ``apply``.
    """
    root = config.data_dir.resolve()
    results_root = root / "results"
    with DataDirectoryLock(root, operation="research reconciliation"):
        with MetaStore(config.meta_path) as meta:
            rows = meta.research_runs()
            catalog_by_directory: dict[Path, Any] = {}
            for row in rows:
                layout = research_run_layout(
                    root, str(row["study_name"]), str(row["run_id"])
                )
                catalog_by_directory[layout.directory] = row

            stale = tuple(
                sorted(str(row["run_id"]) for row in rows if row["status"] == "running")
            )
            artifact_directories = _research_artifact_directories(results_root)
            orphan_paths = tuple(
                sorted(
                    path.relative_to(root).as_posix()
                    for path in artifact_directories
                    if path not in catalog_by_directory
                    or catalog_by_directory[path]["status"] == "failed"
                )
            )

            failed: list[str] = []
            removed: list[str] = []
            if apply:
                for run_id in stale:
                    row = meta.research_run(run_id)
                    assert row is not None
                    layout = research_run_layout(root, str(row["study_name"]), run_id)
                    artifacts_existed = layout.directory.exists()
                    cleanup_error = _cleanup_run_directory(layout.directory)
                    detail = "recovered abandoned running research execution"
                    if cleanup_error is not None:
                        detail += f"; artifact cleanup failed: {cleanup_error}"
                    elif artifacts_existed:
                        removed.append(layout.directory.relative_to(root).as_posix())
                    meta.fail_research_run(run_id, detail)
                    failed.append(run_id)
                for relative in orphan_paths:
                    path = resolve_data_path(root, relative)
                    cleanup_error = _cleanup_run_directory(path)
                    if cleanup_error is None:
                        removed.append(relative)

    return ResearchReconciliationReport(
        applied=apply,
        stale_running_run_ids=stale,
        orphan_directories=orphan_paths,
        failed_run_ids=tuple(failed),
        removed_directories=tuple(sorted(set(removed))),
    )


def verify_research_input_fingerprint(
    config: Config, run_id: str
) -> InputFingerprintStatus:
    """Compare a succeeded run's manifest with the files currently on disk."""
    run_id = run_id.strip()
    if not run_id:
        raise ValueError("run_id must not be empty")
    with MetaStore(config.meta_path) as meta:
        row = meta.research_run(run_id)
    if row is None:
        raise ValueError(f"unknown research run {run_id!r}")
    if row["status"] != "succeeded":
        raise ValueError(f"research run {run_id!r} is not succeeded")
    expected = str(row["input_fingerprint"])
    layout = research_run_layout(config.data_dir, str(row["study_name"]), run_id)
    manifest_path = resolve_data_path(config.data_dir, str(row["manifest_path"]))
    if manifest_path != layout.manifest:
        raise RuntimeError(f"research run {run_id!r} has an unexpected manifest path")
    manifest = pl.read_parquet(manifest_path, glob=False)
    if manifest.schema != pl.Schema(INPUT_MANIFEST_SCHEMA):
        raise RuntimeError(f"research input manifest has an invalid schema: {run_id}")
    manifest_run_ids = manifest["run_id"]
    if manifest_run_ids.null_count() or (
        manifest.height
        and (manifest_run_ids.min() != run_id or manifest_run_ids.max() != run_id)
    ):
        raise RuntimeError(f"research input manifest has an invalid run_id: {run_id}")

    encoded_patterns = manifest["input_patterns_json"].unique().to_list()
    if len(encoded_patterns) != 1:
        raise RuntimeError(
            f"research input manifest has inconsistent input patterns: {run_id}"
        )
    try:
        decoded_patterns = json.loads(str(encoded_patterns[0]))
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"research input manifest has invalid input patterns: {run_id}"
        ) from exc
    if not isinstance(decoded_patterns, list) or not all(
        isinstance(pattern, str) for pattern in decoded_patterns
    ):
        raise RuntimeError(
            f"research input manifest has invalid input patterns: {run_id}"
        )
    patterns = _validate_input_globs(decoded_patterns)
    if canonical_json(list(patterns)) != encoded_patterns[0]:
        raise RuntimeError(
            f"research input manifest has noncanonical input patterns: {run_id}"
        )
    metadata_column = manifest["input_metadata_json"]
    encoded_metadata = metadata_column.drop_nulls().unique().to_list()
    if (
        len(encoded_metadata) != 1
        or metadata_column.null_count() != manifest.height - 1
    ):
        raise RuntimeError(
            f"research input manifest has inconsistent input metadata: {run_id}"
        )
    try:
        input_metadata = json.loads(str(encoded_metadata[0]))
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"research input manifest has invalid input metadata: {run_id}"
        ) from exc
    if not isinstance(input_metadata, dict):
        raise RuntimeError(
            f"research input manifest has invalid input metadata: {run_id}"
        )
    try:
        input_metadata = _normalize_input_metadata(input_metadata)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"research input manifest has invalid input metadata: {run_id}"
        ) from exc
    if canonical_json(input_metadata) != str(encoded_metadata[0]):
        raise RuntimeError(
            f"research input manifest has noncanonical input metadata: {run_id}"
        )

    recorded_pairs = [
        (str(path), str(digest))
        for path, digest in manifest.select(
            "relative_path", "content_sha256"
        ).iter_rows()
    ]
    if len(dict(recorded_pairs)) != len(recorded_pairs):
        raise RuntimeError(
            f"research input manifest contains duplicate file paths: {run_id}"
        )
    if _aggregate_fingerprint(patterns, recorded_pairs, input_metadata) != expected:
        raise RuntimeError(
            f"research input manifest does not match the catalog fingerprint: {run_id}"
        )

    current_files = _expand_input_globs(
        config.data_dir, patterns, require_each_pattern=False
    )
    current_relative_paths = {
        path.relative_to(config.data_dir.resolve()).as_posix(): path
        for path in current_files
    }
    recorded_paths = {relative_path for relative_path, _digest in recorded_pairs}
    missing = sorted(recorded_paths - current_relative_paths.keys())
    added = sorted(current_relative_paths.keys() - recorded_paths)
    current_pairs: list[tuple[str, str]] = []
    changed: list[str] = []
    recorded_digests = dict(recorded_pairs)
    for relative_path, path in sorted(current_relative_paths.items()):
        digest = _sha256_file(path)
        current_pairs.append((relative_path, digest))
        if (
            relative_path in recorded_digests
            and digest != recorded_digests[relative_path]
        ):
            changed.append(relative_path)
    current_metadata = _current_input_metadata(config, input_metadata)
    metadata_changed = canonical_json(current_metadata) != canonical_json(
        input_metadata
    )
    current = (
        None
        if missing
        else _aggregate_fingerprint(patterns, current_pairs, current_metadata)
    )
    return InputFingerprintStatus(
        run_id=run_id,
        expected_fingerprint=expected,
        current_fingerprint=current,
        matches=not missing and not added and current == expected,
        missing_files=tuple(missing),
        added_files=tuple(added),
        changed_files=tuple(changed),
        metadata_changed=metadata_changed,
    )


def _normalize_input_metadata(value: Mapping[str, Any]) -> dict[str, Any]:
    if not value:
        return {}
    if set(value) != {"identity_aliases"}:
        raise ValueError("unsupported research input metadata")
    identity = value["identity_aliases"]
    if not isinstance(identity, Mapping) or set(identity) != {
        "instrument_ids",
        "rows",
    }:
        raise ValueError("identity input metadata has an invalid shape")
    raw_ids = identity["instrument_ids"]
    raw_rows = identity["rows"]
    if not isinstance(raw_ids, list) or not all(
        isinstance(instrument_id, str)
        and instrument_id.strip() == instrument_id
        and instrument_id
        for instrument_id in raw_ids
    ):
        raise ValueError("identity input metadata has invalid instrument ids")
    instrument_ids = sorted(dict.fromkeys(raw_ids))
    if instrument_ids != raw_ids:
        raise ValueError("identity input metadata instrument ids are not canonical")
    if not isinstance(raw_rows, list):
        raise ValueError("identity input metadata rows must be a list")
    rows: list[dict[str, str]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, Mapping) or set(raw_row) != {
            "instrument_id",
            "ticker",
            "start_date",
            "end_date",
        }:
            raise ValueError("identity input metadata contains an invalid row")
        row = {key: raw_row[key] for key in raw_row}
        if not all(isinstance(item, str) for item in row.values()):
            raise TypeError("identity input metadata row values must be strings")
        if row["instrument_id"] not in instrument_ids or not row["ticker"].strip():
            raise ValueError("identity input metadata row has an invalid owner")
        start = date.fromisoformat(row["start_date"])
        end = date.fromisoformat(row["end_date"])
        if start > end:
            raise ValueError("identity input metadata alias range is reversed")
        rows.append(cast(dict[str, str], row))
    canonical_rows = sorted(
        rows,
        key=lambda row: (
            row["instrument_id"],
            row["start_date"],
            row["end_date"],
            row["ticker"],
        ),
    )
    if rows != canonical_rows:
        raise ValueError("identity input metadata rows are not canonical")
    return {
        "identity_aliases": {
            "instrument_ids": instrument_ids,
            "rows": rows,
        }
    }


def _current_input_metadata(
    config: Config, recorded: Mapping[str, Any]
) -> dict[str, Any]:
    normalized = _normalize_input_metadata(recorded)
    if not normalized:
        return {}
    instrument_ids = normalized["identity_aliases"]["instrument_ids"]
    with MetaStore(config.meta_path) as meta:
        return _identity_input_metadata(meta, instrument_ids)


def _require_study_name(value: str) -> str:
    normalized = value.strip()
    if not _STUDY_NAME.fullmatch(normalized):
        raise ValueError(
            "study_name must be a lowercase filesystem-safe slug of at most "
            "64 characters"
        )
    return normalized


def _validate_input_globs(values: Sequence[str | Path]) -> tuple[str, ...]:
    patterns: list[str] = []
    for value in values:
        pattern = str(value)
        if not pattern:
            raise ValueError("research input globs must not be empty")
        try:
            normalized = normalize_relative_data_path(pattern)
        except ValueError as exc:
            raise ValueError(
                "research input globs must be relative to the data root"
            ) from exc
        patterns.append(normalized)
    if not patterns:
        raise ValueError("at least one research input glob is required")
    return tuple(dict.fromkeys(patterns))


def _expand_input_globs(
    data_dir: Path,
    patterns: Sequence[str],
    *,
    require_each_pattern: bool = True,
) -> tuple[Path, ...]:
    root = data_dir.resolve()
    files: set[Path] = set()
    for pattern in patterns:
        pattern_files: set[Path] = set()
        absolute_pattern = f"{glob.escape(str(root))}/{pattern}"
        for match in glob.iglob(absolute_pattern, recursive=True):
            path = Path(match).resolve()
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise RuntimeError(
                    f"research input path escapes the data root: {path}"
                ) from exc
            if path.is_file():
                pattern_files.add(path)
        if require_each_pattern and not pattern_files:
            raise ValueError(f"research input glob matched no files: {pattern!r}")
        files.update(pattern_files)
    if require_each_pattern and not files:
        raise ValueError("research input globs matched no files")
    return tuple(sorted(files, key=lambda path: path.relative_to(root).as_posix()))


def _build_input_manifest(
    data_dir: Path,
    run_id: str,
    input_patterns: Sequence[str],
    input_files: Sequence[Path],
    input_metadata: Mapping[str, Any],
) -> tuple[pl.DataFrame, str]:
    root = data_dir.resolve()
    patterns_json = canonical_json(list(input_patterns))
    metadata_json = canonical_json(dict(input_metadata))
    records: list[dict[str, Any]] = []
    pairs: list[tuple[str, str]] = []
    for index, path in enumerate(input_files):
        relative_path = path.relative_to(root).as_posix()
        digest = _sha256_file(path)
        first_date, last_date = _parquet_date_bounds(path)
        records.append(
            {
                "run_id": run_id,
                "input_patterns_json": patterns_json,
                # Metadata can be much larger than the file-pattern declaration;
                # store it once rather than once per manifest row.
                "input_metadata_json": metadata_json if index == 0 else None,
                "relative_path": relative_path,
                "content_sha256": digest,
                "size_bytes": path.stat().st_size,
                "first_date": first_date,
                "last_date": last_date,
            }
        )
        pairs.append((relative_path, digest))
    return (
        pl.DataFrame(records, schema=INPUT_MANIFEST_SCHEMA),
        _aggregate_fingerprint(input_patterns, pairs, input_metadata),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _aggregate_fingerprint(
    patterns: Sequence[str],
    pairs: Sequence[tuple[str, str]],
    input_metadata: Mapping[str, Any],
) -> str:
    payload = canonical_json(
        {
            "patterns": list(patterns),
            "files": [list(pair) for pair in sorted(pairs)],
            "metadata": dict(input_metadata),
        }
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _parquet_date_bounds(path: Path) -> tuple[date | None, date | None]:
    scan = pl.scan_parquet(path, glob=False)
    schema = scan.collect_schema()
    if "date" in schema:
        bounds = scan.select(
            pl.col("date").cast(pl.Date).min().alias("first_date"),
            pl.col("date").cast(pl.Date).max().alias("last_date"),
        ).collect()
    elif "ts" in schema:
        bounds = scan.select(
            pl.col("ts").dt.date().min().alias("first_date"),
            pl.col("ts").dt.date().max().alias("last_date"),
        ).collect()
    else:
        return None, None
    return bounds["first_date"].item(), bounds["last_date"].item()


def _prepare_observations(
    meta: MetaStore, run_id: str, observations: pl.DataFrame
) -> pl.DataFrame:
    if not isinstance(observations, pl.DataFrame):
        raise TypeError("research observations must be a polars DataFrame")
    if "instrument_id" not in observations.columns:
        raise ValueError("research observations must contain instrument_id")
    if observations.schema["instrument_id"] != pl.Utf8:
        raise ValueError("research observation instrument_id must be a string")
    if (
        observations["instrument_id"].null_count()
        or observations.filter(pl.col("instrument_id").str.strip_chars() == "").height
    ):
        raise ValueError("research observation instrument_id must not be empty")
    unknown = set(observations["instrument_id"].unique().to_list()) - (
        meta.instrument_ids()
    )
    if unknown:
        raise ValueError(
            f"research observations contain unknown instrument_ids: {sorted(unknown)}"
        )
    if "run_id" in observations.columns:
        if observations.schema["run_id"] != pl.Utf8:
            raise ValueError("research observation run_id must be a string")
        run_ids = observations["run_id"]
        if run_ids.null_count() or (
            observations.height and (run_ids.min() != run_id or run_ids.max() != run_id)
        ):
            raise ValueError("research observation run_id does not match the run")
        return observations
    return observations.with_columns(run_id=pl.lit(run_id)).select(
        "run_id", pl.exclude("run_id")
    )


def _publish_artifacts(
    layout: ResearchRunLayout, observations: pl.DataFrame, manifest: pl.DataFrame
) -> tuple[Path, Path]:
    layout.directory.mkdir(parents=True, exist_ok=False)
    atomic_write_parquet(observations, layout.observations, validate=True)
    atomic_write_parquet(manifest, layout.manifest, validate=True)
    return layout.observations, layout.manifest


def _cleanup_run_directory(run_dir: Path) -> str | None:
    if not run_dir.exists():
        return None
    try:
        shutil.rmtree(run_dir)
        parent = run_dir.parent
        try:
            parent.rmdir()
        except OSError:
            pass
    except OSError as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def _research_artifact_directories(results_root: Path) -> set[Path]:
    if not results_root.is_dir():
        return set()
    directories: set[Path] = set()
    for study_dir in results_root.iterdir():
        if not study_dir.is_dir() or study_dir.is_symlink():
            continue
        for run_dir in study_dir.iterdir():
            if run_dir.is_dir() and not run_dir.is_symlink():
                directories.add(run_dir.resolve())
    return directories
