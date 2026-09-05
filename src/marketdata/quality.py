"""Read-only data-quality findings and consumer-defined quality gates.

Checks aggregate inside DuckDB so a full-warehouse scan does not materialize
canonical datasets in Python memory. Only bounded, per-instrument findings are
returned to Python.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast

import duckdb
import polars as pl

from marketdata.calendar import session_schedule
from marketdata.config import Config
from marketdata.identity import DATASET_KEYS, DatasetKey, require_dataset_key
from marketdata.query import connect
from marketdata.store.bars import (
    CANONICAL_EOD_SCHEMA,
    CANONICAL_INTRADAY_SCHEMA,
)
from marketdata.store.meta import MetaStore

QualityCheck = Literal[
    "missing_expected_sessions",
    "duplicate_keys",
    "ohlc_invariants",
    "negative_values",
    "zero_volume_runs",
    "split_sanity",
    "off_session_intraday",
    "coverage_delisting_summary",
]
FindingSeverity = Literal["info", "warning", "error"]
_CheckRequirement = Literal["rows", "coverage", "subjects"]

DEFAULT_ZERO_VOLUME_RUN_LENGTH = 5
MIN_ZERO_VOLUME_RUN_LENGTH = 2
QUALITY_DUCKDB_MEMORY_LIMIT = "4GB"
_MEMORY_LIMIT = re.compile(r"^[0-9]+(\.[0-9]+)?\s*(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$", re.I)
_SAMPLE_LIMIT = 10
_RAW_OHLC_COLUMNS = ("open", "high", "low", "close")
_ADJUSTED_OHLC_COLUMNS = ("adj_open", "adj_high", "adj_low", "adj_close")
_EOD_NONNEGATIVE_COLUMNS = (
    *_RAW_OHLC_COLUMNS,
    "volume",
    *_ADJUSTED_OHLC_COLUMNS,
    "adj_volume",
    "div_cash",
)
_INTRADAY_NONNEGATIVE_COLUMNS = (*_RAW_OHLC_COLUMNS, "volume")


@dataclass(frozen=True)
class QualityFinding:
    """One bounded, structured finding; canonical bars are never modified."""

    check: QualityCheck
    severity: FindingSeverity
    dataset_key: DatasetKey
    message: str
    instrument_id: str | None = None
    count: int = 1
    sample: tuple[str, ...] = ()
    details: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "severity": self.severity,
            "dataset_key": self.dataset_key,
            "instrument_id": self.instrument_id,
            "count": self.count,
            "message": self.message,
            "sample": list(self.sample),
            "details": dict(self.details or {}),
        }


@dataclass(frozen=True)
class QualityReport:
    """Deterministic findings for an explicit stored-data scope."""

    dataset_keys: tuple[DatasetKey, ...]
    instrument_ids: tuple[str, ...] | None
    start: date | None
    end: date | None
    zero_volume_run_length: int
    checks_run: tuple[QualityCheck, ...]
    findings: tuple[QualityFinding, ...]

    def finding_counts(self) -> dict[str, int]:
        counts = {"info": 0, "warning": 0, "error": 0}
        for finding in self.findings:
            counts[finding.severity] += 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": {
                "dataset_keys": list(self.dataset_keys),
                "instrument_ids": (
                    list(self.instrument_ids)
                    if self.instrument_ids is not None
                    else None
                ),
                "start": self.start.isoformat() if self.start else None,
                "end": self.end.isoformat() if self.end else None,
                "zero_volume_run_length": self.zero_volume_run_length,
            },
            "checks_run": list(self.checks_run),
            "finding_counts": self.finding_counts(),
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class QualityGateResult:
    """Outcome after a consumer declares which quality checks are blocking."""

    blocking_checks: tuple[QualityCheck, ...]
    blocking_findings: tuple[QualityFinding, ...]
    checks_not_run: tuple[QualityCheck, ...]

    @property
    def passed(self) -> bool:
        return not self.blocking_findings and not self.checks_not_run

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "blocking_checks": list(self.blocking_checks),
            "checks_not_run": list(self.checks_not_run),
            "blocking_finding_count": len(self.blocking_findings),
            "blocking_findings": [
                finding.to_dict() for finding in self.blocking_findings
            ],
        }


@dataclass(frozen=True)
class _DatasetContext:
    con: duckdb.DuckDBPyConnection
    dataset_key: DatasetKey
    raw_relation: str | None
    valid_relation: str | None
    schedule_relation: str | None
    coverage_relation: str | None
    raw_count: int
    subjects: frozenset[str]
    scoped_coverage: Mapping[str, tuple[date, date]]
    lifecycle: Mapping[str, str]
    selected_ids: tuple[str, ...] | None
    zero_volume_run_length: int


@dataclass(frozen=True)
class _CheckSpec:
    check: QualityCheck
    datasets: frozenset[DatasetKey]
    requirement: _CheckRequirement
    runner: Callable[[_DatasetContext], list[QualityFinding]]


def require_memory_limit(value: str) -> str:
    """Validate a DuckDB memory-limit literal such as ``'24GB'``."""
    normalized = str(value).strip()
    if not _MEMORY_LIMIT.match(normalized):
        raise ValueError(f"invalid DuckDB memory limit {value!r}")
    return normalized


def check_quality(
    config: Config,
    *,
    dataset_keys: Sequence[str] = DATASET_KEYS,
    instrument_ids: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    zero_volume_run_length: int = DEFAULT_ZERO_VOLUME_RUN_LENGTH,
    empty_row_checks_are_run: bool = False,
    memory_limit: str | None = None,
) -> QualityReport:
    """Scan canonical bars and return structured, non-repairing findings.

    ``empty_row_checks_are_run`` is for consumers whose explicit empty scope
    makes row predicates vacuously complete. Coverage/subject checks retain
    their ordinary fail-closed behavior. ``memory_limit`` is the DuckDB cap
    for this scan (default ``QUALITY_DUCKDB_MEMORY_LIMIT``); a study over the
    full five-minute archive needs more than the CLI default.
    """
    memory_limit = require_memory_limit(memory_limit or QUALITY_DUCKDB_MEMORY_LIMIT)
    normalized_datasets = _normalize_dataset_keys(dataset_keys)
    normalized_ids = _normalize_instrument_ids(instrument_ids)
    start_date = _as_date(start)
    end_date = _as_date(end)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("quality start must not be after end")
    if zero_volume_run_length < MIN_ZERO_VOLUME_RUN_LENGTH:
        raise ValueError(
            f"zero_volume_run_length must be at least {MIN_ZERO_VOLUME_RUN_LENGTH}"
        )
    if not config.meta_path.exists():
        raise RuntimeError("quality checks require meta.db")

    with MetaStore(config.meta_path) as meta:
        if meta.storage_generation() != "v2":
            raise RuntimeError("quality checks require the v2 storage generation")
        known_ids = meta.instrument_ids()
        if normalized_ids is not None:
            unknown = sorted(set(normalized_ids) - known_ids)
            if unknown:
                raise ValueError(f"unknown instrument_ids: {unknown}")
        lifecycle = meta.instrument_lifecycle()
        coverage = {
            dataset_key: meta.coverage(dataset_key)
            for dataset_key in normalized_datasets
        }

    findings: list[QualityFinding] = []
    checks_run_by_dataset: dict[DatasetKey, set[QualityCheck]] = {
        dataset_key: set() for dataset_key in normalized_datasets
    }
    temp_root = config.data_dir / ".duckdb-tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="quality-", dir=temp_root) as quality_temp:
        con = connect(config)
        con.execute(f"SET temp_directory = {_sql_string(quality_temp)}")
        con.execute(f"SET memory_limit = {_sql_string(memory_limit)}")
        # Findings are aggregated and sorted explicitly; row order never
        # matters, and dropping it lets large scans spill instead of failing.
        con.execute("SET preserve_insertion_order = false")
        try:
            for dataset_key in normalized_datasets:
                _require_check_schema_coverage(dataset_key)
                context = _build_context(
                    con,
                    dataset_key,
                    coverage[dataset_key],
                    lifecycle,
                    normalized_ids,
                    start_date,
                    end_date,
                    zero_volume_run_length,
                )
                for spec in _CHECK_SPECS:
                    if dataset_key not in spec.datasets:
                        continue
                    if not _eligible(spec, context):
                        if empty_row_checks_are_run and spec.requirement == "rows":
                            checks_run_by_dataset[dataset_key].add(spec.check)
                        continue
                    findings.extend(spec.runner(context))
                    checks_run_by_dataset[dataset_key].add(spec.check)
        finally:
            con.close()

    ordered = tuple(sorted(findings, key=_finding_sort_key))
    ordered_checks = _fully_run_checks(normalized_datasets, checks_run_by_dataset)
    return QualityReport(
        dataset_keys=normalized_datasets,
        instrument_ids=normalized_ids,
        start=start_date,
        end=end_date,
        zero_volume_run_length=zero_volume_run_length,
        checks_run=ordered_checks,
        findings=ordered,
    )


def evaluate_quality(
    report: QualityReport, blocking_checks: Iterable[str]
) -> QualityGateResult:
    """Fail closed on unrun checks and warning/error findings for declared checks."""
    normalized = _normalize_checks(blocking_checks)
    checks_not_run = tuple(
        check for check in normalized if check not in report.checks_run
    )
    blocking_findings = tuple(
        finding
        for finding in report.findings
        if finding.check in normalized and finding.severity != "info"
    )
    return QualityGateResult(normalized, blocking_findings, checks_not_run)


def _build_context(
    con: duckdb.DuckDBPyConnection,
    dataset_key: DatasetKey,
    coverage: Mapping[str, tuple[date, date]],
    lifecycle: Mapping[str, str],
    selected_ids: tuple[str, ...] | None,
    start: date | None,
    end: date | None,
    zero_volume_run_length: int,
) -> _DatasetContext:
    suffix = dataset_key
    raw_relation: str | None = None
    raw_count = 0
    observed_ids: set[str] = set()
    raw_first: date | None = None
    raw_last: date | None = None
    if _view_exists(con, dataset_key):
        raw_relation = f"quality_raw_{suffix}"
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {raw_relation} AS "
            f"SELECT * FROM {dataset_key} WHERE "
            f"{_scope_predicate(dataset_key, selected_ids, start, end)}"
        )
        date_expr = _date_sql(dataset_key)
        raw_count, raw_first, raw_last = con.execute(
            f"SELECT count(*), min({date_expr}), max({date_expr}) FROM {raw_relation}"
        ).fetchone()
        observed_ids = {
            str(row[0])
            for row in con.execute(
                f"SELECT DISTINCT instrument_id FROM {raw_relation}"
            ).fetchall()
        }

    scoped_coverage = _scope_coverage(coverage, selected_ids, start, end)
    subjects = (
        set(selected_ids)
        if selected_ids is not None
        else set(scoped_coverage) | observed_ids
    )
    schedule_firsts = [interval[0] for interval in scoped_coverage.values()]
    schedule_lasts = [interval[1] for interval in scoped_coverage.values()]
    if raw_first is not None:
        schedule_firsts.append(raw_first)
    if raw_last is not None:
        schedule_lasts.append(raw_last)

    schedule_relation: str | None = None
    coverage_relation: str | None = None
    if schedule_firsts and schedule_lasts:
        schedule_relation = f"quality_schedule_{suffix}"
        schedule = session_schedule(min(schedule_firsts), max(schedule_lasts))
        schedule = schedule.with_row_index("session_index")
        con.register(schedule_relation, schedule)
    if scoped_coverage:
        coverage_relation = f"quality_coverage_{suffix}"
        coverage_frame = pl.DataFrame(
            {
                "instrument_id": list(scoped_coverage),
                "coverage_first": [
                    scoped_coverage[instrument_id][0]
                    for instrument_id in scoped_coverage
                ],
                "coverage_last": [
                    scoped_coverage[instrument_id][1]
                    for instrument_id in scoped_coverage
                ],
            },
            schema={
                "instrument_id": pl.Utf8,
                "coverage_first": pl.Date,
                "coverage_last": pl.Date,
            },
        )
        con.register(coverage_relation, coverage_frame)

    valid_relation = _create_valid_intraday_relation(
        con, dataset_key, raw_relation, schedule_relation
    )
    return _DatasetContext(
        con=con,
        dataset_key=dataset_key,
        raw_relation=raw_relation,
        valid_relation=valid_relation,
        schedule_relation=schedule_relation,
        coverage_relation=coverage_relation,
        raw_count=int(raw_count),
        subjects=frozenset(subjects),
        scoped_coverage=scoped_coverage,
        lifecycle=lifecycle,
        selected_ids=selected_ids,
        zero_volume_run_length=zero_volume_run_length,
    )


def _create_valid_intraday_relation(
    con: duckdb.DuckDBPyConnection,
    dataset_key: DatasetKey,
    raw_relation: str | None,
    schedule_relation: str | None,
) -> str | None:
    if dataset_key == "eod" or raw_relation is None or schedule_relation is None:
        return None
    relation = f"quality_valid_{dataset_key}"
    freq = dataset_key.removeprefix("intraday_")
    bar_minutes = 60 if freq == "1hour" else 5
    alignment = (
        "date_part('minute', timezone('America/New_York', raw.ts)) = 0"
        if freq == "1hour"
        else f"date_diff('minute', schedule.session_open, raw.ts) % {bar_minutes} = 0"
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP VIEW {relation} AS
            SELECT raw.*, schedule.session_date, schedule.session_index
            FROM {raw_relation} AS raw
            JOIN {schedule_relation} AS schedule
              ON {_date_sql(dataset_key, "raw")} = schedule.session_date
            WHERE raw.ts >= schedule.session_open
              AND raw.ts + INTERVAL {bar_minutes} MINUTE <= schedule.session_close
              AND {alignment}"""
    )
    return relation


def _eligible(spec: _CheckSpec, context: _DatasetContext) -> bool:
    if spec.requirement == "rows":
        return context.raw_count > 0
    if spec.requirement == "coverage":
        return bool(context.scoped_coverage)
    return bool(context.subjects)


def _fully_run_checks(
    dataset_keys: tuple[DatasetKey, ...],
    checks_run_by_dataset: Mapping[DatasetKey, set[QualityCheck]],
) -> tuple[QualityCheck, ...]:
    """Checks count as run only across every requested applicable dataset."""
    checks: list[QualityCheck] = []
    for spec in _CHECK_SPECS:
        applicable = [key for key in dataset_keys if key in spec.datasets]
        if applicable and all(
            spec.check in checks_run_by_dataset[key] for key in applicable
        ):
            checks.append(spec.check)
    return tuple(checks)


def _duplicate_findings(context: _DatasetContext) -> list[QualityFinding]:
    assert context.raw_relation is not None
    time_key = _time_key(context.dataset_key)
    rows = context.con.execute(
        f"""WITH duplicates AS (
                SELECT instrument_id, {time_key}, count(*) AS row_count
                FROM {context.raw_relation}
                GROUP BY instrument_id, {time_key}
                HAVING count(*) > 1
            )
            SELECT instrument_id, count(*) AS duplicate_groups,
                   sum(row_count - 1) AS extra_rows,
                   list_slice(
                       list(CAST({time_key} AS VARCHAR) ORDER BY {time_key}),
                       1, {_SAMPLE_LIMIT}
                   )
            FROM duplicates
            GROUP BY instrument_id
            ORDER BY instrument_id"""
    ).fetchall()
    return [
        QualityFinding(
            check="duplicate_keys",
            severity="error",
            dataset_key=context.dataset_key,
            instrument_id=str(instrument_id),
            count=int(group_count),
            message=f"duplicate canonical (instrument_id, {time_key}) keys",
            sample=_sample_values(sample),
            details={"extra_rows": int(extra_rows)},
        )
        for instrument_id, group_count, extra_rows, sample in rows
    ]


def _ohlc_findings(context: _DatasetContext) -> list[QualityFinding]:
    raw_bad = _ohlc_predicate(_RAW_OHLC_COLUMNS)
    details = {"raw_rows": raw_bad}
    invalid = raw_bad
    if context.dataset_key == "eod":
        adjusted_bad = _ohlc_predicate(_ADJUSTED_OHLC_COLUMNS)
        invalid = f"({raw_bad}) OR ({adjusted_bad})"
        details["adjusted_rows"] = adjusted_bad
    return _predicate_findings(
        context,
        invalid,
        check="ohlc_invariants",
        severity="error",
        message="OHLC values are non-finite or violate low <= open/close <= high",
        detail_predicates=details,
    )


def _negative_findings(context: _DatasetContext) -> list[QualityFinding]:
    columns = (
        _EOD_NONNEGATIVE_COLUMNS
        if context.dataset_key == "eod"
        else _INTRADAY_NONNEGATIVE_COLUMNS
    )
    predicates = {
        f"{column}_rows": (
            f"({column} IS NULL OR {column} < 0)"
            if column in {"volume", "adj_volume"}
            else f"{column} < 0"
        )
        for column in columns
    }
    return _predicate_findings(
        context,
        " OR ".join(f"({predicate})" for predicate in predicates.values()),
        check="negative_values",
        severity="error",
        message="negative prices/cash distributions or negative/missing volume",
        detail_predicates=predicates,
    )


def _zero_volume_findings(context: _DatasetContext) -> list[QualityFinding]:
    assert context.raw_relation is not None
    assert context.schedule_relation is not None
    if context.dataset_key == "eod":
        source = f"""SELECT raw.instrument_id, raw.date AS bar_time,
                            schedule.session_date, schedule.session_index,
                            count(raw.volume) = count(*)
                                AND min(raw.volume) = 0
                                AND max(raw.volume) = 0 AS is_zero
                     FROM {context.raw_relation} AS raw
                     JOIN {context.schedule_relation} AS schedule
                       ON raw.date = schedule.session_date
                     GROUP BY raw.instrument_id, raw.date,
                              schedule.session_date, schedule.session_index"""
        contiguous = "session_index = previous_index + 1"
    else:
        assert context.valid_relation is not None
        bar_minutes = 60 if context.dataset_key == "intraday_1hour" else 5
        source = f"""SELECT instrument_id, ts AS bar_time,
                            session_date, session_index,
                            count(volume) = count(*)
                                AND min(volume) = 0
                                AND max(volume) = 0 AS is_zero
                     FROM {context.valid_relation}
                     GROUP BY instrument_id, ts, session_date, session_index"""
        contiguous = (
            "session_date = previous_session "
            f"AND date_diff('minute', previous_time, bar_time) = {bar_minutes}"
        )
    rows = context.con.execute(
        f"""WITH unique_bars AS ({source}),
            ordered AS (
                SELECT *,
                       lag(is_zero) OVER bar_window AS previous_zero,
                       lag(bar_time) OVER bar_window AS previous_time,
                       lag(session_date) OVER bar_window AS previous_session,
                       lag(session_index) OVER bar_window AS previous_index
                FROM unique_bars
                WINDOW bar_window AS (
                    PARTITION BY instrument_id ORDER BY bar_time
                )
            ),
            flagged AS (
                SELECT *, CASE
                    WHEN is_zero AND coalesce(previous_zero, false)
                         AND {contiguous} THEN 0 ELSE 1 END AS new_group
                FROM ordered
            ),
            grouped AS (
                SELECT *, sum(new_group) OVER (
                    PARTITION BY instrument_id ORDER BY bar_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS run_group
                FROM flagged
            ),
            runs AS (
                SELECT instrument_id, min(bar_time) AS first_bar,
                       max(bar_time) AS last_bar, count(*) AS run_length
                FROM grouped
                WHERE is_zero
                GROUP BY instrument_id, run_group
                HAVING count(*) >= {context.zero_volume_run_length}
            )
            SELECT instrument_id, count(*) AS run_count,
                   sum(run_length) AS zero_rows, max(run_length) AS longest_run,
                   list_slice(
                       list(CAST(first_bar AS VARCHAR) ORDER BY first_bar),
                       1, {_SAMPLE_LIMIT}
                   ),
                   list_slice(
                       list(CAST(last_bar AS VARCHAR) ORDER BY first_bar),
                       1, {_SAMPLE_LIMIT}
                   ),
                   list_slice(list(run_length ORDER BY first_bar), 1, {_SAMPLE_LIMIT})
            FROM runs
            GROUP BY instrument_id
            ORDER BY instrument_id"""
    ).fetchall()
    note = (
        "; intraday volume is IEX-only and is not composite market liquidity"
        if context.dataset_key != "eod"
        else ""
    )
    findings: list[QualityFinding] = []
    for instrument_id, run_count, zero_rows, longest, firsts, lasts, lengths in rows:
        samples = tuple(
            f"{_format_value(first)}..{_format_value(last)} ({length})"
            for first, last, length in zip(firsts, lasts, lengths, strict=True)
        )
        findings.append(
            QualityFinding(
                check="zero_volume_runs",
                severity="warning",
                dataset_key=context.dataset_key,
                instrument_id=str(instrument_id),
                count=int(run_count),
                message=(
                    f"runs of at least {context.zero_volume_run_length} "
                    f"consecutive zero-volume bars{note}"
                ),
                sample=samples,
                details={
                    "zero_rows_in_runs": int(zero_rows),
                    "longest_run": int(longest),
                },
            )
        )
    return findings


def _split_findings(context: _DatasetContext) -> list[QualityFinding]:
    return _predicate_findings(
        context,
        "split_factor IS NULL OR NOT isfinite(split_factor) OR split_factor <= 0",
        check="split_sanity",
        severity="error",
        message="split factors must be finite and greater than zero",
        detail_predicates=None,
    )


def _off_session_findings(context: _DatasetContext) -> list[QualityFinding]:
    assert context.raw_relation is not None
    assert context.valid_relation is not None
    rows = context.con.execute(
        f"""WITH invalid AS (
                SELECT raw.* FROM {context.raw_relation} AS raw
                ANTI JOIN (
                    SELECT DISTINCT instrument_id, ts FROM {context.valid_relation}
                ) AS valid USING (instrument_id, ts)
            )
            SELECT instrument_id, count(*),
                   list_slice(
                       list(CAST(ts AS VARCHAR) ORDER BY ts), 1, {_SAMPLE_LIMIT}
                   )
            FROM invalid
            GROUP BY instrument_id
            ORDER BY instrument_id"""
    ).fetchall()
    return [
        QualityFinding(
            check="off_session_intraday",
            severity="warning",
            dataset_key=context.dataset_key,
            instrument_id=str(instrument_id),
            count=int(count),
            message="intraday rows fall outside XNYS regular-session bar labels",
            sample=_sample_values(sample),
        )
        for instrument_id, count, sample in rows
    ]


def _coverage_findings(context: _DatasetContext) -> list[QualityFinding]:
    raw_summaries = _observed_summaries(context, context.raw_relation)
    valid_summaries = (
        _observed_summaries(context, context.valid_relation)
        if context.dataset_key != "eod"
        else raw_summaries
    )
    findings: list[QualityFinding] = []
    for instrument_id in sorted(context.subjects):
        covered = context.scoped_coverage.get(instrument_id)
        raw = raw_summaries.get(instrument_id)
        valid = valid_summaries.get(instrument_id)
        details: dict[str, Any] = {
            "lifecycle_status": context.lifecycle.get(instrument_id, "unknown"),
            "is_delisted": context.lifecycle.get(instrument_id) == "inactive",
            "coverage_first": covered[0].isoformat() if covered else None,
            "coverage_last": covered[1].isoformat() if covered else None,
            "observed_first": raw[1].isoformat() if raw else None,
            "observed_last": raw[2].isoformat() if raw else None,
            "row_count": raw[0] if raw else 0,
            "time_key": _time_key(context.dataset_key),
        }
        if context.dataset_key != "eod":
            details["regular_session_row_count"] = valid[0] if valid else 0
        severity: FindingSeverity = "info"
        message = "stored coverage and lifecycle summary"
        if raw and covered is None:
            severity = "warning"
            message = "stored bars exist without durable coverage in this scope"
        elif covered is None and context.selected_ids is not None:
            severity = "warning"
            message = "no durable coverage exists in the requested scope"
        elif raw and covered and (raw[1] < covered[0] or raw[2] > covered[1]):
            severity = "warning"
            message = "stored bars extend outside durable coverage in this scope"
        findings.append(
            QualityFinding(
                check="coverage_delisting_summary",
                severity=severity,
                dataset_key=context.dataset_key,
                instrument_id=instrument_id,
                message=message,
                details=details,
            )
        )
    return findings


def _missing_session_findings(context: _DatasetContext) -> list[QualityFinding]:
    assert context.coverage_relation is not None
    assert context.schedule_relation is not None
    if context.dataset_key == "eod" and context.raw_relation is not None:
        actual = (
            f"SELECT DISTINCT instrument_id, date AS session_date "
            f"FROM {context.raw_relation}"
        )
    elif context.valid_relation is not None:
        actual = (
            f"SELECT DISTINCT instrument_id, session_date FROM {context.valid_relation}"
        )
    else:
        actual = (
            "SELECT CAST(NULL AS VARCHAR) AS instrument_id, "
            "CAST(NULL AS DATE) AS session_date WHERE false"
        )
    rows = context.con.execute(
        f"""WITH expected AS (
                SELECT coverage.instrument_id, schedule.session_date
                FROM {context.coverage_relation} AS coverage
                JOIN {context.schedule_relation} AS schedule
                  ON schedule.session_date BETWEEN coverage.coverage_first
                                               AND coverage.coverage_last
            ),
            actual AS ({actual}),
            missing AS (
                SELECT expected.* FROM expected
                ANTI JOIN actual USING (instrument_id, session_date)
            ),
            expected_counts AS (
                SELECT instrument_id, count(*) AS expected_count
                FROM expected GROUP BY instrument_id
            )
            SELECT missing.instrument_id, count(*) AS missing_count,
                   list_slice(
                       list(missing.session_date ORDER BY missing.session_date),
                       1, {_SAMPLE_LIMIT}
                   ), expected_counts.expected_count
            FROM missing
            JOIN expected_counts USING (instrument_id)
            GROUP BY missing.instrument_id, expected_counts.expected_count
            ORDER BY missing.instrument_id"""
    ).fetchall()
    findings: list[QualityFinding] = []
    for instrument_id, count, sample, expected_count in rows:
        first, last = context.scoped_coverage[str(instrument_id)]
        findings.append(
            QualityFinding(
                check="missing_expected_sessions",
                severity="warning",
                dataset_key=context.dataset_key,
                instrument_id=str(instrument_id),
                count=int(count),
                message="expected XNYS sessions have no stored regular-session bars",
                sample=_sample_values(sample),
                details={
                    "coverage_first": first.isoformat(),
                    "coverage_last": last.isoformat(),
                    "expected_session_count": int(expected_count),
                },
            )
        )
    return findings


def _predicate_findings(
    context: _DatasetContext,
    predicate: str,
    *,
    check: QualityCheck,
    severity: FindingSeverity,
    message: str,
    detail_predicates: Mapping[str, str] | None,
) -> list[QualityFinding]:
    assert context.raw_relation is not None
    time_key = _time_key(context.dataset_key)
    detail_sql = "".join(
        f", count(*) FILTER (WHERE {detail}) AS {label}"
        for label, detail in (detail_predicates or {}).items()
    )
    rows = context.con.execute(
        f"""SELECT instrument_id, count(*) AS finding_count,
                   list_slice(
                       list(CAST({time_key} AS VARCHAR) ORDER BY {time_key}),
                       1, {_SAMPLE_LIMIT}
                   )
                   {detail_sql}
            FROM {context.raw_relation}
            WHERE {predicate}
            GROUP BY instrument_id
            ORDER BY instrument_id"""
    ).fetchall()
    labels = tuple((detail_predicates or {}).keys())
    findings: list[QualityFinding] = []
    for row in rows:
        details = {
            label: int(value)
            for label, value in zip(labels, row[3:], strict=True)
            if value
        }
        findings.append(
            QualityFinding(
                check=check,
                severity=severity,
                dataset_key=context.dataset_key,
                instrument_id=str(row[0]),
                count=int(row[1]),
                message=message,
                sample=_sample_values(row[2]),
                details=details or None,
            )
        )
    return findings


def _observed_summaries(
    context: _DatasetContext, relation: str | None
) -> dict[str, tuple[int, date, date]]:
    if relation is None:
        return {}
    rows = context.con.execute(
        f"""SELECT instrument_id, count(*),
                   min({_date_sql(context.dataset_key)}),
                   max({_date_sql(context.dataset_key)})
            FROM {relation}
            GROUP BY instrument_id"""
    ).fetchall()
    return {
        str(instrument_id): (int(count), first, last)
        for instrument_id, count, first, last in rows
    }


def _ohlc_predicate(columns: tuple[str, str, str, str]) -> str:
    open_, high, low, close = columns
    non_finite = " OR ".join(
        f"{column} IS NULL OR NOT isfinite({column})" for column in columns
    )
    return (
        f"({non_finite}) OR {high} < greatest({open_}, {low}, {close}) "
        f"OR {low} > least({open_}, {high}, {close})"
    )


def _require_check_schema_coverage(dataset_key: DatasetKey) -> None:
    schema = CANONICAL_EOD_SCHEMA if dataset_key == "eod" else CANONICAL_INTRADAY_SCHEMA
    time_key = _time_key(dataset_key)
    numeric_columns = set(schema) - {"instrument_id", time_key}
    checked_columns = set(_RAW_OHLC_COLUMNS) | {"volume"}
    if dataset_key == "eod":
        checked_columns |= set(_ADJUSTED_OHLC_COLUMNS) | {
            "adj_volume",
            "div_cash",
            "split_factor",
        }
    if checked_columns != numeric_columns:
        raise RuntimeError(
            f"quality checks do not cover canonical {dataset_key} numeric schema; "
            f"missing={sorted(numeric_columns - checked_columns)}, "
            f"extra={sorted(checked_columns - numeric_columns)}"
        )


def _scope_predicate(
    dataset_key: DatasetKey,
    instrument_ids: tuple[str, ...] | None,
    start: date | None,
    end: date | None,
) -> str:
    clauses: list[str] = []
    if instrument_ids is not None:
        if not instrument_ids:
            return "false"
        values = ", ".join(_sql_string(value) for value in instrument_ids)
        clauses.append(f"instrument_id IN ({values})")
    date_expr = _date_sql(dataset_key)
    if start is not None:
        clauses.append(f"{date_expr} >= DATE {_sql_string(start.isoformat())}")
    if end is not None:
        clauses.append(f"{date_expr} <= DATE {_sql_string(end.isoformat())}")
    return " AND ".join(clauses) if clauses else "true"


def _scope_coverage(
    coverage: Mapping[str, tuple[date, date]],
    selected_ids: tuple[str, ...] | None,
    start: date | None,
    end: date | None,
) -> dict[str, tuple[date, date]]:
    selected = set(selected_ids) if selected_ids is not None else None
    result: dict[str, tuple[date, date]] = {}
    for instrument_id, interval in coverage.items():
        if selected is not None and instrument_id not in selected:
            continue
        scoped = _scoped_interval(interval, start, end)
        if scoped is not None:
            result[instrument_id] = scoped
    return result


def _normalize_dataset_keys(dataset_keys: Sequence[str]) -> tuple[DatasetKey, ...]:
    normalized = tuple(
        cast(DatasetKey, require_dataset_key(dataset_key))
        for dataset_key in dict.fromkeys(dataset_keys)
    )
    if not normalized:
        raise ValueError("at least one dataset_key is required")
    return normalized


def _normalize_instrument_ids(
    instrument_ids: Sequence[str] | None,
) -> tuple[str, ...] | None:
    if instrument_ids is None:
        return None
    normalized = tuple(dict.fromkeys(value.strip() for value in instrument_ids))
    if any(not value for value in normalized):
        raise ValueError("instrument_ids must not contain empty values")
    return normalized


def _normalize_checks(checks: Iterable[str]) -> tuple[QualityCheck, ...]:
    values: list[QualityCheck] = []
    for check in dict.fromkeys(checks):
        if check not in _CHECK_ORDER:
            raise ValueError(f"unknown quality check {check!r}")
        values.append(cast(QualityCheck, check))
    return tuple(sorted(values, key=_CHECK_ORDER.__getitem__))


def _as_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _time_key(dataset_key: DatasetKey) -> str:
    return "date" if dataset_key == "eod" else "ts"


def _date_sql(dataset_key: DatasetKey, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    if dataset_key == "eod":
        return f"{prefix}date"
    return f"CAST({prefix}ts AT TIME ZONE 'UTC' AS DATE)"


def _view_exists(con: duckdb.DuckDBPyConnection, view: str) -> bool:
    return (
        con.execute(
            "SELECT count(*) FROM duckdb_views() WHERE view_name = ?", [view]
        ).fetchone()[0]
        > 0
    )


def _scoped_interval(
    interval: tuple[date, date] | None,
    start: date | None,
    end: date | None,
) -> tuple[date, date] | None:
    if interval is None:
        return None
    first = max(interval[0], start) if start is not None else interval[0]
    last = min(interval[1], end) if end is not None else interval[1]
    return (first, last) if first <= last else None


def _sample_values(values: Sequence[Any] | None) -> tuple[str, ...]:
    return tuple(_format_value(value) for value in (values or ())[:_SAMPLE_LIMIT])


def _format_value(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, str) and len(value) > 10 and value[10] == " ":
        value = value[:10] + "T" + value[11:]
        if value.endswith("+00"):
            value += ":00"
    return str(value)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _finding_sort_key(finding: QualityFinding) -> tuple[Any, ...]:
    return (
        DATASET_KEYS.index(finding.dataset_key),
        _CHECK_ORDER[finding.check],
        finding.instrument_id or "",
        finding.severity,
        finding.message,
    )


_EOD_ONLY = frozenset({cast(DatasetKey, "eod")})
_INTRADAY_ONLY = frozenset(
    {cast(DatasetKey, "intraday_1hour"), cast(DatasetKey, "intraday_5min")}
)
_ALL_DATASETS = frozenset(DATASET_KEYS)
_CHECK_SPECS: tuple[_CheckSpec, ...] = (
    _CheckSpec(
        "missing_expected_sessions",
        _ALL_DATASETS,
        "coverage",
        _missing_session_findings,
    ),
    _CheckSpec("duplicate_keys", _ALL_DATASETS, "rows", _duplicate_findings),
    _CheckSpec("ohlc_invariants", _ALL_DATASETS, "rows", _ohlc_findings),
    _CheckSpec("negative_values", _ALL_DATASETS, "rows", _negative_findings),
    _CheckSpec("zero_volume_runs", _ALL_DATASETS, "rows", _zero_volume_findings),
    _CheckSpec("split_sanity", _EOD_ONLY, "rows", _split_findings),
    _CheckSpec(
        "off_session_intraday",
        _INTRADAY_ONLY,
        "rows",
        _off_session_findings,
    ),
    _CheckSpec(
        "coverage_delisting_summary",
        _ALL_DATASETS,
        "subjects",
        _coverage_findings,
    ),
)
QUALITY_CHECKS: tuple[QualityCheck, ...] = tuple(spec.check for spec in _CHECK_SPECS)
NONLOCAL_EVENT_GATE_CHECKS: tuple[QualityCheck, ...] = tuple(
    spec.check for spec in _CHECK_SPECS if spec.requirement != "rows"
)
_CHECK_ORDER = {check: index for index, check in enumerate(QUALITY_CHECKS)}
