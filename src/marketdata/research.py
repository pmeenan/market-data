"""Failure-safe publication primitives for cataloged research results."""

from __future__ import annotations

import glob
import hashlib
import json
import re
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl

from marketdata.config import Config
from marketdata.jsonutil import canonical_json
from marketdata.locking import DataDirectoryLock
from marketdata.research_layout import (
    ResearchRunLayout,
    normalize_relative_data_path,
    research_run_layout,
    resolve_data_path,
)
from marketdata.store.bars import (
    BarStore,
    atomic_write_parquet,
    require_canonical_generation,
)
from marketdata.store.meta import MetaStore

_STUDY_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
INPUT_MANIFEST_SCHEMA = {
    "run_id": pl.Utf8,
    "input_patterns_json": pl.Utf8,
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


def run_research_publication(
    config: Config,
    *,
    study_name: str,
    study_schema_version: int,
    parameters: Mapping[str, Any],
    input_globs: Sequence[str | Path],
    evaluate: ResearchEvaluator,
    source_revision: str | None = None,
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
                manifest, fingerprint = _build_input_manifest(
                    config.data_dir, run_id, patterns, input_files
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
    if _aggregate_fingerprint(patterns, recorded_pairs) != expected:
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
    current = None if missing else _aggregate_fingerprint(patterns, current_pairs)
    return InputFingerprintStatus(
        run_id=run_id,
        expected_fingerprint=expected,
        current_fingerprint=current,
        matches=not missing and not added and current == expected,
        missing_files=tuple(missing),
        added_files=tuple(added),
        changed_files=tuple(changed),
    )


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
) -> tuple[pl.DataFrame, str]:
    root = data_dir.resolve()
    patterns_json = canonical_json(list(input_patterns))
    records: list[dict[str, Any]] = []
    pairs: list[tuple[str, str]] = []
    for path in input_files:
        relative_path = path.relative_to(root).as_posix()
        digest = _sha256_file(path)
        first_date, last_date = _parquet_date_bounds(path)
        records.append(
            {
                "run_id": run_id,
                "input_patterns_json": patterns_json,
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
        _aggregate_fingerprint(input_patterns, pairs),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _aggregate_fingerprint(
    patterns: Sequence[str], pairs: Sequence[tuple[str, str]]
) -> str:
    payload = canonical_json(
        {
            "patterns": list(patterns),
            "files": [list(pair) for pair in sorted(pairs)],
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
