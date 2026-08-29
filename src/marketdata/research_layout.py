"""Shared path and layout contract for cataloged research artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath

OBSERVATIONS_FILE_NAME = "observations.parquet"
INPUT_MANIFEST_FILE_NAME = "input_files.parquet"


@dataclass(frozen=True)
class ResearchRunLayout:
    directory: Path
    observations: Path
    manifest: Path


def normalize_relative_data_path(value: str) -> str:
    """Return one normalized relative POSIX path or reject it."""
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError("data paths must be relative to the data root")
    if any(part in {"", "."} for part in path.parts):
        raise ValueError("data paths must be normalized")
    return path.as_posix()


def resolve_data_path(data_dir: Path, relative_path: str) -> Path:
    """Resolve a normalized catalog path without permitting root escape."""
    normalized = normalize_relative_data_path(relative_path)
    root = data_dir.resolve()
    resolved = (root / Path(*PurePosixPath(normalized).parts)).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"data path escapes the data root: {relative_path!r}") from exc
    return resolved


def research_run_layout(
    data_dir: Path, study_name: str, run_id: str
) -> ResearchRunLayout:
    """Return the sole supported directory and filenames for one run."""
    study = normalize_path_component(study_name, "study_name")
    run = normalize_path_component(run_id, "run_id")
    directory = data_dir.resolve() / "results" / study / run
    return ResearchRunLayout(
        directory=directory,
        observations=directory / OBSERVATIONS_FILE_NAME,
        manifest=directory / INPUT_MANIFEST_FILE_NAME,
    )


def normalize_path_component(value: str, label: str) -> str:
    """Require one non-special POSIX path component."""
    if not value or PurePosixPath(value).parts != (value,) or value in {".", ".."}:
        raise ValueError(f"{label} must be one safe path component")
    return value
