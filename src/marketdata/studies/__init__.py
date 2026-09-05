"""Built-in focused event studies plus the private, gitignored study loader.

The repository is public. Reference studies live here; refined strategies
live under the gitignored ``private/studies`` directory (or the directory
named by ``MARKET_DATA_PRIVATE_DIR``) as ordinary modules that call
:func:`marketdata.research.register_event_study` at import time.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

from marketdata.research import register_event_study, registered_event_studies
from marketdata.studies.gap_recovery import STUDY_NAME, run_gap_recovery_study
from marketdata.studies.gap_recovery_opening import (
    STUDY_NAME as OPENING_STUDY_NAME,
)
from marketdata.studies.gap_recovery_opening import (
    run_gap_recovery_opening_study,
)

DEFAULT_PRIVATE_DIR = "private"
PRIVATE_DIR_ENV = "MARKET_DATA_PRIVATE_DIR"

register_event_study(STUDY_NAME, run_gap_recovery_study, replace=True)
register_event_study(OPENING_STUDY_NAME, run_gap_recovery_opening_study, replace=True)


def private_studies_dir(root: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the private study directory without creating it."""
    base = (
        Path(root)
        if root is not None
        else Path(os.environ.get(PRIVATE_DIR_ENV, DEFAULT_PRIVATE_DIR))
    )
    return base / "studies"


def load_private_studies(
    root: str | os.PathLike[str] | None = None,
) -> tuple[str, ...]:
    """Import every ``*.py`` module under the private study directory.

    Each module registers its own studies; this returns the names that were
    newly registered. A missing directory is not an error, so the public
    checkout keeps working without any private tree.
    """
    directory = private_studies_dir(root)
    if not directory.is_dir():
        return ()
    before = set(registered_event_studies())
    for path in sorted(directory.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module_name = f"marketdata_private_studies.{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load private study module {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
    return tuple(sorted(set(registered_event_studies()) - before))


__all__ = [
    "load_private_studies",
    "private_studies_dir",
    "run_gap_recovery_opening_study",
    "run_gap_recovery_study",
]
