"""Built-in focused event studies registered for the common CLI entry point."""

from __future__ import annotations

from marketdata.research import register_event_study
from marketdata.studies.gap_recovery import STUDY_NAME, run_gap_recovery_study

register_event_study(STUDY_NAME, run_gap_recovery_study, replace=True)

__all__ = ["run_gap_recovery_study"]
