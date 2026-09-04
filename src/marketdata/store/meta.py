"""SQLite metadata store: identity, universes, coverage, and scheduler state.

Bar data lives in Parquet (see bars.py); this database only tracks the
small relational state around it, so it stays tiny and trivially portable.
The schema is versioned via PRAGMA user_version with explicit migrations.

The v1 ticker registry and ticker-keyed coverage remain temporarily readable
under explicitly named legacy storage while M1 converts ingestion. Canonical
coverage is instrument-keyed and uses exact dataset keys.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Collection, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from marketdata.budget import tiingo_billing_month_start
from marketdata.identity import (
    ACTIVE_ALIAS_END,
    AliasResolutionReport,
    IdentifierEvidenceSegment,
    IdentifierResolution,
    ResolutionSegment,
    UniverseResolution,
    require_dataset_key,
    require_validation_state,
)
from marketdata.jsonutil import canonical_json
from marketdata.research_layout import (
    normalize_path_component,
    normalize_relative_data_path,
)

SCHEMA_VERSION = 8

_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS tickers (
    ticker      TEXT PRIMARY KEY,
    name        TEXT,
    exchange    TEXT,
    asset_type  TEXT,
    start_date  TEXT,
    end_date    TEXT
);

-- One row per (year, ticker): the annually rebuilt universe, ranked by
-- a dollar-volume metric. Records how the dataset was seeded and scopes
-- ingestion; backtests select from stored bars directly (D-010).
CREATE TABLE IF NOT EXISTS universe (
    year               INTEGER NOT NULL,
    ticker             TEXT    NOT NULL,
    rank               INTEGER,
    avg_dollar_volume  REAL,
    PRIMARY KEY (year, ticker)
);

-- Per-(ticker, dataset) closed interval of ingested data.
CREATE TABLE IF NOT EXISTS coverage (
    ticker      TEXT NOT NULL,
    dataset     TEXT NOT NULL,
    first_date  TEXT NOT NULL,
    last_date   TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (ticker, dataset)
);
"""

_SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id  TEXT PRIMARY KEY,
    lifecycle_status TEXT NOT NULL DEFAULT 'unknown',
    description    TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    CHECK (length(trim(instrument_id)) > 0),
    CHECK (lifecycle_status IN ('unknown', 'active', 'inactive'))
);

CREATE TABLE IF NOT EXISTS instrument_aliases (
    alias_id       INTEGER PRIMARY KEY,
    instrument_id  TEXT NOT NULL REFERENCES instruments(instrument_id),
    ticker         TEXT NOT NULL,
    exchange       TEXT NOT NULL DEFAULT '',
    asset_type     TEXT NOT NULL DEFAULT '',
    start_date     TEXT NOT NULL,
    end_date       TEXT NOT NULL,
    evidence       TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    CHECK (length(ticker) > 0),
    CHECK (start_date <= end_date),
    UNIQUE (instrument_id, ticker, exchange, asset_type, start_date, end_date)
);
CREATE INDEX IF NOT EXISTS instrument_aliases_ticker_dates
    ON instrument_aliases (ticker, start_date, end_date);

CREATE TABLE IF NOT EXISTS vendor_identifiers (
    vendor_identifier_id INTEGER PRIMARY KEY,
    instrument_id  TEXT NOT NULL REFERENCES instruments(instrument_id),
    dataset_key    TEXT NOT NULL,
    identifier_type TEXT NOT NULL,
    identifier_value TEXT NOT NULL,
    valid_from     TEXT NOT NULL,
    valid_to       TEXT NOT NULL,
    validation_state TEXT NOT NULL,
    evidence       TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    CHECK (dataset_key IN ('eod', 'intraday_1hour', 'intraday_5min')),
    CHECK (validation_state IN ('unvalidated', 'validated', 'rejected', 'conflict')),
    CHECK (valid_from <= valid_to),
    UNIQUE (
        instrument_id, dataset_key, identifier_type, identifier_value,
        valid_from, valid_to
    )
);
CREATE INDEX IF NOT EXISTS vendor_identifiers_lookup
    ON vendor_identifiers (instrument_id, dataset_key, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS vendor_identifiers_identity_lookup
    ON vendor_identifiers (
        dataset_key, identifier_type, identifier_value, validation_state,
        valid_from, valid_to, instrument_id
    );

-- The imported universe ticker remains immutable source data.  Its derived
-- resolution is replaceable and records all candidates rather than guessing.
CREATE TABLE IF NOT EXISTS universe_resolutions (
    year           INTEGER NOT NULL,
    ticker         TEXT NOT NULL,
    status         TEXT NOT NULL,
    instrument_id  TEXT REFERENCES instruments(instrument_id),
    candidate_instrument_ids TEXT NOT NULL,
    resolved_at    TEXT NOT NULL,
    PRIMARY KEY (year, ticker),
    FOREIGN KEY (year, ticker) REFERENCES universe(year, ticker) ON DELETE CASCADE,
    CHECK (status IN ('resolved', 'zero_matches', 'multiple_matches')),
    CHECK (
        (status = 'resolved' AND instrument_id IS NOT NULL)
        OR (status != 'resolved' AND instrument_id IS NULL)
    )
);
"""

_SCHEMA_V3 = """
CREATE TABLE IF NOT EXISTS coverage (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    dataset_key   TEXT NOT NULL,
    first_date    TEXT NOT NULL,
    last_date     TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (instrument_id, dataset_key),
    CHECK (dataset_key IN ('eod', 'intraday_1hour', 'intraday_5min')),
    CHECK (first_date <= last_date)
);

CREATE TABLE IF NOT EXISTS storage_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT OR IGNORE INTO storage_state (key, value)
VALUES ('storage_generation', 'v1');
"""

_SCHEMA_V4 = """
CREATE TABLE IF NOT EXISTS api_request_attempts (
    attempt_id       INTEGER PRIMARY KEY,
    occurred_at      TEXT NOT NULL,
    work_kind        TEXT NOT NULL,
    operation        TEXT NOT NULL,
    reserved_bytes   INTEGER NOT NULL,
    observed_bytes   INTEGER NOT NULL DEFAULT 0,
    settled          INTEGER NOT NULL DEFAULT 0,
    complete         INTEGER NOT NULL DEFAULT 0,
    bytes_known      INTEGER NOT NULL DEFAULT 0,
    CHECK (work_kind IN ('current', 'historical')),
    CHECK (reserved_bytes >= 0),
    CHECK (observed_bytes >= 0),
    CHECK (settled IN (0, 1)),
    CHECK (complete IN (0, 1)),
    CHECK (bytes_known IN (0, 1))
);
CREATE INDEX IF NOT EXISTS api_request_attempts_time
    ON api_request_attempts (occurred_at);
CREATE INDEX IF NOT EXISTS api_request_attempts_kind_time
    ON api_request_attempts (work_kind, occurred_at);

CREATE TABLE IF NOT EXISTS history_jobs (
    job_id           TEXT PRIMARY KEY,
    phase            INTEGER,
    dataset_key      TEXT NOT NULL,
    range_start      TEXT NOT NULL,
    range_end        TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    cohort_hash      TEXT NOT NULL,
    force            INTEGER NOT NULL DEFAULT 0,
    cancelled        INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL,
    sweep            INTEGER NOT NULL DEFAULT 0,
    cursor           INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    CHECK (phase IS NULL OR phase IN (1, 2, 3)),
    CHECK (dataset_key IN ('eod', 'intraday_1hour', 'intraday_5min')),
    CHECK (range_start <= range_end),
    CHECK (force IN (0, 1)),
    CHECK (cancelled IN (0, 1)),
    CHECK (status IN ('active', 'complete', 'blocked')),
    CHECK (sweep >= 0),
    CHECK (cursor >= 0)
);

CREATE TABLE IF NOT EXISTS history_targets (
    job_id           TEXT NOT NULL REFERENCES history_jobs(job_id) ON DELETE CASCADE,
    target_ordinal   INTEGER NOT NULL,
    instrument_id    TEXT NOT NULL REFERENCES instruments(instrument_id),
    successful_depth INTEGER NOT NULL DEFAULT 0,
    attempted_turns  INTEGER NOT NULL DEFAULT 0,
    last_attempt_status TEXT,
    last_attempt_detail TEXT NOT NULL DEFAULT '',
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (job_id, target_ordinal),
    UNIQUE (job_id, instrument_id),
    CHECK (target_ordinal >= 0),
    CHECK (successful_depth >= 0),
    CHECK (attempted_turns >= 0)
);

CREATE TABLE IF NOT EXISTS history_ranges (
    job_id           TEXT NOT NULL,
    target_ordinal   INTEGER NOT NULL,
    range_ordinal    INTEGER NOT NULL,
    ticker           TEXT NOT NULL,
    range_start      TEXT NOT NULL,
    range_end        TEXT NOT NULL,
    frontier_end     TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'active',
    terminal_blocked INTEGER NOT NULL DEFAULT 0,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (job_id, target_ordinal, range_ordinal),
    FOREIGN KEY (job_id, target_ordinal)
        REFERENCES history_targets(job_id, target_ordinal) ON DELETE CASCADE,
    CHECK (range_start <= range_end),
    CHECK (frontier_end <= range_end),
    CHECK (status IN ('active', 'complete')),
    CHECK (terminal_blocked IN (0, 1))
);
CREATE INDEX IF NOT EXISTS history_ranges_job_status
    ON history_ranges (job_id, status);

CREATE TABLE IF NOT EXISTS history_blocked_ranges (
    job_id           TEXT NOT NULL REFERENCES history_jobs(job_id) ON DELETE CASCADE,
    blocked_ordinal  INTEGER NOT NULL,
    ticker           TEXT NOT NULL,
    range_start      TEXT NOT NULL,
    range_end        TEXT NOT NULL,
    status           TEXT NOT NULL,
    detail           TEXT NOT NULL,
    PRIMARY KEY (job_id, blocked_ordinal),
    CHECK (range_start <= range_end)
);
"""

_SCHEMA_V5 = """
CREATE TABLE IF NOT EXISTS identity_episodes (
    instrument_id       TEXT PRIMARY KEY REFERENCES instruments(instrument_id),
    source_instrument_id TEXT REFERENCES instruments(instrument_id),
    dataset_key         TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    display_label       TEXT NOT NULL,
    episode_ordinal     INTEGER NOT NULL,
    basis               TEXT NOT NULL,
    confidence          TEXT NOT NULL,
    observed_first_date TEXT,
    observed_last_date  TEXT,
    evidence            TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    CHECK (dataset_key IN ('eod', 'intraday_1hour', 'intraday_5min')),
    CHECK (length(trim(ticker)) > 0),
    CHECK (length(trim(display_label)) > 0),
    CHECK (episode_ordinal >= 1),
    CHECK (basis IN ('archive_record', 'observed_gap')),
    CHECK (confidence IN ('metadata_validated', 'archive_bound', 'inferred')),
    CHECK (
        (observed_first_date IS NULL AND observed_last_date IS NULL)
        OR (
            observed_first_date IS NOT NULL
            AND observed_last_date IS NOT NULL
            AND observed_first_date <= observed_last_date
        )
    )
);
CREATE INDEX IF NOT EXISTS identity_episodes_source
    ON identity_episodes (source_instrument_id, dataset_key);
CREATE INDEX IF NOT EXISTS identity_episodes_ticker
    ON identity_episodes (ticker, dataset_key);
"""

_SCHEMA_V6 = """
CREATE TABLE IF NOT EXISTS research_runs (
    run_id                TEXT PRIMARY KEY,
    study_name            TEXT NOT NULL,
    study_schema_version  INTEGER NOT NULL,
    status                TEXT NOT NULL,
    started_at            TEXT NOT NULL,
    completed_at          TEXT,
    source_revision       TEXT,
    input_fingerprint     TEXT,
    observation_path      TEXT UNIQUE,
    manifest_path         TEXT UNIQUE,
    observation_count     INTEGER,
    error_summary         TEXT,
    CHECK (length(trim(run_id)) > 0),
    CHECK (length(trim(study_name)) > 0),
    CHECK (study_schema_version >= 1),
    CHECK (status IN ('running', 'succeeded', 'failed')),
    CHECK (source_revision IS NULL OR length(source_revision) <= 256),
    CHECK (input_fingerprint IS NULL OR length(input_fingerprint) = 64),
    CHECK (observation_count IS NULL OR observation_count >= 0),
    CHECK (error_summary IS NULL OR length(error_summary) <= 4096),
    CHECK (
        (status = 'running'
         AND completed_at IS NULL
         AND input_fingerprint IS NULL
         AND observation_path IS NULL
         AND manifest_path IS NULL
         AND observation_count IS NULL
         AND error_summary IS NULL)
        OR
        (status = 'succeeded'
         AND completed_at IS NOT NULL
         AND input_fingerprint IS NOT NULL
         AND observation_path IS NOT NULL
         AND manifest_path IS NOT NULL
         AND observation_count IS NOT NULL
         AND error_summary IS NULL)
        OR
        (status = 'failed'
         AND completed_at IS NOT NULL
         AND input_fingerprint IS NULL
         AND observation_path IS NULL
         AND manifest_path IS NULL
         AND observation_count IS NULL
         AND error_summary IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS research_runs_study_status
    ON research_runs (study_name, study_schema_version, status, started_at, run_id);

CREATE TABLE IF NOT EXISTS research_parameters (
    run_id      TEXT NOT NULL REFERENCES research_runs(run_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    value_json  TEXT NOT NULL,
    PRIMARY KEY (run_id, name),
    CHECK (length(trim(name)) > 0)
);

CREATE TABLE IF NOT EXISTS research_metrics (
    run_id           TEXT NOT NULL REFERENCES research_runs(run_id) ON DELETE CASCADE,
    metric_name      TEXT NOT NULL,
    dimensions_json  TEXT NOT NULL,
    value            REAL NOT NULL,
    unit             TEXT,
    PRIMARY KEY (run_id, metric_name, dimensions_json),
    CHECK (length(trim(metric_name)) > 0),
    CHECK (unit IS NULL OR length(trim(unit)) > 0)
);

CREATE TRIGGER IF NOT EXISTS research_runs_succeeded_no_update
BEFORE UPDATE ON research_runs
WHEN OLD.status = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_runs_succeeded_no_delete
BEFORE DELETE ON research_runs
WHEN OLD.status = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_parameters_succeeded_no_insert
BEFORE INSERT ON research_parameters
WHEN (SELECT status FROM research_runs WHERE run_id = NEW.run_id) = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_parameters_succeeded_no_update
BEFORE UPDATE ON research_parameters
WHEN (SELECT status FROM research_runs WHERE run_id = OLD.run_id) = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_parameters_succeeded_no_delete
BEFORE DELETE ON research_parameters
WHEN (SELECT status FROM research_runs WHERE run_id = OLD.run_id) = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_metrics_succeeded_no_insert
BEFORE INSERT ON research_metrics
WHEN (SELECT status FROM research_runs WHERE run_id = NEW.run_id) = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_metrics_succeeded_no_update
BEFORE UPDATE ON research_metrics
WHEN (SELECT status FROM research_runs WHERE run_id = OLD.run_id) = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_metrics_succeeded_no_delete
BEFORE DELETE ON research_metrics
WHEN (SELECT status FROM research_runs WHERE run_id = OLD.run_id) = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'succeeded research runs are immutable');
END;
"""

_SCHEMA_V7 = """
CREATE TABLE IF NOT EXISTS backfill_programs (
    program_id       TEXT PRIMARY KEY,
    definition_hash  TEXT NOT NULL,
    status           TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    CHECK (length(trim(program_id)) BETWEEN 1 AND 128),
    CHECK (length(definition_hash) = 64),
    CHECK (status IN ('active', 'complete', 'complete_with_exclusions'))
);

CREATE TABLE IF NOT EXISTS backfill_program_components (
    program_id       TEXT NOT NULL REFERENCES backfill_programs(program_id)
                     ON DELETE CASCADE,
    component_key    TEXT NOT NULL,
    component_ordinal INTEGER NOT NULL,
    phase            INTEGER NOT NULL,
    dataset_key      TEXT NOT NULL,
    scope_key        TEXT NOT NULL,
    range_start      TEXT NOT NULL,
    range_end        TEXT NOT NULL,
    job_id           TEXT NOT NULL UNIQUE,
    identity_status  TEXT NOT NULL DEFAULT 'pending',
    identity_cursor  INTEGER NOT NULL DEFAULT 0,
    state            TEXT NOT NULL DEFAULT 'pending',
    last_stop_reason TEXT,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (program_id, component_key),
    UNIQUE (program_id, component_ordinal),
    CHECK (length(trim(component_key)) > 0),
    CHECK (component_ordinal >= 0),
    CHECK (phase IN (1, 2, 3)),
    CHECK (dataset_key IN ('eod', 'intraday_1hour', 'intraday_5min')),
    CHECK (length(trim(scope_key)) > 0),
    CHECK (range_start <= range_end),
    CHECK (length(trim(job_id)) BETWEEN 1 AND 128),
    CHECK (identity_status IN ('pending', 'prepared')),
    CHECK (identity_cursor >= 0),
    CHECK (state IN ('pending', 'preparing', 'active', 'complete', 'blocked'))
);
CREATE INDEX IF NOT EXISTS backfill_program_components_phase
    ON backfill_program_components (program_id, phase, component_ordinal);

CREATE TABLE IF NOT EXISTS backfill_program_scopes (
    program_id       TEXT NOT NULL REFERENCES backfill_programs(program_id)
                     ON DELETE CASCADE,
    scope_key        TEXT NOT NULL,
    source_kind      TEXT NOT NULL,
    cohort_hash      TEXT NOT NULL,
    ticker_count     INTEGER NOT NULL,
    created_at       TEXT NOT NULL,
    PRIMARY KEY (program_id, scope_key),
    CHECK (length(trim(scope_key)) > 0),
    CHECK (source_kind IN ('seed_universes', 'tiingo_supported_us')),
    CHECK (length(cohort_hash) = 64),
    CHECK (ticker_count > 0)
);

CREATE TABLE IF NOT EXISTS backfill_program_tickers (
    program_id       TEXT NOT NULL,
    scope_key        TEXT NOT NULL,
    ticker_ordinal   INTEGER NOT NULL,
    ticker           TEXT NOT NULL,
    PRIMARY KEY (program_id, scope_key, ticker_ordinal),
    UNIQUE (program_id, scope_key, ticker),
    FOREIGN KEY (program_id, scope_key)
        REFERENCES backfill_program_scopes(program_id, scope_key)
        ON DELETE CASCADE,
    CHECK (ticker_ordinal >= 0),
    CHECK (length(trim(ticker)) > 0)
);

CREATE TABLE IF NOT EXISTS backfill_program_supported_records (
    program_id       TEXT NOT NULL,
    scope_key        TEXT NOT NULL,
    record_ordinal   INTEGER NOT NULL,
    ticker           TEXT NOT NULL,
    exchange         TEXT NOT NULL,
    asset_type       TEXT NOT NULL,
    price_currency   TEXT NOT NULL,
    start_date       TEXT NOT NULL,
    end_date         TEXT NOT NULL,
    PRIMARY KEY (program_id, scope_key, record_ordinal),
    FOREIGN KEY (program_id, scope_key)
        REFERENCES backfill_program_scopes(program_id, scope_key)
        ON DELETE CASCADE,
    CHECK (record_ordinal >= 0),
    CHECK (length(trim(ticker)) > 0),
    CHECK (start_date <= end_date)
);
CREATE INDEX IF NOT EXISTS backfill_program_supported_ticker
    ON backfill_program_supported_records (program_id, scope_key, ticker);
"""

_SCHEMA_V8 = """
ALTER TABLE history_jobs
ADD COLUMN work_kind TEXT NOT NULL DEFAULT 'historical'
CHECK (work_kind IN ('current', 'historical'));

ALTER TABLE history_jobs
ADD COLUMN refresh_overlap_days INTEGER NOT NULL DEFAULT 0
CHECK (refresh_overlap_days >= 0);

CREATE TABLE IF NOT EXISTS ongoing_programs (
    program_id          TEXT PRIMARY KEY,
    definition_hash     TEXT NOT NULL,
    initial_session     TEXT NOT NULL,
    cohort_size         INTEGER NOT NULL,
    lookback_sessions   INTEGER NOT NULL,
    min_observations    INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'active',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    CHECK (length(trim(program_id)) BETWEEN 1 AND 128),
    CHECK (length(definition_hash) = 64),
    CHECK (cohort_size > 0),
    CHECK (lookback_sessions > 0),
    CHECK (min_observations > 0),
    CHECK (min_observations <= lookback_sessions),
    CHECK (status = 'active')
);

CREATE TABLE IF NOT EXISTS ongoing_supported_snapshots (
    snapshot_id         TEXT PRIMARY KEY,
    as_of_session       TEXT NOT NULL,
    ticker_count        INTEGER NOT NULL,
    record_count        INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    CHECK (length(snapshot_id) = 64),
    CHECK (ticker_count > 0),
    CHECK (record_count >= ticker_count)
);

CREATE TABLE IF NOT EXISTS ongoing_supported_records (
    snapshot_id         TEXT NOT NULL REFERENCES ongoing_supported_snapshots(snapshot_id)
                        ON DELETE CASCADE,
    record_ordinal      INTEGER NOT NULL,
    ticker              TEXT NOT NULL,
    exchange            TEXT NOT NULL,
    asset_type          TEXT NOT NULL,
    price_currency      TEXT NOT NULL,
    start_date          TEXT NOT NULL,
    end_date            TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, record_ordinal),
    CHECK (record_ordinal >= 0),
    CHECK (length(trim(ticker)) > 0),
    CHECK (start_date <= end_date)
);
CREATE INDEX IF NOT EXISTS ongoing_supported_records_ticker
    ON ongoing_supported_records (snapshot_id, ticker);

CREATE TABLE IF NOT EXISTS ongoing_cycles (
    program_id          TEXT NOT NULL REFERENCES ongoing_programs(program_id)
                        ON DELETE CASCADE,
    session_date        TEXT NOT NULL,
    supported_snapshot_id TEXT NOT NULL
                        REFERENCES ongoing_supported_snapshots(snapshot_id),
    state               TEXT NOT NULL DEFAULT 'eod_identity',
    eod_identity_cursor INTEGER NOT NULL DEFAULT 0,
    cohort_snapshot_id  TEXT REFERENCES ongoing_cohort_snapshots(snapshot_id),
    hourly_identity_cursor INTEGER NOT NULL DEFAULT 0,
    five_min_identity_cursor INTEGER NOT NULL DEFAULT 0,
    eod_job_id          TEXT NOT NULL UNIQUE,
    hourly_job_id       TEXT NOT NULL UNIQUE,
    five_min_job_id     TEXT NOT NULL UNIQUE,
    last_stop_reason    TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (program_id, session_date),
    CHECK (state IN (
        'eod_identity', 'eod', 'cohort',
        'hourly_identity', 'hourly',
        'five_min_identity', 'five_min',
        'complete', 'complete_with_exclusions'
    )),
    CHECK (eod_identity_cursor >= 0),
    CHECK (hourly_identity_cursor >= 0),
    CHECK (five_min_identity_cursor >= 0)
);
CREATE INDEX IF NOT EXISTS ongoing_cycles_state
    ON ongoing_cycles (program_id, state, session_date);

CREATE TABLE IF NOT EXISTS ongoing_cohort_snapshots (
    snapshot_id         TEXT PRIMARY KEY,
    program_id          TEXT NOT NULL REFERENCES ongoing_programs(program_id)
                        ON DELETE CASCADE,
    as_of_session       TEXT NOT NULL,
    lookback_start      TEXT NOT NULL,
    lookback_end        TEXT NOT NULL,
    cohort_size         INTEGER NOT NULL,
    min_observations    INTEGER NOT NULL,
    member_count        INTEGER NOT NULL,
    cohort_hash         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    UNIQUE (program_id, as_of_session),
    CHECK (length(trim(snapshot_id)) BETWEEN 1 AND 160),
    CHECK (lookback_start <= lookback_end),
    CHECK (cohort_size > 0),
    CHECK (min_observations > 0),
    CHECK (member_count > 0),
    CHECK (length(cohort_hash) = 64)
);
CREATE INDEX IF NOT EXISTS ongoing_cohort_snapshots_program_date
    ON ongoing_cohort_snapshots (program_id, as_of_session);

CREATE TABLE IF NOT EXISTS ongoing_cohort_members (
    snapshot_id         TEXT NOT NULL REFERENCES ongoing_cohort_snapshots(snapshot_id)
                        ON DELETE CASCADE,
    rank                INTEGER NOT NULL,
    instrument_id       TEXT NOT NULL REFERENCES instruments(instrument_id),
    ticker              TEXT NOT NULL,
    avg_dollar_volume   REAL NOT NULL,
    observation_count   INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, rank),
    UNIQUE (snapshot_id, instrument_id),
    CHECK (rank > 0),
    CHECK (length(trim(ticker)) > 0),
    CHECK (avg_dollar_volume >= 0),
    CHECK (observation_count > 0)
);
"""


def _apply_v4_compatibility(con: sqlite3.Connection) -> None:
    """Fold pre-commit v4 working-tree variants into the numbered v5 migration."""
    attempt_columns = {
        row[1]
        for row in con.execute("PRAGMA table_info('api_request_attempts')").fetchall()
    }
    if "bytes_known" not in attempt_columns:
        con.execute(
            """ALTER TABLE api_request_attempts
               ADD COLUMN bytes_known INTEGER NOT NULL DEFAULT 0
               CHECK (bytes_known IN (0, 1))"""
        )
        con.execute(
            """UPDATE api_request_attempts SET bytes_known = 1
               WHERE complete = 1"""
        )
    job_columns = {
        row[1] for row in con.execute("PRAGMA table_info('history_jobs')").fetchall()
    }
    if "cancelled" not in job_columns:
        con.execute(
            """ALTER TABLE history_jobs
               ADD COLUMN cancelled INTEGER NOT NULL DEFAULT 0
               CHECK (cancelled IN (0, 1))"""
        )
    range_columns = {
        row[1] for row in con.execute("PRAGMA table_info('history_ranges')").fetchall()
    }
    if "terminal_blocked" not in range_columns:
        con.execute(
            """ALTER TABLE history_ranges
               ADD COLUMN terminal_blocked INTEGER NOT NULL DEFAULT 0
               CHECK (terminal_blocked IN (0, 1))"""
        )
    con.execute(
        """CREATE INDEX IF NOT EXISTS history_ranges_job_status
           ON history_ranges (job_id, status)"""
    )


def _migrate(con: sqlite3.Connection) -> None:
    version = con.execute("PRAGMA user_version").fetchone()[0]
    if version < 1:
        with con:
            con.executescript(_SCHEMA_V1)
            # v0 tracked a single high watermark; superseded by coverage.
            # Rebuild state from Parquet with `market-data reconcile`.
            con.execute("DROP TABLE IF EXISTS watermarks")
            con.execute("PRAGMA user_version = 1")
        version = 1
    if version < 2:
        with con:
            con.executescript(_SCHEMA_V2)
            con.execute("PRAGMA user_version = 2")
        version = 2
    if version < 3:
        with con:
            columns = {
                row[1]
                for row in con.execute("PRAGMA table_info('coverage')").fetchall()
            }
            if "ticker" in columns:
                con.execute("ALTER TABLE coverage RENAME TO ticker_coverage_v1")
            con.executescript(_SCHEMA_V3)
            con.execute("PRAGMA user_version = 3")
        version = 3
    if version < 4:
        with con:
            con.executescript(_SCHEMA_V4)
            con.execute("PRAGMA user_version = 4")
        version = 4
    if version < 5:
        with con:
            _apply_v4_compatibility(con)
            con.executescript(_SCHEMA_V5)
            con.execute("PRAGMA user_version = 5")
        version = 5
    if version < 6:
        with con:
            con.executescript(_SCHEMA_V6)
            con.execute("PRAGMA user_version = 6")
        version = 6
    if version < 7:
        with con:
            con.executescript(_SCHEMA_V7)
            con.execute("PRAGMA user_version = 7")
        version = 7
    if version < 8:
        with con:
            con.executescript(_SCHEMA_V8)
            con.execute("PRAGMA user_version = 8")
        version = 8
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"meta.db schema version {version} is newer than this code "
            f"supports ({SCHEMA_VERSION}) — update the tool"
        )


class MetaStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(self.path)
        self._con.row_factory = sqlite3.Row
        self._con.execute("PRAGMA foreign_keys = ON")
        _migrate(self._con)

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> MetaStore:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- warehouse generation ------------------------------------------

    def storage_generation(self) -> str:
        row = self._con.execute(
            "SELECT value FROM storage_state WHERE key = 'storage_generation'"
        ).fetchone()
        if row is None or row["value"] not in {"v1", "v2"}:
            raise RuntimeError("meta.db has no valid storage_generation marker")
        return str(row["value"])

    def activate_canonical_generation(self) -> None:
        """Record the v2 boundary and discard derived ticker coverage."""
        with self._con:
            self._con.execute(
                """UPDATE storage_state SET value = 'v2'
                   WHERE key = 'storage_generation'"""
            )
            self._con.execute("DELETE FROM ticker_coverage_v1")

    # ---- tickers ---------------------------------------------------------

    def upsert_tickers(self, rows: list[dict]) -> None:
        with self._con:
            self._con.executemany(
                """INSERT INTO tickers (ticker, name, exchange, asset_type, start_date, end_date)
                   VALUES (:ticker, :name, :exchange, :asset_type, :start_date, :end_date)
                   ON CONFLICT(ticker) DO UPDATE SET
                     name=excluded.name, exchange=excluded.exchange,
                     asset_type=excluded.asset_type,
                     start_date=excluded.start_date, end_date=excluded.end_date""",
                [
                    {
                        "ticker": r["ticker"].upper(),
                        "name": r.get("name"),
                        "exchange": r.get("exchange"),
                        "asset_type": r.get("asset_type"),
                        "start_date": r.get("start_date"),
                        "end_date": r.get("end_date"),
                    }
                    for r in rows
                ],
            )

    # ---- stable identity -------------------------------------------------

    def upsert_instrument(
        self,
        instrument_id: str | None = None,
        *,
        lifecycle_status: str | None = None,
        description: str | None = None,
    ) -> str:
        """Create or update an instrument without erasing omitted attributes."""
        if lifecycle_status is not None and lifecycle_status not in {
            "unknown",
            "active",
            "inactive",
        }:
            raise ValueError(f"invalid lifecycle_status {lifecycle_status!r}")
        if instrument_id is None:
            instrument_id = uuid4().hex
        elif not instrument_id.strip():
            raise ValueError("instrument_id must not be empty or whitespace")
        now = _now()
        with self._con:
            self._con.execute(
                """INSERT INTO instruments
                       (instrument_id, lifecycle_status, description, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(instrument_id) DO UPDATE SET
                     lifecycle_status=CASE WHEN ? IS NULL
                         THEN instruments.lifecycle_status
                         ELSE excluded.lifecycle_status END,
                     description=CASE WHEN ? IS NULL
                         THEN instruments.description
                         ELSE excluded.description END,
                     updated_at=excluded.updated_at""",
                (
                    instrument_id,
                    lifecycle_status or "unknown",
                    description,
                    now,
                    now,
                    lifecycle_status,
                    description,
                ),
            )
        return instrument_id

    def instrument_ids(self) -> set[str]:
        rows = self._con.execute("SELECT instrument_id FROM instruments").fetchall()
        return {str(row["instrument_id"]) for row in rows}

    def instrument_lifecycle(self) -> dict[str, str]:
        """Return lifecycle state keyed by stable instrument id."""
        rows = self._con.execute(
            """SELECT instrument_id, lifecycle_status FROM instruments
               ORDER BY instrument_id"""
        ).fetchall()
        return {str(row["instrument_id"]): str(row["lifecycle_status"]) for row in rows}

    def eod_identity_sources(self) -> list[sqlite3.Row]:
        """Return exact single-alias EOD identities eligible for episode auditing."""
        return self._con.execute(
            """SELECT DISTINCT
                      i.instrument_id, i.lifecycle_status, i.description,
                      a.ticker, a.exchange, a.asset_type,
                      a.start_date, a.end_date, a.evidence AS alias_evidence
                 FROM instruments AS i
                 JOIN instrument_aliases AS a USING (instrument_id)
                 JOIN vendor_identifiers AS v
                   ON v.instrument_id = i.instrument_id
                  AND v.dataset_key = 'eod'
                  AND v.validation_state = 'validated'
                  AND lower(v.identifier_type) = 'ticker'
                  AND upper(v.identifier_value) = a.ticker
                  AND v.valid_from = a.start_date
                  AND v.valid_to = a.end_date
                WHERE 1 = (
                    SELECT count(*) FROM instrument_aliases AS own_alias
                    WHERE own_alias.instrument_id = i.instrument_id
                  )
                ORDER BY i.instrument_id"""
        ).fetchall()

    def identity_episodes(self) -> list[sqlite3.Row]:
        """Return recorded listing-episode provenance in deterministic order."""
        return self._con.execute(
            """SELECT * FROM identity_episodes
               ORDER BY dataset_key, ticker, episode_ordinal, instrument_id"""
        ).fetchall()

    def instrument_alias_records(self, instrument_id: str) -> list[sqlite3.Row]:
        """Return one instrument's alias evidence in effective-date order."""
        return self._con.execute(
            """SELECT * FROM instrument_aliases WHERE instrument_id = ?
               ORDER BY start_date, end_date, alias_id""",
            (instrument_id,),
        ).fetchall()

    def instrument_aliases_for_instruments(
        self, instrument_ids: Collection[str]
    ) -> list[sqlite3.Row]:
        """Return alias envelopes for an explicit stable-instrument cohort."""
        selected = tuple(
            sorted(
                {
                    instrument_id.strip()
                    for instrument_id in instrument_ids
                    if instrument_id.strip()
                }
            )
        )
        if not selected:
            return []
        rows = _select_in_chunks(
            self._con,
            """SELECT instrument_id, ticker, start_date, end_date
                 FROM instrument_aliases
                WHERE instrument_id IN ({placeholders})""",
            selected,
        )
        return sorted(
            rows,
            key=lambda row: (
                str(row["instrument_id"]),
                str(row["start_date"]),
                str(row["end_date"]),
                str(row["ticker"]),
            ),
        )

    def identity_aliases(self, tickers: Collection[str]) -> list[sqlite3.Row]:
        """Return stable alias envelopes for a normalized ticker cohort."""
        normalized = sorted(
            {ticker.strip().upper() for ticker in tickers if ticker.strip()}
        )
        if not normalized:
            return []
        rows = _select_in_chunks(
            self._con,
            """SELECT a.instrument_id, a.ticker, a.exchange, a.asset_type,
                      a.start_date, a.end_date, a.evidence AS alias_evidence
                 FROM instrument_aliases AS a
                WHERE a.ticker IN ({placeholders})""",
            normalized,
        )
        return sorted(
            rows,
            key=lambda row: (
                str(row["ticker"]),
                str(row["start_date"]),
                str(row["end_date"]),
                str(row["instrument_id"]),
            ),
        )

    def record_identity_episode(
        self,
        instrument_id: str,
        *,
        source_instrument_id: str | None,
        dataset_key: str,
        ticker: str,
        display_label: str,
        episode_ordinal: int,
        basis: str,
        confidence: str,
        observed_first: date | None,
        observed_last: date | None,
        evidence: Mapping[str, Any] | str | None = None,
    ) -> None:
        """Record why one internal instrument represents one listing episode."""
        dataset_key = require_dataset_key(dataset_key)
        if episode_ordinal < 1:
            raise ValueError("episode_ordinal must be positive")
        if basis not in {"archive_record", "observed_gap"}:
            raise ValueError(f"invalid identity episode basis {basis!r}")
        if confidence not in {"metadata_validated", "archive_bound", "inferred"}:
            raise ValueError(f"invalid identity episode confidence {confidence!r}")
        if (observed_first is None) != (observed_last is None):
            raise ValueError("observed episode bounds must both be set or both omitted")
        if observed_first is not None and observed_first > observed_last:
            raise ValueError("observed episode start must not be after its end")
        normalized_ticker = ticker.strip().upper()
        normalized_label = display_label.strip()
        if not normalized_ticker or not normalized_label:
            raise ValueError("episode ticker and display label must not be blank")
        now = _now()
        with self._con:
            self._con.execute(
                """INSERT INTO identity_episodes
                       (instrument_id, source_instrument_id, dataset_key, ticker,
                        display_label, episode_ordinal, basis, confidence,
                        observed_first_date, observed_last_date, evidence,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(instrument_id) DO UPDATE SET
                     source_instrument_id=excluded.source_instrument_id,
                     dataset_key=excluded.dataset_key,
                     ticker=excluded.ticker,
                     display_label=excluded.display_label,
                     episode_ordinal=excluded.episode_ordinal,
                     basis=excluded.basis,
                     confidence=excluded.confidence,
                     observed_first_date=excluded.observed_first_date,
                     observed_last_date=excluded.observed_last_date,
                     evidence=excluded.evidence,
                     updated_at=excluded.updated_at""",
                (
                    instrument_id,
                    source_instrument_id,
                    dataset_key,
                    normalized_ticker,
                    normalized_label,
                    episode_ordinal,
                    basis,
                    confidence,
                    observed_first.isoformat() if observed_first else None,
                    observed_last.isoformat() if observed_last else None,
                    _evidence_json(evidence),
                    now,
                    now,
                ),
            )

    def retire_eod_identity_source(self, instrument_id: str) -> None:
        """Remove superseded EOD evidence after its bars move to episode owners."""
        other_datasets = self._con.execute(
            """SELECT DISTINCT dataset_key FROM vendor_identifiers
               WHERE instrument_id = ? AND dataset_key != 'eod'""",
            (instrument_id,),
        ).fetchall()
        if other_datasets:
            keys = sorted(str(row["dataset_key"]) for row in other_datasets)
            raise ValueError(
                f"cannot retire {instrument_id!r}; it has non-EOD evidence {keys}"
            )
        replacement_count = self._con.execute(
            """SELECT count(*) FROM identity_episodes
               WHERE source_instrument_id = ? AND dataset_key = 'eod'""",
            (instrument_id,),
        ).fetchone()[0]
        if not replacement_count:
            raise ValueError(
                f"cannot retire {instrument_id!r} without recorded EOD episodes"
            )
        with self._con:
            self._con.execute(
                "DELETE FROM coverage WHERE instrument_id = ? AND dataset_key = 'eod'",
                (instrument_id,),
            )
            self._con.execute(
                "DELETE FROM vendor_identifiers WHERE instrument_id = ? AND dataset_key = 'eod'",
                (instrument_id,),
            )
            self._con.execute(
                "DELETE FROM instrument_aliases WHERE instrument_id = ?",
                (instrument_id,),
            )

    def rollback_unpublished_eod_episodes(self, source_instrument_id: str) -> None:
        """Remove replacement evidence when a staged EOD root was never swapped."""
        replacements = self._con.execute(
            """SELECT instrument_id FROM identity_episodes
               WHERE source_instrument_id = ? AND dataset_key = 'eod'""",
            (source_instrument_id,),
        ).fetchall()
        if not replacements:
            raise ValueError(
                f"no unpublished EOD episodes found for {source_instrument_id!r}"
            )
        instrument_ids = [str(row["instrument_id"]) for row in replacements]
        placeholders = ",".join("?" for _ in instrument_ids)
        covered = self._con.execute(
            f"SELECT instrument_id FROM coverage WHERE instrument_id IN ({placeholders})",
            instrument_ids,
        ).fetchall()
        if covered:
            raise ValueError(
                "cannot roll back replacement episodes with coverage: "
                f"{sorted(str(row['instrument_id']) for row in covered)}"
            )
        with self._con:
            self._con.execute(
                f"DELETE FROM identity_episodes WHERE instrument_id IN ({placeholders})",
                instrument_ids,
            )
            self._con.execute(
                f"DELETE FROM vendor_identifiers WHERE instrument_id IN ({placeholders})",
                instrument_ids,
            )
            self._con.execute(
                f"DELETE FROM instrument_aliases WHERE instrument_id IN ({placeholders})",
                instrument_ids,
            )

    def backup_to(self, destination: Path | str) -> None:
        """Create a consistent SQLite backup without closing the live connection."""
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise FileExistsError(f"metadata backup already exists: {target}")
        backup = sqlite3.connect(target)
        try:
            self._con.backup(backup)
        finally:
            backup.close()

    def prune_archive_episode_envelope(
        self,
        instrument_id: str,
        ticker: str,
        start: date,
        end: date,
    ) -> bool:
        """Drop stale duplicate archive snapshots for one effective episode."""
        if not self.has_exact_identity_evidence(
            instrument_id, "eod", ticker, start, end
        ):
            raise ValueError(
                f"cannot prune {instrument_id!r} without its retained EOD envelope"
            )
        normalized_ticker = ticker.strip().upper()
        with self._con:
            alias_cursor = self._con.execute(
                """DELETE FROM instrument_aliases
                   WHERE instrument_id = ? AND ticker = ?
                     AND NOT (start_date = ? AND end_date = ?)""",
                (
                    instrument_id,
                    normalized_ticker,
                    start.isoformat(),
                    end.isoformat(),
                ),
            )
            vendor_cursor = self._con.execute(
                """DELETE FROM vendor_identifiers
                   WHERE instrument_id = ? AND dataset_key = 'eod'
                     AND lower(identifier_type) = 'ticker'
                     AND upper(identifier_value) = ?
                     AND NOT (valid_from = ? AND valid_to = ?)""",
                (
                    instrument_id,
                    normalized_ticker,
                    start.isoformat(),
                    end.isoformat(),
                ),
            )
        return bool(alias_cursor.rowcount or vendor_cursor.rowcount)

    def remove_uncovered_archive_episode(self, instrument_id: str) -> None:
        """Fail closed when a formerly archive-bound singleton needs validation."""
        episode = self._con.execute(
            """SELECT source_instrument_id, basis FROM identity_episodes
               WHERE instrument_id = ? AND dataset_key = 'eod'""",
            (instrument_id,),
        ).fetchone()
        if (
            episode is None
            or episode["source_instrument_id"] is not None
            or episode["basis"] != "archive_record"
        ):
            raise ValueError(f"{instrument_id!r} is not an archive episode")
        covered = self._con.execute(
            "SELECT 1 FROM coverage WHERE instrument_id = ? LIMIT 1",
            (instrument_id,),
        ).fetchone()
        if covered is not None:
            raise ValueError(f"cannot remove covered archive episode {instrument_id!r}")
        with self._con:
            self._con.execute(
                "DELETE FROM identity_episodes WHERE instrument_id = ?",
                (instrument_id,),
            )
            self._con.execute(
                "DELETE FROM vendor_identifiers WHERE instrument_id = ?",
                (instrument_id,),
            )
            self._con.execute(
                "DELETE FROM instrument_aliases WHERE instrument_id = ?",
                (instrument_id,),
            )

    def set_identity_episode_ordinal(
        self, instrument_id: str, episode_ordinal: int
    ) -> None:
        """Normalize display ordering after duplicate archive snapshots collapse."""
        if episode_ordinal < 1:
            raise ValueError("episode_ordinal must be positive")
        with self._con:
            cursor = self._con.execute(
                """UPDATE identity_episodes
                   SET episode_ordinal = ?, updated_at = ?
                   WHERE instrument_id = ?""",
                (episode_ordinal, _now(), instrument_id),
            )
        if cursor.rowcount != 1:
            raise ValueError(f"identity episode not found: {instrument_id!r}")

    def add_instrument_alias(
        self,
        instrument_id: str,
        ticker: str,
        start: date,
        end: date | None = None,
        *,
        exchange: str | None = None,
        asset_type: str | None = None,
        evidence: Mapping[str, Any] | str | None = None,
    ) -> int:
        """Register alias evidence; ``end=None`` means currently active."""
        end = end or ACTIVE_ALIAS_END
        if start > end:
            raise ValueError("alias start must not be after end")
        normalized_ticker = ticker.strip().upper()
        if not normalized_ticker:
            raise ValueError("ticker must not be empty")
        with self._con:
            cursor = self._con.execute(
                """INSERT INTO instrument_aliases
                       (instrument_id, ticker, exchange, asset_type, start_date,
                        end_date, evidence, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(
                       instrument_id, ticker, exchange, asset_type, start_date, end_date
                   ) DO UPDATE SET evidence=excluded.evidence
                   RETURNING alias_id""",
                (
                    instrument_id,
                    normalized_ticker,
                    exchange or "",
                    asset_type or "",
                    start.isoformat(),
                    end.isoformat(),
                    _evidence_json(evidence),
                    _now(),
                ),
            )
            return int(cursor.fetchone()[0])

    def add_vendor_identifier(
        self,
        instrument_id: str,
        dataset_key: str,
        identifier_type: str,
        identifier_value: str,
        valid_from: date,
        valid_to: date,
        *,
        validation_state: str = "unvalidated",
        evidence: Mapping[str, Any] | str | None = None,
    ) -> int:
        """Store dataset-specific identifier evidence and its closed envelope."""
        dataset_key = require_dataset_key(dataset_key)
        validation_state = require_validation_state(validation_state)
        if valid_from > valid_to:
            raise ValueError("identifier valid_from must not be after valid_to")
        identifier_type, identifier_value = _normalize_identifier(
            identifier_type, identifier_value
        )
        now = _now()
        with self._con:
            cursor = self._con.execute(
                """INSERT INTO vendor_identifiers
                       (instrument_id, dataset_key, identifier_type, identifier_value,
                        valid_from, valid_to, validation_state, evidence,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(
                       instrument_id, dataset_key, identifier_type, identifier_value,
                       valid_from, valid_to
                   ) DO UPDATE SET
                       validation_state=excluded.validation_state,
                       evidence=excluded.evidence,
                       updated_at=excluded.updated_at
                   RETURNING vendor_identifier_id""",
                (
                    instrument_id,
                    dataset_key,
                    identifier_type,
                    identifier_value,
                    valid_from.isoformat(),
                    valid_to.isoformat(),
                    validation_state,
                    _evidence_json(evidence),
                    now,
                    now,
                ),
            )
            return int(cursor.fetchone()[0])

    def vendor_identifier_evidence_segments(
        self,
        instrument_id: str,
        dataset_key: str,
        identifier_type: str,
        identifier_value: str,
        start: date,
        end: date,
    ) -> tuple[IdentifierEvidenceSegment, ...]:
        """Partition a span by reusable evidence for one identifier candidate."""
        dataset_key = require_dataset_key(dataset_key)
        if start > end:
            raise ValueError("evidence start must not be after end")
        identifier_type, identifier_value = _normalize_identifier(
            identifier_type, identifier_value
        )
        if identifier_type == "ticker":
            identifier_clause = (
                "lower(identifier_type) = 'ticker' AND upper(identifier_value) = ?"
            )
            identifier_parameters = (identifier_value,)
        else:
            identifier_clause = "identifier_type = ? AND identifier_value = ?"
            identifier_parameters = (identifier_type, identifier_value)
        rows = self._con.execute(
            f"""SELECT vendor_identifier_id, validation_state,
                       valid_from, valid_to
                FROM vendor_identifiers
                WHERE instrument_id = ? AND dataset_key = ?
                  AND {identifier_clause}
                  AND valid_from <= ? AND valid_to >= ?
                ORDER BY valid_from, valid_to, validation_state,
                         vendor_identifier_id""",
            (
                instrument_id,
                dataset_key,
                *identifier_parameters,
                end.isoformat(),
                start.isoformat(),
            ),
        ).fetchall()

        boundaries = {start.toordinal(), end.toordinal() + 1}
        intervals: list[tuple[int, str, int, int]] = []
        for row in rows:
            row_start = max(start, date.fromisoformat(row["valid_from"]))
            row_end = min(end, date.fromisoformat(row["valid_to"]))
            intervals.append(
                (
                    int(row["vendor_identifier_id"]),
                    str(row["validation_state"]),
                    row_start.toordinal(),
                    row_end.toordinal(),
                )
            )
            boundaries.add(row_start.toordinal())
            boundaries.add(row_end.toordinal() + 1)

        ordered = sorted(boundaries)
        segments: list[IdentifierEvidenceSegment] = []
        for segment_start, next_start in zip(ordered, ordered[1:], strict=False):
            segment_end = next_start - 1
            active = [
                (identifier_id, validation_state)
                for identifier_id, validation_state, row_start, row_end in intervals
                if row_start <= segment_start and row_end >= segment_end
            ]
            terminal_states = {state for _, state in active if state != "unvalidated"}
            if len(terminal_states) > 1:
                validation_state = "conflict"
            elif terminal_states:
                validation_state = next(iter(terminal_states))
            elif active:
                validation_state = "unvalidated"
            else:
                validation_state = None
            segments.append(
                IdentifierEvidenceSegment(
                    start=date.fromordinal(segment_start),
                    end=date.fromordinal(segment_end),
                    validation_state=(
                        require_validation_state(validation_state)
                        if validation_state is not None
                        else None
                    ),
                    vendor_identifier_ids=tuple(
                        sorted(identifier_id for identifier_id, _ in active)
                    ),
                )
            )
        return tuple(segments)

    def resolve_alias_range(
        self, ticker: str, start: date, end: date
    ) -> AliasResolutionReport:
        """Partition a ticker/date request into explicit identity outcomes.

        Boundaries come from every overlapping alias.  Consequently a rename,
        symbol reuse, evidence gap, and conflicting overlap are all represented
        without selecting the newest or otherwise preferred listing.
        """
        if start > end:
            raise ValueError("resolution start must not be after end")
        normalized_ticker = ticker.strip().upper()
        if not normalized_ticker:
            raise ValueError("ticker must not be empty")
        rows = self._con.execute(
            """SELECT alias_id, instrument_id, start_date, end_date
               FROM instrument_aliases
               WHERE ticker = ? AND start_date <= ? AND end_date >= ?
               ORDER BY start_date, end_date, instrument_id, alias_id""",
            (normalized_ticker, end.isoformat(), start.isoformat()),
        ).fetchall()

        boundaries = {start.toordinal(), end.toordinal() + 1}
        aliases: list[tuple[int, str, int, int]] = []
        for row in rows:
            alias_start = max(start, date.fromisoformat(row["start_date"]))
            alias_end = min(end, date.fromisoformat(row["end_date"]))
            aliases.append(
                (
                    row["alias_id"],
                    row["instrument_id"],
                    alias_start.toordinal(),
                    alias_end.toordinal(),
                )
            )
            boundaries.add(alias_start.toordinal())
            boundaries.add(alias_end.toordinal() + 1)

        ordered = sorted(boundaries)
        segments: list[ResolutionSegment] = []
        for segment_start_ordinal, next_start_ordinal in zip(
            ordered, ordered[1:], strict=False
        ):
            segment_end_ordinal = next_start_ordinal - 1
            active = [
                (alias_id, instrument_id)
                for alias_id, instrument_id, alias_start, alias_end in aliases
                if alias_start <= segment_start_ordinal
                and alias_end >= segment_end_ordinal
            ]
            instrument_ids = tuple(
                sorted({instrument_id for _, instrument_id in active})
            )
            if not instrument_ids:
                status = "zero_matches"
            elif len(instrument_ids) == 1:
                status = "resolved"
            else:
                status = "multiple_matches"
            segments.append(
                ResolutionSegment(
                    start=date.fromordinal(segment_start_ordinal),
                    end=date.fromordinal(segment_end_ordinal),
                    status=status,
                    instrument_ids=instrument_ids,
                    alias_ids=tuple(sorted(alias_id for alias_id, _ in active)),
                )
            )
        return AliasResolutionReport(
            ticker=normalized_ticker,
            start=start,
            end=end,
            segments=tuple(segments),
        )

    def instrument_aliases_cover_range(
        self, instrument_id: str, start: date, end: date
    ) -> bool:
        """Whether this instrument's alias evidence covers a span without gaps."""
        if start > end:
            raise ValueError("resolution start must not be after end")
        rows = self._con.execute(
            """SELECT start_date AS valid_from, end_date AS valid_to
               FROM instrument_aliases
               WHERE instrument_id = ? AND start_date <= ? AND end_date >= ?
               ORDER BY start_date, end_date""",
            (instrument_id, end.isoformat(), start.isoformat()),
        ).fetchall()
        return _rows_cover(rows, start, end)

    def has_exact_identity_evidence(
        self,
        instrument_id: str,
        dataset_key: str,
        ticker: str,
        start: date,
        end: date,
    ) -> bool:
        """Whether one exact alias and validated ticker identifier are recorded."""
        dataset_key = require_dataset_key(dataset_key)
        normalized_ticker = ticker.strip().upper()
        row = self._con.execute(
            """SELECT
                   EXISTS(
                       SELECT 1 FROM instrument_aliases
                       WHERE instrument_id = ? AND ticker = ?
                         AND start_date = ? AND end_date = ?
                   ) AS has_alias,
                   EXISTS(
                       SELECT 1 FROM vendor_identifiers
                       WHERE instrument_id = ? AND dataset_key = ?
                         AND validation_state = 'validated'
                         AND lower(identifier_type) = 'ticker'
                         AND upper(identifier_value) = ?
                         AND valid_from = ? AND valid_to = ?
                   ) AS has_identifier""",
            (
                instrument_id,
                normalized_ticker,
                start.isoformat(),
                end.isoformat(),
                instrument_id,
                dataset_key,
                normalized_ticker,
                start.isoformat(),
                end.isoformat(),
            ),
        ).fetchone()
        return bool(row["has_alias"] and row["has_identifier"])

    def resolve_vendor_identifier(
        self, instrument_id: str, dataset_key: str, start: date, end: date
    ) -> IdentifierResolution:
        """Resolve one validated identifier for this exact dataset and span."""
        dataset_key = require_dataset_key(dataset_key)
        if start > end:
            raise ValueError("resolution start must not be after end")
        rows = self._con.execute(
            """SELECT vendor_identifier_id, identifier_type, identifier_value,
                      valid_from, valid_to
               FROM vendor_identifiers
               WHERE instrument_id = ? AND dataset_key = ?
                 AND validation_state = 'validated'
                 AND valid_from <= ? AND valid_to >= ?
               ORDER BY identifier_type, identifier_value, valid_from, valid_to,
                        vendor_identifier_id""",
            (instrument_id, dataset_key, end.isoformat(), start.isoformat()),
        ).fetchall()
        by_key: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            key = (row["identifier_type"], row["identifier_value"])
            by_key.setdefault(key, []).append(row)
        candidate_keys = {
            key
            for key, evidence_rows in by_key.items()
            if _rows_cover(evidence_rows, start, end)
        }
        ids = tuple(
            sorted(
                int(row["vendor_identifier_id"])
                for key in candidate_keys
                for row in by_key[key]
            )
        )
        conflicting_instrument_ids: set[str] = set()
        for identifier_type, identifier_value in candidate_keys:
            matching = self._con.execute(
                """SELECT DISTINCT instrument_id
                   FROM vendor_identifiers
                   WHERE dataset_key = ? AND identifier_type = ?
                     AND identifier_value = ? AND validation_state = 'validated'
                     AND valid_from <= ? AND valid_to >= ?""",
                (
                    dataset_key,
                    identifier_type,
                    identifier_value,
                    end.isoformat(),
                    start.isoformat(),
                ),
            ).fetchall()
            conflicting_instrument_ids.update(
                row["instrument_id"]
                for row in matching
                if row["instrument_id"] != instrument_id
            )
        status = (
            "zero_matches"
            if not candidate_keys
            else "multiple_matches"
            if len(candidate_keys) > 1 or conflicting_instrument_ids
            else "resolved"
        )
        reported_key = next(iter(candidate_keys)) if len(candidate_keys) == 1 else None
        individually_covering_ids = (
            [
                int(row["vendor_identifier_id"])
                for row in by_key[reported_key]
                if row["valid_from"] <= start.isoformat()
                and row["valid_to"] >= end.isoformat()
            ]
            if reported_key and status == "resolved"
            else []
        )
        return IdentifierResolution(
            instrument_id=instrument_id,
            dataset_key=dataset_key,
            start=start,
            end=end,
            status=status,
            vendor_identifier_ids=ids,
            vendor_identifier_id=(
                min(individually_covering_ids) if individually_covering_ids else None
            ),
            identifier_type=reported_key[0] if reported_key else None,
            identifier_value=reported_key[1] if reported_key else None,
            conflicting_instrument_ids=tuple(sorted(conflicting_instrument_ids)),
        )

    def resolve_vendor_identifier_range(
        self, instrument_id: str, dataset_key: str, start: date, end: date
    ) -> tuple[IdentifierResolution, ...]:
        """Partition a span wherever exact-dataset identifier evidence changes.

        ``resolve_vendor_identifier`` deliberately answers one whole-span
        question.  Ingestion needs the finer-grained form so validated portions
        on either side of missing or conflicting evidence remain visible rather
        than collapsing the entire request into one failure.
        """
        dataset_key = require_dataset_key(dataset_key)
        if start > end:
            raise ValueError("resolution start must not be after end")

        own_rows = self._con.execute(
            """SELECT identifier_type, identifier_value, valid_from, valid_to
               FROM vendor_identifiers
               WHERE instrument_id = ? AND dataset_key = ?
                 AND validation_state = 'validated'
                 AND valid_from <= ? AND valid_to >= ?""",
            (instrument_id, dataset_key, end.isoformat(), start.isoformat()),
        ).fetchall()
        keys = {
            (str(row["identifier_type"]), str(row["identifier_value"]))
            for row in own_rows
        }
        relevant_rows = list(own_rows)
        for identifier_type, identifier_value in sorted(keys):
            relevant_rows.extend(
                self._con.execute(
                    """SELECT identifier_type, identifier_value, valid_from, valid_to
                       FROM vendor_identifiers
                       WHERE instrument_id != ? AND dataset_key = ?
                         AND identifier_type = ? AND identifier_value = ?
                         AND validation_state = 'validated'
                         AND valid_from <= ? AND valid_to >= ?""",
                    (
                        instrument_id,
                        dataset_key,
                        identifier_type,
                        identifier_value,
                        end.isoformat(),
                        start.isoformat(),
                    ),
                ).fetchall()
            )

        boundaries = {start.toordinal(), end.toordinal() + 1}
        for row in relevant_rows:
            row_start = max(start, date.fromisoformat(row["valid_from"]))
            row_end = min(end, date.fromisoformat(row["valid_to"]))
            boundaries.add(row_start.toordinal())
            boundaries.add(row_end.toordinal() + 1)

        ordered = sorted(boundaries)
        return tuple(
            self.resolve_vendor_identifier(
                instrument_id,
                dataset_key,
                date.fromordinal(segment_start),
                date.fromordinal(next_start - 1),
            )
            for segment_start, next_start in zip(ordered, ordered[1:], strict=False)
        )

    def resolve_universe(self, year: int) -> list[UniverseResolution]:
        """Resolve and atomically record every source ticker for one year."""
        source_rows = self._con.execute(
            "SELECT ticker FROM universe WHERE year = ? ORDER BY rank, ticker", (year,)
        ).fetchall()
        year_start = date(year, 1, 1).isoformat()
        year_end = date(year, 12, 31).isoformat()
        reports: list[UniverseResolution] = []
        for source in source_rows:
            rows = self._con.execute(
                """SELECT DISTINCT instrument_id FROM instrument_aliases
                   WHERE ticker = ? AND start_date <= ? AND end_date >= ?
                   ORDER BY instrument_id""",
                (source["ticker"], year_end, year_start),
            ).fetchall()
            instrument_ids = tuple(row["instrument_id"] for row in rows)
            status = (
                "zero_matches"
                if not instrument_ids
                else "resolved"
                if len(instrument_ids) == 1
                else "multiple_matches"
            )
            reports.append(
                UniverseResolution(
                    year=year,
                    ticker=source["ticker"],
                    status=status,
                    instrument_ids=instrument_ids,
                )
            )

        now = _now()
        with self._con:
            self._con.execute(
                "DELETE FROM universe_resolutions WHERE year = ?", (year,)
            )
            self._con.executemany(
                """INSERT INTO universe_resolutions
                       (year, ticker, status, instrument_id,
                        candidate_instrument_ids, resolved_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (
                        report.year,
                        report.ticker,
                        report.status,
                        report.instrument_id,
                        json.dumps(report.instrument_ids, separators=(",", ":")),
                        now,
                    )
                    for report in reports
                ],
            )
        return reports

    # ---- universe --------------------------------------------------------

    def set_universe(self, year: int, entries: list[dict]) -> None:
        """Replace the universe for a year. Entries: ticker, optional rank
        and avg_dollar_volume."""
        self.replace_universes({year: entries})

    def replace_universes(self, by_year: dict[int, list[dict]]) -> None:
        """Replace universes atomically while retaining unchanged resolutions."""
        with self._con:
            for year, entries in by_year.items():
                normalized = [
                    (
                        year,
                        entry["ticker"].upper(),
                        entry.get("rank"),
                        entry.get("avg_dollar_volume"),
                    )
                    for entry in entries
                ]
                self._con.executemany(
                    """INSERT INTO universe (year, ticker, rank, avg_dollar_volume)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(year, ticker) DO UPDATE SET
                         rank=excluded.rank,
                         avg_dollar_volume=excluded.avg_dollar_volume""",
                    normalized,
                )
                tickers = [entry[1] for entry in normalized]
                if tickers:
                    placeholders = ",".join("?" for _ in tickers)
                    self._con.execute(
                        f"""DELETE FROM universe
                            WHERE year = ? AND ticker NOT IN ({placeholders})""",
                        (year, *tickers),
                    )
                else:
                    self._con.execute("DELETE FROM universe WHERE year = ?", (year,))

    def universe(self, year: int) -> list[sqlite3.Row]:
        return self._con.execute(
            "SELECT * FROM universe WHERE year = ? ORDER BY rank, ticker", (year,)
        ).fetchall()

    def universe_years(self) -> list[int]:
        rows = self._con.execute(
            "SELECT DISTINCT year FROM universe ORDER BY year"
        ).fetchall()
        return [r["year"] for r in rows]

    def all_universe_tickers(self) -> list[str]:
        """Every ticker that has ever been in any year's universe."""
        rows = self._con.execute(
            "SELECT DISTINCT ticker FROM universe ORDER BY ticker"
        ).fetchall()
        return [r["ticker"] for r in rows]

    def latest_universe_tickers(self) -> list[str]:
        rows = self._con.execute(
            """SELECT ticker FROM universe
               WHERE year = (SELECT MAX(year) FROM universe)
               ORDER BY rank, ticker"""
        ).fetchall()
        return [r["ticker"] for r in rows]

    # ---- legacy v1 ticker coverage --------------------------------------

    def get_ticker_coverage_v1(
        self, ticker: str, dataset: str
    ) -> tuple[date, date] | None:
        """Read quarantined v1 coverage (temporary ingestion compatibility)."""
        row = self._con.execute(
            """SELECT first_date, last_date FROM ticker_coverage_v1
               WHERE ticker = ? AND dataset = ?""",
            (ticker.upper(), dataset),
        ).fetchone()
        if row is None:
            return None
        return date.fromisoformat(row["first_date"]), date.fromisoformat(
            row["last_date"]
        )

    def set_ticker_coverage_v1(
        self, ticker: str, dataset: str, first: date, last: date
    ) -> None:
        """Set quarantined v1 ticker coverage."""
        with self._con:
            self._con.execute(
                """INSERT INTO ticker_coverage_v1
                       (ticker, dataset, first_date, last_date, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(ticker, dataset) DO UPDATE SET
                     first_date=excluded.first_date, last_date=excluded.last_date,
                     updated_at=excluded.updated_at""",
                (ticker.upper(), dataset, first.isoformat(), last.isoformat(), _now()),
            )

    def extend_ticker_coverage_v1(
        self, ticker: str, dataset: str, first: date, last: date
    ) -> None:
        """Widen the covered interval to include [first, last]."""
        existing = self.get_ticker_coverage_v1(ticker, dataset)
        if existing:
            first = min(first, existing[0])
            last = max(last, existing[1])
        self.set_ticker_coverage_v1(ticker, dataset, first, last)

    def ticker_coverage_v1(self, dataset: str) -> dict[str, tuple[date, date]]:
        rows = self._con.execute(
            """SELECT ticker, first_date, last_date FROM ticker_coverage_v1
               WHERE dataset = ?""",
            (dataset,),
        ).fetchall()
        return {
            r["ticker"]: (
                date.fromisoformat(r["first_date"]),
                date.fromisoformat(r["last_date"]),
            )
            for r in rows
        }

    def replace_ticker_coverage_v1(
        self, entries: dict[tuple[str, str], tuple[date, date]]
    ) -> None:
        """Atomically replace ALL coverage rows (used by reconcile): stale
        entries for vanished files must not survive a rebuild."""
        with self._con:
            self._con.execute("DELETE FROM ticker_coverage_v1")
            now = _now()
            self._con.executemany(
                """INSERT INTO ticker_coverage_v1
                       (ticker, dataset, first_date, last_date, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [
                    (ticker.upper(), dataset, first.isoformat(), last.isoformat(), now)
                    for (ticker, dataset), (first, last) in entries.items()
                ],
            )

    def clear_ticker_coverage_v1(self, dataset: str | None = None) -> None:
        with self._con:
            if dataset is None:
                self._con.execute("DELETE FROM ticker_coverage_v1")
            else:
                self._con.execute(
                    "DELETE FROM ticker_coverage_v1 WHERE dataset = ?", (dataset,)
                )

    # ---- canonical instrument coverage ----------------------------------

    def get_coverage(
        self, instrument_id: str, dataset_key: str
    ) -> tuple[date, date] | None:
        dataset_key = require_dataset_key(dataset_key)
        row = self._con.execute(
            """SELECT first_date, last_date FROM coverage
               WHERE instrument_id = ? AND dataset_key = ?""",
            (instrument_id, dataset_key),
        ).fetchone()
        if row is None:
            return None
        return date.fromisoformat(row["first_date"]), date.fromisoformat(
            row["last_date"]
        )

    def coverage(self, dataset_key: str) -> dict[str, tuple[date, date]]:
        dataset_key = require_dataset_key(dataset_key)
        rows = self._con.execute(
            """SELECT instrument_id, first_date, last_date FROM coverage
               WHERE dataset_key = ? ORDER BY instrument_id""",
            (dataset_key,),
        ).fetchall()
        return {
            row["instrument_id"]: (
                date.fromisoformat(row["first_date"]),
                date.fromisoformat(row["last_date"]),
            )
            for row in rows
        }

    def set_coverage(
        self,
        instrument_id: str,
        dataset_key: str,
        first: date,
        last: date,
    ) -> None:
        dataset_key = require_dataset_key(dataset_key)
        if first > last:
            raise ValueError("coverage first date must not be after last date")
        with self._con:
            self._con.execute(
                """INSERT INTO coverage
                       (instrument_id, dataset_key, first_date, last_date, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(instrument_id, dataset_key) DO UPDATE SET
                     first_date=excluded.first_date, last_date=excluded.last_date,
                     updated_at=excluded.updated_at""",
                (
                    instrument_id,
                    dataset_key,
                    first.isoformat(),
                    last.isoformat(),
                    _now(),
                ),
            )

    def extend_coverage(
        self,
        instrument_id: str,
        dataset_key: str,
        first: date,
        last: date,
    ) -> None:
        """Widen one canonical coverage interval without ever shrinking it."""
        existing = self.get_coverage(instrument_id, dataset_key)
        if existing is not None:
            first = min(first, existing[0])
            last = max(last, existing[1])
        self.set_coverage(instrument_id, dataset_key, first, last)

    def replace_coverage(
        self, entries: dict[tuple[str, str], tuple[date, date]]
    ) -> None:
        """Atomically replace canonical coverage after reconciliation."""
        normalized = []
        now = _now()
        for (instrument_id, dataset_key), (first, last) in entries.items():
            dataset_key = require_dataset_key(dataset_key)
            if first > last:
                raise ValueError("coverage first date must not be after last date")
            normalized.append(
                (
                    instrument_id,
                    dataset_key,
                    first.isoformat(),
                    last.isoformat(),
                    now,
                )
            )
        with self._con:
            self._con.execute("DELETE FROM coverage")
            self._con.executemany(
                """INSERT INTO coverage
                       (instrument_id, dataset_key, first_date, last_date, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                normalized,
            )

    # ---- research result catalog ---------------------------------------

    def create_research_run(
        self,
        *,
        run_id: str,
        study_name: str,
        study_schema_version: int,
        parameters: Mapping[str, Any],
        source_revision: str | None = None,
        started_at: datetime | None = None,
    ) -> None:
        """Register a new running research execution and typed parameters."""
        run_id = _research_run_id(run_id)
        study_name = normalize_path_component(study_name.strip(), "study_name")
        if study_schema_version < 1:
            raise ValueError("study_schema_version must be at least 1")
        if source_revision is not None:
            source_revision = source_revision.strip()
            if not source_revision:
                source_revision = None
            elif len(source_revision) > 256:
                raise ValueError("source_revision must not exceed 256 characters")
        encoded_parameters: list[tuple[str, str, str]] = []
        normalized_names: set[str] = set()
        for name, value in parameters.items():
            if not isinstance(name, str):
                raise TypeError("research parameter names must be strings")
            normalized_name = name.strip()
            if not normalized_name:
                raise ValueError("research parameter names must not be empty")
            if normalized_name in normalized_names:
                raise ValueError(
                    "research parameter names must be unique after trimming"
                )
            normalized_names.add(normalized_name)
            encoded_parameters.append((run_id, normalized_name, canonical_json(value)))
        encoded_parameters.sort(key=lambda row: row[1])
        timestamp = _utc_timestamp(started_at or datetime.now(UTC))
        with self._con:
            self._con.execute(
                """INSERT INTO research_runs
                       (run_id, study_name, study_schema_version, status,
                        started_at, source_revision)
                   VALUES (?, ?, ?, 'running', ?, ?)""",
                (
                    run_id,
                    study_name,
                    study_schema_version,
                    timestamp,
                    source_revision,
                ),
            )
            self._con.executemany(
                """INSERT INTO research_parameters (run_id, name, value_json)
                   VALUES (?, ?, ?)""",
                encoded_parameters,
            )

    def succeed_research_run(
        self,
        *,
        run_id: str,
        input_fingerprint: str,
        observation_path: str,
        manifest_path: str,
        observation_count: int,
        metrics: Collection[Mapping[str, Any]],
        completed_at: datetime | None = None,
    ) -> None:
        """Atomically record tidy metrics and publish one successful run."""
        if len(input_fingerprint) != 64 or any(
            char not in "0123456789abcdef" for char in input_fingerprint
        ):
            raise ValueError("input_fingerprint must be a lowercase SHA-256 digest")
        run_id = _research_run_id(run_id)
        observation_path = normalize_relative_data_path(observation_path)
        manifest_path = normalize_relative_data_path(manifest_path)
        if observation_count < 0:
            raise ValueError("observation_count must not be negative")
        encoded_metrics: list[tuple[str, str, str, float, str | None]] = []
        for metric in metrics:
            name = str(metric["name"]).strip()
            if not name:
                raise ValueError("research metric names must not be empty")
            raw_value = metric["value"]
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                raise TypeError("research metric values must be numeric")
            value = float(raw_value)
            if not math.isfinite(value):
                raise ValueError("research metric values must be finite")
            dimensions = metric.get("dimensions", {})
            if not isinstance(dimensions, Mapping):
                raise TypeError("research metric dimensions must be a mapping")
            unit_value = metric.get("unit")
            unit = None if unit_value is None else str(unit_value).strip()
            if unit_value is not None and not unit:
                raise ValueError("research metric units must not be empty")
            encoded_metrics.append(
                (run_id, name, canonical_json(dict(dimensions)), value, unit)
            )
        if len({(row[1], row[2]) for row in encoded_metrics}) != len(encoded_metrics):
            raise ValueError("research metrics must have unique name/dimensions pairs")
        timestamp = _utc_timestamp(completed_at or datetime.now(UTC))
        with self._con:
            row = self._con.execute(
                "SELECT status FROM research_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown research run {run_id!r}")
            if row["status"] != "running":
                raise ValueError(f"research run {run_id!r} is already {row['status']}")
            self._con.executemany(
                """INSERT INTO research_metrics
                       (run_id, metric_name, dimensions_json, value, unit)
                   VALUES (?, ?, ?, ?, ?)""",
                encoded_metrics,
            )
            updated = self._con.execute(
                """UPDATE research_runs
                   SET status = 'succeeded', completed_at = ?,
                       input_fingerprint = ?, observation_path = ?,
                       manifest_path = ?, observation_count = ?
                   WHERE run_id = ? AND status = 'running'""",
                (
                    timestamp,
                    input_fingerprint,
                    observation_path,
                    manifest_path,
                    observation_count,
                    run_id,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("research run changed while publishing")

    def fail_research_run(
        self,
        run_id: str,
        error_summary: str,
        *,
        completed_at: datetime | None = None,
    ) -> None:
        """Mark one running execution failed without attaching artifacts."""
        run_id = _research_run_id(run_id)
        error_summary = _bounded_utf8(error_summary.strip(), 4096)
        if not error_summary:
            error_summary = "research execution failed without a diagnostic"
        timestamp = _utc_timestamp(completed_at or datetime.now(UTC))
        with self._con:
            updated = self._con.execute(
                """UPDATE research_runs
                   SET status = 'failed', completed_at = ?, error_summary = ?
                   WHERE run_id = ? AND status = 'running'""",
                (timestamp, error_summary, run_id),
            )
            if updated.rowcount != 1:
                row = self.research_run(run_id)
                if row is None:
                    raise ValueError(f"unknown research run {run_id!r}")
                raise ValueError(f"research run {run_id!r} is already {row['status']}")

    def research_run(self, run_id: str) -> sqlite3.Row | None:
        run_id = _research_run_id(run_id)
        return self._con.execute(
            "SELECT * FROM research_runs WHERE run_id = ?", (run_id,)
        ).fetchone()

    def research_runs(self) -> list[sqlite3.Row]:
        return self._con.execute(
            "SELECT * FROM research_runs ORDER BY started_at, run_id"
        ).fetchall()

    def research_parameters(self, run_id: str) -> dict[str, Any]:
        run_id = _research_run_id(run_id)
        rows = self._con.execute(
            """SELECT name, value_json FROM research_parameters
               WHERE run_id = ? ORDER BY name""",
            (run_id,),
        ).fetchall()
        return {str(row["name"]): json.loads(row["value_json"]) for row in rows}

    def research_metrics(self, run_id: str) -> list[sqlite3.Row]:
        run_id = _research_run_id(run_id)
        return self._con.execute(
            """SELECT metric_name, dimensions_json, value, unit
               FROM research_metrics WHERE run_id = ?
               ORDER BY metric_name, dimensions_json""",
            (run_id,),
        ).fetchall()

    def select_research_artifacts(self, run_ids: Collection[str]) -> list[sqlite3.Row]:
        """Resolve explicit compatible succeeded runs without directory globs."""
        selected = tuple(dict.fromkeys(_research_run_id(run_id) for run_id in run_ids))
        if not selected:
            raise ValueError("at least one run_id is required")
        rows = _select_in_chunks(
            self._con,
            "SELECT * FROM research_runs WHERE run_id IN ({placeholders})",
            selected,
        )
        by_id = {str(row["run_id"]): row for row in rows}
        missing = sorted(set(selected) - by_id.keys())
        if missing:
            raise ValueError(f"unknown research run_ids: {missing}")
        ordered = [by_id[run_id] for run_id in selected]
        unsuccessful = [
            str(row["run_id"]) for row in ordered if row["status"] != "succeeded"
        ]
        if unsuccessful:
            raise ValueError(f"research runs are not succeeded: {unsuccessful}")
        versions = {
            (str(row["study_name"]), int(row["study_schema_version"]))
            for row in ordered
        }
        if len(versions) != 1:
            raise ValueError(
                "selected research runs must have one study and schema version"
            )
        return ordered

    # ---- durable backfill program --------------------------------------

    def create_backfill_program(
        self,
        *,
        program_id: str,
        definition_hash: str,
        components: list[Mapping[str, Any]],
    ) -> None:
        """Create one immutable ordered phase definition.

        Component progress and frozen cohorts are mutable operational state,
        but the ordered phase/dataset/range/job declaration is not. This keeps
        a later deployment from silently changing what a predecessor meant.
        """
        normalized_id = program_id.strip()
        if not normalized_id or len(normalized_id) > 128:
            raise ValueError("backfill program_id must contain 1..128 characters")
        if len(definition_hash) != 64:
            raise ValueError("backfill program definition hash must be SHA-256")
        if not components:
            raise ValueError("backfill program must declare at least one component")
        existing = self.backfill_program(normalized_id)
        if existing is not None:
            if str(existing["definition_hash"]) != definition_hash:
                raise ValueError(
                    f"backfill program {normalized_id!r} already has a different "
                    "definition"
                )
            return
        ordinals: set[int] = set()
        keys: set[str] = set()
        jobs: set[str] = set()
        rows: list[tuple[Any, ...]] = []
        now = _now()
        for component in components:
            key = str(component["component_key"]).strip()
            ordinal = int(component["component_ordinal"])
            phase = int(component["phase"])
            dataset_key = require_dataset_key(str(component["dataset_key"]))
            scope_key = str(component["scope_key"]).strip()
            start = component["start"]
            end = component["end"]
            job_id = str(component["job_id"]).strip()
            if not isinstance(start, date) or not isinstance(end, date):
                raise ValueError("backfill program ranges must be dates")
            if start > end:
                raise ValueError("backfill program start must not be after end")
            if phase not in {1, 2, 3}:
                raise ValueError("backfill program phase must be 1, 2, or 3")
            if not key or not scope_key or not job_id or len(job_id) > 128:
                raise ValueError("backfill program component fields must not be blank")
            if key in keys or ordinal in ordinals or job_id in jobs:
                raise ValueError("backfill program component keys must be unique")
            keys.add(key)
            ordinals.add(ordinal)
            jobs.add(job_id)
            rows.append(
                (
                    normalized_id,
                    key,
                    ordinal,
                    phase,
                    dataset_key,
                    scope_key,
                    start.isoformat(),
                    end.isoformat(),
                    job_id,
                    now,
                )
            )
        with self._con:
            self._con.execute(
                """INSERT INTO backfill_programs
                       (program_id, definition_hash, status, created_at, updated_at)
                   VALUES (?, ?, 'active', ?, ?)""",
                (normalized_id, definition_hash, now, now),
            )
            self._con.executemany(
                """INSERT INTO backfill_program_components
                       (program_id, component_key, component_ordinal, phase,
                        dataset_key, scope_key, range_start, range_end, job_id,
                        updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def backfill_program(self, program_id: str) -> sqlite3.Row | None:
        return self._con.execute(
            "SELECT * FROM backfill_programs WHERE program_id = ?",
            (program_id.strip(),),
        ).fetchone()

    def backfill_programs(self) -> list[sqlite3.Row]:
        return self._con.execute(
            "SELECT * FROM backfill_programs ORDER BY created_at, program_id"
        ).fetchall()

    def backfill_program_components(self, program_id: str) -> list[sqlite3.Row]:
        return self._con.execute(
            """SELECT * FROM backfill_program_components
               WHERE program_id = ? ORDER BY component_ordinal""",
            (program_id.strip(),),
        ).fetchall()

    def backfill_program_component(
        self, program_id: str, component_key: str
    ) -> sqlite3.Row | None:
        return self._con.execute(
            """SELECT * FROM backfill_program_components
               WHERE program_id = ? AND component_key = ?""",
            (program_id.strip(), component_key.strip()),
        ).fetchone()

    def backfill_program_component_for_job(self, job_id: str) -> sqlite3.Row | None:
        return self._con.execute(
            """SELECT * FROM backfill_program_components WHERE job_id = ?""",
            (job_id,),
        ).fetchone()

    def freeze_backfill_program_scope(
        self,
        *,
        program_id: str,
        scope_key: str,
        source_kind: str,
        tickers: Collection[str],
        supported_records: Collection[Mapping[str, Any]] = (),
    ) -> sqlite3.Row:
        """Persist the first cohort snapshot and leave it immutable on reruns."""
        existing = self.backfill_program_scope(program_id, scope_key)
        if existing is not None:
            return existing
        if self.backfill_program(program_id) is None:
            raise ValueError(f"unknown backfill program {program_id!r}")
        if source_kind not in {"seed_universes", "tiingo_supported_us"}:
            raise ValueError(f"invalid backfill scope source {source_kind!r}")
        normalized_tickers = sorted(
            {ticker.strip().upper() for ticker in tickers if ticker.strip()}
        )
        if not normalized_tickers:
            raise ValueError("backfill program scope must not be empty")
        normalized_ticker_set = set(normalized_tickers)
        records = sorted(
            (
                {
                    "ticker": str(row.get("ticker") or "").strip().upper(),
                    "exchange": str(row.get("exchange") or "").strip(),
                    "assetType": str(row.get("assetType") or "").strip(),
                    "priceCurrency": str(row.get("priceCurrency") or "").strip(),
                    "startDate": str(row.get("startDate") or "").strip(),
                    "endDate": str(row.get("endDate") or "").strip(),
                }
                for row in supported_records
            ),
            key=lambda row: (
                row["ticker"],
                row["startDate"],
                row["endDate"],
                row["exchange"],
                row["assetType"],
                row["priceCurrency"],
            ),
        )
        if source_kind == "tiingo_supported_us":
            if not records or any(
                not row["ticker"]
                or not row["startDate"]
                or not row["endDate"]
                or row["ticker"] not in normalized_ticker_set
                for row in records
            ):
                raise ValueError(
                    "supported-ticker scope requires valid archive records"
                )
        elif records:
            raise ValueError("seed-universe scope must not contain supported records")
        snapshot = {"tickers": normalized_tickers, "supported_records": records}
        cohort_hash = hashlib.sha256(canonical_json(snapshot).encode()).hexdigest()
        now = _now()
        with self._con:
            self._con.execute(
                """INSERT INTO backfill_program_scopes
                       (program_id, scope_key, source_kind, cohort_hash,
                        ticker_count, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    program_id,
                    scope_key,
                    source_kind,
                    cohort_hash,
                    len(normalized_tickers),
                    now,
                ),
            )
            self._con.executemany(
                """INSERT INTO backfill_program_tickers
                       (program_id, scope_key, ticker_ordinal, ticker)
                   VALUES (?, ?, ?, ?)""",
                [
                    (program_id, scope_key, ordinal, ticker)
                    for ordinal, ticker in enumerate(normalized_tickers)
                ],
            )
            self._con.executemany(
                """INSERT INTO backfill_program_supported_records
                       (program_id, scope_key, record_ordinal, ticker, exchange,
                        asset_type, price_currency, start_date, end_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        program_id,
                        scope_key,
                        ordinal,
                        row["ticker"],
                        row["exchange"],
                        row["assetType"],
                        row["priceCurrency"],
                        row["startDate"],
                        row["endDate"],
                    )
                    for ordinal, row in enumerate(records)
                ],
            )
        scope = self.backfill_program_scope(program_id, scope_key)
        assert scope is not None
        return scope

    def backfill_program_scope(
        self, program_id: str, scope_key: str
    ) -> sqlite3.Row | None:
        return self._con.execute(
            """SELECT * FROM backfill_program_scopes
               WHERE program_id = ? AND scope_key = ?""",
            (program_id.strip(), scope_key.strip()),
        ).fetchone()

    def backfill_program_tickers(self, program_id: str, scope_key: str) -> list[str]:
        rows = self._con.execute(
            """SELECT ticker FROM backfill_program_tickers
               WHERE program_id = ? AND scope_key = ?
               ORDER BY ticker_ordinal""",
            (program_id.strip(), scope_key.strip()),
        ).fetchall()
        return [str(row["ticker"]) for row in rows]

    def backfill_program_supported_records(
        self,
        program_id: str,
        scope_key: str,
        tickers: Collection[str] | None = None,
    ) -> list[dict[str, str]]:
        parameters: list[Any] = [program_id.strip(), scope_key.strip()]
        predicate = ""
        if tickers is not None:
            normalized = sorted(
                {ticker.strip().upper() for ticker in tickers if ticker.strip()}
            )
            if not normalized:
                return []
            placeholders = ",".join("?" for _ in normalized)
            predicate = f" AND ticker IN ({placeholders})"
            parameters.extend(normalized)
        rows = self._con.execute(
            """SELECT ticker, exchange, asset_type, price_currency,
                      start_date, end_date
                 FROM backfill_program_supported_records
                WHERE program_id = ? AND scope_key = ?"""
            + predicate
            + " ORDER BY record_ordinal",
            parameters,
        ).fetchall()
        return [
            {
                "ticker": str(row["ticker"]),
                "exchange": str(row["exchange"]),
                "assetType": str(row["asset_type"]),
                "priceCurrency": str(row["price_currency"]),
                "startDate": str(row["start_date"]),
                "endDate": str(row["end_date"]),
            }
            for row in rows
        ]

    def advance_backfill_program_identity(
        self,
        *,
        program_id: str,
        component_key: str,
        cursor: int,
        prepared: bool,
        stop_reason: str | None,
    ) -> None:
        component = self.backfill_program_component(program_id, component_key)
        if component is None:
            raise ValueError(
                f"unknown backfill program component {program_id!r}/{component_key!r}"
            )
        scope = self.backfill_program_scope(program_id, str(component["scope_key"]))
        if scope is None:
            raise ValueError("backfill program component has no frozen scope")
        if cursor < int(component["identity_cursor"]) or cursor > int(
            scope["ticker_count"]
        ):
            raise ValueError("backfill identity cursor must advance within its scope")
        if prepared and cursor != int(scope["ticker_count"]):
            raise ValueError("prepared identity must cover the complete frozen scope")
        with self._con:
            self._con.execute(
                """UPDATE backfill_program_components
                   SET identity_status = ?, identity_cursor = ?, state = ?,
                       last_stop_reason = ?, updated_at = ?
                   WHERE program_id = ? AND component_key = ?""",
                (
                    "prepared" if prepared else "pending",
                    cursor,
                    "pending" if prepared else "preparing",
                    stop_reason,
                    _now(),
                    program_id,
                    component_key,
                ),
            )

    def set_backfill_program_component_state(
        self,
        *,
        program_id: str,
        component_key: str,
        state: str,
        stop_reason: str | None = None,
    ) -> None:
        if state not in {"pending", "preparing", "active", "complete", "blocked"}:
            raise ValueError(f"invalid backfill component state {state!r}")
        with self._con:
            changed = self._con.execute(
                """UPDATE backfill_program_components
                   SET state = ?, last_stop_reason = ?, updated_at = ?
                   WHERE program_id = ? AND component_key = ?""",
                (state, stop_reason, _now(), program_id, component_key),
            ).rowcount
        if changed != 1:
            raise ValueError(
                f"unknown backfill program component {program_id!r}/{component_key!r}"
            )

    def set_backfill_program_status(self, program_id: str, status: str) -> None:
        if status not in {"active", "complete", "complete_with_exclusions"}:
            raise ValueError(f"invalid backfill program status {status!r}")
        with self._con:
            changed = self._con.execute(
                """UPDATE backfill_programs SET status = ?, updated_at = ?
                   WHERE program_id = ?""",
                (status, _now(), program_id),
            ).rowcount
        if changed != 1:
            raise ValueError(f"unknown backfill program {program_id!r}")

    def backfill_program_prerequisite_stop_reason(
        self,
        job_id: str,
        phase: int,
        *,
        request_hash: str | None = None,
    ) -> str | None:
        """Return a fail-closed program gate for phase-2/3 history jobs."""
        if phase == 1:
            return None
        if phase not in {2, 3}:
            raise ValueError("history phase must be 1, 2, or 3")
        component = self.backfill_program_component_for_job(job_id)
        if component is None or int(component["phase"]) != phase:
            return "phase_program_required"
        scope = self.backfill_program_scope(
            str(component["program_id"]), str(component["scope_key"])
        )
        if scope is None:
            return "phase_scope_not_frozen"
        if str(component["identity_status"]) != "prepared":
            return "phase_identity_not_prepared"
        rows = self.backfill_program_components(str(component["program_id"]))
        lower = [row for row in rows if int(row["phase"]) < phase]
        if not lower:
            return "phase_predecessor_missing"
        if any(str(row["state"]) not in {"complete", "blocked"} for row in lower):
            return "phase_predecessor_active"
        expected_payload = {
            "dataset_key": str(component["dataset_key"]),
            "tickers": self.backfill_program_tickers(
                str(component["program_id"]), str(component["scope_key"])
            ),
            "start": str(component["range_start"]),
            "end": str(component["range_end"]),
            "phase": phase,
            "force": False,
        }
        expected_hash = hashlib.sha256(
            canonical_json(expected_payload).encode()
        ).hexdigest()
        if request_hash is not None and request_hash != expected_hash:
            return "phase_program_request_mismatch"
        job = self.history_job(job_id)
        if job is not None:
            actual = (
                int(job["phase"]),
                str(job["dataset_key"]),
                str(job["range_start"]),
                str(job["range_end"]),
                str(job["request_hash"]),
                bool(job["force"]),
            )
            expected = (
                phase,
                str(component["dataset_key"]),
                str(component["range_start"]),
                str(component["range_end"]),
                expected_hash,
                False,
            )
            if actual != expected:
                return "phase_program_request_mismatch"
        return None

    # ---- durable request accounting ------------------------------------

    def reserve_request_attempt(
        self,
        *,
        now: datetime,
        work_kind: str,
        operation: str,
        reserved_bytes: int,
        hourly_request_limit: int,
        daily_request_limit: int,
        total_byte_limit: int,
        historical_total_byte_limit: int,
        billing_month_start: datetime,
    ) -> tuple[int | None, str | None]:
        """Atomically reserve one request or return its quota stop reason.

        Historical admission compares total charged usage with the caller's
        date-dependent ceiling; current attempts use only the total hard cap.
        Incomplete attempts retain ``reserved_bytes`` in the charged total.
        This intentionally overcounts an interrupted transfer rather than
        permitting a process crash to reopen budget that may have been spent.
        """
        if work_kind not in {"current", "historical"}:
            raise ValueError(f"invalid work_kind {work_kind!r}")
        if not operation or len(operation) > 512:
            raise ValueError("request operation must contain 1..512 characters")
        limits = (
            hourly_request_limit,
            daily_request_limit,
            total_byte_limit,
            historical_total_byte_limit,
        )
        if any(value <= 0 for value in limits) or reserved_bytes < 0:
            raise ValueError("request accounting limits must be positive")
        if billing_month_start.tzinfo is None:
            raise ValueError("billing month start must be timezone-aware")
        if billing_month_start > now:
            raise ValueError("billing month start must not be after now")
        timestamp = _utc_timestamp(now)
        try:
            self._con.execute("BEGIN IMMEDIATE")
            usage = self._request_window_usage(
                now=now, billing_month_start=billing_month_start
            )
            reason = None
            if usage["hourly_requests"] >= hourly_request_limit:
                reason = "hourly_request_limit"
            elif usage["daily_requests"] >= daily_request_limit:
                reason = "daily_request_limit"
            elif usage["charged_bytes"] + reserved_bytes > total_byte_limit:
                reason = "monthly_total_byte_limit"
            elif (
                work_kind == "historical"
                and usage["charged_bytes"] + reserved_bytes
                > historical_total_byte_limit
            ):
                reason = "monthly_historical_byte_limit"
            if reason is not None:
                self._con.commit()
                return None, reason
            cursor = self._con.execute(
                """INSERT INTO api_request_attempts
                       (occurred_at, work_kind, operation, reserved_bytes)
                   VALUES (?, ?, ?, ?)""",
                (timestamp, work_kind, operation, reserved_bytes),
            )
            attempt_id = int(cursor.lastrowid)
            self._con.commit()
            return attempt_id, None
        except BaseException:
            self._con.rollback()
            raise

    def settle_request_attempt(
        self,
        attempt_id: int,
        observed_bytes: int,
        *,
        complete: bool,
        bytes_known: bool | None = None,
    ) -> None:
        if observed_bytes < 0:
            raise ValueError("observed response bytes must not be negative")
        if bytes_known is None:
            bytes_known = complete
        with self._con:
            cursor = self._con.execute(
                """UPDATE api_request_attempts
                   SET observed_bytes = ?, settled = 1, complete = ?, bytes_known = ?
                   WHERE attempt_id = ? AND settled = 0""",
                (observed_bytes, int(complete), int(bytes_known), attempt_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"request attempt {attempt_id} is missing or already settled"
                )

    def can_start_request_batch(
        self,
        *,
        now: datetime,
        work_kind: str,
        attempts: int,
        reserved_bytes: int,
        hourly_request_limit: int,
        daily_request_limit: int,
        total_byte_limit: int,
        historical_total_byte_limit: int,
        billing_month_start: datetime,
    ) -> bool:
        """Conservative preflight used only to retain normal bucket batching.

        The historical ceiling applies to total charged usage just as it does
        for the authoritative per-attempt reservation below.
        Individual attempts still reserve atomically. A false result falls
        back to one target, where an exact quota stop can retain its cursor.
        """
        if attempts <= 0:
            raise ValueError("batch attempt allowance must be positive")
        usage = self._request_window_usage(
            now=now, billing_month_start=billing_month_start
        )
        reservation = attempts * reserved_bytes
        return (
            usage["hourly_requests"] + attempts <= hourly_request_limit
            and usage["daily_requests"] + attempts <= daily_request_limit
            and usage["charged_bytes"] + reservation <= total_byte_limit
            and (
                work_kind != "historical"
                or usage["charged_bytes"] + reservation <= historical_total_byte_limit
            )
        )

    def request_usage(self, *, now: datetime) -> dict[str, int]:
        usage = self._request_window_usage(
            now=now, billing_month_start=tiingo_billing_month_start(now)
        )
        return {
            key: usage[key]
            for key in (
                "requests",
                "observed_bytes",
                "charged_bytes",
                "incomplete_attempts",
            )
        }

    def _request_window_usage(
        self, *, now: datetime, billing_month_start: datetime
    ) -> dict[str, int]:
        """Return one view of rolling request and billing-month byte usage."""
        hour_start = _utc_timestamp(now - timedelta(hours=1))
        day_start = _utc_timestamp(now - timedelta(days=1))
        period_start = _utc_timestamp(billing_month_start)
        row = self._con.execute(
            """WITH windowed AS (
                   SELECT *,
                          CASE WHEN settled = 1 AND bytes_known = 1
                               THEN observed_bytes
                               ELSE MAX(reserved_bytes, observed_bytes)
                          END AS charged_bytes
                   FROM api_request_attempts
                   WHERE occurred_at >= ?
               )
               SELECT COALESCE(SUM(CASE WHEN occurred_at > ? THEN 1 ELSE 0 END), 0)
                          AS hourly_requests,
                      COALESCE(SUM(CASE WHEN occurred_at > ? THEN 1 ELSE 0 END), 0)
                          AS daily_requests,
                      COUNT(*) AS requests,
                      COALESCE(SUM(observed_bytes), 0) AS observed_bytes,
                      COALESCE(SUM(charged_bytes), 0) AS charged_bytes,
                      COALESCE(SUM(CASE WHEN complete = 0 THEN 1 ELSE 0 END), 0)
                           AS incomplete_attempts
               FROM windowed""",
            (period_start, hour_start, day_start),
        ).fetchone()
        return {key: int(row[key]) for key in row.keys()}

    def request_attempts(self) -> list[sqlite3.Row]:
        return self._con.execute(
            """SELECT * FROM api_request_attempts ORDER BY attempt_id"""
        ).fetchall()

    # ---- durable ongoing collection -----------------------------------

    def create_ongoing_program(
        self,
        *,
        program_id: str,
        definition_hash: str,
        initial_session: date,
        cohort_size: int,
        lookback_sessions: int,
        min_observations: int,
    ) -> None:
        """Create one immutable ongoing-collection definition."""
        normalized_id = program_id.strip()
        if not normalized_id or len(normalized_id) > 128:
            raise ValueError("ongoing program_id must contain 1..128 characters")
        if len(definition_hash) != 64:
            raise ValueError("ongoing program definition hash must be SHA-256")
        if cohort_size <= 0 or lookback_sessions <= 0 or min_observations <= 0:
            raise ValueError("ongoing cohort parameters must be positive")
        if min_observations > lookback_sessions:
            raise ValueError("minimum observations cannot exceed the lookback")
        definition = (
            definition_hash,
            initial_session.isoformat(),
            cohort_size,
            lookback_sessions,
            min_observations,
        )
        existing = self.ongoing_program(normalized_id)
        if existing is not None:
            actual = tuple(
                existing[key]
                for key in (
                    "definition_hash",
                    "initial_session",
                    "cohort_size",
                    "lookback_sessions",
                    "min_observations",
                )
            )
            if actual != definition:
                raise ValueError(
                    f"ongoing program {normalized_id!r} already has a different "
                    "definition"
                )
            return
        now = _now()
        with self._con:
            self._con.execute(
                """INSERT INTO ongoing_programs
                       (program_id, definition_hash, initial_session, cohort_size,
                        lookback_sessions, min_observations, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (normalized_id, *definition, now, now),
            )

    def ongoing_program(self, program_id: str) -> sqlite3.Row | None:
        return self._con.execute(
            "SELECT * FROM ongoing_programs WHERE program_id = ?",
            (program_id.strip(),),
        ).fetchone()

    def create_ongoing_supported_snapshot(
        self,
        *,
        as_of_session: date,
        records: Collection[Mapping[str, Any]],
    ) -> sqlite3.Row:
        """Persist one content-addressed active supported-list snapshot."""
        normalized = sorted(
            (
                {
                    "ticker": str(row.get("ticker") or "").strip().upper(),
                    "exchange": str(row.get("exchange") or "").strip(),
                    "assetType": str(row.get("assetType") or "").strip(),
                    "priceCurrency": str(row.get("priceCurrency") or "").strip(),
                    "startDate": str(row.get("startDate") or "").strip(),
                    "endDate": str(row.get("endDate") or "").strip(),
                }
                for row in records
            ),
            key=lambda row: (
                row["ticker"],
                row["startDate"],
                row["endDate"],
                row["exchange"],
                row["assetType"],
                row["priceCurrency"],
            ),
        )
        if not normalized:
            raise ValueError("ongoing supported snapshot must not be empty")
        if any(
            not row["ticker"]
            or not row["startDate"]
            or not row["endDate"]
            or row["startDate"] > row["endDate"]
            for row in normalized
        ):
            raise ValueError("ongoing supported snapshot contains invalid records")
        snapshot_id = hashlib.sha256(canonical_json(normalized).encode()).hexdigest()
        existing = self.ongoing_supported_snapshot(snapshot_id)
        if existing is not None:
            return existing
        tickers = {row["ticker"] for row in normalized}
        now = _now()
        with self._con:
            self._con.execute(
                """INSERT INTO ongoing_supported_snapshots
                       (snapshot_id, as_of_session, ticker_count, record_count,
                        created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    snapshot_id,
                    as_of_session.isoformat(),
                    len(tickers),
                    len(normalized),
                    now,
                ),
            )
            self._con.executemany(
                """INSERT INTO ongoing_supported_records
                       (snapshot_id, record_ordinal, ticker, exchange, asset_type,
                        price_currency, start_date, end_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        snapshot_id,
                        ordinal,
                        row["ticker"],
                        row["exchange"],
                        row["assetType"],
                        row["priceCurrency"],
                        row["startDate"],
                        row["endDate"],
                    )
                    for ordinal, row in enumerate(normalized)
                ],
            )
        snapshot = self.ongoing_supported_snapshot(snapshot_id)
        assert snapshot is not None
        return snapshot

    def ongoing_supported_snapshot(self, snapshot_id: str) -> sqlite3.Row | None:
        return self._con.execute(
            "SELECT * FROM ongoing_supported_snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()

    def ongoing_supported_tickers(self, snapshot_id: str) -> list[str]:
        rows = self._con.execute(
            """SELECT DISTINCT ticker FROM ongoing_supported_records
               WHERE snapshot_id = ? ORDER BY ticker""",
            (snapshot_id,),
        ).fetchall()
        return [str(row["ticker"]) for row in rows]

    def ongoing_supported_records(
        self, snapshot_id: str, tickers: Collection[str] | None = None
    ) -> list[dict[str, str]]:
        parameters: list[Any] = [snapshot_id]
        predicate = ""
        if tickers is not None:
            normalized = sorted(
                {ticker.strip().upper() for ticker in tickers if ticker.strip()}
            )
            if not normalized:
                return []
            predicate = f" AND ticker IN ({','.join('?' for _ in normalized)})"
            parameters.extend(normalized)
        rows = self._con.execute(
            """SELECT ticker, exchange, asset_type, price_currency,
                      start_date, end_date
                 FROM ongoing_supported_records WHERE snapshot_id = ?"""
            + predicate
            + " ORDER BY record_ordinal",
            parameters,
        ).fetchall()
        return [
            {
                "ticker": str(row["ticker"]),
                "exchange": str(row["exchange"]),
                "assetType": str(row["asset_type"]),
                "priceCurrency": str(row["price_currency"]),
                "startDate": str(row["start_date"]),
                "endDate": str(row["end_date"]),
            }
            for row in rows
        ]

    def create_ongoing_cycle(
        self,
        *,
        program_id: str,
        session_date: date,
        supported_snapshot_id: str,
        eod_job_id: str,
        hourly_job_id: str,
        five_min_job_id: str,
    ) -> sqlite3.Row:
        existing = self.ongoing_cycle(program_id, session_date)
        if existing is not None:
            expected = (
                supported_snapshot_id,
                eod_job_id,
                hourly_job_id,
                five_min_job_id,
            )
            actual = tuple(
                existing[key]
                for key in (
                    "supported_snapshot_id",
                    "eod_job_id",
                    "hourly_job_id",
                    "five_min_job_id",
                )
            )
            if actual != expected:
                raise ValueError("ongoing cycle already has a different definition")
            return existing
        if self.ongoing_program(program_id) is None:
            raise ValueError(f"unknown ongoing program {program_id!r}")
        if self.ongoing_supported_snapshot(supported_snapshot_id) is None:
            raise ValueError("unknown ongoing supported snapshot")
        now = _now()
        with self._con:
            self._con.execute(
                """INSERT INTO ongoing_cycles
                       (program_id, session_date, supported_snapshot_id,
                        eod_job_id, hourly_job_id, five_min_job_id,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    program_id,
                    session_date.isoformat(),
                    supported_snapshot_id,
                    eod_job_id,
                    hourly_job_id,
                    five_min_job_id,
                    now,
                    now,
                ),
            )
        cycle = self.ongoing_cycle(program_id, session_date)
        assert cycle is not None
        return cycle

    def ongoing_cycle(self, program_id: str, session_date: date) -> sqlite3.Row | None:
        return self._con.execute(
            """SELECT * FROM ongoing_cycles
               WHERE program_id = ? AND session_date = ?""",
            (program_id.strip(), session_date.isoformat()),
        ).fetchone()

    def ongoing_cycles(self, program_id: str) -> list[sqlite3.Row]:
        return self._con.execute(
            """SELECT * FROM ongoing_cycles WHERE program_id = ?
               ORDER BY session_date""",
            (program_id.strip(),),
        ).fetchall()

    def latest_ongoing_cycle(self, program_id: str) -> sqlite3.Row | None:
        return self._con.execute(
            """SELECT * FROM ongoing_cycles WHERE program_id = ?
               ORDER BY session_date DESC LIMIT 1""",
            (program_id.strip(),),
        ).fetchone()

    def update_ongoing_cycle(
        self, program_id: str, session_date: date, **changes: Any
    ) -> None:
        """Update bounded mutable cycle state using a fixed column allow-list."""
        allowed = {
            "state",
            "eod_identity_cursor",
            "cohort_snapshot_id",
            "hourly_identity_cursor",
            "five_min_identity_cursor",
            "last_stop_reason",
        }
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError(f"invalid ongoing cycle fields: {sorted(unknown)}")
        if not changes:
            return
        if "state" in changes and changes["state"] not in {
            "eod_identity",
            "eod",
            "cohort",
            "hourly_identity",
            "hourly",
            "five_min_identity",
            "five_min",
            "complete",
            "complete_with_exclusions",
        }:
            raise ValueError(f"invalid ongoing cycle state {changes['state']!r}")
        for name in (
            "eod_identity_cursor",
            "hourly_identity_cursor",
            "five_min_identity_cursor",
        ):
            if name in changes and int(changes[name]) < 0:
                raise ValueError("ongoing identity cursor must not be negative")
        assignments = [f"{name} = ?" for name in changes]
        values = list(changes.values())
        assignments.append("updated_at = ?")
        values.append(_now())
        values.extend((program_id.strip(), session_date.isoformat()))
        with self._con:
            changed = self._con.execute(
                f"UPDATE ongoing_cycles SET {', '.join(assignments)} "
                "WHERE program_id = ? AND session_date = ?",
                values,
            ).rowcount
        if changed != 1:
            raise ValueError(f"unknown ongoing cycle {program_id!r}/{session_date}")

    def create_ongoing_cohort_snapshot(
        self,
        *,
        program_id: str,
        as_of_session: date,
        lookback_start: date,
        lookback_end: date,
        cohort_size: int,
        min_observations: int,
        members: Sequence[Mapping[str, Any]],
    ) -> sqlite3.Row:
        if not members:
            raise ValueError("ongoing intraday cohort must not be empty")
        normalized = [
            {
                "rank": int(row["rank"]),
                "instrument_id": str(row["instrument_id"]),
                "ticker": str(row["ticker"]).strip().upper(),
                "avg_dollar_volume": float(row["avg_dollar_volume"]),
                "observation_count": int(row["observation_count"]),
            }
            for row in members
        ]
        if [row["rank"] for row in normalized] != list(range(1, len(normalized) + 1)):
            raise ValueError("ongoing cohort ranks must be contiguous from one")
        if len({row["instrument_id"] for row in normalized}) != len(normalized):
            raise ValueError("ongoing cohort instrument ids must be unique")
        cohort_hash = hashlib.sha256(canonical_json(normalized).encode()).hexdigest()
        snapshot_id = f"{program_id.strip()}-{as_of_session:%Y%m%d}"
        existing = self.ongoing_cohort_snapshot(snapshot_id)
        if existing is not None:
            if str(existing["cohort_hash"]) != cohort_hash:
                raise ValueError("ongoing cohort snapshot is immutable")
            return existing
        now = _now()
        with self._con:
            self._con.execute(
                """INSERT INTO ongoing_cohort_snapshots
                       (snapshot_id, program_id, as_of_session, lookback_start,
                        lookback_end, cohort_size, min_observations, member_count,
                        cohort_hash, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    snapshot_id,
                    program_id,
                    as_of_session.isoformat(),
                    lookback_start.isoformat(),
                    lookback_end.isoformat(),
                    cohort_size,
                    min_observations,
                    len(normalized),
                    cohort_hash,
                    now,
                ),
            )
            self._con.executemany(
                """INSERT INTO ongoing_cohort_members
                       (snapshot_id, rank, instrument_id, ticker,
                        avg_dollar_volume, observation_count)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (
                        snapshot_id,
                        row["rank"],
                        row["instrument_id"],
                        row["ticker"],
                        row["avg_dollar_volume"],
                        row["observation_count"],
                    )
                    for row in normalized
                ],
            )
        snapshot = self.ongoing_cohort_snapshot(snapshot_id)
        assert snapshot is not None
        return snapshot

    def ongoing_cohort_snapshot(self, snapshot_id: str) -> sqlite3.Row | None:
        return self._con.execute(
            "SELECT * FROM ongoing_cohort_snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()

    def latest_ongoing_cohort_snapshot(
        self, program_id: str, *, through: date | None = None
    ) -> sqlite3.Row | None:
        predicate = ""
        parameters: list[Any] = [program_id.strip()]
        if through is not None:
            predicate = " AND as_of_session <= ?"
            parameters.append(through.isoformat())
        return self._con.execute(
            """SELECT * FROM ongoing_cohort_snapshots
               WHERE program_id = ?"""
            + predicate
            + " ORDER BY as_of_session DESC LIMIT 1",
            parameters,
        ).fetchone()

    def ongoing_cohort_members(self, snapshot_id: str) -> list[sqlite3.Row]:
        return self._con.execute(
            """SELECT * FROM ongoing_cohort_members WHERE snapshot_id = ?
               ORDER BY rank""",
            (snapshot_id,),
        ).fetchall()

    # ---- durable breadth-first/current progress ------------------------

    def create_history_job(
        self,
        *,
        job_id: str,
        phase: int | None,
        dataset_key: str,
        start: date,
        end: date,
        request_hash: str,
        cohort_hash: str,
        force: bool,
        targets: list[dict[str, Any]],
        blocked_ranges: list[dict[str, Any]],
        work_kind: str = "historical",
        refresh_overlap_days: int = 0,
    ) -> None:
        """Create an immutable cohort snapshot; an identical job is a no-op."""
        if not job_id.strip() or len(job_id) > 128:
            raise ValueError("history job_id must contain 1..128 characters")
        dataset_key = require_dataset_key(dataset_key)
        if start > end:
            raise ValueError("history job start must not be after end")
        if phase not in {None, 1, 2, 3}:
            raise ValueError("history phase must be 1, 2, 3, or None")
        if work_kind not in {"current", "historical"}:
            raise ValueError(f"invalid job work kind {work_kind!r}")
        if refresh_overlap_days < 0:
            raise ValueError("refresh overlap must not be negative")
        if work_kind == "historical" and refresh_overlap_days:
            raise ValueError("historical jobs cannot declare a refresh overlap")
        existing = self._con.execute(
            """SELECT phase, dataset_key, range_start, range_end, request_hash,
                      cohort_hash, force, work_kind, refresh_overlap_days
               FROM history_jobs WHERE job_id = ?""",
            (job_id,),
        ).fetchone()
        definition = (
            phase,
            dataset_key,
            start.isoformat(),
            end.isoformat(),
            request_hash,
            cohort_hash,
            int(force),
            work_kind,
            refresh_overlap_days,
        )
        if existing is not None:
            if tuple(existing) != definition:
                raise ValueError(
                    f"history job {job_id!r} already has a different definition"
                )
            return
        now = _now()
        status = "active" if targets else "blocked" if blocked_ranges else "complete"
        with self._con:
            self._con.execute(
                """INSERT INTO history_jobs
                       (job_id, phase, dataset_key, range_start, range_end,
                        request_hash, cohort_hash, force, work_kind,
                        refresh_overlap_days, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (job_id, *definition, status, now, now),
            )
            self._con.executemany(
                """INSERT INTO history_targets
                       (job_id, target_ordinal, instrument_id, updated_at)
                   VALUES (?, ?, ?, ?)""",
                [
                    (job_id, ordinal, target["instrument_id"], now)
                    for ordinal, target in enumerate(targets)
                ],
            )
            self._con.executemany(
                """INSERT INTO history_ranges
                       (job_id, target_ordinal, range_ordinal, ticker,
                        range_start, range_end, frontier_end, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        job_id,
                        target_ordinal,
                        range_ordinal,
                        item["ticker"],
                        item["start"].isoformat(),
                        item["end"].isoformat(),
                        item["end"].isoformat(),
                        now,
                    )
                    for target_ordinal, target in enumerate(targets)
                    for range_ordinal, item in enumerate(target["ranges"])
                ],
            )
            self._con.executemany(
                """INSERT INTO history_blocked_ranges
                       (job_id, blocked_ordinal, ticker, range_start, range_end,
                        status, detail)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        job_id,
                        ordinal,
                        item["ticker"],
                        item["start"].isoformat(),
                        item["end"].isoformat(),
                        item["status"],
                        item["detail"],
                    )
                    for ordinal, item in enumerate(blocked_ranges)
                ],
            )

    def history_job(self, job_id: str) -> sqlite3.Row | None:
        return self._con.execute(
            "SELECT * FROM history_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()

    def history_targets(self, job_id: str) -> list[sqlite3.Row]:
        return self._con.execute(
            """SELECT * FROM history_targets WHERE job_id = ?
               ORDER BY target_ordinal""",
            (job_id,),
        ).fetchall()

    def history_ranges(
        self, job_id: str, target_ordinal: int | None = None
    ) -> list[sqlite3.Row]:
        if target_ordinal is None:
            return self._con.execute(
                """SELECT * FROM history_ranges WHERE job_id = ?
                   ORDER BY target_ordinal, range_ordinal""",
                (job_id,),
            ).fetchall()
        return self._con.execute(
            """SELECT * FROM history_ranges
               WHERE job_id = ? AND target_ordinal = ?
               ORDER BY range_ordinal""",
            (job_id, target_ordinal),
        ).fetchall()

    def history_blocked_ranges(self, job_id: str) -> list[sqlite3.Row]:
        return self._con.execute(
            """SELECT * FROM history_blocked_ranges WHERE job_id = ?
               ORDER BY blocked_ordinal""",
            (job_id,),
        ).fetchall()

    def history_target_count(self, job_id: str) -> int:
        return int(
            self._con.execute(
                "SELECT COUNT(*) FROM history_targets WHERE job_id = ?", (job_id,)
            ).fetchone()[0]
        )

    def history_retry_pending_count(self, job_id: str) -> int:
        """Return current targets waiting for another bounded retry sweep."""
        return int(
            self._con.execute(
                """SELECT COUNT(*) FROM history_targets
                   WHERE job_id = ? AND last_attempt_status =
                       'current_retry_pending'""",
                (job_id,),
            ).fetchone()[0]
        )

    def history_has_only_retry_pending_work(self, job_id: str) -> bool:
        """Whether every active range belongs to a deferred current retry."""
        row = self._con.execute(
            """SELECT COUNT(*) AS active_count,
                      SUM(
                          CASE WHEN targets.last_attempt_status =
                                    'current_retry_pending'
                               THEN 0 ELSE 1 END
                      ) AS other_count
               FROM history_ranges AS ranges
               JOIN history_targets AS targets
                 ON targets.job_id = ranges.job_id
                 AND targets.target_ordinal = ranges.target_ordinal
               WHERE ranges.job_id = ? AND ranges.status = 'active'
                 AND ranges.terminal_blocked = 0""",
            (job_id,),
        ).fetchone()
        return bool(row["active_count"]) and not bool(row["other_count"])

    def history_has_active_range(
        self,
        job_id: str,
        *,
        excluding: tuple[int, int] | None = None,
    ) -> bool:
        if excluding is None:
            row = self._con.execute(
                """SELECT 1 FROM history_ranges
                   WHERE job_id = ? AND status = 'active'
                     AND terminal_blocked = 0 LIMIT 1""",
                (job_id,),
            ).fetchone()
        else:
            row = self._con.execute(
                """SELECT 1 FROM history_ranges
                   WHERE job_id = ? AND status = 'active'
                     AND terminal_blocked = 0
                     AND NOT (target_ordinal = ? AND range_ordinal = ?)
                   LIMIT 1""",
                (job_id, *excluding),
            ).fetchone()
        return row is not None

    def history_has_blockers(self, job_id: str) -> bool:
        row = self._con.execute(
            """SELECT 1 FROM history_blocked_ranges WHERE job_id = ? LIMIT 1""",
            (job_id,),
        ).fetchone()
        if row is not None:
            return True
        return (
            self._con.execute(
                """SELECT 1 FROM history_ranges
                   WHERE job_id = ? AND terminal_blocked = 1 LIMIT 1""",
                (job_id,),
            ).fetchone()
            is not None
        )

    def reactivate_history_job(self, job_id: str) -> bool:
        """Retry runtime-blocked ranges after explicit operator approval."""
        job = self.history_job(job_id)
        if job is None or job["cancelled"]:
            return False
        with self._con:
            changed = self._con.execute(
                """UPDATE history_ranges
                   SET terminal_blocked = 0, updated_at = ?
                   WHERE job_id = ? AND terminal_blocked = 1""",
                (_now(), job_id),
            ).rowcount
            if changed:
                self._con.execute(
                    """UPDATE history_jobs SET status = 'active', updated_at = ?
                       WHERE job_id = ? AND status = 'blocked'""",
                    (_now(), job_id),
                )
        return bool(changed)

    def cancel_history_job(self, job_id: str) -> None:
        """Terminally cancel a durable job without deleting its audit trail."""
        with self._con:
            cursor = self._con.execute(
                """UPDATE history_jobs
                   SET status = 'blocked', cancelled = 1, updated_at = ?
                   WHERE job_id = ? AND status IN ('active', 'blocked')
                     AND cancelled = 0""",
                (_now(), job_id),
            )
            if cursor.rowcount != 1:
                row = self.history_job(job_id)
                if row is None:
                    raise ValueError(f"unknown history job {job_id!r}")
                raise ValueError(f"history job {job_id!r} is already {row['status']}")

    def checkpoint_history_turn(
        self,
        *,
        job_id: str,
        target_ordinal: int,
        range_ordinal: int,
        frontier_end: date,
        range_status: str,
        attempt_status: str,
        detail: str,
        attempted: bool,
        successful: bool,
        terminal_blocked: bool,
        cursor: int,
        sweep: int,
        job_status: str,
    ) -> None:
        if range_status not in {"active", "complete"}:
            raise ValueError(f"invalid history range status {range_status!r}")
        if job_status not in {"active", "complete", "blocked"}:
            raise ValueError(f"invalid history job status {job_status!r}")
        now = _now()
        with self._con:
            updated = self._con.execute(
                """UPDATE history_ranges
                   SET frontier_end = ?, status = ?, terminal_blocked = ?, updated_at = ?
                   WHERE job_id = ? AND target_ordinal = ? AND range_ordinal = ?""",
                (
                    frontier_end.isoformat(),
                    range_status,
                    int(terminal_blocked),
                    now,
                    job_id,
                    target_ordinal,
                    range_ordinal,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("history range disappeared while checkpointing")
            self._con.execute(
                """UPDATE history_targets
                   SET successful_depth = successful_depth + ?,
                       attempted_turns = attempted_turns + ?,
                       last_attempt_status = ?, last_attempt_detail = ?,
                       updated_at = ?
                   WHERE job_id = ? AND target_ordinal = ?""",
                (
                    int(successful),
                    int(attempted),
                    attempt_status,
                    detail[:1000],
                    now,
                    job_id,
                    target_ordinal,
                ),
            )
            self._con.execute(
                """UPDATE history_jobs
                   SET cursor = ?, sweep = ?,
                       status = CASE WHEN cancelled = 1 THEN 'blocked' ELSE ? END,
                       updated_at = ?
                   WHERE job_id = ?""",
                (cursor, sweep, job_status, now, job_id),
            )


def _utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _select_in_chunks(
    con: sqlite3.Connection, query: str, values: Sequence[Any]
) -> list[sqlite3.Row]:
    """Execute one single-column IN query without exceeding SQLite's limit."""
    if "{placeholders}" not in query:
        raise ValueError("chunked IN query lacks the placeholders marker")
    variable_limit = con.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
    rows: list[sqlite3.Row] = []
    for offset in range(0, len(values), variable_limit):
        chunk = values[offset : offset + variable_limit]
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            con.execute(
                query.format(placeholders=placeholders), tuple(chunk)
            ).fetchall()
        )
    return rows


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _evidence_json(evidence: Mapping[str, Any] | str | None) -> str:
    if evidence is None:
        return "{}"
    if isinstance(evidence, str):
        return canonical_json({"note": evidence})
    return canonical_json(evidence)


def _bounded_utf8(value: str, byte_limit: int) -> str:
    raw = value.encode("utf-8")
    if len(raw) <= byte_limit:
        return value
    return raw[:byte_limit].decode("utf-8", errors="ignore")


def _research_run_id(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("run_id must not be empty")
    return normalize_path_component(normalized, "run_id")


def _normalize_identifier(
    identifier_type: str, identifier_value: str
) -> tuple[str, str]:
    normalized_type = identifier_type.strip()
    normalized_value = identifier_value.strip()
    if not normalized_type or not normalized_value:
        raise ValueError("identifier type and value must not be empty")
    if normalized_type.casefold() == "ticker":
        return "ticker", normalized_value.upper()
    return normalized_type, normalized_value


def _rows_cover(rows: list[sqlite3.Row], start: date, end: date) -> bool:
    """Whether evidence covers a range, allowing only weekend non-sessions."""
    cursor = start.toordinal()
    target_end = end.toordinal()
    intervals = sorted(
        (
            date.fromisoformat(row["valid_from"]).toordinal(),
            date.fromisoformat(row["valid_to"]).toordinal(),
        )
        for row in rows
    )
    for interval_start, interval_end in intervals:
        if interval_start > cursor and not _ordinals_are_weekend(
            cursor, interval_start - 1
        ):
            return False
        if interval_end >= cursor:
            cursor = interval_end + 1
        if cursor > target_end:
            return True
    return False


def _ordinals_are_weekend(start: int, end: int) -> bool:
    if end - start >= 2:
        return False
    return all(
        date.fromordinal(value).weekday() >= 5 for value in range(start, end + 1)
    )
