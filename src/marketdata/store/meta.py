"""SQLite metadata store: identity, universes, and coverage intervals.

Bar data lives in Parquet (see bars.py); this database only tracks the
small relational state around it, so it stays tiny and trivially portable.
The schema is versioned via PRAGMA user_version with explicit migrations.

The v1 ticker registry and ticker-keyed coverage remain temporarily readable
under explicitly named legacy storage while M1 converts ingestion. Canonical
coverage is instrument-keyed and uses exact dataset keys.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from marketdata.identity import (
    ACTIVE_ALIAS_END,
    AliasResolutionReport,
    IdentifierResolution,
    ResolutionSegment,
    UniverseResolution,
    require_dataset_key,
    require_validation_state,
)

SCHEMA_VERSION = 3

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
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"meta.db schema version {version} is newer than this code "
            f"supports ({SCHEMA_VERSION}) — update the tool"
        )
    # Keep additions made during the still-uncommitted v3 implementation
    # idempotent for local databases opened by an earlier working-tree build.
    if version == 3:
        with con:
            con.executescript(_SCHEMA_V3)


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
        if not identifier_type.strip() or not identifier_value.strip():
            raise ValueError("identifier type and value must not be empty")
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
                    identifier_type.strip(),
                    identifier_value.strip(),
                    valid_from.isoformat(),
                    valid_to.isoformat(),
                    validation_state,
                    _evidence_json(evidence),
                    now,
                    now,
                ),
            )
            return int(cursor.fetchone()[0])

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


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _evidence_json(evidence: Mapping[str, Any] | str | None) -> str:
    if evidence is None:
        return "{}"
    if isinstance(evidence, str):
        return json.dumps({"note": evidence}, sort_keys=True, separators=(",", ":"))
    return json.dumps(evidence, sort_keys=True, separators=(",", ":"))


def _rows_cover(rows: list[sqlite3.Row], start: date, end: date) -> bool:
    """Whether closed evidence envelopes cover a range without a date gap."""
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
        if interval_start > cursor:
            return False
        if interval_end >= cursor:
            cursor = interval_end + 1
        if cursor > target_end:
            return True
    return False
