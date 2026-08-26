"""SQLite metadata store: ticker registry, annual universes, coverage intervals.

Bar data lives in Parquet (see bars.py); this database only tracks the
small relational state around it, so it stays tiny and trivially portable.
The schema is versioned via PRAGMA user_version with explicit migrations.

Coverage is tracked as a per-(ticker, dataset) interval [first_date,
last_date] of ingested data — not a single high watermark — so an explicit
backfill can fetch missing *leading* history as well as trailing history.
Coverage is reconcilable from the canonical Parquet files (`market-data
reconcile`).
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1

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
-- a dollar-volume metric so lookback membership is point-in-time and
-- survivorship-bias-aware.
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


def _migrate(con: sqlite3.Connection) -> None:
    version = con.execute("PRAGMA user_version").fetchone()[0]
    if version < 1:
        with con:
            con.executescript(_SCHEMA_V1)
            # v0 tracked a single high watermark; superseded by coverage.
            # Rebuild state from Parquet with `market-data reconcile`.
            con.execute("DROP TABLE IF EXISTS watermarks")
            con.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    elif version > SCHEMA_VERSION:
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
        _migrate(self._con)

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> "MetaStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

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

    # ---- universe --------------------------------------------------------

    def set_universe(self, year: int, entries: list[dict]) -> None:
        """Replace the universe for a year. Entries: ticker, optional rank
        and avg_dollar_volume."""
        self.replace_universes({year: entries})

    def replace_universes(self, by_year: dict[int, list[dict]]) -> None:
        """Replace several years' universes in one atomic transaction."""
        with self._con:
            for year, entries in by_year.items():
                self._con.execute("DELETE FROM universe WHERE year = ?", (year,))
                self._con.executemany(
                    """INSERT INTO universe (year, ticker, rank, avg_dollar_volume)
                       VALUES (?, ?, ?, ?)""",
                    [
                        (year, e["ticker"].upper(), e.get("rank"), e.get("avg_dollar_volume"))
                        for e in entries
                    ],
                )

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

    # ---- coverage --------------------------------------------------------

    def get_coverage(self, ticker: str, dataset: str) -> tuple[date, date] | None:
        row = self._con.execute(
            "SELECT first_date, last_date FROM coverage WHERE ticker = ? AND dataset = ?",
            (ticker.upper(), dataset),
        ).fetchone()
        if row is None:
            return None
        return date.fromisoformat(row["first_date"]), date.fromisoformat(row["last_date"])

    def set_coverage(self, ticker: str, dataset: str, first: date, last: date) -> None:
        """Set the covered interval outright (used by reconcile and full
        refreshes)."""
        with self._con:
            self._con.execute(
                """INSERT INTO coverage (ticker, dataset, first_date, last_date, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(ticker, dataset) DO UPDATE SET
                     first_date=excluded.first_date, last_date=excluded.last_date,
                     updated_at=excluded.updated_at""",
                (ticker.upper(), dataset, first.isoformat(), last.isoformat(), _now()),
            )

    def extend_coverage(self, ticker: str, dataset: str, first: date, last: date) -> None:
        """Widen the covered interval to include [first, last]."""
        existing = self.get_coverage(ticker, dataset)
        if existing:
            first = min(first, existing[0])
            last = max(last, existing[1])
        self.set_coverage(ticker, dataset, first, last)

    def coverage(self, dataset: str) -> dict[str, tuple[date, date]]:
        rows = self._con.execute(
            "SELECT ticker, first_date, last_date FROM coverage WHERE dataset = ?",
            (dataset,),
        ).fetchall()
        return {
            r["ticker"]: (date.fromisoformat(r["first_date"]), date.fromisoformat(r["last_date"]))
            for r in rows
        }

    def replace_coverage(
        self, entries: dict[tuple[str, str], tuple[date, date]]
    ) -> None:
        """Atomically replace ALL coverage rows (used by reconcile): stale
        entries for vanished files must not survive a rebuild."""
        with self._con:
            self._con.execute("DELETE FROM coverage")
            now = _now()
            self._con.executemany(
                """INSERT INTO coverage (ticker, dataset, first_date, last_date, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [
                    (ticker.upper(), dataset, first.isoformat(), last.isoformat(), now)
                    for (ticker, dataset), (first, last) in entries.items()
                ],
            )

    def clear_coverage(self, dataset: str | None = None) -> None:
        with self._con:
            if dataset is None:
                self._con.execute("DELETE FROM coverage")
            else:
                self._con.execute("DELETE FROM coverage WHERE dataset = ?", (dataset,))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
