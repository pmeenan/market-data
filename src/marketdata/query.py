"""DuckDB query surface over the Parquet warehouse.

This is the read path for research and backtesting: get a DuckDB
connection with an `eod` view plus one `intraday_{freq}` view per
frequency present on disk (and the SQLite metadata tables attached), or
pull bars straight into a polars frame.
"""

from __future__ import annotations

from datetime import date

import duckdb
import polars as pl

from marketdata.config import Config
from marketdata.store.bars import BarStore


def connect(config: Config) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with views over the warehouse.

    Views (when the underlying files exist):
      eod              all daily bars, all tickers
      intraday_{freq}  one view per intraday frequency present on disk
                       (intraday_1hour or intraday_5min)
      meta.*           the SQLite metadata tables (universe, coverage, ...)
    """
    bars = BarStore(config.data_dir)
    con = duckdb.connect()
    if any(config.eod_dir.glob("*.parquet")):
        con.execute(
            f"CREATE VIEW eod AS SELECT * FROM read_parquet('{bars.eod_glob()}')"
        )
    intraday_root = config.data_dir / "intraday"
    if intraday_root.exists():
        for freq_dir in sorted(p for p in intraday_root.iterdir() if p.is_dir()):
            if any(freq_dir.rglob("*.parquet")):
                con.execute(
                    f"CREATE VIEW intraday_{freq_dir.name} AS SELECT * FROM "
                    f"read_parquet('{bars.intraday_glob(freq_dir.name)}')"
                )
    if config.meta_path.exists():
        con.execute(f"ATTACH '{config.meta_path}' AS meta (TYPE sqlite, READ_ONLY)")
    return con


def load_eod(
    config: Config,
    tickers: list[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
) -> pl.DataFrame:
    """Daily bars as a polars frame, filtered by ticker and date range."""
    con = connect(config)
    clauses, params = [], []
    if tickers:
        placeholders = ",".join("?" for _ in tickers)
        clauses.append(f"ticker IN ({placeholders})")
        params.extend(t.upper() for t in tickers)
    if start:
        clauses.append("date >= ?")
        params.append(str(start))
    if end:
        clauses.append("date <= ?")
        params.append(str(end))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return con.execute(f"SELECT * FROM eod {where} ORDER BY ticker, date", params).pl()


def load_intraday(
    config: Config,
    tickers: list[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    freq: str = "1hour",
) -> pl.DataFrame:
    """Intraday bars as a polars frame, filtered by ticker and date range.
    Timestamps are UTC; date filters compare against the UTC date of `ts`."""
    con = connect(config)
    clauses, params = [], []
    if tickers:
        placeholders = ",".join("?" for _ in tickers)
        clauses.append(f"ticker IN ({placeholders})")
        params.extend(t.upper() for t in tickers)
    if start:
        clauses.append("CAST(ts AS DATE) >= ?")
        params.append(str(start))
    if end:
        clauses.append("CAST(ts AS DATE) <= ?")
        params.append(str(end))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return con.execute(
        f"SELECT * FROM intraday_{freq} {where} ORDER BY ticker, ts", params
    ).pl()
