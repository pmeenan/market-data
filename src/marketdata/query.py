"""Read-only DuckDB query surface over canonical instrument-owned bars."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from marketdata.calendar import label_intraday_sessions
from marketdata.config import Config
from marketdata.store.bars import (
    CANONICAL_EOD_SCHEMA,
    CANONICAL_INTRADAY_SCHEMA,
    INTRADAY_FREQS,
    BarStore,
    create_canonical_parquet_view,
    require_canonical_generation,
    require_intraday_freq,
)
from marketdata.store.meta import MetaStore


def _sql_path(path: str | Path) -> str:
    return str(path).replace("'", "''")


def connect(config: Config) -> duckdb.DuckDBPyConnection:
    """Expose active v2 bars, alias display views, and read-only metadata."""
    bars = BarStore(config.data_dir)
    if not config.meta_path.exists():
        raise RuntimeError("canonical queries require meta.db")
    with MetaStore(config.meta_path) as meta:
        require_canonical_generation(bars, meta.storage_generation())

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(
        f"ATTACH '{_sql_path(config.meta_path)}' AS meta (TYPE sqlite, READ_ONLY)"
    )
    if bars.canonical_eod_files():
        create_canonical_parquet_view(con, "eod", bars.canonical_eod_glob())
        _create_alias_view(con, "eod")
    for freq in INTRADAY_FREQS:
        if bars.canonical_intraday_files(freq):
            view = f"intraday_{freq}"
            create_canonical_parquet_view(con, view, bars.canonical_intraday_glob(freq))
            _create_alias_view(con, view)
    return con


def _create_alias_view(con: duckdb.DuckDBPyConnection, view: str) -> None:
    """Derive a display ticker only where alias evidence is unambiguous."""
    date_expr = _date_expression(view, "bars")
    con.execute(
        f"""CREATE VIEW {view}_with_alias AS
            SELECT bars.*,
                   (SELECT CASE WHEN count(DISTINCT aliases.ticker) = 1
                                THEN min(aliases.ticker) END
                    FROM meta.instrument_aliases AS aliases
                    WHERE aliases.instrument_id = bars.instrument_id
                      AND {date_expr}
                          BETWEEN CAST(aliases.start_date AS DATE)
                              AND CAST(aliases.end_date AS DATE)) AS ticker
            FROM {view} AS bars
            """
    )


def load_eod(
    config: Config,
    *,
    instrument_ids: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
) -> pl.DataFrame:
    """Load daily bars filtered by stable instrument ids and date."""
    return _load(config, "eod", instrument_ids, start, end)


def load_intraday(
    config: Config,
    *,
    instrument_ids: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    freq: str = "1hour",
) -> pl.DataFrame:
    """Load UTC intraday bars filtered by stable ids and UTC date."""
    require_intraday_freq(freq)
    return _load(config, f"intraday_{freq}", instrument_ids, start, end)


def load_intraday_sessions(
    config: Config,
    *,
    instrument_ids: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    freq: str = "1hour",
) -> pl.DataFrame:
    """Load intraday bars filtered and labelled by XNYS session semantics."""
    raw = load_intraday(
        config,
        instrument_ids=instrument_ids,
        start=start,
        end=end,
        freq=freq,
    )
    return label_intraday_sessions(raw, freq=freq)


def load_eod_by_ticker(
    config: Config,
    tickers: Sequence[str],
    *,
    start: date | str,
    end: date | str,
) -> pl.DataFrame:
    """Resolve ticker aliases over an explicit range, then load daily bars."""
    return _load_by_ticker(config, "eod", tickers, start, end)


def load_intraday_by_ticker(
    config: Config,
    tickers: Sequence[str],
    *,
    start: date | str,
    end: date | str,
    freq: str = "1hour",
) -> pl.DataFrame:
    """Resolve ticker aliases over an explicit range, then load intraday bars."""
    require_intraday_freq(freq)
    return _load_by_ticker(config, f"intraday_{freq}", tickers, start, end)


def load_intraday_sessions_by_ticker(
    config: Config,
    tickers: Sequence[str],
    *,
    start: date | str,
    end: date | str,
    freq: str = "1hour",
) -> pl.DataFrame:
    """Resolve tickers, then filter and label bars by XNYS sessions."""
    raw = load_intraday_by_ticker(
        config,
        tickers,
        start=start,
        end=end,
        freq=freq,
    )
    return label_intraday_sessions(raw, freq=freq)


def _load(
    config: Config,
    view: str,
    instrument_ids: Sequence[str] | None,
    start: date | str | None,
    end: date | str | None,
) -> pl.DataFrame:
    con = connect(config)
    selected_ids = _validated_instrument_ids(con, instrument_ids)
    if selected_ids == () or not _view_exists(con, view):
        return _empty_frame(view)

    clauses: list[str] = []
    params: list[str] = []
    if selected_ids is not None:
        placeholders = ",".join("?" for _ in selected_ids)
        clauses.append(f"instrument_id IN ({placeholders})")
        params.extend(selected_ids)
    date_expr = _date_expression(view)
    if start is not None:
        clauses.append(f"{date_expr} >= ?")
        params.append(str(start))
    if end is not None:
        clauses.append(f"{date_expr} <= ?")
        params.append(str(end))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return con.execute(
        f"SELECT * FROM {view} {where} ORDER BY instrument_id, {_time_column(view)}",
        params,
    ).pl()


def _load_by_ticker(
    config: Config,
    view: str,
    tickers: Sequence[str],
    start: date | str,
    end: date | str,
) -> pl.DataFrame:
    stripped_tickers = [ticker.strip() for ticker in tickers]
    if any(not ticker for ticker in stripped_tickers):
        raise ValueError("tickers must not contain empty values")
    normalized_tickers = tuple(
        dict.fromkeys(ticker.upper() for ticker in stripped_tickers)
    )
    con = connect(config)
    if not normalized_tickers:
        return _empty_frame(view, include_ticker=True)
    start_date = date.fromisoformat(str(start))
    end_date = date.fromisoformat(str(end))
    if start_date > end_date:
        raise ValueError("start must not be after end")

    segments: list[tuple[str, str, date, date]] = []
    with MetaStore(config.meta_path) as meta:
        for ticker in normalized_tickers:
            report = meta.resolve_alias_range(ticker, start_date, end_date)
            if not report.resolved:
                detail = ", ".join(
                    f"{segment.start}..{segment.end}={segment.status}"
                    for segment in report.segments
                    if segment.instrument_id is None
                )
                raise ValueError(f"ticker {report.ticker} is unresolved: {detail}")
            segments.extend(
                (report.ticker, segment.instrument_id, segment.start, segment.end)
                for segment in report.segments
                if segment.instrument_id is not None
            )
    if not _view_exists(con, view):
        return _empty_frame(view, include_ticker=True)

    values = ", ".join("(?, ?, CAST(? AS DATE), CAST(? AS DATE))" for _ in segments)
    params = [str(value) for segment in segments for value in segment]
    date_expr = _date_expression(view, "bars")
    return con.execute(
        f"""WITH segments(ticker, instrument_id, start_date, end_date) AS (
                VALUES {values}
            )
            SELECT bars.*, segments.ticker
            FROM {view} AS bars
            JOIN segments
              ON segments.instrument_id = bars.instrument_id
             AND {date_expr} BETWEEN segments.start_date AND segments.end_date
            ORDER BY bars.instrument_id, bars.{_time_column(view)}""",
        params,
    ).pl()


def _validated_instrument_ids(
    con: duckdb.DuckDBPyConnection, instrument_ids: Sequence[str] | None
) -> tuple[str, ...] | None:
    if instrument_ids is None:
        return None
    normalized = tuple(
        dict.fromkeys(instrument_id.strip() for instrument_id in instrument_ids)
    )
    if any(not instrument_id for instrument_id in normalized):
        raise ValueError("instrument_ids must not contain empty values")
    if not normalized:
        return ()
    placeholders = ",".join("?" for _ in normalized)
    known = {
        row[0]
        for row in con.execute(
            f"SELECT instrument_id FROM meta.instruments "
            f"WHERE instrument_id IN ({placeholders})",
            list(normalized),
        ).fetchall()
    }
    unknown = set(normalized) - known
    if unknown:
        raise ValueError(f"unknown instrument_ids: {sorted(unknown)}")
    return normalized


def _view_exists(con: duckdb.DuckDBPyConnection, view: str) -> bool:
    return bool(
        con.execute(
            "SELECT count(*) FROM duckdb_views() WHERE view_name = ?", [view]
        ).fetchone()[0]
    )


def _date_expression(view: str, table: str | None = None) -> str:
    prefix = f"{table}." if table else ""
    if view == "eod":
        return f"{prefix}date"
    return f"CAST({prefix}ts AT TIME ZONE 'UTC' AS DATE)"


def _time_column(view: str) -> str:
    return "date" if view == "eod" else "ts"


def _empty_frame(view: str, *, include_ticker: bool = False) -> pl.DataFrame:
    schema = CANONICAL_EOD_SCHEMA if view == "eod" else CANONICAL_INTRADAY_SCHEMA
    if include_ticker:
        schema = schema | {"ticker": pl.Utf8}
    return pl.DataFrame(schema=schema)
