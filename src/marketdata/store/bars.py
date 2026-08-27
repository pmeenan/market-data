"""Parquet-backed bar storage.

Layout:
    {data_dir}/eod/{TICKER}.parquet                     full daily history per ticker
    {data_dir}/intraday/{freq}/{TICKER}/{year}.parquet  one file per ticker-year

Each file carries a `ticker` column so DuckDB can query globs directly
without relying on filenames. Writes are merge-upserts keyed on the
timestamp column: refetching an overlapping range replaces those rows,
which also picks up Tiingo's restated adjusted values after splits and
dividends.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

EOD_SCHEMA = {
    "ticker": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "adj_open": pl.Float64,
    "adj_high": pl.Float64,
    "adj_low": pl.Float64,
    "adj_close": pl.Float64,
    "adj_volume": pl.Int64,
    "div_cash": pl.Float64,
    "split_factor": pl.Float64,
}

INTRADAY_SCHEMA = {
    "ticker": pl.Utf8,
    "ts": pl.Datetime("us", "UTC"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
}

_TIINGO_EOD_FIELDS = {
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "adjOpen": "adj_open",
    "adjHigh": "adj_high",
    "adjLow": "adj_low",
    "adjClose": "adj_close",
    "adjVolume": "adj_volume",
    "divCash": "div_cash",
    "splitFactor": "split_factor",
}


def eod_frame(ticker: str, rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Convert Tiingo EOD JSON rows into the canonical EOD frame."""
    records = [
        {out: row.get(src) for src, out in _TIINGO_EOD_FIELDS.items()} for row in rows
    ]
    df = pl.DataFrame(
        records,
        schema={k: v for k, v in EOD_SCHEMA.items() if k != "ticker"}
        | {"date": pl.Utf8},
    )
    return (
        df.with_columns(
            pl.col("date").str.slice(0, 10).str.to_date(),
            ticker=pl.lit(ticker.upper()),
        )
        .select(EOD_SCHEMA.keys())
        .cast(EOD_SCHEMA)
    )


def intraday_frame(ticker: str, rows: list[dict[str, Any]]) -> pl.DataFrame:
    records = [
        {
            "ts": row["date"],
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
        }
        for row in rows
    ]
    df = pl.DataFrame(
        records,
        schema={
            "ts": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
        },
    )
    return (
        df.with_columns(
            pl.col("ts").str.to_datetime(time_zone="UTC", time_unit="us"),
            ticker=pl.lit(ticker.upper()),
        )
        .select(INTRADAY_SCHEMA.keys())
        .cast(INTRADAY_SCHEMA)
    )


class BarStore:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)

    # ---- paths -----------------------------------------------------------

    def eod_path(self, ticker: str) -> Path:
        return self.data_dir / "eod" / f"{_safe(ticker)}.parquet"

    def intraday_path(self, ticker: str, year: int, freq: str = "1min") -> Path:
        return self.data_dir / "intraday" / freq / _safe(ticker) / f"{year}.parquet"

    def eod_glob(self) -> str:
        return str(self.data_dir / "eod" / "*.parquet")

    def intraday_glob(self, freq: str = "1min") -> str:
        return str(self.data_dir / "intraday" / freq / "*" / "*.parquet")

    # ---- EOD -------------------------------------------------------------

    def write_eod(self, ticker: str, df: pl.DataFrame) -> int:
        """Merge-upsert daily bars for one ticker. Returns rows in the file."""
        path = self.eod_path(ticker)
        merged = _merge(path, df, key="date")
        _atomic_write(merged, path)
        return merged.height

    def replace_eod(self, ticker: str, df: pl.DataFrame) -> int:
        """Atomically replace a ticker's daily file with a full snapshot
        (no merge): used by corporate-action refreshes, where merge-upsert
        could leave stale dates the new snapshot omits."""
        snapshot = df.unique(subset=["date"], keep="last").sort("date")
        _atomic_write(snapshot, self.eod_path(ticker))
        return snapshot.height

    def read_eod(self, ticker: str) -> pl.DataFrame | None:
        path = self.eod_path(ticker)
        return pl.read_parquet(path) if path.exists() else None

    def eod_last_date(self, ticker: str) -> date | None:
        path = self.eod_path(ticker)
        if not path.exists():
            return None
        return pl.scan_parquet(path).select(pl.col("date").max()).collect().item()

    def eod_tickers(self) -> list[str]:
        eod_dir = self.data_dir / "eod"
        if not eod_dir.exists():
            return []
        return sorted(p.stem for p in eod_dir.glob("*.parquet"))

    # ---- intraday --------------------------------------------------------

    def write_intraday(self, ticker: str, df: pl.DataFrame, freq: str = "1min") -> int:
        """Merge-upsert intraday bars, splitting rows across per-year files."""
        total = 0
        for (year,), part in df.group_by(pl.col("ts").dt.year(), maintain_order=True):
            path = self.intraday_path(ticker, int(year), freq)
            merged = _merge(path, part, key="ts")
            _atomic_write(merged, path)
            total += merged.height
        return total

    def read_intraday(self, ticker: str, freq: str = "1min") -> pl.DataFrame | None:
        tdir = self.data_dir / "intraday" / freq / _safe(ticker)
        files = sorted(tdir.glob("*.parquet")) if tdir.exists() else []
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files]).sort("ts")


def _safe(ticker: str) -> str:
    return ticker.upper().replace("/", "-")


def _merge(path: Path, incoming: pl.DataFrame, key: str) -> pl.DataFrame:
    if path.exists():
        existing = pl.read_parquet(path)
        combined = pl.concat([existing, incoming.select(existing.columns)])
    else:
        combined = incoming
    return combined.unique(subset=[key], keep="last").sort(key)


def _atomic_write(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(path)
