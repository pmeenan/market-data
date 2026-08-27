"""Parquet-backed legacy and canonical bar storage.

Layout:
    {data_dir}/eod/{TICKER}.parquet                     full daily history per ticker
    {data_dir}/intraday/{freq}/{TICKER}/{year}.parquet  one file per ticker-year

The ticker-keyed paths are the quarantined v1 generation and remain here only
until ingestion is moved to stable identities.  Canonical v2 bars live below
``bars/`` in stable SHA-256 buckets and carry ``instrument_id`` instead.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from marketdata.bar_fields import TIINGO_EOD_FIELD_MAP, TIINGO_INTRADAY_FIELD_MAP

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

CANONICAL_EOD_SCHEMA = {"instrument_id": pl.Utf8} | {
    key: value for key, value in EOD_SCHEMA.items() if key != "ticker"
}

CANONICAL_INTRADAY_SCHEMA = {"instrument_id": pl.Utf8} | {
    key: value for key, value in INTRADAY_SCHEMA.items() if key != "ticker"
}

INTRADAY_FREQS = ("1hour", "5min")


def eod_frame(ticker: str, rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Convert Tiingo EOD response rows into the normalized legacy frame."""
    records = [
        {out: row.get(src) for src, out in TIINGO_EOD_FIELD_MAP.items()} for row in rows
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
            out: row[src] if src == "date" else row.get(src)
            for src, out in TIINGO_INTRADAY_FIELD_MAP.items()
        }
        for row in rows
    ]
    df = pl.DataFrame(
        records,
        schema={
            out: pl.Utf8 if out == "ts" else INTRADAY_SCHEMA[out]
            for out in TIINGO_INTRADAY_FIELD_MAP.values()
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

    def intraday_path(self, ticker: str, year: int, freq: str = "1hour") -> Path:
        return self.data_dir / "intraday" / freq / _safe(ticker) / f"{year}.parquet"

    def eod_glob(self) -> str:
        return str(self.data_dir / "eod" / "*.parquet")

    def intraday_glob(self, freq: str = "1hour") -> str:
        return str(self.data_dir / "intraday" / freq / "*" / "*.parquet")

    # ---- canonical v2 paths ---------------------------------------------

    def canonical_eod_path(self, instrument_id: str) -> Path:
        return canonical_bucket_path(
            self.data_dir, "eod", instrument_bucket(instrument_id)
        )

    def canonical_intraday_path(
        self, instrument_id: str, year: int, freq: str = "1hour"
    ) -> Path:
        return canonical_bucket_path(
            self.data_dir,
            f"intraday_{freq}",
            instrument_bucket(instrument_id),
            year,
        )

    def canonical_eod_glob(self) -> str:
        return canonical_dataset_glob(self.data_dir, "eod")

    def canonical_intraday_glob(self, freq: str = "1hour") -> str:
        return canonical_dataset_glob(self.data_dir, f"intraday_{freq}")

    def canonical_eod_files(self) -> list[Path]:
        return sorted(
            canonical_dataset_root(self.data_dir, "eod").glob("bucket=*/bars.parquet")
        )

    def canonical_intraday_files(self, freq: str) -> list[Path]:
        return sorted(
            canonical_dataset_root(self.data_dir, f"intraday_{freq}").glob(
                "year=*/bucket=*/bars.parquet"
            )
        )

    def has_canonical_bars(self) -> bool:
        return any((self.data_dir / "bars").rglob("*.parquet"))

    def has_legacy_bars(self) -> bool:
        return any((self.data_dir / "eod").glob("*.parquet")) or any(
            (self.data_dir / "intraday").rglob("*.parquet")
        )

    def validate_generation(self, generation: str) -> None:
        """Reject bar files that contradict the durable generation marker."""
        if generation == "v2" and self.has_legacy_bars():
            raise RuntimeError(
                "v1 bar files exist after the recorded v2 generation boundary"
            )
        if generation == "v1" and self.has_canonical_bars():
            raise RuntimeError(
                "canonical bars exist but meta.db records v1; restore the matching "
                "metadata backup or rerun the migration"
            )

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

    def write_intraday(self, ticker: str, df: pl.DataFrame, freq: str = "1hour") -> int:
        """Merge-upsert intraday bars, splitting rows across per-year files."""
        total = 0
        for (year,), part in df.group_by(pl.col("ts").dt.year(), maintain_order=True):
            path = self.intraday_path(ticker, int(year), freq)
            merged = _merge(path, part, key="ts")
            _atomic_write(merged, path)
            total += merged.height
        return total

    def read_intraday(self, ticker: str, freq: str = "1hour") -> pl.DataFrame | None:
        tdir = self.data_dir / "intraday" / freq / _safe(ticker)
        files = sorted(tdir.glob("*.parquet")) if tdir.exists() else []
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files]).sort("ts")

    # ---- canonical v2 publication ---------------------------------------

    def canonicalize_eod(self, instrument_id: str, frame: pl.DataFrame) -> pl.DataFrame:
        """Validate and normalize one instrument's EOD publication frame."""
        return _canonical_frame(instrument_id, frame, CANONICAL_EOD_SCHEMA, "date")

    def canonicalize_intraday(
        self, instrument_id: str, frame: pl.DataFrame
    ) -> pl.DataFrame:
        """Validate and normalize one instrument's intraday publication frame."""
        return _canonical_frame(instrument_id, frame, CANONICAL_INTRADAY_SCHEMA, "ts")

    def publish_eod(
        self,
        frames: Mapping[str, pl.DataFrame],
        *,
        replace_instruments: frozenset[str] = frozenset(),
    ) -> dict[str, int]:
        """Publish validated instrument frames, rewriting each bucket once.

        Normal frames merge-upsert by ``(instrument_id, date)``. Instruments
        named in ``replace_instruments`` have their complete slice replaced,
        which is the corporate-action snapshot operation from D-019.
        """
        missing_replacements = replace_instruments - frames.keys()
        if missing_replacements:
            raise ValueError(
                "replacement snapshots require frames for every instrument: "
                f"{sorted(missing_replacements)}"
            )
        grouped: dict[str, list[pl.DataFrame]] = {}
        for instrument_id, frame in frames.items():
            if instrument_id in replace_instruments and frame.is_empty():
                raise ValueError(
                    f"replacement snapshot for {instrument_id!r} must not be empty"
                )
            canonical = self.canonicalize_eod(instrument_id, frame)
            grouped.setdefault(instrument_bucket(instrument_id), []).append(canonical)

        result: dict[str, int] = {}
        for bucket, incoming_frames in sorted(grouped.items()):
            path = canonical_bucket_path(self.data_dir, "eod", bucket)
            incoming = pl.concat(incoming_frames)
            merged = _merge_canonical(
                path,
                incoming,
                key=["instrument_id", "date"],
                replace_instruments=replace_instruments,
            )
            _atomic_write(merged, path)
            counts = dict(merged.group_by("instrument_id").len().iter_rows())
            result.update(
                {
                    instrument_id: counts[instrument_id]
                    for instrument_id in incoming["instrument_id"].unique()
                }
            )
        return result

    def publish_intraday(
        self, frames: Mapping[str, pl.DataFrame], *, freq: str = "1hour"
    ) -> dict[tuple[str, int], int]:
        """Publish validated intraday frames, split by year and bucket."""
        require_intraday_freq(freq)
        grouped: dict[tuple[int, str], list[pl.DataFrame]] = {}
        for instrument_id, frame in frames.items():
            canonical = self.canonicalize_intraday(instrument_id, frame)
            for (year,), part in canonical.group_by(
                pl.col("ts").dt.year(), maintain_order=True
            ):
                group = (int(year), instrument_bucket(instrument_id))
                grouped.setdefault(group, []).append(part)

        result: dict[tuple[str, int], int] = {}
        for (year, bucket), incoming_frames in sorted(grouped.items()):
            path = canonical_bucket_path(
                self.data_dir, f"intraday_{freq}", bucket, year
            )
            incoming = pl.concat(incoming_frames)
            merged = _merge_canonical(path, incoming, key=["instrument_id", "ts"])
            _atomic_write(merged, path)
            counts = dict(merged.group_by("instrument_id").len().iter_rows())
            result.update(
                {
                    (instrument_id, year): counts[instrument_id]
                    for instrument_id in incoming["instrument_id"].unique()
                }
            )
        return result

    def read_canonical_eod(self, instrument_id: str) -> pl.DataFrame | None:
        path = self.canonical_eod_path(instrument_id)
        if not path.exists():
            return None
        frame = (
            pl.read_parquet(path)
            .filter(pl.col("instrument_id") == instrument_id)
            .sort("date")
        )
        return frame if frame.height else None

    def read_canonical_intraday(
        self, instrument_id: str, freq: str = "1hour"
    ) -> pl.DataFrame | None:
        files = self.canonical_intraday_files(freq)
        if not files:
            return None
        frame = (
            pl.scan_parquet(files)
            .filter(pl.col("instrument_id") == instrument_id)
            .collect()
        )
        return frame.sort("ts") if frame.height else None


def instrument_bucket(instrument_id: str) -> str:
    """Return D-019's stable first-SHA-256-byte bucket."""
    if not instrument_id:
        raise ValueError("instrument_id must not be empty")
    return hashlib.sha256(instrument_id.encode("utf-8")).hexdigest()[:2]


def canonical_dataset_root(data_dir: Path, dataset_key: str) -> Path:
    if dataset_key == "eod":
        return Path(data_dir) / "bars" / "eod"
    if dataset_key.startswith("intraday_"):
        freq = dataset_key.removeprefix("intraday_")
        require_intraday_freq(freq)
        return Path(data_dir) / "bars" / "intraday" / freq
    raise ValueError(f"unsupported canonical dataset_key {dataset_key!r}")


def canonical_bucket_path(
    data_dir: Path, dataset_key: str, bucket: str, year: int | None = None
) -> Path:
    root = canonical_dataset_root(data_dir, dataset_key)
    if dataset_key == "eod":
        if year is not None:
            raise ValueError("EOD bucket paths do not have a year")
    else:
        if year is None:
            raise ValueError("intraday bucket paths require a year")
        root = root / f"year={year}"
    return root / f"bucket={bucket}" / "bars.parquet"


def canonical_dataset_glob(data_dir: Path, dataset_key: str) -> str:
    root = canonical_dataset_root(data_dir, dataset_key)
    if dataset_key == "eod":
        return str(root / "bucket=*" / "bars.parquet")
    return str(root / "year=*" / "bucket=*" / "bars.parquet")


def _safe(ticker: str) -> str:
    return ticker.upper().replace("/", "-")


def _merge(path: Path, incoming: pl.DataFrame, key: str) -> pl.DataFrame:
    if path.exists():
        existing = pl.read_parquet(path)
        combined = pl.concat([existing, incoming.select(existing.columns)])
    else:
        combined = incoming
    return combined.unique(subset=[key], keep="last").sort(key)


def _canonical_frame(
    instrument_id: str,
    frame: pl.DataFrame,
    schema: dict[str, pl.DataType],
    time_key: str,
) -> pl.DataFrame:
    if not instrument_id:
        raise ValueError("instrument_id must not be empty")
    if "ticker" in frame.columns:
        frame = frame.drop("ticker")
    if "instrument_id" in frame.columns:
        ids = frame["instrument_id"].drop_nulls().unique().to_list()
        if ids != [instrument_id]:
            raise ValueError(f"frame identity {ids!r} does not match {instrument_id!r}")
    else:
        frame = frame.with_columns(instrument_id=pl.lit(instrument_id))
    missing = set(schema) - set(frame.columns)
    extra = set(frame.columns) - set(schema)
    if missing or extra:
        raise ValueError(
            f"invalid canonical schema; missing={sorted(missing)}, extra={sorted(extra)}"
        )
    canonical = frame.select(schema.keys()).cast(schema)
    if (
        canonical.select(
            pl.any_horizontal(pl.col(["instrument_id", time_key]).is_null())
        )
        .to_series()
        .any()
    ):
        raise ValueError("canonical keys must not contain nulls")
    canonical = canonical.unique(maintain_order=True)
    if canonical.select(
        pl.struct(["instrument_id", time_key]).is_duplicated().any()
    ).item():
        raise ValueError(f"duplicate canonical key (instrument_id, {time_key})")
    return canonical.sort(["instrument_id", time_key])


def _merge_canonical(
    path: Path,
    incoming: pl.DataFrame,
    *,
    key: list[str],
    replace_instruments: frozenset[str] = frozenset(),
) -> pl.DataFrame:
    if path.exists():
        existing = pl.read_parquet(path)
        if existing.schema != incoming.schema:
            raise ValueError(f"canonical schema mismatch in {path}")
        if replace_instruments:
            existing = existing.filter(
                ~pl.col("instrument_id").is_in(list(replace_instruments))
            )
        combined = pl.concat([existing, incoming])
    else:
        combined = incoming
    return combined.unique(subset=key, keep="last").sort(key)


def require_intraday_freq(freq: str) -> None:
    if freq not in INTRADAY_FREQS:
        raise ValueError(f"freq must be one of {INTRADAY_FREQS}, got {freq!r}")


def require_canonical_generation(bars: BarStore, generation: str) -> None:
    """Validate the durable boundary and require active canonical storage."""
    bars.validate_generation(generation)
    if generation != "v2":
        raise RuntimeError("instrument-owned APIs require the v2 storage generation")


def _atomic_write(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(path)
