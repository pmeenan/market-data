"""Durable coverage recovery from canonical or pre-migration Parquet bars."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from marketdata.store.bars import INTRADAY_FREQS, BarStore, instrument_bucket
from marketdata.store.meta import MetaStore


@dataclass(frozen=True)
class ReconciliationIssue:
    instrument_id: str
    dataset_key: str
    issue: str
    detail: str


@dataclass(frozen=True)
class ReconciliationReport:
    counts: dict[str, int]
    coverage: dict[tuple[str, str], tuple[date, date]]
    issues: tuple[ReconciliationIssue, ...]


def reconcile_canonical(bars: BarStore, meta: MetaStore) -> ReconciliationReport:
    """Conservatively rebuild instrument coverage from canonical files."""
    entries: dict[tuple[str, str], tuple[date, date]] = {}
    issues: list[ReconciliationIssue] = []
    dataset_keys = ("eod", *(f"intraday_{freq}" for freq in INTRADAY_FREQS))
    counts = dict.fromkeys(dataset_keys, 0)
    known_instruments = meta.instrument_ids()

    eod_files = bars.canonical_eod_files()
    if eod_files:
        frame = (
            pl.scan_parquet(eod_files, include_file_paths="_source_path")
            .group_by("instrument_id", "_source_path")
            .agg(pl.col("date").min().alias("lo"), pl.col("date").max().alias("hi"))
            .collect()
        )
        for row in frame.iter_rows(named=True):
            path = Path(row["_source_path"])
            expected_bucket = path.parent.name.removeprefix("bucket=")
            if row["instrument_id"] not in known_instruments:
                issues.append(
                    ReconciliationIssue(
                        row["instrument_id"],
                        "eod",
                        "unknown_instrument",
                        path.relative_to(bars.data_dir).as_posix(),
                    )
                )
                continue
            if instrument_bucket(row["instrument_id"]) != expected_bucket:
                issues.append(
                    ReconciliationIssue(
                        row["instrument_id"],
                        "eod",
                        "wrong_bucket",
                        path.relative_to(bars.data_dir).as_posix(),
                    )
                )
                continue
            entries[(row["instrument_id"], "eod")] = (row["lo"], row["hi"])
            counts["eod"] += 1

    cap = date.today() - timedelta(days=1)
    for freq in INTRADAY_FREQS:
        dataset_key = f"intraday_{freq}"
        files = bars.canonical_intraday_files(freq)
        by_instrument: dict[str, list[tuple[int, date, date]]] = {}
        if files:
            frame = (
                pl.scan_parquet(files, include_file_paths="_source_path")
                .group_by("instrument_id", "_source_path")
                .agg(
                    pl.col("ts").dt.date().min().alias("lo"),
                    pl.col("ts").dt.date().max().alias("hi"),
                )
                .collect()
            )
        else:
            frame = pl.DataFrame()
        for row in frame.iter_rows(named=True):
            path = Path(row["_source_path"])
            year = int(path.parent.parent.name.removeprefix("year="))
            if row["instrument_id"] not in known_instruments:
                issues.append(
                    ReconciliationIssue(
                        row["instrument_id"],
                        dataset_key,
                        "unknown_instrument",
                        path.relative_to(bars.data_dir).as_posix(),
                    )
                )
                continue
            if instrument_bucket(row["instrument_id"]) != path.parent.name.removeprefix(
                "bucket="
            ):
                issues.append(
                    ReconciliationIssue(
                        row["instrument_id"],
                        dataset_key,
                        "wrong_bucket",
                        path.relative_to(bars.data_dir).as_posix(),
                    )
                )
                continue
            by_instrument.setdefault(row["instrument_id"], []).append(
                (year, row["lo"], row["hi"])
            )
        for instrument_id, partitions in sorted(by_instrument.items()):
            years = sorted(year for year, _, _ in partitions)
            expected = list(range(years[0], years[-1] + 1))
            if years != expected:
                issues.append(
                    ReconciliationIssue(
                        instrument_id,
                        dataset_key,
                        "disconnected_years",
                        f"present={years}, expected={expected}",
                    )
                )
                continue
            lo = min(partition[1] for partition in partitions)
            hi = min(max(partition[2] for partition in partitions), cap)
            if lo <= hi:
                entries[(instrument_id, dataset_key)] = (lo, hi)
                counts[dataset_key] += 1
            else:
                issues.append(
                    ReconciliationIssue(
                        instrument_id,
                        dataset_key,
                        "current_day_only",
                        f"earliest bar {lo} is after completed-day cap {cap}",
                    )
                )

    meta.replace_coverage(entries)
    return ReconciliationReport(counts, entries, tuple(issues))


def reconcile_legacy(bars: BarStore, meta: MetaStore) -> dict[str, int]:
    """Rebuild pre-migration ticker coverage without changing generations."""
    entries: dict[tuple[str, str], tuple[date, date]] = {}
    counts = {"eod": 0}
    for ticker in bars.eod_tickers():
        frame = (
            pl.scan_parquet(bars.eod_path(ticker))
            .select(pl.col("date").min().alias("lo"), pl.col("date").max().alias("hi"))
            .collect()
        )
        lo, hi = frame["lo"][0], frame["hi"][0]
        if lo is not None:
            entries[(ticker, "eod")] = (lo, hi)
            counts["eod"] += 1

    intraday_root = bars.data_dir / "intraday"
    if intraday_root.exists():
        cap = date.today() - timedelta(days=1)
        for freq_dir in sorted(
            path for path in intraday_root.iterdir() if path.is_dir()
        ):
            dataset = f"intraday_{freq_dir.name}"
            counts[dataset] = 0
            for ticker_dir in sorted(
                path for path in freq_dir.iterdir() if path.is_dir()
            ):
                files = sorted(ticker_dir.glob("*.parquet"))
                if not files:
                    continue
                frame = (
                    pl.scan_parquet(files)
                    .select(
                        pl.col("ts").dt.date().min().alias("lo"),
                        pl.col("ts").dt.date().max().alias("hi"),
                    )
                    .collect()
                )
                lo, hi = frame["lo"][0], frame["hi"][0]
                if lo is not None and lo <= cap:
                    entries[(ticker_dir.name, dataset)] = (lo, min(hi, cap))
                    counts[dataset] += 1
    meta.replace_ticker_coverage_v1(entries)
    return counts
