"""Generation-safe migration from quarantined v1 bars to canonical v2."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl

from marketdata.reconcile import ReconciliationIssue, reconcile_canonical
from marketdata.store.bars import BarStore, instrument_bucket
from marketdata.store.meta import MetaStore

MigrationStatus = Literal[
    "migrated", "unresolved", "conflict", "invalid_source", "publish_failed"
]

QUARANTINE_V1_RELATIVE = Path("quarantine/v1-ticker-bars")
DEFAULT_MIGRATION_REPORT = "migration-report.json"


@dataclass(frozen=True)
class MigrationItem:
    source: str
    dataset_key: str
    ticker: str | None
    first_date: str | None
    last_date: str | None
    rows: int | None
    status: MigrationStatus
    instrument_ids: tuple[str, ...] = ()
    target: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class MigrationReport:
    items: tuple[MigrationItem, ...]
    reconciliation_issues: tuple[ReconciliationIssue, ...]

    def counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in self.items:
            counts[item.status] = counts.get(item.status, 0) + 1
        return dict(sorted(counts.items()))

    def to_dict(self) -> dict:
        return {
            "schema_version": 1,
            "counts": self.counts(),
            "items": [asdict(item) for item in self.items],
            "reconciliation_issues": [
                asdict(issue) for issue in self.reconciliation_issues
            ],
        }


@dataclass
class _Candidate:
    item_index: int
    instrument_id: str
    dataset_key: str
    year: int | None
    source: Path


def default_migration_report_path(data_dir: Path) -> Path:
    return Path(data_dir) / QUARANTINE_V1_RELATIVE / DEFAULT_MIGRATION_REPORT


def migrate_v1_bars(
    bars: BarStore, meta: MetaStore, report_path: Path | None = None
) -> MigrationReport:
    """Quarantine v1 roots, resolve complete files, and publish safe rows.

    Sources remain in quarantine after successful copies. A rerun therefore
    re-evaluates current identity evidence and converges via canonical upsert.
    """
    destination = report_path or default_migration_report_path(bars.data_dir)
    try:
        quarantine_root = _quarantine_v1_roots(bars.data_dir, meta.storage_generation())
        meta.activate_canonical_generation()
    except (OSError, RuntimeError) as exc:
        report = MigrationReport(
            (
                MigrationItem(
                    source="<generation-boundary>",
                    dataset_key="all",
                    ticker=None,
                    first_date=None,
                    last_date=None,
                    rows=None,
                    status="publish_failed",
                    detail=str(exc),
                ),
            ),
            (),
        )
        try:
            _write_json_atomic(report.to_dict(), destination)
        except OSError:
            pass
        raise RuntimeError(
            f"could not establish v2 generation boundary: {exc}"
        ) from exc
    items: list[MigrationItem] = []
    candidates: list[_Candidate] = []

    for source, dataset_key, expected_year in _legacy_files(quarantine_root):
        relative_source = source.relative_to(bars.data_dir).as_posix()
        try:
            ticker, first, last, row_count = _source_envelope(
                source, dataset_key, expected_year
            )
        except Exception as exc:
            items.append(
                MigrationItem(
                    source=relative_source,
                    dataset_key=dataset_key,
                    ticker=None,
                    first_date=None,
                    last_date=None,
                    rows=None,
                    status="invalid_source",
                    detail=str(exc),
                )
            )
            continue

        resolution = meta.resolve_alias_range(ticker, first, last)
        instrument_ids = tuple(
            sorted(
                {
                    instrument_id
                    for segment in resolution.segments
                    for instrument_id in segment.instrument_ids
                }
            )
        )
        resolved_ids = {
            segment.instrument_id
            for segment in resolution.segments
            if segment.status == "resolved"
        }
        fully_resolved = all(
            segment.status == "resolved" for segment in resolution.segments
        )
        if not fully_resolved or len(resolved_ids) != 1:
            if len(resolved_ids) > 1:
                status: MigrationStatus = "conflict"
                detail = "source range resolves to multiple successive instruments"
            elif any(
                segment.status == "multiple_matches" for segment in resolution.segments
            ):
                status = "conflict"
                detail = "; ".join(
                    f"{segment.start}..{segment.end}:{segment.status}"
                    for segment in resolution.segments
                    if segment.status == "multiple_matches"
                )
            else:
                status = "unresolved"
                detail = "; ".join(
                    f"{segment.start}..{segment.end}:{segment.status}"
                    for segment in resolution.segments
                    if segment.status != "resolved"
                )
            items.append(
                MigrationItem(
                    source=relative_source,
                    dataset_key=dataset_key,
                    ticker=ticker,
                    first_date=first.isoformat(),
                    last_date=last.isoformat(),
                    rows=row_count,
                    status=status,
                    instrument_ids=instrument_ids,
                    detail=detail,
                )
            )
            continue

        instrument_id = next(iter(resolved_ids))
        year = expected_year if dataset_key != "eod" else None
        target = _target_path(bars, instrument_id, dataset_key, year)
        item_index = len(items)
        items.append(
            MigrationItem(
                source=relative_source,
                dataset_key=dataset_key,
                ticker=ticker,
                first_date=first.isoformat(),
                last_date=last.isoformat(),
                rows=row_count,
                status="migrated",
                instrument_ids=(instrument_id,),
                target=target.relative_to(bars.data_dir).as_posix(),
            )
        )
        candidates.append(
            _Candidate(item_index, instrument_id, dataset_key, year, source)
        )

    grouped: dict[tuple[str, int | None, str], list[_Candidate]] = {}
    for candidate in candidates:
        group = (
            candidate.dataset_key,
            candidate.year,
            instrument_bucket(candidate.instrument_id),
        )
        grouped.setdefault(group, []).append(candidate)

    for (dataset_key, _year, _bucket), group_candidates in sorted(grouped.items()):
        by_instrument: dict[str, list[_Candidate]] = {}
        for candidate in group_candidates:
            by_instrument.setdefault(candidate.instrument_id, []).append(candidate)

        ready: dict[str, pl.DataFrame] = {}
        for instrument_id, instrument_candidates in by_instrument.items():
            try:
                ready[instrument_id] = pl.read_parquet(
                    [candidate.source for candidate in instrument_candidates]
                )
            except Exception as exc:
                _mark_publish_failed(items, instrument_candidates, exc)

        if not ready:
            continue
        try:
            _publish_group(bars, dataset_key, ready)
        except Exception:
            # Frame validation failures are instrument-local. Retry separately
            # so one malformed source does not block unrelated bucket peers.
            for instrument_id, frame in ready.items():
                try:
                    _publish_group(bars, dataset_key, {instrument_id: frame})
                except Exception as exc:
                    _mark_publish_failed(items, by_instrument[instrument_id], exc)

    reconciliation = reconcile_canonical(bars, meta)
    report = MigrationReport(tuple(items), reconciliation.issues)
    _write_json_atomic(report.to_dict(), destination)
    return report


def _publish_group(
    bars: BarStore, dataset_key: str, frames: dict[str, pl.DataFrame]
) -> None:
    if dataset_key == "eod":
        bars.publish_eod(frames)
    else:
        bars.publish_intraday(frames, freq=dataset_key.removeprefix("intraday_"))


def _mark_publish_failed(
    items: list[MigrationItem], candidates: list[_Candidate], exc: Exception
) -> None:
    for candidate in candidates:
        items[candidate.item_index] = replace(
            items[candidate.item_index], status="publish_failed", detail=str(exc)
        )


def _quarantine_v1_roots(data_dir: Path, generation: str) -> Path:
    quarantine = data_dir / QUARANTINE_V1_RELATIVE
    sources = (
        (data_dir / "eod", quarantine / "eod"),
        (data_dir / "intraday", quarantine / "intraday"),
    )
    for source, target in sources:
        if generation == "v2" and source.exists():
            if source.is_dir() and not any(source.iterdir()):
                source.rmdir()
                continue
            raise RuntimeError(
                f"legacy data reappeared after the v2 boundary: {source}"
            )
        if source.exists() and target.exists():
            # Transitional ``init``/legacy setup may recreate an empty v1 EOD
            # root after the boundary. It carries no data and must not make an
            # otherwise safe migration retry impossible.
            if source.is_dir() and not any(source.iterdir()):
                source.rmdir()
                continue
            raise RuntimeError(
                f"both legacy source and quarantine target exist: {source}, {target}"
            )
    quarantine.mkdir(parents=True, exist_ok=True)
    # Complete the generation boundary before the caller publishes anything.
    if generation == "v1":
        for source, target in sources:
            if source.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                source.replace(target)
    return quarantine


def _legacy_files(root: Path) -> list[tuple[Path, str, int | None]]:
    out: list[tuple[Path, str, int | None]] = []
    out.extend((path, "eod", None) for path in sorted((root / "eod").glob("*.parquet")))
    intraday = root / "intraday"
    if intraday.exists():
        for freq_dir in sorted(path for path in intraday.iterdir() if path.is_dir()):
            dataset_key = f"intraday_{freq_dir.name}"
            for path in sorted(freq_dir.glob("*/*.parquet")):
                try:
                    year = int(path.stem)
                except ValueError:
                    year = None
                out.append((path, dataset_key, year))
    return out


def _source_envelope(
    source: Path, dataset_key: str, expected_year: int | None
) -> tuple[str, date, date, int]:
    if dataset_key not in {"eod", "intraday_1hour", "intraday_5min"}:
        raise ValueError(f"unsupported legacy dataset {dataset_key!r}")
    schema = pl.read_parquet_schema(source)
    if "ticker" not in schema:
        raise ValueError("legacy file has no ticker column")
    time_column = "date" if dataset_key == "eod" else "ts"
    if time_column not in schema:
        raise ValueError(f"legacy file has no {time_column} column")
    time_expr = pl.col("date") if dataset_key == "eod" else pl.col("ts").dt.date()
    expressions = [
        pl.len().alias("rows"),
        pl.col("ticker").drop_nulls().n_unique().alias("ticker_count"),
        pl.col("ticker").drop_nulls().first().alias("ticker"),
        time_expr.min().alias("first"),
        time_expr.max().alias("last"),
    ]
    if dataset_key != "eod":
        expressions.extend(
            [
                pl.col("ts").dt.year().min().alias("first_year"),
                pl.col("ts").dt.year().max().alias("last_year"),
            ]
        )
    summary = pl.scan_parquet(source).select(expressions).collect().row(0, named=True)
    if summary["rows"] == 0:
        raise ValueError("empty legacy file has no resolvable identity envelope")
    if summary["ticker_count"] != 1:
        raise ValueError(
            "legacy file must contain one non-null ticker, "
            f"found {summary['ticker_count']}"
        )
    ticker = summary["ticker"].strip().upper()
    if not ticker:
        raise ValueError("legacy ticker is empty")
    first, last = summary["first"], summary["last"]
    if type(first) is not date or type(last) is not date:
        raise ValueError(
            f"legacy {time_column} envelope contains null or invalid values"
        )
    if dataset_key != "eod":
        if expected_year is None:
            raise ValueError("intraday filename is not a numeric year")
        if (
            summary["first_year"] != expected_year
            or summary["last_year"] != expected_year
        ):
            raise ValueError(
                f"intraday file year {expected_year} contains timestamp years "
                f"{summary['first_year']}..{summary['last_year']}"
            )
    return ticker, first, last, int(summary["rows"])


def _target_path(
    bars: BarStore,
    instrument_id: str,
    dataset_key: str,
    year: int | None,
) -> Path:
    if dataset_key == "eod":
        return bars.canonical_eod_path(instrument_id)
    if year is None:
        raise ValueError("intraday target requires year")
    return bars.canonical_intraday_path(
        instrument_id, year, dataset_key.removeprefix("intraday_")
    )


def _write_json_atomic(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
