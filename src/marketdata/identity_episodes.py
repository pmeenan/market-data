"""One-time and incremental EOD listing-episode repair.

Tiingo's archive occasionally represents two symbol uses as one long listing
envelope.  This module detects conservative, observable boundaries, gives each
continuous episode a stable instrument id, and quarantines rows that cannot be
published without violating canonical OHLC invariants.
"""

from __future__ import annotations

import os
import shutil
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

import duckdb
import polars as pl

from marketdata.calendar import session_schedule
from marketdata.eod_quality import eod_ohlc_invalid_sql
from marketdata.locking import data_directory_locked
from marketdata.store.bars import (
    CANONICAL_EOD_SCHEMA,
    BarStore,
    atomic_write_parquet,
    canonical_bucket_path,
    canonical_dataset_root,
    create_canonical_parquet_view,
    instrument_bucket,
    merge_canonical_frames,
)
from marketdata.store.meta import MetaStore

DEFAULT_EPISODE_GAP_SESSIONS = 252
MIN_EPISODE_GAP_SESSIONS = 63
MIN_INFERRED_EPISODE_ROWS = 20


@dataclass(frozen=True)
class EpisodeBoundary:
    split_date: date
    basis: str
    first_missing_or_zero: date
    last_missing_or_zero: date
    session_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "split_date": self.split_date.isoformat(),
            "basis": self.basis,
            "first_missing_or_zero": self.first_missing_or_zero.isoformat(),
            "last_missing_or_zero": self.last_missing_or_zero.isoformat(),
            "session_count": self.session_count,
        }


@dataclass(frozen=True)
class EpisodePlan:
    instrument_id: str
    display_label: str
    ordinal: int
    alias_start: date
    alias_end: date
    observed_first: date
    observed_last: date
    frame: pl.DataFrame = field(repr=False)


@dataclass(frozen=True)
class SourceRepairPlan:
    source_instrument_id: str
    ticker: str
    exchange: str
    asset_type: str
    lifecycle_status: str
    description: str | None
    boundaries: tuple[EpisodeBoundary, ...]
    episodes: tuple[EpisodePlan, ...]
    quarantined: pl.DataFrame = field(repr=False)


@dataclass
class EodEpisodeRepairResult:
    scanned_instruments: int
    applied: bool = False
    recovered_sources: int = 0
    split_sources: int = 0
    created_episodes: int = 0
    quarantined_rows: int = 0
    backup_path: str | None = None
    quarantine_path: str | None = None
    repairs: list[dict[str, object]] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "scanned_instruments": self.scanned_instruments,
            "applied": self.applied,
            "recovered_sources": self.recovered_sources,
            "split_sources": self.split_sources,
            "created_episodes": self.created_episodes,
            "quarantined_rows": self.quarantined_rows,
            "backup_path": self.backup_path,
            "quarantine_path": self.quarantine_path,
            "repairs": self.repairs,
            "ok": True,
        }


@data_directory_locked("identity:repair-eod-episodes")
def repair_eod_episodes(
    bars: BarStore,
    meta: MetaStore,
    *,
    min_gap_sessions: int = DEFAULT_EPISODE_GAP_SESSIONS,
    apply: bool = False,
) -> EodEpisodeRepairResult:
    """Detect and optionally apply deterministic EOD listing-episode repairs."""
    if min_gap_sessions < MIN_EPISODE_GAP_SESSIONS:
        raise ValueError(
            f"min_gap_sessions must be at least {MIN_EPISODE_GAP_SESSIONS}"
        )
    if meta.storage_generation() != "v2":
        raise RuntimeError("EOD episode repair requires canonical v2 storage")
    files = bars.canonical_eod_files()
    source_rows = meta.eod_identity_sources()
    recovered_sources = 0
    if apply:
        recovered_sources = _finish_interrupted_repairs(bars, meta, source_rows)
        if recovered_sources:
            source_rows = meta.eod_identity_sources()
    sources = {str(row["instrument_id"]): row for row in source_rows}
    result = EodEpisodeRepairResult(scanned_instruments=len(sources))
    result.recovered_sources = recovered_sources
    result.applied = recovered_sources > 0
    if not files:
        return result

    boundaries, invalid = _audit_eod(
        bars, tuple(sources), min_gap_sessions=min_gap_sessions
    )
    invalid_dates_by_source: dict[str, list[date]] = defaultdict(list)
    for row in invalid.iter_rows(named=True):
        invalid_dates_by_source[str(row["instrument_id"])].append(row["date"])
    plans: list[SourceRepairPlan] = []
    quarantine_frames: list[pl.DataFrame] = []
    if not invalid.is_empty():
        quarantine_frames.append(
            invalid.with_columns(quarantine_reason=pl.lit("ohlc_invariants"))
        )

    for source_id, source_boundaries in sorted(boundaries.items()):
        source = sources[source_id]
        coverage = meta.get_coverage(source_id, "eod")
        source_boundaries = tuple(
            boundary
            for boundary in source_boundaries
            if coverage is not None
            and coverage[0] <= boundary.first_missing_or_zero
            and coverage[1] >= boundary.last_missing_or_zero
        )
        if not source_boundaries:
            continue
        frame = bars.read_canonical_eod(source_id)
        if frame is None:
            continue
        source_quarantine = _zero_bridge_rows(frame, source_boundaries)
        if not source_quarantine.is_empty():
            quarantine_frames.append(source_quarantine)
        invalid_dates = invalid_dates_by_source.get(source_id, [])
        clean = frame.filter(~pl.col("date").is_in(invalid_dates))
        for boundary in source_boundaries:
            if boundary.basis == "zero_volume_bridge":
                clean = clean.filter(
                    ~pl.col("date").is_between(
                        boundary.first_missing_or_zero,
                        boundary.last_missing_or_zero,
                    )
                )
        plan, short_episodes = _source_plan(source, clean, source_boundaries)
        if not short_episodes.is_empty():
            quarantine_frames.append(short_episodes)
        if plan is not None:
            plans.append(plan)

    result.split_sources = len(plans)
    result.created_episodes = sum(len(plan.episodes) for plan in plans)
    result.repairs = [
        {
            "source_instrument_id": plan.source_instrument_id,
            "ticker": plan.ticker,
            "boundaries": [item.to_dict() for item in plan.boundaries],
            "episodes": [
                {
                    "instrument_id": episode.instrument_id,
                    "display_label": episode.display_label,
                    "alias_start": episode.alias_start.isoformat(),
                    "alias_end": episode.alias_end.isoformat(),
                    "observed_first": episode.observed_first.isoformat(),
                    "observed_last": episode.observed_last.isoformat(),
                    "rows": episode.frame.height,
                }
                for episode in plan.episodes
            ],
        }
        for plan in plans
    ]
    quarantine = _combined_quarantine(quarantine_frames)
    result.quarantined_rows = quarantine.height
    if not apply or (not plans and quarantine.is_empty()):
        return result

    operation_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    backup_root = bars.data_dir / "backups" / f"eod-episodes-{operation_id}"
    meta.backup_to(backup_root / "meta.db")
    excluded_keys = {
        (str(row["instrument_id"]), row["date"])
        for row in quarantine.iter_rows(named=True)
    }
    staging = _stage_rewritten_root(bars, plans, excluded_keys, operation_id)
    try:
        _register_planned_episodes(meta, plans, min_gap_sessions)
        quarantine_path = _write_quarantine(bars.data_dir, quarantine, operation_id)
        _swap_eod_root(bars.data_dir, staging, backup_root / "bars" / "eod")
        for plan in plans:
            meta.retire_eod_identity_source(plan.source_instrument_id)
            for episode in plan.episodes:
                meta.set_coverage(
                    episode.instrument_id,
                    "eod",
                    episode.alias_start,
                    episode.alias_end,
                )
        for year in meta.universe_years():
            meta.resolve_universe(year)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise

    result.applied = True
    result.backup_path = str(backup_root)
    result.quarantine_path = str(quarantine_path) if quarantine_path else None
    return result


def _finish_interrupted_repairs(
    bars: BarStore, meta: MetaStore, source_rows: list[object]
) -> int:
    """Finish metadata cleanup when a previous run already swapped EOD bars."""
    episodes_by_source: dict[str, list[object]] = defaultdict(list)
    for episode in meta.identity_episodes():
        source_id = episode["source_instrument_id"]
        if source_id is not None and episode["dataset_key"] == "eod":
            episodes_by_source[str(source_id)].append(episode)

    recovered: list[tuple[str, list[object]]] = []
    rolled_back = 0
    for source in source_rows:
        source_id = str(source["instrument_id"])  # type: ignore[index]
        episodes = episodes_by_source.get(source_id)
        if not episodes:
            continue
        present = [
            bars.read_canonical_eod(str(episode["instrument_id"])) is not None
            for episode in episodes
        ]
        if bars.read_canonical_eod(source_id) is not None:
            if any(present):
                raise RuntimeError(
                    "interrupted EOD episode repair exposes both source and "
                    f"replacement bars for {source_id!r}; restore its backup"
                )
            meta.rollback_unpublished_eod_episodes(source_id)
            rolled_back += 1
            continue
        if not all(present):
            raise RuntimeError(
                "interrupted EOD episode repair has neither its source history "
                f"nor every replacement episode for {source_id!r}; restore its "
                "recorded backup before retrying"
            )
        recovered.append((source_id, episodes))

    if not recovered:
        if rolled_back:
            for year in meta.universe_years():
                meta.resolve_universe(year)
        return rolled_back
    for source_id, episodes in recovered:
        meta.retire_eod_identity_source(source_id)
        for episode in episodes:
            instrument_id = str(episode["instrument_id"])
            aliases = meta.instrument_alias_records(instrument_id)
            if len(aliases) != 1:
                raise RuntimeError(
                    f"replacement episode {instrument_id!r} does not have exactly "
                    "one alias"
                )
            meta.set_coverage(
                instrument_id,
                "eod",
                date.fromisoformat(str(aliases[0]["start_date"])),
                date.fromisoformat(str(aliases[0]["end_date"])),
            )
    for year in meta.universe_years():
        meta.resolve_universe(year)
    return rolled_back + len(recovered)


@data_directory_locked("identity:recover-eod-episodes")
def recover_interrupted_eod_episode_repairs(bars: BarStore, meta: MetaStore) -> int:
    """Restore a usable identity registry after either repair crash boundary."""
    return _finish_interrupted_repairs(bars, meta, meta.eod_identity_sources())


def _audit_eod(
    bars: BarStore,
    source_ids: tuple[str, ...],
    *,
    min_gap_sessions: int,
) -> tuple[dict[str, tuple[EpisodeBoundary, ...]], pl.DataFrame]:
    con = duckdb.connect()
    create_canonical_parquet_view(con, "audit_eod", bars.canonical_eod_glob())
    bounds = con.execute("SELECT min(date), max(date) FROM audit_eod").fetchone()
    if bounds[0] is None:
        return {}, pl.DataFrame(schema=CANONICAL_EOD_SCHEMA)
    schedule = session_schedule(bounds[0], bounds[1]).select("session_date")
    schedule = schedule.with_row_index("session_index")
    con.register("audit_schedule", schedule)
    con.register(
        "audit_sources",
        pl.DataFrame(
            {"instrument_id": list(source_ids)}, schema={"instrument_id": pl.Utf8}
        ),
    )

    missing = con.execute(
        """WITH ordered AS (
               SELECT bars.instrument_id, bars.date, schedule.session_index,
                      lag(bars.date) OVER bar_window AS previous_date,
                      lag(schedule.session_index) OVER bar_window AS previous_index
               FROM audit_eod AS bars
               JOIN audit_sources USING (instrument_id)
               JOIN audit_schedule AS schedule
                 ON bars.date = schedule.session_date
               WINDOW bar_window AS (
                   PARTITION BY bars.instrument_id ORDER BY bars.date
               )
           )
           SELECT ordered.instrument_id, ordered.date AS split_date,
                  first_missing.session_date, last_missing.session_date,
                  ordered.session_index - ordered.previous_index - 1
                      AS missing_sessions
           FROM ordered
           JOIN audit_schedule AS first_missing
             ON first_missing.session_index = ordered.previous_index + 1
           JOIN audit_schedule AS last_missing
             ON last_missing.session_index = ordered.session_index - 1
           WHERE ordered.session_index - ordered.previous_index - 1 >= ?
           ORDER BY ordered.instrument_id, split_date""",
        [min_gap_sessions],
    ).fetchall()
    zero_bridges = con.execute(
        """WITH numbered AS (
               SELECT bars.instrument_id, bars.date, bars.volume,
                      row_number() OVER bar_window AS row_number,
                      sum(CASE WHEN bars.volume = 0 THEN 0 ELSE 1 END)
                          OVER bar_window AS run_group
               FROM audit_eod AS bars
               JOIN audit_sources USING (instrument_id)
               JOIN audit_schedule AS schedule
                 ON bars.date = schedule.session_date
               WINDOW bar_window AS (
                   PARTITION BY bars.instrument_id ORDER BY bars.date
               )
           ), bounds AS (
               SELECT instrument_id, min(row_number) AS first_row,
                      max(row_number) AS last_row
               FROM numbered GROUP BY instrument_id
           ), runs AS (
               SELECT instrument_id, run_group, min(date) AS first_date,
                      max(date) AS last_date, count(*) AS run_length,
                      min(row_number) AS first_row, max(row_number) AS last_row
               FROM numbered WHERE volume = 0
               GROUP BY instrument_id, run_group
               HAVING count(*) >= ?
           )
           SELECT runs.instrument_id, runs.first_date, runs.last_date,
                  next_bar.date AS split_date, runs.run_length
           FROM runs
           JOIN bounds USING (instrument_id)
           JOIN LATERAL (
               SELECT min(numbered.date) AS date FROM numbered
               WHERE numbered.instrument_id = runs.instrument_id
                 AND numbered.row_number > runs.last_row
                 AND numbered.volume != 0
           ) AS next_bar ON true
           WHERE runs.first_row > bounds.first_row
             AND runs.last_row < bounds.last_row
             AND next_bar.date IS NOT NULL
           ORDER BY runs.instrument_id, split_date""",
        [min_gap_sessions],
    ).fetchall()
    invalid = con.execute(
        f"""SELECT audit_eod.* FROM audit_eod
            JOIN audit_sources USING (instrument_id)
            WHERE {eod_ohlc_invalid_sql()}
            ORDER BY instrument_id, date"""
    ).pl()
    con.close()

    grouped: dict[str, list[EpisodeBoundary]] = defaultdict(list)
    for instrument_id, split_date, first_missing, last_missing, count in missing:
        grouped[str(instrument_id)].append(
            EpisodeBoundary(
                split_date=split_date,
                basis="missing_sessions",
                first_missing_or_zero=first_missing,
                last_missing_or_zero=last_missing,
                session_count=int(count),
            )
        )
    for instrument_id, first, last, split_date, count in zero_bridges:
        grouped[str(instrument_id)].append(
            EpisodeBoundary(
                split_date=split_date,
                basis="zero_volume_bridge",
                first_missing_or_zero=first,
                last_missing_or_zero=last,
                session_count=int(count),
            )
        )
    return {
        instrument_id: tuple(
            sorted(items, key=lambda item: (item.split_date, item.basis))
        )
        for instrument_id, items in grouped.items()
    }, invalid


def _zero_bridge_rows(
    frame: pl.DataFrame, boundaries: tuple[EpisodeBoundary, ...]
) -> pl.DataFrame:
    ranges = [item for item in boundaries if item.basis == "zero_volume_bridge"]
    if not ranges:
        return pl.DataFrame(
            schema=CANONICAL_EOD_SCHEMA | {"quarantine_reason": pl.Utf8}
        )
    predicate = pl.any_horizontal(
        *(
            pl.col("date").is_between(
                item.first_missing_or_zero, item.last_missing_or_zero
            )
            for item in ranges
        )
    )
    return frame.filter(predicate).with_columns(
        quarantine_reason=pl.lit("zero_volume_episode_bridge")
    )


def _source_plan(
    source: object,
    clean: pl.DataFrame,
    boundaries: tuple[EpisodeBoundary, ...],
) -> tuple[SourceRepairPlan | None, pl.DataFrame]:
    quarantine_schema = CANONICAL_EOD_SCHEMA | {"quarantine_reason": pl.Utf8}
    if clean.is_empty():
        return None, pl.DataFrame(schema=quarantine_schema)
    source_id = str(source["instrument_id"])  # type: ignore[index]
    ticker = str(source["ticker"])  # type: ignore[index]
    alias_start = date.fromisoformat(str(source["start_date"]))  # type: ignore[index]
    alias_end = date.fromisoformat(str(source["end_date"]))  # type: ignore[index]
    split_dates = sorted({item.split_date for item in boundaries})
    parts: list[pl.DataFrame] = []
    for ordinal in range(len(split_dates) + 1):
        lower = split_dates[ordinal - 1] if ordinal else None
        upper = split_dates[ordinal] if ordinal < len(split_dates) else None
        part = clean
        if lower is not None:
            part = part.filter(pl.col("date") >= lower)
        if upper is not None:
            part = part.filter(pl.col("date") < upper)
        if not part.is_empty():
            parts.append(part)
    short_parts = [part for part in parts if part.height < MIN_INFERRED_EPISODE_ROWS]
    substantial_parts = [
        part for part in parts if part.height >= MIN_INFERRED_EPISODE_ROWS
    ]
    short = (
        pl.concat(short_parts)
        .with_columns(quarantine_reason=pl.lit("short_inferred_episode"))
        .select(quarantine_schema.keys())
        if short_parts
        else pl.DataFrame(schema=quarantine_schema)
    )
    if len(substantial_parts) < 2:
        return None, short

    episodes: list[EpisodePlan] = []
    for ordinal, part in enumerate(substantial_parts, start=1):
        observed_first = part["date"].min()
        observed_last = part["date"].max()
        episode_start = alias_start if ordinal == 1 else observed_first
        episode_end = alias_end if ordinal == len(substantial_parts) else observed_last
        identity_anchor = "|".join(
            (
                "market-data-eod-episode-v1",
                source_id,
                ticker,
                episode_start.isoformat(),
                str(ordinal),
            )
        )
        instrument_id = uuid5(NAMESPACE_URL, identity_anchor).hex
        episodes.append(
            EpisodePlan(
                instrument_id=instrument_id,
                display_label=f"{ticker}@{episode_start:%Y%m%d}",
                ordinal=ordinal,
                alias_start=episode_start,
                alias_end=episode_end,
                observed_first=observed_first,
                observed_last=observed_last,
                frame=part.with_columns(instrument_id=pl.lit(instrument_id)).select(
                    CANONICAL_EOD_SCHEMA.keys()
                ),
            )
        )
    return (
        SourceRepairPlan(
            source_instrument_id=source_id,
            ticker=ticker,
            exchange=str(source["exchange"]),  # type: ignore[index]
            asset_type=str(source["asset_type"]),  # type: ignore[index]
            lifecycle_status=str(source["lifecycle_status"]),  # type: ignore[index]
            description=source["description"],  # type: ignore[index]
            boundaries=boundaries,
            episodes=tuple(episodes),
            quarantined=short,
        ),
        short,
    )


def _combined_quarantine(frames: list[pl.DataFrame]) -> pl.DataFrame:
    schema = CANONICAL_EOD_SCHEMA | {"quarantine_reason": pl.Utf8}
    if not frames:
        return pl.DataFrame(schema=schema)
    return (
        pl.concat([frame.select(schema.keys()).cast(schema) for frame in frames])
        .unique(subset=["instrument_id", "date"], keep="first")
        .sort(["instrument_id", "date"])
    )


def _register_planned_episodes(
    meta: MetaStore,
    plans: list[SourceRepairPlan],
    min_gap_sessions: int,
) -> None:
    for plan in plans:
        evidence = {
            "source": "canonical-eod-listing-episode-repair",
            "source_instrument_id": plan.source_instrument_id,
            "minimum_gap_sessions": min_gap_sessions,
            "boundaries": [item.to_dict() for item in plan.boundaries],
        }
        for episode in plan.episodes:
            meta.upsert_instrument(
                episode.instrument_id,
                lifecycle_status=plan.lifecycle_status,
                description=plan.description,
            )
            meta.add_instrument_alias(
                episode.instrument_id,
                plan.ticker,
                episode.alias_start,
                episode.alias_end,
                exchange=plan.exchange,
                asset_type=plan.asset_type,
                evidence=evidence,
            )
            meta.add_vendor_identifier(
                episode.instrument_id,
                "eod",
                "ticker",
                plan.ticker,
                episode.alias_start,
                episode.alias_end,
                validation_state="validated",
                evidence=evidence,
            )
            meta.record_identity_episode(
                episode.instrument_id,
                source_instrument_id=plan.source_instrument_id,
                dataset_key="eod",
                ticker=plan.ticker,
                display_label=episode.display_label,
                episode_ordinal=episode.ordinal,
                basis="observed_gap",
                confidence="inferred",
                observed_first=episode.observed_first,
                observed_last=episode.observed_last,
                evidence=evidence,
            )


def _stage_rewritten_root(
    bars: BarStore,
    plans: list[SourceRepairPlan],
    excluded_keys: set[tuple[str, date]],
    operation_id: str,
) -> Path:
    live_root = canonical_dataset_root(bars.data_dir, "eod")
    staging = live_root.parent / f".eod-episodes-{operation_id}"
    staging.mkdir(parents=True)
    for source in bars.canonical_eod_files():
        target = staging / source.relative_to(live_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source, target)
        except OSError:
            shutil.copy2(source, target)

    split_sources = {plan.source_instrument_id for plan in plans}
    additions: dict[str, list[pl.DataFrame]] = defaultdict(list)
    impacted = {instrument_bucket(source_id) for source_id in split_sources}
    impacted.update(
        instrument_bucket(instrument_id) for instrument_id, _ in excluded_keys
    )
    for plan in plans:
        for episode in plan.episodes:
            bucket = instrument_bucket(episode.instrument_id)
            impacted.add(bucket)
            additions[bucket].append(episode.frame)

    bad_frame = pl.DataFrame(
        {
            "instrument_id": [item[0] for item in excluded_keys],
            "date": [item[1] for item in excluded_keys],
        },
        schema={"instrument_id": pl.Utf8, "date": pl.Date},
    )
    for bucket in sorted(impacted):
        live_path = canonical_bucket_path(bars.data_dir, "eod", bucket)
        existing = pl.read_parquet(live_path) if live_path.exists() else None
        if existing is not None and split_sources:
            existing = existing.filter(~pl.col("instrument_id").is_in(split_sources))
        if existing is not None and not bad_frame.is_empty():
            existing = existing.join(
                bad_frame, on=["instrument_id", "date"], how="anti"
            )
        incoming = (
            pl.concat(additions[bucket])
            if additions[bucket]
            else pl.DataFrame(schema=CANONICAL_EOD_SCHEMA)
        )
        frame = merge_canonical_frames(
            existing,
            incoming,
            key=["instrument_id", "date"],
            source=str(live_path),
        )
        staged_path = staging / f"bucket={bucket}" / "bars.parquet"
        if frame.is_empty():
            if staged_path.exists():
                staged_path.unlink()
            continue
        atomic_write_parquet(frame, staged_path)
    _validate_staging(staging, impacted)
    return staging


def _validate_staging(staging: Path, rewritten_buckets: set[str]) -> None:
    expected_schema = pl.Schema(CANONICAL_EOD_SCHEMA)
    paths = [
        staging / f"bucket={bucket}" / "bars.parquet" for bucket in rewritten_buckets
    ]
    for path in sorted(path for path in paths if path.exists()):
        if pl.read_parquet_schema(path) != expected_schema:
            raise ValueError(f"staged canonical schema mismatch in {path}")
        frame = pl.read_parquet(path)
        expected_bucket = path.parent.name.removeprefix("bucket=")
        wrong_ids = [
            instrument_id
            for instrument_id in frame["instrument_id"].unique()
            if instrument_bucket(instrument_id) != expected_bucket
        ]
        if wrong_ids:
            raise ValueError(f"staged rows have the wrong bucket in {path}")
        if frame.select(
            pl.struct(["instrument_id", "date"]).is_duplicated().any()
        ).item():
            raise ValueError(f"staged rows have duplicate canonical keys in {path}")


def _write_quarantine(
    data_dir: Path, frame: pl.DataFrame, operation_id: str
) -> Path | None:
    if frame.is_empty():
        return None
    path = data_dir / "quarantine" / "eod-quality" / f"{operation_id}.parquet"
    atomic_write_parquet(frame, path)
    return path


def _swap_eod_root(data_dir: Path, staging: Path, backup: Path) -> None:
    live = canonical_dataset_root(data_dir, "eod")
    if not live.is_dir():
        raise FileNotFoundError(f"canonical EOD root disappeared: {live}")
    if backup.exists():
        raise FileExistsError(f"canonical EOD backup already exists: {backup}")
    backup.parent.mkdir(parents=True, exist_ok=True)
    live.rename(backup)
    try:
        staging.rename(live)
    except BaseException:
        backup.rename(live)
        raise
