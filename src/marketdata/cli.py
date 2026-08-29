"""market-data CLI: manage the universe, backfill, update, and query."""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
import duckdb
import polars as pl
import requests

from marketdata import universe as universe_mod
from marketdata.backfill_program import (
    DEFAULT_PHASE1_EOD_JOB_ID,
    DEFAULT_PHASE1_HOURLY_JOB_ID,
    DEFAULT_PROGRAM_ID,
    BackfillProgramStepResult,
    initialize_default_backfill_program,
    run_backfill_program_step,
)
from marketdata.calendar import max_intraday_probe_sessions
from marketdata.config import Config, load_config
from marketdata.identity import DATASET_KEYS
from marketdata.identity_bootstrap import (
    IdentityBootstrapResult,
    IntradayIdentityBootstrapResult,
    bootstrap_eod_identities,
    bootstrap_intraday_identities,
)
from marketdata.identity_episodes import (
    DEFAULT_EPISODE_GAP_SESSIONS,
    MIN_EPISODE_GAP_SESSIONS,
    EodEpisodeRepairResult,
    recover_interrupted_eod_episode_repairs,
    repair_eod_episodes,
)
from marketdata.ingest import (
    DEFAULT_INTRADAY_FREQ,
    IngestResult,
)
from marketdata.locking import DataDirectoryLock
from marketdata.migration import (
    _write_json_atomic,
    default_migration_report_path,
    migrate_v1_bars,
)
from marketdata.quality import (
    DEFAULT_ZERO_VOLUME_RUN_LENGTH,
    MIN_ZERO_VOLUME_RUN_LENGTH,
    QUALITY_CHECKS,
    check_quality,
    evaluate_quality,
)
from marketdata.reconcile import reconcile_active
from marketdata.research import (
    reconcile_research_state,
    run_registered_event_study,
)
from marketdata.scheduler import (
    DEFAULT_BUDGET_POLICY,
    IngestionCycleResult,
    SchedulerRunResult,
    cancel_history_job,
    run_history_request,
    run_ingestion_cycle,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import INTRADAY_FREQS
from marketdata.tiingo import TiingoClient, TiingoError

_DATA_OPERATION_ERRORS = (
    OSError,
    RuntimeError,
    ValueError,
    sqlite3.Error,
    duckdb.Error,
    pl.exceptions.PolarsError,
    requests.RequestException,
    TiingoError,
)
_STATUS_DETAIL_LIMIT = 100


def _client(config: Config) -> TiingoClient:
    if not config.tiingo_token:
        message = "TIINGO_API_TOKEN is not set (put it in .env or the environment)"
        raise click.ClickException(message)
    return TiingoClient(config.tiingo_token)


def _operational_client(config: Config) -> TiingoClient:
    """Build a client while making configuration failure status-reportable."""
    try:
        return _client(config)
    except click.ClickException as exc:
        raise TiingoError(exc.format_message()) from exc


class _IngestOperationalError(click.ClickException):
    """Operational ingestion failure, distinct from an identity-only partial."""

    exit_code = 2


def _require_ingestion_ready(meta: MetaStore) -> None:
    if meta.storage_generation() != "v2":
        raise click.ClickException(
            "production ingestion remains paused: migrate the warehouse to v2 first"
        )


def _require_initialized_warehouse(config: Config) -> None:
    if not config.data_dir.is_dir() or not config.meta_path.is_file():
        raise click.ClickException(
            f"warehouse is not initialized at {config.data_dir}; run init first"
        )


def _finish_ingest(
    result: IngestResult,
    summary_json: str | None,
    scheduler: SchedulerRunResult | None = None,
    episode_repair: EodEpisodeRepairResult | None = None,
) -> None:
    click.echo(f"Done: {result.summary()}")
    if scheduler is not None:
        click.echo(
            f"Scheduler {scheduler.job_id}: {scheduler.job_status}; "
            f"sweep {scheduler.sweep_started} -> {scheduler.sweep_ended}; "
            f"{scheduler.attempted_units} attempted, "
            f"{scheduler.advanced_units} advanced"
        )
        if scheduler.stop_reason:
            click.echo(f"Scheduler stopped cleanly: {scheduler.stop_reason}")
    if summary_json:
        payload = scheduler.to_dict() if scheduler is not None else result.to_dict()
        if episode_repair is not None:
            payload["episode_repair"] = episode_repair.to_dict()
        _write_ingest_json(payload, summary_json)
    if result.failed:
        click.echo("Failed segments: " + ", ".join(sorted(result.failed)), err=True)
    if result.blocked:
        click.echo(
            "Identity-blocked segments: " + ", ".join(sorted(result.blocked)),
            err=True,
        )
    if result.partial:
        raise click.exceptions.Exit(1)


def _finish_ingestion_cycle(
    cycle: IngestionCycleResult,
    summary_json: str | None,
    *,
    status_json: str | None = None,
    operation: dict[str, Any] | None = None,
    identity_bootstrap: IdentityBootstrapResult | None = None,
) -> None:
    result = cycle.current
    click.echo(f"Done: {result.summary()}")
    if cycle.stop_reason:
        clean = cycle.stop_reason.endswith("_limit")
        qualifier = " stopped cleanly" if clean else " stopped"
        click.echo(f"Current collection{qualifier}: {cycle.stop_reason}")
    if summary_json:
        payload = cycle.to_dict()
        if operation is not None:
            payload["operation"] = operation
        if identity_bootstrap is not None:
            payload["identity_bootstrap"] = identity_bootstrap.to_dict()
            payload["ok"] = payload["ok"] and identity_bootstrap.ok
        _write_ingest_json(payload, summary_json)
    if status_json:
        _write_ingest_json(
            _bounded_current_status(cycle, operation, identity_bootstrap),
            status_json,
        )
    if result.failed:
        click.echo("Failed segments: " + ", ".join(sorted(result.failed)), err=True)
    if result.blocked:
        click.echo(
            "Identity-blocked segments: " + ", ".join(sorted(result.blocked)),
            err=True,
        )
    if identity_bootstrap is not None and identity_bootstrap.operational_failure:
        raise _IngestOperationalError("scheduled current collection failed")
    if cycle.partial or (identity_bootstrap is not None and identity_bootstrap.partial):
        raise click.exceptions.Exit(1)


def _operation_record(started_at: datetime, client: Any | None) -> dict[str, Any]:
    """Return bounded run telemetry for the replace-in-place status record."""
    return {
        "kind": "current_eod_update",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "request_attempts": int(getattr(client, "request_count", 0)),
        "observed_response_bytes": int(getattr(client, "response_bytes", 0)),
    }


def _bounded_details(values: dict[str, Any]) -> tuple[dict[str, Any], int]:
    selected = dict(sorted(values.items())[:_STATUS_DETAIL_LIMIT])
    return selected, len(values) - len(selected)


def _bounded_result(payload: dict[str, Any]) -> dict[str, Any]:
    """Bound details while deriving every field from a canonical serializer."""
    result: dict[str, Any] = {}
    counts: dict[str, int] = {}
    omitted_details: dict[str, int] = {}
    for name, value in payload.items():
        if isinstance(value, list):
            counts[name] = len(value)
        elif isinstance(value, dict):
            details, omitted = _bounded_details(value)
            counts[name] = len(value)
            result[name] = details
            omitted_details[name] = omitted
        elif isinstance(value, int) and not isinstance(value, bool):
            counts[name] = value
        else:
            result[name] = value
    result["counts"] = counts
    result["omitted_details"] = omitted_details
    return result


def _bounded_current_status(
    cycle: IngestionCycleResult,
    operation: dict[str, Any] | None,
    identity_bootstrap: IdentityBootstrapResult | None,
) -> dict[str, Any]:
    canonical_cycle = cycle.to_dict()
    payload: dict[str, Any] = {
        "operation": operation,
        "current": _bounded_result(canonical_cycle["current"]),
        "stop_reason": canonical_cycle["stop_reason"],
        "ok": canonical_cycle["ok"],
    }
    if identity_bootstrap is not None:
        payload["identity_bootstrap"] = _bounded_result(identity_bootstrap.to_dict())
        payload["ok"] = payload["ok"] and identity_bootstrap.ok
    return payload


def _bounded_program_status(result: BackfillProgramStepResult) -> dict[str, Any]:
    payload = result.to_dict()
    if payload["identity"] is not None:
        payload["identity"] = _bounded_result(payload["identity"])
    if payload["history"] is not None:
        payload["history"] = _bounded_result(payload["history"])
    return payload


def _write_ingest_json(payload: dict[str, Any], path: str) -> None:
    """Publish an ingestion report or fail with the operational exit code."""
    try:
        _write_json_atomic(payload, Path(path))
    except (OSError, TypeError, ValueError) as exc:
        raise _IngestOperationalError(
            f"could not write ingestion report {path}: {exc}"
        ) from exc


def _raise_ingest_error(
    exc: Exception,
    summary_json: str | None,
    *,
    operational: bool = False,
    operation: dict[str, Any] | None = None,
) -> None:
    if summary_json:
        diagnostic = str(exc)
        if len(diagnostic) > 1_000:
            diagnostic = diagnostic[:997] + "..."
        payload = {
            "ok": False,
            "error": diagnostic,
            "failed": {},
            "blocked": {},
        }
        if operation is not None:
            payload["operation"] = operation
        _write_ingest_json(payload, summary_json)
    error_type = _IngestOperationalError if operational else click.ClickException
    raise error_type(str(exc)) from exc


def _echo_identity_bootstrap(result: IdentityBootstrapResult) -> None:
    click.echo(
        "Identity bootstrap: "
        f"{len(result.validated)} validated, {len(result.skipped)} already valid, "
        f"{result.registered_episodes} episodes registered, "
        f"{len(result.overlaps)} overlap-blocked, "
        f"{len(result.blocked)} blocked, {len(result.failed)} failed"
    )
    if result.stop_reason:
        click.echo(f"Identity bootstrap stopped: {result.stop_reason}", err=True)


def _echo_intraday_identity_bootstrap(
    result: IntradayIdentityBootstrapResult,
) -> None:
    click.echo(
        f"{result.dataset_key} identity bootstrap: "
        f"{len(result.validated)} validated, {len(result.skipped)} already valid, "
        f"{len(result.out_of_range)} outside the requested range, "
        f"{len(result.blocked)} blocked, {len(result.failed)} failed; "
        f"{result.probe_attempts} probes, {result.probe_rows} rows"
    )
    if result.stop_reason:
        click.echo(f"Identity bootstrap stopped: {result.stop_reason}", err=True)


def _validate_cli_range(start, end, summary_json: str | None) -> None:
    if end is not None and start.date() > end.date():
        _raise_ingest_error(ValueError("--start must not be after --end"), summary_json)


class _QualityCommandError(click.ClickException):
    """Operational scan/report failure, distinct from a quality-gate failure."""

    exit_code = 2


@click.group()
@click.option(
    "--data-dir",
    envvar="MARKET_DATA_DIR",
    default=None,
    help="Warehouse directory (default: ./data or $MARKET_DATA_DIR)",
)
@click.option("-v", "--verbose", is_flag=True, help="Debug logging")
@click.pass_context
def main(ctx: click.Context, data_dir: str | None, verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    ctx.obj = load_config(data_dir)


@main.command()
@click.pass_obj
def init(config: Config) -> None:
    """Create the warehouse directories and metadata database."""
    config.ensure_dirs()
    MetaStore(config.meta_path).close()
    click.echo(f"Initialized warehouse at {config.data_dir}")


# ---- universe ------------------------------------------------------------


@main.group()
def universe() -> None:
    """Manage the annual ticker universes."""


@universe.command("candidates")
@click.option(
    "--year", type=int, required=True, help="Only tickers active in this year"
)
@click.option(
    "--out", type=click.Path(), default=None, help="Also write tickers to a file"
)
@click.pass_obj
def universe_candidates(config: Config, year: int, out: str | None) -> None:
    """Seed candidate tickers from Tiingo's supported-tickers list
    (US exchanges, common stocks) and register them in the metadata DB."""
    client = _client(config)
    with MetaStore(config.meta_path) as meta:
        supported = client.supported_tickers()
        candidates = universe_mod.seed_candidates_from_tiingo(
            meta, supported, active_in_year=year
        )
    if out:
        Path(out).write_text("\n".join(candidates) + "\n")
        click.echo(f"{len(candidates)} candidates written to {out}")
    else:
        click.echo(f"{len(candidates)} candidates registered")


@universe.command("rank")
@click.option("--year", type=int, required=True)
@click.option("--top", type=int, default=1000, show_default=True)
@click.option(
    "--min-days",
    type=int,
    default=60,
    show_default=True,
    help="Minimum trading days in the year to qualify",
)
@click.pass_obj
def universe_rank(config: Config, year: int, top: int, min_days: int) -> None:
    """Rank stored tickers by avg daily dollar volume over YEAR and save
    the top N as that year's universe."""
    with MetaStore(config.meta_path) as meta:
        if meta.storage_generation() != "v1":
            raise click.ClickException(
                "universe rank currently reads pre-migration ticker bars only"
            )
        n = universe_mod.rank_by_dollar_volume(
            meta, BarStore(config.data_dir), year, top, min_days
        )
    click.echo(f"Universe {year}: {n} tickers stored")


@universe.command("import")
@click.option(
    "--year",
    type=int,
    default=None,
    help="Required only if the CSV has no 'year' column",
)
@click.argument("csv_file", type=click.Path(exists=True))
@click.pass_obj
def universe_import(config: Config, year: int | None, csv_file: str) -> None:
    """Import universe lists from a CSV with a `ticker` column.

    A `year` column imports all years in one pass; ranks come from a
    `rank` column, a dollar-volume column (e.g. MedianDollarVolume,
    ranked descending), or file order — in that priority.
    """
    with MetaStore(config.meta_path) as meta:
        try:
            counts, warnings = universe_mod.import_csv(meta, csv_file, year)
        except ValueError as e:
            raise click.ClickException(str(e)) from e
    for w in warnings:
        click.echo(f"warning: {w}", err=True)
    for y, n in counts.items():
        click.echo(f"Universe {y}: {n} tickers imported")


@universe.command("list")
@click.option("--year", type=int, default=None, help="Default: latest year")
@click.option("--limit", type=int, default=25, show_default=True)
@click.pass_obj
def universe_list(config: Config, year: int | None, limit: int) -> None:
    """Show a universe (rank, ticker, avg dollar volume)."""
    with MetaStore(config.meta_path) as meta:
        years = meta.universe_years()
        if not years:
            raise click.ClickException("No universes stored yet")
        year = year or years[-1]
        rows = meta.universe(year)
    click.echo(
        f"Universe {year} ({len(rows)} tickers, showing {min(limit, len(rows))}):"
    )
    for r in rows[:limit]:
        adv = f"${r['avg_dollar_volume']:,.0f}" if r["avg_dollar_volume"] else "-"
        click.echo(f"  {r['rank'] or '-':>5}  {r['ticker']:<8} {adv}")


# ---- ingestion -----------------------------------------------------------

_ticker_opts = [
    click.option(
        "--tickers", "-t", multiple=True, help="Explicit tickers (repeatable)"
    ),
    click.option(
        "--tickers-file",
        type=click.Path(exists=True),
        help="File with one ticker per line",
    ),
    click.option(
        "--universe",
        "universe_year",
        type=int,
        help="Use the stored universe for this year",
    ),
]


def _with_ticker_opts(f):
    for opt in reversed(_ticker_opts):
        f = opt(f)
    return f


def _resolve_tickers(
    meta: MetaStore,
    tickers: tuple[str, ...],
    tickers_file: str | None,
    universe_year: int | None,
    *,
    default_scope: str = "all",
    summary_json: str | None = None,
) -> list[str]:
    if tickers:
        normalized = [ticker.strip().upper() for ticker in tickers]
        if any(not ticker for ticker in normalized):
            _raise_ingest_error(
                ValueError("--tickers values must not be blank"), summary_json
            )
        resolved = list(dict.fromkeys(normalized))
    elif tickers_file:
        text = Path(tickers_file).read_text()
        resolved = list(
            dict.fromkeys(ticker.strip().upper() for ticker in text.split() if ticker)
        )
    elif universe_year:
        rows = meta.universe(universe_year)
        if not rows:
            raise click.ClickException(f"No universe stored for {universe_year}")
        resolved = [row["ticker"] for row in rows]
    else:
        resolved = (
            meta.latest_universe_tickers()
            if default_scope == "latest"
            else meta.all_universe_tickers()
        )
    if not resolved:
        raise click.ClickException(
            "No tickers specified and no universe exists yet. "
            "Use --tickers/--tickers-file, or build a universe first."
        )
    return resolved


# ---- identity ------------------------------------------------------------


@main.group()
def identity() -> None:
    """Build and inspect stable instrument identity evidence."""


@identity.command("bootstrap-eod")
@_with_ticker_opts
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write the complete structured bootstrap report to this file",
)
@click.pass_obj
def identity_bootstrap_eod_cmd(
    config: Config,
    tickers: tuple[str, ...],
    tickers_file: str | None,
    universe_year: int | None,
    summary_json: str | None,
) -> None:
    """Validate unambiguous Tiingo archive records for EOD ingestion."""
    _require_initialized_warehouse(config)
    with MetaStore(config.meta_path) as meta:
        _require_ingestion_ready(meta)
        ticker_list = _resolve_tickers(
            meta,
            tickers,
            tickers_file,
            universe_year,
            summary_json=summary_json,
        )
        last_reported = 0

        def progress(position: int, total: int) -> None:
            nonlocal last_reported
            if position == total or position - last_reported >= 500:
                click.echo(f"Identity metadata: {position}/{total}")
                last_reported = position

        try:
            result = bootstrap_eod_identities(
                _operational_client(config),
                meta,
                ticker_list,
                progress=progress,
            )
        except _DATA_OPERATION_ERRORS as exc:
            _raise_ingest_error(exc, summary_json, operational=True)
    if summary_json:
        _write_ingest_json(result.to_dict(), summary_json)
    _echo_identity_bootstrap(result)
    if result.operational_failure:
        raise _IngestOperationalError("identity bootstrap failed")
    if result.partial:
        raise click.exceptions.Exit(1)


@identity.command("bootstrap-intraday")
@_with_ticker_opts
@click.option("--start", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option("--end", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option(
    "--freq",
    type=click.Choice(INTRADAY_FREQS),
    default=DEFAULT_INTRADAY_FREQ,
    show_default=True,
)
@click.option(
    "--probe-sessions",
    type=click.IntRange(min=1),
    default=20,
    show_default=True,
    help="Recent XNYS sessions sampled inside each stable alias segment",
)
@click.option(
    "--retry-blocked",
    is_flag=True,
    help="Retry segments with a recorded empty or contradictory IEX probe",
)
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write the complete structured bootstrap report to this file",
)
@click.pass_obj
def identity_bootstrap_intraday_cmd(
    config: Config,
    tickers: tuple[str, ...],
    tickers_file: str | None,
    universe_year: int | None,
    start: datetime,
    end: datetime,
    freq: str,
    probe_sessions: int,
    retry_blocked: bool,
    summary_json: str | None,
) -> None:
    """Probe and record independently validated IEX identity evidence."""
    _validate_cli_range(start, end, summary_json)
    maximum = max_intraday_probe_sessions(freq)
    if probe_sessions > maximum:
        _raise_ingest_error(
            ValueError(f"--probe-sessions must not exceed {maximum} for --freq {freq}"),
            summary_json,
        )
    _require_initialized_warehouse(config)
    with MetaStore(config.meta_path) as meta:
        _require_ingestion_ready(meta)
        ticker_list = _resolve_tickers(
            meta,
            tickers,
            tickers_file,
            universe_year,
            summary_json=summary_json,
        )
        last_reported = 0

        def progress(position: int, total: int) -> None:
            nonlocal last_reported
            if position == total or position - last_reported >= 250:
                click.echo(f"IEX identity probes: {position}/{total}")
                last_reported = position

        try:
            result = bootstrap_intraday_identities(
                _operational_client(config),
                meta,
                ticker_list,
                start=start.date(),
                end=end.date(),
                freq=freq,
                probe_sessions=probe_sessions,
                retry_blocked=retry_blocked,
                progress=progress,
            )
        except _DATA_OPERATION_ERRORS as exc:
            _raise_ingest_error(exc, summary_json, operational=True)
    if summary_json:
        _write_ingest_json(result.to_dict(), summary_json)
    _echo_intraday_identity_bootstrap(result)
    if result.operational_failure:
        raise _IngestOperationalError("intraday identity bootstrap failed")
    if result.partial:
        raise click.exceptions.Exit(1)


@identity.command("repair-eod-episodes")
@click.option(
    "--min-gap-sessions",
    type=click.IntRange(min=MIN_EPISODE_GAP_SESSIONS),
    default=DEFAULT_EPISODE_GAP_SESSIONS,
    show_default=True,
    help="Minimum missing/zero-volume XNYS sessions that define a boundary",
)
@click.option(
    "--apply/--dry-run",
    default=False,
    help="Apply the repair; the default only reports the deterministic plan",
)
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write the complete structured episode-repair report to this file",
)
@click.pass_obj
def identity_repair_eod_episodes_cmd(
    config: Config,
    min_gap_sessions: int,
    apply: bool,
    summary_json: str | None,
) -> None:
    """Split discontinuous EOD histories into stable listing episodes."""
    _require_initialized_warehouse(config)
    try:
        with MetaStore(config.meta_path) as meta:
            _require_ingestion_ready(meta)
            report = repair_eod_episodes(
                BarStore(config.data_dir),
                meta,
                min_gap_sessions=min_gap_sessions,
                apply=apply,
            )
    except _DATA_OPERATION_ERRORS as exc:
        _raise_ingest_error(exc, summary_json, operational=True)
    if summary_json:
        _write_ingest_json(report.to_dict(), summary_json)
    verb = "Applied" if report.applied else "Planned"
    click.echo(
        f"{verb} EOD episode repair: {report.split_sources} source histories, "
        f"{report.created_episodes} episodes, "
        f"{report.quarantined_rows} quarantined rows"
    )
    if report.backup_path:
        click.echo(f"Recoverable backup: {report.backup_path}")


@main.group()
def backfill() -> None:
    """Backfill historical bars (resumable; rerun after interruptions)."""


@backfill.command("eod")
@_with_ticker_opts
@click.option("--start", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option("--end", type=click.DateTime(["%Y-%m-%d"]), default=None)
@click.option(
    "--force", is_flag=True, help="Refetch even where coverage says up-to-date"
)
@click.option(
    "--job-id",
    default=None,
    help="Durable scheduler job id (default: derived from this exact request)",
)
@click.option(
    "--retry-blocked",
    is_flag=True,
    help="Explicitly reactivate terminal ranges after repairing their evidence",
)
@click.option(
    "--phase",
    type=click.IntRange(min=1, max=3),
    default=None,
    help="D-011 phase; enforces the allowed dataset and earlier-phase gate",
)
@click.option(
    "--max-units",
    type=click.IntRange(min=1),
    default=None,
    help="Stop cleanly after this many instrument turns",
)
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write a machine-readable result summary to this file",
)
@click.option(
    "--repair-episodes/--no-repair-episodes",
    default=True,
    help="Run the idempotent listing-episode repair after this EOD sweep",
)
@click.pass_obj
def backfill_eod_cmd(
    config,
    tickers,
    tickers_file,
    universe_year,
    start,
    end,
    force,
    job_id,
    retry_blocked: bool,
    phase,
    max_units,
    summary_json,
    repair_episodes,
):
    """Backfill daily bars through exact identity evidence."""
    _validate_cli_range(start, end, summary_json)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        _require_ingestion_ready(meta)
        ticker_list = _resolve_tickers(
            meta,
            tickers,
            tickers_file,
            universe_year,
            summary_json=summary_json,
        )
        bars = BarStore(config.data_dir)
        click.echo(f"Backfilling EOD for {len(ticker_list)} tickers...")
        try:
            client = _operational_client(config)
            recovered = recover_interrupted_eod_episode_repairs(bars, meta)
            if recovered:
                click.echo(
                    f"Recovered {recovered} interrupted EOD episode repair source(s)"
                )
            scheduler = run_history_request(
                client,
                bars,
                meta,
                dataset_key="eod",
                tickers=ticker_list,
                start=start.date(),
                end=end.date() if end else None,
                phase=phase,
                force=force,
                job_id=job_id,
                retry_blocked=retry_blocked,
                max_units=max_units,
            )
            result = scheduler.ingest
            episode_repair = (
                repair_eod_episodes(bars, meta, apply=True)
                if repair_episodes and scheduler.job_status == "complete"
                else None
            )
        except _DATA_OPERATION_ERRORS as exc:
            _raise_ingest_error(exc, summary_json, operational=True)
    _finish_ingest(result, summary_json, scheduler, episode_repair)


@backfill.command("intraday")
@_with_ticker_opts
@click.option("--start", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option("--end", type=click.DateTime(["%Y-%m-%d"]), default=None)
@click.option(
    "--freq",
    type=click.Choice(INTRADAY_FREQS),
    default=DEFAULT_INTRADAY_FREQ,
    show_default=True,
)
@click.option(
    "--force", is_flag=True, help="Refetch even where coverage says up-to-date"
)
@click.option(
    "--job-id",
    default=None,
    help="Durable scheduler job id (default: derived from this exact request)",
)
@click.option(
    "--retry-blocked",
    is_flag=True,
    help="Explicitly reactivate terminal ranges after repairing their evidence",
)
@click.option(
    "--phase",
    type=click.IntRange(min=1, max=3),
    default=None,
    help="D-011 phase; enforces the allowed dataset and earlier-phase gate",
)
@click.option(
    "--max-units",
    type=click.IntRange(min=1),
    default=None,
    help="Stop cleanly after this many instrument turns",
)
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write a machine-readable result summary to this file",
)
@click.pass_obj
def backfill_intraday_cmd(
    config,
    tickers,
    tickers_file,
    universe_year,
    start,
    end,
    freq,
    force,
    job_id,
    retry_blocked: bool,
    phase,
    max_units,
    summary_json,
):
    """Backfill intraday bars through exact-frequency identity evidence."""
    _validate_cli_range(start, end, summary_json)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        _require_ingestion_ready(meta)
        ticker_list = _resolve_tickers(
            meta,
            tickers,
            tickers_file,
            universe_year,
            summary_json=summary_json,
        )
        dataset_key = f"intraday_{freq}"
        click.echo(f"Backfilling {freq} bars for {len(ticker_list)} tickers...")
        try:
            client = _operational_client(config)
            scheduler = run_history_request(
                client,
                BarStore(config.data_dir),
                meta,
                dataset_key=dataset_key,
                tickers=ticker_list,
                start=start.date(),
                end=end.date() if end else None,
                phase=phase,
                force=force,
                job_id=job_id,
                retry_blocked=retry_blocked,
                max_units=max_units,
            )
            result = scheduler.ingest
        except _DATA_OPERATION_ERRORS as exc:
            _raise_ingest_error(exc, summary_json, operational=True)
    _finish_ingest(result, summary_json, scheduler)


@backfill.command("cancel")
@click.argument("job_id")
@click.pass_obj
def backfill_cancel_cmd(config: Config, job_id: str) -> None:
    """Cancel a durable history job while retaining its audit trail."""
    _require_initialized_warehouse(config)
    with MetaStore(config.meta_path) as meta:
        try:
            cancel_history_job(meta, job_id)
        except _DATA_OPERATION_ERRORS as exc:
            raise click.ClickException(str(exc)) from exc
    click.echo(f"Cancelled history job {job_id}")


@backfill.command("program-init")
@click.option("--program-id", default=DEFAULT_PROGRAM_ID, show_default=True)
@click.option(
    "--phase1-eod-job-id",
    default=DEFAULT_PHASE1_EOD_JOB_ID,
    show_default=True,
)
@click.option(
    "--phase1-hourly-job-id",
    default=DEFAULT_PHASE1_HOURLY_JOB_ID,
    show_default=True,
)
@click.pass_obj
def backfill_program_init_cmd(
    config: Config,
    program_id: str,
    phase1_eod_job_id: str,
    phase1_hourly_job_id: str,
) -> None:
    """Adopt terminal phase 1 and initialize the ordered backfill program."""
    _require_initialized_warehouse(config)
    try:
        with MetaStore(config.meta_path) as meta:
            _require_ingestion_ready(meta)
            initialize_default_backfill_program(
                meta,
                program_id=program_id,
                phase1_eod_job_id=phase1_eod_job_id,
                phase1_hourly_job_id=phase1_hourly_job_id,
            )
            program = meta.backfill_program(program_id)
            components = meta.backfill_program_components(program_id)
    except _DATA_OPERATION_ERRORS as exc:
        raise _IngestOperationalError(str(exc)) from exc
    assert program is not None
    click.echo(
        f"Backfill program {program_id}: {program['status']}; "
        f"{len(components)} ordered components"
    )


@backfill.command("program-step")
@click.option("--program-id", default=DEFAULT_PROGRAM_ID, show_default=True)
@click.option(
    "--identity-batch-size",
    type=click.IntRange(min=1),
    default=250,
    show_default=True,
)
@click.option(
    "--max-units",
    type=click.IntRange(min=1),
    default=500,
    show_default=True,
    help="Maximum historical instrument turns after preparation is complete",
)
@click.option(
    "--status-json",
    type=click.Path(),
    default=None,
    help="Write a bounded replace-in-place program status record",
)
@click.pass_obj
def backfill_program_step_cmd(
    config: Config,
    program_id: str,
    identity_batch_size: int,
    max_units: int,
    status_json: str | None,
) -> None:
    """Advance one bounded preparation batch or historical sweep prefix."""
    _require_initialized_warehouse(config)
    try:
        with MetaStore(config.meta_path) as meta:
            _require_ingestion_ready(meta)
            client = _operational_client(config)
            result = run_backfill_program_step(
                client,
                BarStore(config.data_dir),
                meta,
                program_id=program_id,
                identity_batch_size=identity_batch_size,
                max_history_units=max_units,
            )
    except _DATA_OPERATION_ERRORS as exc:
        _raise_ingest_error(exc, status_json, operational=True)
    click.echo(
        f"Backfill program {program_id}: {result.program_status}; "
        f"action={result.action}"
    )
    if result.component_key is not None:
        click.echo(
            f"Component {result.component_key}: phase {result.phase} "
            f"{result.dataset_key}; state={result.component_state}"
        )
    if result.identity_cursor is not None and result.cohort_count is not None:
        click.echo(
            f"Identity preparation: {result.identity_cursor}/{result.cohort_count}"
        )
    if result.history is not None:
        click.echo(
            f"History {result.history.job_id}: {result.history.job_status}; "
            f"{result.history.attempted_units} attempted, "
            f"{result.history.advanced_units} advanced"
        )
    if result.stop_reason:
        click.echo(f"Program step stopped: {result.stop_reason}")
    if status_json:
        _write_ingest_json(_bounded_program_status(result), status_json)
    if result.partial:
        raise click.exceptions.Exit(1)


@backfill.command("program-status")
@click.option("--program-id", default=DEFAULT_PROGRAM_ID, show_default=True)
@click.pass_obj
def backfill_program_status_cmd(config: Config, program_id: str) -> None:
    """Show persisted backfill-program state without API calls or mutation."""
    _require_initialized_warehouse(config)
    try:
        with MetaStore(config.meta_path) as meta:
            program = meta.backfill_program(program_id)
            if program is None:
                raise click.ClickException(f"unknown backfill program {program_id!r}")
            components = meta.backfill_program_components(program_id)
            scopes = {
                str(row["scope_key"]): meta.backfill_program_scope(
                    program_id, str(row["scope_key"])
                )
                for row in components
            }
    except _DATA_OPERATION_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Backfill program {program_id}: {program['status']}")
    for component in components:
        scope = scopes[str(component["scope_key"])]
        cohort = int(scope["ticker_count"]) if scope is not None else 0
        click.echo(
            f"  {component['component_key']}: phase {component['phase']} "
            f"{component['dataset_key']} {component['state']}; "
            f"identity={component['identity_status']} "
            f"{component['identity_cursor']}/{cohort}; job={component['job_id']}"
        )


@main.command()
@_with_ticker_opts
@click.option(
    "--all-universes",
    is_flag=True,
    help="Update every ticker from every year's universe, not just the latest",
)
@click.option(
    "--refresh-identities",
    is_flag=True,
    help="Refresh EOD identity evidence for the update cohort before collection",
)
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write a machine-readable result summary to this file",
)
@click.option(
    "--status-json",
    type=click.Path(),
    default=None,
    help="Write a bounded operational status record to this file",
)
@click.pass_obj
def update(
    config,
    tickers,
    tickers_file,
    universe_year,
    all_universes,
    refresh_identities,
    summary_json,
    status_json,
):
    """Incremental identity-validated EOD update."""
    if summary_json and status_json:
        raise click.UsageError(
            "--summary-json and --status-json are mutually exclusive"
        )
    report_json = status_json or summary_json
    started_at = datetime.now(UTC)
    client = None
    identity_result = None
    try:
        config.ensure_dirs()
        with MetaStore(config.meta_path) as meta:
            _require_ingestion_ready(meta)
            ticker_list = _resolve_tickers(
                meta,
                tickers,
                tickers_file,
                universe_year,
                default_scope="all" if all_universes else "latest",
                summary_json=report_json,
            )
            client = _operational_client(config)
            bars = BarStore(config.data_dir)
            operation_lock = (
                DataDirectoryLock(
                    config.data_dir, operation="ingest:scheduled-eod-update"
                )
                if refresh_identities
                else nullcontext()
            )
            with operation_lock:
                if refresh_identities:
                    identity_result = bootstrap_eod_identities(
                        client, meta, ticker_list
                    )
                    _echo_identity_bootstrap(identity_result)
                click.echo(f"Updating EOD for {len(ticker_list)} tickers...")
                cycle = run_ingestion_cycle(
                    client,
                    bars,
                    meta,
                    current_tickers=ticker_list,
                    current_datasets=["eod"],
                    history_job_id=None,
                )
    except click.ClickException as exc:
        _raise_ingest_error(
            RuntimeError(exc.format_message()),
            report_json,
            operational=True,
            operation=_operation_record(started_at, client),
        )
    except _DATA_OPERATION_ERRORS as exc:
        _raise_ingest_error(
            exc,
            report_json,
            operational=True,
            operation=_operation_record(started_at, client),
        )
    _finish_ingestion_cycle(
        cycle,
        summary_json,
        status_json=status_json,
        operation=_operation_record(started_at, client),
        identity_bootstrap=identity_result,
    )


@main.command("reconcile")
@click.pass_obj
def reconcile_cmd(config):
    """Rebuild coverage metadata from the active generation's Parquet files."""
    _require_initialized_warehouse(config)
    bars = BarStore(config.data_dir)
    try:
        with MetaStore(config.meta_path) as meta:
            report = reconcile_active(bars, meta)
    except _DATA_OPERATION_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    for issue in report.issues:
        click.echo(
            f"warning: {issue.dataset_key} {issue.instrument_id}: "
            f"{issue.issue} ({issue.detail})",
            err=True,
        )
    owner_label = "instruments" if report.generation == "v2" else "tickers"
    for dataset, n in report.counts.items():
        click.echo(f"{dataset}: coverage rebuilt for {n} {owner_label}")
    if report.issues:
        raise click.exceptions.Exit(1)


@main.command("migrate-v2-bars")
@click.option(
    "--report",
    "report_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Migration report path (default: inside the v1 quarantine)",
)
@click.pass_obj
def migrate_v2_bars_cmd(config: Config, report_path: Path | None) -> None:
    """Quarantine ticker-keyed bars and copy resolvable files into v2."""
    config.data_dir.mkdir(parents=True, exist_ok=True)
    try:
        with MetaStore(config.meta_path) as meta:
            report = migrate_v1_bars(
                BarStore(config.data_dir), meta, report_path=report_path
            )
    except _DATA_OPERATION_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    counts = ", ".join(f"{count} {status}" for status, count in report.counts().items())
    click.echo(f"Migration pass complete: {counts or 'no source files'}")
    if report.reconciliation_issues:
        click.echo(
            f"Coverage omitted for {len(report.reconciliation_issues)} "
            "disconnected or invalid canonical slices",
            err=True,
        )
    click.echo(
        f"Report: {report_path or default_migration_report_path(config.data_dir)}"
    )
    if report.reconciliation_issues or any(
        item.status != "migrated" for item in report.items
    ):
        raise click.exceptions.Exit(1)


@main.command("research-reconcile")
@click.option(
    "--apply/--dry-run",
    default=False,
    help="Mark abandoned runs failed and remove their/unowned artifacts",
)
@click.pass_obj
def research_reconcile_cmd(config: Config, apply: bool) -> None:
    """Report or explicitly repair interrupted research publication state."""
    _require_initialized_warehouse(config)
    try:
        report = reconcile_research_state(config, apply=apply)
    except _DATA_OPERATION_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    verb = "Reconciled" if apply else "Found"
    click.echo(
        f"{verb} research state: "
        f"{len(report.stale_running_run_ids)} abandoned running rows, "
        f"{len(report.orphan_directories)} unowned artifact directories"
    )
    if apply:
        click.echo(
            f"Marked {len(report.failed_run_ids)} runs failed; "
            f"removed {len(report.removed_directories)} directories"
        )
    elif report.stale_running_run_ids or report.orphan_directories:
        raise click.exceptions.Exit(1)


@main.command("research-run")
@click.argument("study_name")
@click.option(
    "--parameters-json",
    default="{}",
    show_default=True,
    help="Study parameters as one JSON object",
)
@click.pass_obj
def research_run_cmd(config: Config, study_name: str, parameters_json: str) -> None:
    """Run one registered vectorized event study and publish its artifacts."""
    _require_initialized_warehouse(config)
    try:
        parameters = json.loads(parameters_json)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"invalid --parameters-json: {exc.msg}") from exc
    if not isinstance(parameters, dict):
        raise click.ClickException("--parameters-json must decode to a JSON object")
    try:
        published = run_registered_event_study(config, study_name, parameters)
    except _DATA_OPERATION_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"Published event study {published.study_name}: run {published.run_id}, "
        f"{published.observation_count} observations"
    )
    click.echo(f"Input fingerprint: {published.input_fingerprint}")
    click.echo("Semantics: event observations only; no portfolio or order simulation")


# ---- inspection ----------------------------------------------------------


@main.command("quality")
@click.option(
    "--dataset",
    "dataset_keys",
    type=click.Choice(DATASET_KEYS),
    multiple=True,
    help="Dataset to scan (repeatable; default: all three)",
)
@click.option(
    "--instrument-id",
    "instrument_ids",
    multiple=True,
    help="Stable instrument id to scan (repeatable; default: all stored)",
)
@click.option("--start", type=click.DateTime(["%Y-%m-%d"]), default=None)
@click.option("--end", type=click.DateTime(["%Y-%m-%d"]), default=None)
@click.option(
    "--zero-volume-run",
    type=click.IntRange(min=MIN_ZERO_VOLUME_RUN_LENGTH),
    default=DEFAULT_ZERO_VOLUME_RUN_LENGTH,
    show_default=True,
    help="Minimum consecutive zero-volume bars reported as suspicious",
)
@click.option(
    "--block-on",
    type=click.Choice(QUALITY_CHECKS),
    multiple=True,
    help=(
        "Check whose warning/error findings make this command fail; a declared "
        "check that could not run also fails closed"
    ),
)
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write the complete structured report to this file",
)
@click.pass_obj
def quality_cmd(
    config: Config,
    dataset_keys: tuple[str, ...],
    instrument_ids: tuple[str, ...],
    start,
    end,
    zero_volume_run: int,
    block_on: tuple[str, ...],
    summary_json: str | None,
) -> None:
    """Report stored-data quality without modifying canonical vendor bars."""
    try:
        report = check_quality(
            config,
            dataset_keys=dataset_keys or DATASET_KEYS,
            instrument_ids=instrument_ids or None,
            start=start.date() if start else None,
            end=end.date() if end else None,
            zero_volume_run_length=zero_volume_run,
        )
        gate = evaluate_quality(report, block_on)
    except _DATA_OPERATION_ERRORS as exc:
        # A failed scan has no gate outcome. Preserve any standing successful
        # report rather than replacing it with a structurally different file.
        raise _QualityCommandError(f"quality scan failed: {exc}") from exc

    payload = report.to_dict() | {"gate": gate.to_dict()}
    if summary_json:
        try:
            _write_json_atomic(payload, Path(summary_json))
        except OSError as exc:
            raise _QualityCommandError(
                f"could not write quality summary {summary_json}: {exc}"
            ) from exc
    counts = report.finding_counts()
    click.echo(
        "Quality findings: "
        f"{counts['error']} error, {counts['warning']} warning, "
        f"{counts['info']} info"
    )
    for finding in report.findings:
        if finding.severity == "info":
            continue
        owner = f" {finding.instrument_id}" if finding.instrument_id else ""
        click.echo(
            f"{finding.severity}: {finding.dataset_key}{owner} "
            f"{finding.check}: {finding.message} ({finding.count})",
            err=finding.severity == "error",
        )
    if block_on:
        if gate.checks_not_run:
            click.echo(
                "Quality checks not run: " + ", ".join(gate.checks_not_run),
                err=True,
            )
        click.echo(
            "Quality gate: " + ("passed" if gate.passed else "failed"),
            err=not gate.passed,
        )
    if not gate.passed:
        raise click.exceptions.Exit(1)


@main.command()
@click.pass_obj
def status(config: Config) -> None:
    """Warehouse coverage summary."""
    bars = BarStore(config.data_dir)
    click.echo(f"Warehouse: {config.data_dir}")
    with MetaStore(config.meta_path) as meta:
        generation = meta.storage_generation()
        try:
            bars.validate_generation(generation)
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"Storage generation: {generation}")
        if generation == "v2":
            files = bars.canonical_eod_files()
            if files:
                summary = (
                    pl.scan_parquet(files)
                    .select(
                        pl.len().alias("rows"),
                        pl.col("instrument_id").n_unique().alias("instruments"),
                        pl.col("date").min().alias("lo"),
                        pl.col("date").max().alias("hi"),
                    )
                    .collect()
                    .row(0, named=True)
                )
                click.echo(f"EOD instruments: {summary['instruments']}")
                click.echo(
                    f"EOD bars: {summary['rows']:,} rows, "
                    f"{summary['lo']} .. {summary['hi']}"
                )
            else:
                click.echo("EOD instruments: 0")
        else:
            tickers = bars.eod_tickers()
            click.echo(f"EOD tickers: {len(tickers)}")
            if tickers:
                summary = (
                    pl.scan_parquet([bars.eod_path(ticker) for ticker in tickers])
                    .select(
                        pl.len().alias("rows"),
                        pl.col("date").min().alias("lo"),
                        pl.col("date").max().alias("hi"),
                    )
                    .collect()
                    .row(0, named=True)
                )
                click.echo(
                    f"EOD bars: {summary['rows']:,} rows, "
                    f"{summary['lo']} .. {summary['hi']}"
                )
        years = meta.universe_years()
        if years:
            counts = ", ".join(f"{y}: {len(meta.universe(y))}" for y in years)
            click.echo(f"Universes: {counts}")
        cov = (
            meta.coverage("eod")
            if generation == "v2"
            else meta.ticker_coverage_v1("eod")
        )
        if cov:
            oldest_edge = min(last for _, last in cov.values())
            click.echo(f"Oldest EOD coverage edge: {oldest_edge}")
        for program in meta.backfill_programs():
            components = meta.backfill_program_components(str(program["program_id"]))
            current = next(
                (
                    row
                    for row in components
                    if str(row["state"]) not in {"complete", "blocked"}
                ),
                None,
            )
            position = (
                "terminal"
                if current is None
                else f"{current['component_key']} ({current['state']})"
            )
            click.echo(
                f"Backfill program {program['program_id']}: {program['status']}; "
                f"current={position}"
            )
        now = datetime.now(UTC)
        usage = meta.request_usage(
            now=now, rolling_days=DEFAULT_BUDGET_POLICY.rolling_days
        )
        if usage["requests"]:
            click.echo(
                "Tiingo rolling usage: "
                f"{usage['requests']:,} requests, "
                f"{usage['observed_bytes']:,} observed bytes, "
                f"{usage['charged_bytes']:,} budgeted bytes"
            )
        historical_limit = DEFAULT_BUDGET_POLICY.historical_total_byte_limit(now)
        historical_headroom = max(
            0,
            historical_limit
            - usage["charged_bytes"]
            - DEFAULT_BUDGET_POLICY.response_reservation_bytes,
        )
        click.echo(
            "Historical admission ceiling: "
            f"{historical_limit:,} total budgeted bytes, "
            f"{historical_headroom:,} bytes available after the next "
            f"{DEFAULT_BUDGET_POLICY.response_reservation_bytes:,}-byte reservation"
        )


@main.command()
@click.argument("query")
@click.pass_obj
def sql(config: Config, query: str) -> None:
    """Run ad-hoc SQL against the warehouse (views: eod, plus
    intraday_<freq> for each frequency on disk; metadata attached as
    meta.*)."""
    from marketdata.query import connect

    con = None
    try:
        con = connect(config)
        con.execute(query)
        click.echo(con.pl())
    except (RuntimeError, duckdb.Error) as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    main()
