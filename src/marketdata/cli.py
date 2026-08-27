"""market-data CLI: manage the universe, backfill, update, and query."""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import click
import duckdb
import polars as pl

from marketdata import universe as universe_mod
from marketdata.config import Config, load_config
from marketdata.identity import DATASET_KEYS
from marketdata.ingest import (
    DEFAULT_INTRADAY_FREQ,
    IngestResult,
    backfill_eod_validated,
    backfill_intraday_validated,
    update_eod_validated,
)
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
from marketdata.reconcile import reconcile_canonical, reconcile_legacy
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import INTRADAY_FREQS
from marketdata.tiingo import TiingoClient


def _client(config: Config) -> TiingoClient:
    if not config.tiingo_token:
        raise click.ClickException(
            "TIINGO_API_TOKEN is not set (put it in .env or the environment)"
        )
    return TiingoClient(config.tiingo_token)


def _require_ingestion_ready(meta: MetaStore) -> None:
    if meta.storage_generation() != "v2":
        raise click.ClickException(
            "production ingestion remains paused: migrate the warehouse to v2 first"
        )


def _finish_ingest(result: IngestResult, summary_json: str | None) -> None:
    click.echo(f"Done: {result.summary()}")
    if summary_json:
        _write_json_atomic(result.to_dict(), Path(summary_json))
    if result.failed:
        click.echo("Failed segments: " + ", ".join(sorted(result.failed)), err=True)
    if result.blocked:
        click.echo(
            "Identity-blocked segments: " + ", ".join(sorted(result.blocked)),
            err=True,
        )
    if not result.ok:
        raise click.exceptions.Exit(1)


def _raise_ingest_error(exc: Exception, summary_json: str | None) -> None:
    if summary_json:
        _write_json_atomic(
            {"ok": False, "error": str(exc), "failed": {}, "blocked": {}},
            Path(summary_json),
        )
    raise click.ClickException(str(exc)) from exc


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
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write a machine-readable result summary to this file",
)
@click.pass_obj
def backfill_eod_cmd(
    config, tickers, tickers_file, universe_year, start, end, force, summary_json
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
        client = _client(config)
        click.echo(f"Backfilling EOD for {len(ticker_list)} tickers...")
        try:
            result = backfill_eod_validated(
                client,
                BarStore(config.data_dir),
                meta,
                ticker_list,
                start.date(),
                end.date() if end else None,
                force=force,
            )
        except (
            OSError,
            RuntimeError,
            ValueError,
            sqlite3.Error,
            pl.exceptions.PolarsError,
        ) as exc:
            _raise_ingest_error(exc, summary_json)
    _finish_ingest(result, summary_json)


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
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write a machine-readable result summary to this file",
)
@click.pass_obj
def backfill_intraday_cmd(
    config, tickers, tickers_file, universe_year, start, end, freq, summary_json
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
        client = _client(config)
        click.echo(f"Backfilling {freq} bars for {len(ticker_list)} tickers...")
        try:
            result = backfill_intraday_validated(
                client,
                BarStore(config.data_dir),
                meta,
                ticker_list,
                start.date(),
                end.date() if end else None,
                freq=freq,
            )
        except (
            OSError,
            RuntimeError,
            ValueError,
            sqlite3.Error,
            pl.exceptions.PolarsError,
        ) as exc:
            _raise_ingest_error(exc, summary_json)
    _finish_ingest(result, summary_json)


@main.command()
@_with_ticker_opts
@click.option(
    "--all-universes",
    is_flag=True,
    help="Update every ticker from every year's universe, not just the latest",
)
@click.option(
    "--summary-json",
    type=click.Path(),
    default=None,
    help="Write a machine-readable result summary to this file",
)
@click.pass_obj
def update(config, tickers, tickers_file, universe_year, all_universes, summary_json):
    """Incremental identity-validated EOD update."""
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        _require_ingestion_ready(meta)
        ticker_list = _resolve_tickers(
            meta,
            tickers,
            tickers_file,
            universe_year,
            default_scope="all" if all_universes else "latest",
            summary_json=summary_json,
        )
        client = _client(config)
        click.echo(f"Updating EOD for {len(ticker_list)} tickers...")
        try:
            result = update_eod_validated(
                client, BarStore(config.data_dir), meta, ticker_list
            )
        except (
            OSError,
            RuntimeError,
            ValueError,
            sqlite3.Error,
            pl.exceptions.PolarsError,
        ) as exc:
            _raise_ingest_error(exc, summary_json)
    _finish_ingest(result, summary_json)


@main.command("reconcile")
@click.pass_obj
def reconcile_cmd(config):
    """Rebuild coverage metadata from the active generation's Parquet files."""
    bars = BarStore(config.data_dir)
    owner_label = "tickers"
    report = None
    try:
        with MetaStore(config.meta_path) as meta:
            generation = meta.storage_generation()
            bars.validate_generation(generation)
            if generation == "v2":
                report = reconcile_canonical(bars, meta)
                counts = report.counts
                owner_label = "instruments"
                for issue in report.issues:
                    click.echo(
                        f"warning: {issue.dataset_key} {issue.instrument_id}: "
                        f"{issue.issue} ({issue.detail})",
                        err=True,
                    )
            else:
                counts = reconcile_legacy(bars, meta)
    except (OSError, RuntimeError, sqlite3.Error, pl.exceptions.PolarsError) as exc:
        raise click.ClickException(str(exc)) from exc
    for dataset, n in counts.items():
        click.echo(f"{dataset}: coverage rebuilt for {n} {owner_label}")
    if report is not None and report.issues:
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
    except (
        OSError,
        RuntimeError,
        ValueError,
        sqlite3.Error,
        pl.exceptions.PolarsError,
    ) as exc:
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
    except (
        OSError,
        RuntimeError,
        ValueError,
        sqlite3.Error,
        duckdb.Error,
        pl.exceptions.PolarsError,
    ) as exc:
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


@main.command()
@click.argument("query")
@click.pass_obj
def sql(config: Config, query: str) -> None:
    """Run ad-hoc SQL against the warehouse (views: eod, plus
    intraday_<freq> for each frequency on disk; metadata attached as
    meta.*)."""
    from marketdata.query import connect

    try:
        con = connect(config)
        con.execute(query)
        click.echo(con.pl())
    except (RuntimeError, duckdb.Error) as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
