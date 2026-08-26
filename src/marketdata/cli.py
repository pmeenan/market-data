"""market-data CLI: manage the universe, backfill, update, and query."""

from __future__ import annotations

import json
import logging
import sys
from datetime import date
from pathlib import Path

import click

from marketdata import universe as universe_mod
from marketdata.config import Config, load_config
from marketdata.ingest import (
    DEFAULT_INTRADAY_FREQ,
    INTRADAY_FREQS,
    IngestResult,
    backfill_eod,
    backfill_intraday,
    reconcile,
    update_eod,
)
from marketdata.store import BarStore, MetaStore
from marketdata.tiingo import TiingoClient


def _finish_ingest(result: IngestResult, summary_json: str | None) -> None:
    """Report an ingest result; nonzero exit if any ticker failed (so cron
    notices partial failures)."""
    click.echo(f"Done: {result.summary()}")
    if summary_json:
        Path(summary_json).write_text(json.dumps(result.to_dict(), indent=2) + "\n")
    if result.failed:
        click.echo("Failed tickers: " + ", ".join(sorted(result.failed)), err=True)
        sys.exit(1)


def _client(config: Config) -> TiingoClient:
    if not config.tiingo_token:
        raise click.ClickException(
            "TIINGO_API_TOKEN is not set (put it in .env or the environment)"
        )
    return TiingoClient(config.tiingo_token)


def _resolve_tickers(
    meta: MetaStore,
    tickers: tuple[str, ...],
    tickers_file: str | None,
    universe_year: int | None,
    *,
    default_scope: str = "all",
) -> list[str]:
    """Pick the working ticker set. With no explicit option, `default_scope`
    decides: "all" = every ticker in any year's universe (backfills), or
    "latest" = the most recent universe only (nightly updates — otherwise
    thousands of delisted historical members consume requests forever)."""
    if tickers:
        return [t.upper() for t in tickers]
    if tickers_file:
        text = Path(tickers_file).read_text()
        return [t.strip().upper() for t in text.split() if t.strip()]
    if universe_year:
        rows = meta.universe(universe_year)
        if not rows:
            raise click.ClickException(f"No universe stored for {universe_year}")
        return [r["ticker"] for r in rows]
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
@click.option("--year", type=int, required=True, help="Only tickers active in this year")
@click.option("--out", type=click.Path(), default=None, help="Also write tickers to a file")
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
@click.option("--min-days", type=int, default=60, show_default=True,
              help="Minimum trading days in the year to qualify")
@click.pass_obj
def universe_rank(config: Config, year: int, top: int, min_days: int) -> None:
    """Rank stored tickers by avg daily dollar volume over YEAR and save
    the top N as that year's universe."""
    with MetaStore(config.meta_path) as meta:
        n = universe_mod.rank_by_dollar_volume(
            meta, BarStore(config.data_dir), year, top, min_days
        )
    click.echo(f"Universe {year}: {n} tickers stored")


@universe.command("import")
@click.option("--year", type=int, default=None,
              help="Required only if the CSV has no 'year' column")
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
            raise click.ClickException(str(e))
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
    click.echo(f"Universe {year} ({len(rows)} tickers, showing {min(limit, len(rows))}):")
    for r in rows[:limit]:
        adv = f"${r['avg_dollar_volume']:,.0f}" if r["avg_dollar_volume"] else "-"
        click.echo(f"  {r['rank'] or '-':>5}  {r['ticker']:<8} {adv}")


# ---- ingestion -----------------------------------------------------------

_ticker_opts = [
    click.option("--tickers", "-t", multiple=True, help="Explicit tickers (repeatable)"),
    click.option("--tickers-file", type=click.Path(exists=True),
                 help="File with one ticker per line"),
    click.option("--universe", "universe_year", type=int,
                 help="Use the stored universe for this year"),
]


def _with_ticker_opts(f):
    for opt in reversed(_ticker_opts):
        f = opt(f)
    return f


@main.group()
def backfill() -> None:
    """Backfill historical bars (resumable; rerun after interruptions)."""


@backfill.command("eod")
@_with_ticker_opts
@click.option("--start", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option("--end", type=click.DateTime(["%Y-%m-%d"]), default=None)
@click.option("--force", is_flag=True, help="Refetch even where coverage says up-to-date")
@click.option("--summary-json", type=click.Path(), default=None,
              help="Write a machine-readable result summary to this file")
@click.pass_obj
def backfill_eod_cmd(config, tickers, tickers_file, universe_year, start, end, force,
                     summary_json):
    """Backfill daily bars (fills missing leading and trailing history)."""
    client = _client(config)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        ticker_list = _resolve_tickers(meta, tickers, tickers_file, universe_year)
        click.echo(f"Backfilling EOD for {len(ticker_list)} tickers...")
        result = backfill_eod(
            client, BarStore(config.data_dir), meta, ticker_list,
            start.date(), end.date() if end else None, force=force,
        )
    _finish_ingest(result, summary_json)


@backfill.command("intraday")
@_with_ticker_opts
@click.option("--start", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option("--end", type=click.DateTime(["%Y-%m-%d"]), default=None)
@click.option("--freq", type=click.Choice(INTRADAY_FREQS),
              default=DEFAULT_INTRADAY_FREQ, show_default=True)
@click.option("--summary-json", type=click.Path(), default=None,
              help="Write a machine-readable result summary to this file")
@click.pass_obj
def backfill_intraday_cmd(config, tickers, tickers_file, universe_year, start, end, freq,
                          summary_json):
    """Backfill intraday bars (IEX feed: recent years only, unadjusted,
    IEX-only volume)."""
    client = _client(config)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        ticker_list = _resolve_tickers(meta, tickers, tickers_file, universe_year)
        click.echo(f"Backfilling {freq} bars for {len(ticker_list)} tickers...")
        result = backfill_intraday(
            client, BarStore(config.data_dir), meta, ticker_list,
            start.date(), end.date() if end else None, freq=freq,
        )
    _finish_ingest(result, summary_json)


@main.command()
@_with_ticker_opts
@click.option("--all-universes", is_flag=True,
              help="Update every ticker from every year's universe, not just the latest")
@click.option("--summary-json", type=click.Path(), default=None,
              help="Write a machine-readable result summary to this file")
@click.pass_obj
def update(config, tickers, tickers_file, universe_year, all_universes, summary_json):
    """Incremental EOD update (cron-friendly; exits nonzero on any failure).

    Refetches a rolling overlap window to pick up corrections and restated
    adjustments; a newly observed split/dividend triggers a full-history
    refresh for that ticker. With no ticker options, updates the MAX(year)
    universe (delisted historical members don't burn requests nightly);
    pass --all-universes for every ticker ever in any universe. The
    MAX(year) default is a pragmatic ingestion scope (D-010) until ongoing
    all-ticker collection (D-011) supersedes universe-scoped updates.
    """
    client = _client(config)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        ticker_list = _resolve_tickers(
            meta, tickers, tickers_file, universe_year,
            default_scope="all" if all_universes else "latest",
        )
        click.echo(f"Updating EOD for {len(ticker_list)} tickers...")
        result = update_eod(client, BarStore(config.data_dir), meta, ticker_list)
    _finish_ingest(result, summary_json)


@main.command("reconcile")
@click.pass_obj
def reconcile_cmd(config):
    """Rebuild coverage metadata from the canonical Parquet files."""
    with MetaStore(config.meta_path) as meta:
        counts = reconcile(BarStore(config.data_dir), meta)
    for dataset, n in counts.items():
        click.echo(f"{dataset}: coverage rebuilt for {n} tickers")


# ---- inspection ----------------------------------------------------------


@main.command()
@click.pass_obj
def status(config: Config) -> None:
    """Warehouse coverage summary."""
    from marketdata.query import connect

    bars = BarStore(config.data_dir)
    tickers = bars.eod_tickers()
    click.echo(f"Warehouse: {config.data_dir}")
    click.echo(f"EOD tickers: {len(tickers)}")
    if tickers:
        con = connect(config)
        n, lo, hi = con.execute(
            "SELECT count(*), min(date), max(date) FROM eod"
        ).fetchone()
        click.echo(f"EOD bars: {n:,} rows, {lo} .. {hi}")
    with MetaStore(config.meta_path) as meta:
        years = meta.universe_years()
        if years:
            counts = ", ".join(f"{y}: {len(meta.universe(y))}" for y in years)
            click.echo(f"Universes: {counts}")
        cov = meta.coverage("eod")
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

    con = connect(config)
    con.execute(query)
    click.echo(con.pl())


if __name__ == "__main__":
    main()
