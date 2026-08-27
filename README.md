# market-data

A local market data warehouse and strategy-testing toolkit — a personal
research tool for testing trading hypotheses against a dataset you own and
control. Almost all code is written by AI agents working from the project
documentation, directed and reviewed by a human (see
[AGENTS.md](AGENTS.md)).

- **Source**: [Tiingo](https://www.tiingo.com/) (EOD daily bars with split/dividend adjustments; IEX intraday bars)
- **Storage**: canonical instrument-keyed Parquet in stable hash buckets + a
  small SQLite database for identities, universes, and coverage intervals
- **Query engine**: [DuckDB](https://duckdb.org/) over the Parquet files, with the metadata DB attached — fast analytical scans for backtesting, no database server to run
- **Interface**: a `market-data` CLI for ingestion/maintenance and an importable `marketdata` Python library for research and strategy code

## Setup

```bash
tools/install-uv
make sync
cp .env.example .env   # then add your TIINGO_API_TOKEN
```

## Workflow

The universe is rebuilt annually, ranked by dollar volume, and stored
per-year as the record of how the dataset was seeded. It scopes ingestion,
not backtests: studies select tickers from the stored data directly, and
survivorship-bias protection comes from backfilling all tickers including
delisted ones (D-010, D-011).

If you already have per-year ticker lists, drop them in `seeds/`
(committed to git — they seed the initial dataset) and import directly. A
`Year` column imports every year in one pass, and ranks are derived from
the dollar-volume column:

```bash
market-data init

# CSV columns: Year,Ticker,MedianDollarVolume (header names are flexible)
market-data universe import seeds/universe_by_dollar_volume.csv
market-data universe list --year 2011

# Then backfill: with no ticker options this covers every ticker that
# appears in any year's universe (20-year scope per D-011)
market-data backfill eod --start 2006-01-01
```

Or bootstrap universes from scratch via Tiingo:

```bash
# 1. Create the warehouse (default ./data, override with MARKET_DATA_DIR)
market-data init

# 2. Seed candidate tickers from Tiingo's supported list (US stocks active in the year)
market-data universe candidates --year 2025 --out candidates.txt

# 3. Backfill the ranking year's EOD data for the candidates (resumable — rerun if interrupted)
market-data backfill eod --tickers-file candidates.txt --start 2025-01-01 --end 2025-12-31

# 4. Rank by average daily dollar volume, keep the top N as the 2025 universe
market-data universe rank --year 2025 --top 1000
market-data universe list

# 5. Backfill history for the universe (20-year scope per D-011)
market-data backfill eod --universe 2025 --start 2006-01-01

# Optional: intraday bars (Tiingo IEX — recent years only, unadjusted)
market-data backfill intraday --universe 2025 --start 2024-01-01

# Keep current (cron this nightly after market close). Defaults to the
# MAX(year) universe as a pragmatic ingestion scope (D-010);
# --all-universes updates every historical member too.
market-data update

# Inspect
market-data status
market-data sql "SELECT ticker, max(date), count(*) FROM eod GROUP BY ticker ORDER BY 2 DESC LIMIT 10"
```

All ingestion is idempotent and resumable: each (ticker, dataset) pair tracks
a coverage interval (so backfills fill missing leading history too), Parquet
writes are merge-upserts keyed on date/timestamp, nightly updates refetch a
rolling overlap to pick up corrections and restated adjustments, and a newly
observed split/dividend triggers a full-history refresh for that ticker.
Partial failures exit nonzero (cron-friendly; add `--summary-json out.json`
for a machine-readable result). If coverage rows are lost or in doubt,
`market-data reconcile` rebuilds them from Parquet. This does not recreate a
lost `meta.db`: identity evidence cannot be reconstructed from bar files, so
restore the metadata database from backup first.

### M1 bar migration

Production ingestion remains paused while its APIs are converted to stable
identities. The completed storage migration can be exercised with:

```bash
market-data migrate-v2-bars
```

The command first moves both ticker-keyed roots beneath
`quarantine/v1-ticker-bars/`, then copies only complete source-file ranges
that resolve to one instrument. Its durable `migration-report.json` lists
every migrated, unresolved, conflicting, invalid, or failed file and its
target. Quarantined sources are retained: add or correct identity evidence and
rerun the same command safely; canonical writes are merge-upserts and coverage
is rebuilt conservatively after every run. The command exits nonzero while any
source or canonical coverage slice remains unsafe. Establishing the v2 boundary
records a durable generation marker, clears derived v1 ticker coverage, and
disables the legacy ingestion/query commands until their `instrument_id` APIs
land; they fail clearly instead of recreating or reading ticker-keyed files.
Schema v3 names the canonical SQL table `meta.coverage` and retains the old
shape explicitly as `meta.ticker_coverage_v1` during the transition.

## Using the library (transitional v1 surface)

The calls below describe the pre-migration query API. They intentionally fail
closed after the v2 boundary until the next M1 step converts filters and views
to `instrument_id`.

```python
from marketdata import load_config
from marketdata.query import connect, load_eod

config = load_config()

# Polars frame of daily bars
df = load_eod(config, ["AAPL", "MSFT"], start="2020-01-01")

# Or raw DuckDB for arbitrary SQL (views: eod, intraday_<freq> per frequency
# present on disk, e.g. intraday_1hour; metadata at meta.*)
con = connect(config)
con.execute("""
    SELECT ticker, avg(adj_close * adj_volume) AS adv
    FROM eod WHERE date >= '2025-01-01'
    GROUP BY ticker ORDER BY adv DESC LIMIT 20
""").pl()
```

Use `adj_*` columns for strategy math (they are split- and dividend-adjusted);
raw columns reflect prices as traded.

## Layout

```
data/                       (gitignored; set MARKET_DATA_DIR to relocate)
  meta.db                   SQLite: identities, generation, universes, coverage
  bars/eod/bucket=ab/bars.parquet
  bars/intraday/1hour/year=2025/bucket=ab/bars.parquet
  quarantine/v1-ticker-bars/
    migration-report.json  durable per-source migration outcome
src/marketdata/
  identity.py               fail-closed identity resolution result contracts
  tiingo.py                 Tiingo REST client (throttling, retries)
  store/bars.py             Parquet bar storage (merge-upsert writes)
  store/meta.py             SQLite metadata store
  universe.py               candidate seeding + dollar-volume ranking
  ingest.py                 resumable backfill / incremental update
  query.py                  DuckDB views + polars loaders
  cli.py                    the market-data CLI
```

## Status

Milestone **M1 (identity-safe canonical warehouse)** is in progress after the
owner approved and closed M0 on 2026-08-27. The identity registry and explicit
resolution reports plus the v2 hash-bucket storage migration, atomic bar
publication, conservative reconciliation, and operator report are implemented.
Moving ingestion and query APIs to `instrument_id` is next. Production ingestion
remains paused until M1 is complete. The first planned study asks whether stocks
that open significantly down tend to recover over the next few hours; the
research/backtesting layer begins in M3.

## Start here

- [AGENTS.md](AGENTS.md) — rulebook and doc map for agents (and a good
  orientation for humans)
- [docs/vision.md](docs/vision.md) — why this exists, success criteria,
  non-goals
- [docs/features.md](docs/features.md) — confirmed scope, proposals, open
  questions
- [docs/plan.md](docs/plan.md) — milestones and current work
- [docs/workflow.md](docs/workflow.md) — how changes get built and committed
- [docs/rough-edges.md](docs/rough-edges.md) — known quirks and findings

## License

Apache-2.0 ([LICENSE](LICENSE)).

## Tests

```bash
make check
```

This runs the lockfile check, Ruff lint/format checks, the offline pytest
suite, and the dependency-license audit. Use `make format` to apply automatic
lint and formatting fixes.
