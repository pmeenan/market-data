# market-data

A local market data warehouse and strategy-testing toolkit — a personal
research tool for testing trading hypotheses against a dataset you own and
control. Almost all code is written by AI agents working from the project
documentation, directed and reviewed by a human (see
[AGENTS.md](AGENTS.md)).

- **Source**: [Tiingo](https://www.tiingo.com/) (EOD daily bars with split/dividend adjustments; IEX intraday bars)
- **Storage**: Parquet files (zstd-compressed, one file per ticker for EOD, per ticker-year for intraday) + a small SQLite database for metadata (ticker universes, coverage intervals)
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
for a machine-readable result). If coverage metadata is ever lost or in
doubt, `market-data reconcile` rebuilds it from the Parquet files.

## Using the library

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
  meta.db                   SQLite: universes, tickers, coverage intervals
  eod/AAPL.parquet          full daily history per ticker
  intraday/1hour/AAPL/2025.parquet
src/marketdata/
  tiingo.py                 Tiingo REST client (throttling, retries)
  store/bars.py             Parquet bar storage (merge-upsert writes)
  store/meta.py             SQLite metadata store
  universe.py               candidate seeding + dollar-volume ranking
  ingest.py                 resumable backfill / incremental update
  query.py                  DuckDB views + polars loaders
  cli.py                    the market-data CLI
```

## Status

Milestone **M0 (plan the plan)**. The data warehouse above (ingestion,
storage, CLI) is built and tested; the research/backtesting layer is being
planned. The first study: whether stocks that open significantly down tend to
recover over the next few hours. Candidate features beyond confirmed scope
(web UI, realtime layer, data-quality tooling) are tracked in
[docs/features.md](docs/features.md) pending triage — they are not product
yet.

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
