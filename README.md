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
not backtests: studies select stable instruments from the stored data directly,
and survivorship-bias protection comes from backfilling all listings including
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

# Inspect
market-data status
market-data quality --dataset eod --summary-json data/quality-eod.json
market-data sql "SELECT instrument_id, max(date), count(*) FROM eod GROUP BY instrument_id ORDER BY 2 DESC LIMIT 10"
```

On a v2 warehouse, `backfill` and `update` partition each ticker/date request
through alias and exact-dataset identifier evidence. Validated segments may
proceed; unresolved/conflicting segments are reported and make the command exit
nonzero. An unmigrated v1 warehouse remains blocked.

The ingestion primitives are idempotent and resumable: each
(`instrument_id`, exact dataset key) pair tracks
a coverage interval (so backfills fill missing leading history too), Parquet
writes are merge-upserts keyed on date/timestamp, nightly updates refetch a
rolling overlap to pick up corrections and restated adjustments, and a newly
observed split/dividend triggers a full-history refresh for that instrument.
Operational ingestion is permitted only for request segments with independently
validated exact-dataset identity evidence; unresolved work remains fail-closed
and visible. Intraday history uses frequency-specific, sub-10,000-row request
units through the next XNYS session; lookahead rows are validated and discarded,
while only rows inside the target's identity envelope can be normalized or
published (D-021). If coverage rows are lost or in doubt,
`market-data reconcile` rebuilds them from active v2 files or an unmigrated v1
warehouse. This does not recreate a lost `meta.db`: identity evidence cannot be
reconstructed from bar files, so restore the metadata database from backup
first.

`market-data quality` scans canonical bars without repairing or rewriting them.
It reports missing expected XNYS sessions, duplicate keys, OHLC and negative
or missing-volume violations, suspicious calendar-contiguous zero-volume runs,
split-factor sanity,
off-session intraday rows, and coverage/lifecycle summaries. Findings alone do
not imply one universal policy: use repeatable `--block-on CHECK` options when a
consumer needs selected warning/error findings to return nonzero. The JSON
report records the complete check scope and gate outcome; a declared check that
was not applicable to the scanned datasets—or had an empty scope—fails closed.
Full scans aggregate in DuckDB under a 4 GB memory limit and spill beneath the
warehouse if necessary instead of materializing whole datasets in Python.
Gate failures exit 1; scan/report operational failures exit 2 and preserve any
standing successful summary JSON.

### M1 bar migration

M1's controlled EOD/IEX canary passed on 2026-08-27. The storage migration can
be exercised with:

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
disables legacy ticker-owned paths. Instrument-keyed queries are active; the
operator ingestion commands validate and report every request segment before
calling the canonical primitives, and reject any response outside that segment.
Schema v3 names the canonical SQL table `meta.coverage` and retains the old
shape explicitly as `meta.ticker_coverage_v1` during the transition.

## Using the library

```python
from marketdata import load_config
from marketdata.quality import check_quality, evaluate_quality
from marketdata.query import connect, load_eod, load_intraday_sessions

config = load_config()

# Polars frame of daily bars selected by stable ids. The keyword-only selector
# prevents an old positional ticker list from silently changing meaning.
df = load_eod(
    config,
    instrument_ids=["apple-id", "microsoft-id"],
    start="2020-01-01",
)

# Calendar-filtered intraday bars. Canonical timestamps remain unchanged;
# the projection adds UTC session bounds, minutes from open, early-close state,
# and the frequency's explicit bar-label semantics.
intraday = load_intraday_sessions(
    config,
    instrument_ids=["apple-id"],
    start="2024-01-01",
    freq="5min",
)

# Each study declares its own blocking set. M3 will define the first study's
# exact policy and persist this outcome with its run.
quality = check_quality(
    config,
    dataset_keys=["eod", "intraday_1hour"],
    instrument_ids=["apple-id"],
    start="2024-01-01",
)
gate = evaluate_quality(
    quality,
    ["missing_expected_sessions", "ohlc_invariants", "negative_values"],
)

# Or raw DuckDB for arbitrary SQL (views: eod, intraday_<freq> per frequency
# present on disk, e.g. intraday_1hour; metadata at meta.*)
con = connect(config)
con.execute("""
    SELECT instrument_id, avg(adj_close * adj_volume) AS adv
    FROM eod WHERE date >= '2025-01-01'
    GROUP BY instrument_id ORDER BY adv DESC LIMIT 20
""").pl()
```

`load_eod_by_ticker` and `load_intraday_by_ticker` are display conveniences;
they require both range endpoints and fail if any alias segment has zero or
multiple matches. Raw canonical views and frames never use ticker as an owner
or join key. Companion `*_with_alias` views derive a nullable as-of ticker.

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
  bar_fields.py              shared Tiingo bar-field contract
  calendar.py                XNYS sessions, IEX request bounds, bar labels
  quality.py                 structured checks + consumer-declared gates
  identity.py               fail-closed identity resolution result contracts
  tiingo.py                 Tiingo REST client (CSV bars, retries, metering)
  store/bars.py             Parquet bar storage (merge-upsert writes)
  store/meta.py             SQLite metadata store
  universe.py               candidate seeding + dollar-volume ranking
  ingest.py                 resumable backfill / incremental update
  query.py                  DuckDB views + polars loaders
  reconcile.py              v1/v2 coverage recovery from Parquet
  cli.py                    the market-data CLI
```

## Status

Milestone **M2 (trustworthy scheduled ingestion)** is in progress. Its
cap-safe intraday request planner, XNYS calendar/session-label surface, and
structured quality/gating layer are implemented; durable scheduler/budget
state, shared process locking, and scheduled operations remain. M1 closed on
2026-08-27 after its controlled EOD/IEX canary passed. Production ingestion is
permitted only for validated segments; unresolved work remains fail-closed and
visible. The research/backtesting layer begins in M3.

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
