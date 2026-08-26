# Feature matrix

The scope ledger for the M0 planning conversations. Three tiers:

- **Confirmed** — stated project scope. Milestone assignment happens in
  [plan.md](plan.md) as the plan firms up.
- **Proposed** — candidate additions awaiting a yes/no from the project owner.
- **Open questions** — things that shape architecture and need an answer
  during M0.

Status legend: `confirmed` · `proposed` · `rejected (D-NNN)`

## Data acquisition & storage

| Feature | Status | Notes |
| --- | --- | --- |
| Annual universes imported from owner's seed CSV (Year,Ticker,MedianDollarVolume) | confirmed | Built and tested |
| Universe bootstrap from Tiingo supported-tickers + dollar-volume ranking | confirmed | Built; for years the seed CSV doesn't cover |
| EOD daily backfill + nightly incremental update (resumable, idempotent) | confirmed | Built and tested |
| Hourly intraday ingestion | confirmed | Built hourly-first (validated freq enum, per-freq views, loader); semantics against real data still need the OQ-2/OQ-3 spike |
| Point-in-time universe storage (per-year membership) | confirmed | Built; survivorship-bias-aware backtests depend on it |
| Coverage-interval ingestion with correction/adjustment refresh | confirmed | Built per D-009 (leading backfills, rolling refresh, corp-action full refresh, `reconcile`) |
| Data-quality checks (missing trading days, zero-volume runs, OHLC invariants, split sanity, universe coverage) | proposed | Bad data silently corrupts study results; the 2026-08-26 external review recommends treating this as required before trusting any backtest |
| Universe provenance metadata (ranking period, effective dates, methodology, selection params) | proposed | External-review suggestion; would also resolve OQ-5 structurally and record seed (median) vs built-in ranker (mean) methodology |
| Exchange calendar / session semantics (NY sessions, DST, half-days, bar-label convention) | proposed | Research on hourly bars needs this to be explicit; ties into OQ-3 |
| Tiingo request-budget tracking / response caching | proposed | Avoids burning the API quota when iterating on backfills |
| Corporate-action awareness beyond Tiingo's adjusted columns | proposed | Only if adjusted columns prove insufficient for a study |
| Delisting/coverage report per universe year | proposed | Quantifies how much of each year's universe actually has data |

## Research & backtesting

| Feature | Status | Notes |
| --- | --- | --- |
| Strategy testing against the local dataset | confirmed | Engine choice is OQ-1 |
| Morning gap-down over-reaction/recovery study | confirmed | The first strategy; drives the hourly-bar requirement |
| DuckDB SQL + polars query surface | confirmed | Built (`query.py`, `market-data sql`) |
| Backtest result persistence (runs, parameters, metrics) | proposed | Reproducibility: rerunnable studies beat one-off notebook outputs |
| Benchmark/risk-free comparison series in evaluation | proposed | SPY is already in the seed data; formalize usage in metrics |
| Example notebooks for study workflows | proposed | Cheap documentation of the intended research loop |

## Interface & operations

| Feature | Status | Notes |
| --- | --- | --- |
| CLI for ingestion/maintenance + importable Python library | confirmed | Built |
| Nightly cron update | confirmed | `market-data update` exists; cron wiring + failure visibility is M1 |
| Web UI for coverage browsing and backtest results | proposed | Owner said "maybe"; server has a public IP if wanted |
| Realtime/streaming layer | proposed | Owner said "maybe eventually"; research-only per D-007 either way |
| Live/automated trade execution | rejected (D-007) | Research tool only |
| Options, futures, crypto data | rejected (D-008) | US stocks + ETFs only |

## Open questions (answer during M0)

1. **OQ-1 — Backtest engine:** custom vectorized loop over polars/DuckDB, or
   an existing library (e.g. vectorbt)? Answered by the M0 engine spike in
   [plan.md](plan.md); the gap study is the acceptance test.
2. **OQ-2 — Intraday history depth:** how far back does Tiingo's IEX feed
   actually go for representative tickers, and is that enough sample for the
   gap-recovery study? Needs measurement, not recall (M0 spike).
3. **OQ-3 — Hourly bar semantics:** request `resampleFreq=1hour` directly vs
   resample from finer bars; how do bars align to the 9:30 open, and is
   IEX-only volume acceptable for signal thresholds? (M0 spike, same fetch as
   OQ-2.)
4. **OQ-4 — Seed coverage:** which years does the owner's seed CSV cover, and
   how are later/current years added — extend the CSV, or bootstrap via the
   built-in ranking?
5. **OQ-5 — Universe semantics in backtests:** does year-Y membership apply to
   trades during Y (lookahead risk: ranked on Y's own volume) or during Y+1?
   Must be settled before the first study is credible.
6. **OQ-6 — Results storage:** where do backtest runs/outputs live (files
   under `data/`, a SQLite table, plain notebooks)?
7. **OQ-7 — Tiingo plan tier:** which tier is the owner on, and what are the
   real request/bandwidth limits that bound backfill throughput?
