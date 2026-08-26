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
| Intraday ingestion: 1-hour **and 5-minute** bars | confirmed | Built hourly-first (validated freq enum, per-freq views, loader); 5-min added at triage (D-011); semantics/depth against real data still need the OQ-2/OQ-3 spike |
| Phased backfill: seed EOD 20y + 1-hour ≤10y, then all-ticker EOD 20y, then seed 5-minute ≤10y, then ongoing all-ticker collection | confirmed | Scope, priority, and bandwidth math in D-011; 5-minute history deliberately last (owner, 2026-08-26) |
| Point-in-time universe storage (per-year membership) | confirmed | Built; reframed by D-010 — a dataset seed filter and historical record, not backtest membership |
| Coverage-interval ingestion with correction/adjustment refresh | confirmed | Built per D-009 (leading backfills, rolling refresh, corp-action full refresh, `reconcile`) |
| Data-quality checks (missing trading days, zero-volume runs, OHLC invariants, split sanity, per-dataset coverage/delisting reporting) | confirmed | Promoted at triage 2026-08-26; treat as required before trusting any backtest. Absorbs the former delisting/coverage-report row |
| Universe provenance metadata (ranking period, effective dates, methodology, selection params) | rejected (D-010) | Universes no longer drive backtests, so provenance has nothing to guard |
| Exchange calendar / session semantics (NY sessions, DST, half-days, bar-label convention) | confirmed | Promoted at triage 2026-08-26; intraday research needs this explicit; ties into OQ-3 |
| Tiingo request-budget / bandwidth tracking with budget-aware backfill scheduling | confirmed | Promoted at triage 2026-08-26; required by D-011 — bandwidth (40 GB/mo) is the binding constraint on the backfill. Bulk fetches use `format=csv` (client switch from JSON lands in M1) |
| Corporate-action awareness beyond Tiingo's adjusted columns | confirmed | Promoted at triage 2026-08-26; implementation deferred until a study actually hits the limit of adjusted columns |

## Research & backtesting

| Feature | Status | Notes |
| --- | --- | --- |
| Strategy testing against the local dataset | confirmed | Engine choice is OQ-1 |
| Morning gap-down over-reaction/recovery study | confirmed | The first strategy; drives the hourly-bar requirement |
| DuckDB SQL + polars query surface | confirmed | Built (`query.py`, `market-data sql`) |
| Backtest result persistence (runs, parameters, metrics) | confirmed | Promoted at triage 2026-08-26; shape per OQ-6 answer — run metadata/params/metrics in SQLite, large per-trade outputs as Parquet under `data/`, queryable via the same DuckDB surface |
| Benchmark/risk-free comparison series in evaluation | confirmed | Promoted at triage 2026-08-26; SPY is already in the seed data |
| Example notebooks for study workflows | confirmed | Promoted at triage 2026-08-26; lands once the engine (OQ-1) is chosen |

## Interface & operations

| Feature | Status | Notes |
| --- | --- | --- |
| CLI for ingestion/maintenance + importable Python library | confirmed | Built |
| Nightly cron update | confirmed | `market-data update` exists; cron wiring + failure visibility is M1 |
| Web UI for coverage browsing and backtest results | proposed | Triage 2026-08-26: owner deliberately keeps this proposed; revisit after the first study runs end-to-end (M2). Server has a public IP if wanted |
| Realtime/streaming layer | proposed | Triage 2026-08-26: owner deliberately keeps this proposed; revisit after M2. Research-only per D-007 either way |
| Live/automated trade execution | rejected (D-007) | Research tool only |
| Options, futures, crypto data | rejected (D-008) | US stocks + ETFs only |

## Open questions (answer during M0)

Still open:

1. **OQ-1 — Backtest engine:** custom vectorized loop over polars/DuckDB, or
   an existing library (e.g. vectorbt)? Answered by the M0 engine spike in
   [plan.md](plan.md); the gap study is the acceptance test.
2. **OQ-2 — Intraday history depth:** how far back does Tiingo's IEX feed
   actually go for representative tickers — at both 1-hour **and 5-minute**
   resolution (D-011) — and is that enough sample for the gap-recovery study?
   Needs measurement, not recall (M0 spike). Also measure bytes/ticker to
   calibrate D-011's bandwidth projections.
3. **OQ-3 — Hourly bar semantics:** request `resampleFreq=1hour` directly vs
   resample from finer bars (5-min is now stored anyway, D-011); how do bars
   align to the 9:30 open, and is IEX-only volume acceptable for signal
   thresholds? (M0 spike, same fetch as OQ-2.)
8. **OQ-8 — Instrument identity for reused ticker symbols:** the warehouse
   keys everything by bare ticker (`tickers` PK, coverage rows,
   `{TICKER}.parquet` paths), but ticker symbols get reused across distinct
   securities — a review (2026-08-26) counted ~1,000 duplicated symbols in
   Tiingo's supported-tickers list after the US stock/ETF filter (e.g. ACOM:
   2009–2013 NASDAQ stock, 2026 NYSE Arca ETF). Under the current model
   these would overwrite or merge, undermining D-010's survivorship-bias
   guarantee. Investigate what identity Tiingo exposes for reused symbols
   (e.g. permaTicker, listing date ranges) and decide whether the warehouse
   needs a stable instrument id or date-ranged symbol records. **Blocks
   every D-011 historical backfill phase, including the seed-list ones:**
   282 seed ticker strings match multiple supported-ticker records (577
   records; review 2026-08-26) — ACOM itself is seeded for 2011 — so a
   bare-ticker backfill could fetch the wrong security or merge two. The
   spike may narrow the gate if endpoint measurements prove a specific
   dataset or frequency safe.

Answered at the 2026-08-26 triage:

4. **OQ-4 — Seed coverage:** *answered.* The seed CSV covers 2011–2026
   (~1,600–1,900 tickers/year, 5,403 distinct). No mechanism for adding
   future years is needed: once the backfill completes, ongoing collection
   covers all tickers (D-011), so the universe list stops gating ingestion.
5. **OQ-5 — Universe semantics in backtests:** *dissolved by D-010.* The
   universe is a dataset seed filter, not backtest membership; strategies
   select on stored price/volume directly.
6. **OQ-6 — Results storage:** *answered.* Run metadata/parameters/metrics in
   SQLite; large per-trade outputs as Parquet under `data/`; both queryable
   through the existing DuckDB surface. Detailed schema lands with the M0
   architecture draft.
7. **OQ-7 — Tiingo plan tier:** *answered.* Power tier ($30/mo). Published
   limits as of 2026-08-26: 10k requests/hour, 100k/day, 40 GB
   bandwidth/month, ~110k unique symbols/month. Bandwidth is the binding
   constraint (D-011).
