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
| EOD daily backfill + nightly incremental update (resumable, idempotent) | confirmed | Instrument-owned canonical primitives and exact-dataset request validation are built and tested. D-023 partitions archive records and conservatively repairs covered, demonstrably discontinuous EOD histories into listing episodes only after completed jobs; overlapping evidence remains fail-closed. The target server's safe phase-1 EOD pass fetched 282 histories and its live repair now records 125 inferred episodes across 62 reused symbols; honest blockers and nightly current updates remain scheduled |
| Intraday ingestion: 1-hour **and 5-minute** bars | confirmed | Storage/query, cap-safe planning, and exact-frequency identity validation are built. D-024's live phase-1 hourly bootstrap independently validated 4,316 IEX segments while retaining empty, overlapping, and missing-alias exclusions; five-minute evidence remains separate. Measured history begins 2016-12-12; direct hourly omits the opening half-hour (intraday-spike.md, D-012) |
| Phased backfill: seed EOD 20y + 1-hour from 2016-12-12, then all-ticker EOD 20y, then seed 5-minute newest-to-oldest from current to 2016-12-12; current all-ticker collection continues daily | confirmed | Scope and phase priority in D-011; D-020 makes every historical dataset breadth-first by request depth, while D-013 caps 5-minute history at 30 GB/month and reserves the remaining 10 GB for current/ongoing work. The validated phase-1 EOD pass, post-backfill identity repair, and exact-hourly identity bootstrap ran on 2026-08-28; both seed datasets now resume through persisted user-systemd jobs with unresolved ranges explicit |
| Point-in-time universe storage (per-year membership) | confirmed | Built; reframed by D-010 — a dataset seed filter and historical record, not backtest membership |
| Coverage-interval ingestion with correction/adjustment refresh | confirmed | Built per D-009 (leading backfills, rolling refresh, corp-action full refresh, `reconcile`) |
| Data-quality checks (missing trading days, zero-volume runs, OHLC invariants, split sanity, per-dataset coverage/delisting reporting) | confirmed | Built in M2: read-only structured library/CLI findings cover the full architecture minimum, and consumer-declared gates fail closed when a required check was not run. M3 still defines the first study's blocking set |
| Universe provenance metadata (ranking period, effective dates, methodology, selection params) | rejected (D-010) | Universes no longer drive backtests, so provenance has nothing to guard |
| Exchange calendar / session semantics (NY sessions, DST, half-days, bar-label convention) | confirmed | Promoted at triage 2026-08-26; the intraday spike proved it mandatory because long-range IEX responses synthesize non-session rows (D-012, RE-004) |
| Tiingo request-budget / bandwidth tracking with budget-aware backfill scheduling | confirmed | Built in M2 per D-011/D-013/D-020: durable pre-request reservations and encoded-byte settlement track all attempts; a conservative rolling window enforces the 30 GB history/40 GB total ceilings; current-first cycles, phase gates, immutable stable-instrument cohorts, and persisted breadth-first cursors make quota stops resumable. Bulk fetches use `format=csv` |
| Corporate-action awareness beyond Tiingo's adjusted columns | confirmed | Promoted at triage 2026-08-26; implementation deferred until a study actually hits the limit of adjusted columns |

## Research & backtesting

| Feature | Status | Notes |
| --- | --- | --- |
| Strategy testing against the local dataset | confirmed | D-015: project-native vectorized event engine over DuckDB/polars; M3 delivers gap recovery and M4 adds a second focused event study to prove the engine generalizes; add stateful portfolio simulation only when a confirmed study needs it |
| Morning gap-down over-reaction/recovery study | confirmed | The first strategy; uses EOD open plus direct hourly checkpoints for a coarse pass, and 5-minute bars for the complete opening-window study (D-012) |
| DuckDB SQL + polars query surface | confirmed | Built (`query.py`, `market-data sql`) |
| Backtest result persistence (runs, parameters, metrics) | confirmed | Promoted at triage 2026-08-26; D-016 stores run metadata/params/metrics in SQLite and large observation outputs as Parquet under `data/`, queryable via the same DuckDB surface |
| Benchmark/risk-free comparison series in evaluation | confirmed | Promoted at triage 2026-08-26; SPY is already in the seed data |
| Example notebooks for study workflows | confirmed | Promoted at triage 2026-08-26; lands with the M3 first-study implementation now that D-015 has settled the engine |

## Interface & operations

| Feature | Status | Notes |
| --- | --- | --- |
| CLI for ingestion/maintenance + importable Python library | confirmed | Built |
| Nightly cron update | confirmed | The target server has an enabled weekday current-EOD user-systemd timer. Its locked identity-refresh/update pass writes a bounded status, treats per-symbol exclusions as partial, and retries coordinator failures; two actual post-market timer runs remain an M2 exit check. External notification is optional for the personal deployment |
| Web UI for coverage browsing and backtest results | proposed | Triage 2026-08-26: owner deliberately keeps this proposed; revisit after the first study runs end-to-end (M3). Server has a public IP if wanted |
| Realtime/streaming layer | proposed | Triage 2026-08-26: owner deliberately keeps this proposed; revisit after M3. Research-only per D-007 either way |
| Live/automated trade execution | rejected (D-007) | Research tool only |
| Options, futures, crypto data | rejected (D-008) | US stocks + ETFs only |

## Open questions (answer during M0)

Answered by the 2026-08-26 backtest-engine spike:

- **OQ-1 — Backtest engine:** *answered by D-015.* Use a project-native,
  vectorized event-study engine over DuckDB/polars. On a representative
  synthetic 1.2-million-row gap-recovery prototype it was shorter, materially
  faster, and used about half the memory of vectorbt 1.1.0 while producing
  identical results. It also preserves the warehouse's long-form shape;
  vectorbt required dense pivots and is inadmissible under D-001's license
  policy. Defer a portfolio/order simulator until a confirmed study requires
  stateful execution. See
  [backtest-engine-spike.md](backtest-engine-spike.md).

Answered by the 2026-08-26 intraday spike:

- **OQ-2 — Intraday history depth:** *answered.* AAPL, CROX, and SPY all
   begin 2016-12-12 at both frequencies, giving nearly 9 years 9 months
   through the measured completed day and enough temporal depth for the first
   study after exchange-calendar filtering. Mean CSV transfer projects to
   5.4 GB hourly and 68.5 GB 5-minute for 5,403 seed symbols. See
   [intraday-spike.md](intraday-spike.md).
- **OQ-3 — Hourly bar semantics:** *answered by D-012.* Direct hourly bars
   are fixed clock-hour bins from 10:00 onward and omit 09:30–09:59. Keep them
   for cheap later checkpoints; derive opening-window/session-relative bars
   from 5-minute data. IEX-only volume is not valid for composite liquidity or
   absolute cross-sectional thresholds.

Answered by the 2026-08-26 instrument-identity spike:

- **OQ-8 — Instrument identity for reused ticker symbols:** *answered by
  D-014 and D-023.* The current supported-tickers archive contains 993 duplicated US
  stock/ETF symbols (2,025 records), including 282 seed symbols (577 records).
  The warehouse will key bars and coverage by an internal stable instrument
  id and keep ticker/exchange/date as aliases; a Tiingo permaTicker is an
  optional, dataset-validated vendor identifier, not the warehouse key.
  Each archive row is a separate listing episode, and a completed-job EOD audit
  can split covered broader vendor histories only across conservative
  252-session gaps;
  overlaps remain unresolved and short fragments are quarantined.
  Every production write is paused until the M1 migration; afterward every
  response is identity-envelope validated and unresolved request segments fail
  closed while validated segments may ingest. See
  [instrument-identity-spike.md](instrument-identity-spike.md).

Answered at the 2026-08-26 triage:

- **OQ-4 — Seed coverage:** *answered.* The seed CSV covers 2011–2026
   (~1,600–1,900 tickers/year, 5,403 distinct). No mechanism for adding
   future years is needed: once the backfill completes, ongoing collection
   covers all tickers (D-011), so the universe list stops gating ingestion.
- **OQ-5 — Universe semantics in backtests:** *dissolved by D-010.* The
   universe is a dataset seed filter, not backtest membership; strategies
   select on stored price/volume directly.
- **OQ-6 — Results storage:** *answered.* Run metadata/parameters/metrics in
   SQLite; large observation outputs as Parquet under `data/`; both queryable
   through the existing DuckDB surface. The publication, catalog, and input-
   fingerprint contract is specified in the architecture draft and D-016.
- **OQ-7 — Tiingo plan tier:** *answered.* Power tier ($30/mo). Published
   limits as of 2026-08-26: 10k requests/hour, 100k/day, 40 GB
   bandwidth/month, ~110k unique symbols/month. Bandwidth is the binding
   constraint (D-011).
