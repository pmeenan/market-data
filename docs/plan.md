# Plan

**This is a living document.** Milestones will be re-scoped, re-ordered, split,
or added as planning conversations and findings come in. That churn is
expected; what is *not* allowed is silent change. Scope changes get a
decision-log entry; progress is reflected here by checking boxes and updating
status lines as work lands.

Check a box only when the item is done and verified; partially done items stay
unchecked, optionally with a note.

**Status legend:** `pending` · `in progress` · `done` · `parked`

## M0 — Plan the plan  `in progress`

Goal: turn the initial feature list into a settled vision, feature matrix,
architecture, and milestone ladder — through planning conversations with the
project owner plus targeted spikes where a decision needs evidence.

Note: unusually for this workflow, working substrate code predates M0 — the
warehouse (ingestion, storage, CLI, tests) was scaffolded in the kickoff
session before this workflow was adopted. M0 plans the research layer on top
of it.

- [x] Repo scaffolding for the AI-directed workflow (this scaffold).
- [x] Substrate fixes from the external review (2026-08-26): coverage
      intervals replace watermarks (leading backfills work; `reconcile`
      rebuilds state from Parquet), rolling-overlap updates + corp-action
      full refresh (D-009), publication-lag handling for empty responses,
      atomic/validated seed import (real seed exercised by an offline test;
      4 conflicting duplicates resolve keep-max with warnings), hourly-first
      intraday (freq enum, per-freq DuckDB views, `load_intraday`), ETFs in
      universe bootstrap, schema migrations (`PRAGMA user_version`), CLI
      nonzero exit + `--summary-json` on partial failure. Ingest/query/CLI
      test suites added. Second review round: `reconcile` atomically
      replaces coverage (no ghost entries), intraday never marks the current
      day covered, corp-action refreshes are validated snapshot replacements
      that fail loudly when empty/incomplete, and `update` defaults to the
      latest universe. Third round: full-refresh snapshots must retain every
      previously stored date through the old coverage edge (prefix-truncated
      responses can no longer erase history), `reconcile` applies the
      completed-day cap to intraday coverage, and the MAX(year) update
      default is marked provisional pending OQ-5. Fourth round: full-refresh
      snapshots are also validated against the triggering fetch (its dates
      must be present and its div/split values must agree). Fifth round:
      backfill segment frames remain staged until any required full-refresh
      validation succeeds, so failed refreshes leave Parquet untouched
      (36 tests).
- [x] Feature triage (2026-08-26): every `proposed` row resolved — promoted
      data-quality checks, exchange calendar, corp-action awareness
      (implementation deferred), budget-aware backfill scheduling, result
      persistence, benchmark series, notebooks; rejected universe provenance
      (D-010); web UI and realtime deliberately stay `proposed` until after
      M2 (owner's call). OQ-4/5/6/7 answered (see features.md); OQ-1/2/3
      remained for the spikes at triage. Backfill scope/priority and the 5-minute
      intraday addition recorded as D-011; universe reframing as D-010.
- [x] Spike — intraday depth & semantics (OQ-2, OQ-3; 2026-08-26): fetched
      1-hour and 5-minute bars for a handful of representative tickers
      (large-cap, mid-cap, ETF), measured coverage depth at each frequency,
      checked bar alignment to the 9:30 open and IEX-volume caveats, and
      recorded bytes/ticker with `format=csv` to calibrate D-011's bandwidth
      projections.
      Both frequencies begin 2016-12-12. Direct hourly bars omit 09:30–09:59;
      5-minute data is required for opening-window/session-relative bins.
      Measured seed-list projections are 5.4 GB hourly and 68.5 GB 5-minute.
      See [intraday-spike.md](intraday-spike.md), D-012, and RE-002..RE-004.
- [x] Spike — instrument identity (OQ-8; 2026-08-26): confirmed 993 reused
      US stock/ETF symbol strings (2,025 records) in Tiingo's current
      supported-tickers list, including 282 seed symbols (577 records).
      Tiingo permaTickers separate EOD histories when known, but the public
      list exposes no stable id, discovery is incomplete, and an IEX probe
      failed identity validation. D-014 therefore adopts internal stable
      instrument ids with date-ranged symbol aliases and dataset-specific
      vendor identifiers. Every production write is paused until the M1
      migration; afterward every response is identity-envelope validated,
      validated segments may proceed, and unresolved segments fail closed. See
      [instrument-identity-spike.md](instrument-identity-spike.md).
- [x] Spike — backtest engine (OQ-1; 2026-08-26): prototyped the
      gap-recovery event study with DuckDB/polars and vectorbt 1.1.0 on the
      same representative synthetic 1.2-million-row dataset. Both produced
      identical results; the native path was shorter, materially faster, used
      about half the memory, and kept the warehouse's long-form shape.
      vectorbt is also inadmissible under D-001's dependency policy (Commons
      Clause). D-015 chooses a project-native vectorized event engine and
      defers a portfolio/order simulator until a study needs one. See
      [backtest-engine-spike.md](backtest-engine-spike.md).
- [x] Settle universe-membership semantics for backtests (OQ-5): dissolved by
      D-010 — universes are dataset seed filters, not backtest membership.
- [ ] First full draft of architecture.md (research layer + results storage,
      OQ-6).
- [ ] Toolchain hardening decisions: lint/format (e.g. ruff), CI or a local
      pre-commit check, dependency locking, license audit. Record in
      decisions.md.
- [ ] Benchmark the Parquet file layout (per-ticker vs yearly/bucketed
      compaction) with representative cross-sectional scans and intraday
      ingestion before revisiting D-003's layout.
- [ ] Rewrite the provisional ladder below into real milestones with exit
      criteria.

**Exit criteria:** the owner has walked features.md and says the plan is good
enough to build from; the former architecture-blocking question OQ-1 is
answered by D-015 (OQ-8 by D-014, OQ-2/OQ-3 by D-012, and OQ-5 was
dissolved); toolchain decided; M1+ milestones have scopes. M0 is a
conversation, not a phase — it exits on the owner's call, not on a checklist
reaching zero.

## Provisional milestone ladder  `pending — to be rewritten in M0`

Ordered by risk: the riskiest substrate (intraday coverage) and one end-to-end
research path before breadth. Sketch only — do not start work from these
entries, and note that they freely reference `proposed` features.md rows;
nothing here pre-empts the M0 triage.

- **M1 — Data substrate hardening.** Migrate the registry, coverage, Parquet
  paths/rows, and ingestion API from bare tickers to D-014 instrument ids +
  date-ranged aliases; build the identity resolver/validation report and
  enforce envelope checks on every response so unresolved segments fail
  closed while validated work can proceed.
  Intraday ingestion (1-hour + 5-minute)
  hardened against the 10,000-row cap and range-dependent chunk boundaries
  (RE-002, RE-004), then verified end-to-end against real Tiingo data; Tiingo
  client switched to `format=csv` for bulk fetches (D-011); exchange-calendar
  filtering built before research; budget-aware backfill scheduler persists
  global date-band progress, refreshes current data first, hard-caps historical
  5-minute transfer at 30 GB per billing month, preserves the other 10 GB for
  current/ongoing work, and fills history newest-to-oldest (D-013); D-011's
  phases underway in order (seed EOD + 1-hour, then all-ticker EOD, then seed
  5-minute — a metered, multi-month process that continues in the background
  across later milestones); nightly cron in place with visible failures; ruff
  + license audit wired in.
- **M2 — First study end-to-end.** The gap-recovery study runs from stored
  data to summary statistics with the chosen engine, selecting tickers from
  the stored data itself (D-010); results persisted (per OQ-6).
- **M3 — Research breadth.** Data-quality checks, coverage reports,
  benchmark-relative metrics, a second strategy to prove the engine
  generalizes.
- **M4 — Web UI (if promoted).** Coverage browser + backtest-result viewer on
  the server's public IP.
- **M5 — Realtime layer (if promoted).** Intraday-refreshed signals for the
  owner; still research-only (no execution, D-007).
