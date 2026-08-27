# Plan

**This is a living document.** Milestones will be re-scoped, re-ordered, split,
or added as planning conversations and findings come in. That churn is
expected; what is *not* allowed is silent change. Scope changes get a
decision-log entry; progress is reflected here by checking boxes and updating
status lines as work lands.

Check a box only when the item is done and verified; partially done items stay
unchecked, optionally with a note.

**Status legend:** `pending` · `in progress` · `done` · `parked`

## M0 — Plan the plan  `done`

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
      M3 (owner's call). OQ-4/5/6/7 answered (see features.md); OQ-1/2/3
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
- [x] First full draft of architecture.md (2026-08-26): component boundaries,
      D-014 target storage/identity model, ingestion and query flows,
      calendar/quality contracts, D-015 event-study execution, and D-016
      atomic SQLite-catalog + Parquet-observation result persistence settle
      OQ-6, including its honest non-versioned-input limitation. Review
      hardening in D-017 isolates the v1/v2 storage namespaces, unifies exact
      dataset keys, and prevents multi-file crash holes from becoming covered.
- [x] Toolchain hardening (2026-08-26): pinned uv + a committed universal
      lockfile, Ruff formatting/linting, one `make check` entry point, and
      pinned GitHub Actions checks on Python 3.11/3.12. The full resolved
      universal lock plus the build backend is SPDX-license-audited; D-018
      records the tools and the sole narrow MPL-2.0 transitive exception for
      `certifi`.
- [x] Benchmark the Parquet file layout (2026-08-27): a reproducible
      39.5-million-row, five-minute-shaped comparison found that year/hash
      buckets cut file count 93.6% and storage 14.6%, improved representative
      warm cross-sectional queries 2.1–3.9x, and made a 64-instrument batched
      publish 2.3x faster, at the cost of a 6.1x slower isolated write. D-019
      adopts 256 stable SHA-256 buckets and makes bucket batching the normal
      ingestion path. See
      [parquet-layout-benchmark.md](parquet-layout-benchmark.md).
- [x] Rewrite the provisional ladder into scoped milestones with explicit exit
      criteria (2026-08-27). The implementation sequence is M1 identity-safe
      storage, M2 trustworthy scheduled ingestion, M3 the first persisted
      study, and M4 completion of the historical program plus the full
      opening-window and second studies. Optional product layers remain
      outside the ladder until the owner promotes them.
- [x] Owner plan walk and approval (2026-08-27): M0 closed and the M1
      implementation gate opened.

**Exit criteria:** the owner has walked features.md and says the plan is good
enough to build from; the former architecture-blocking question OQ-1 is
answered by D-015 (OQ-8 by D-014, OQ-2/OQ-3 by D-012, and OQ-5 was
dissolved); toolchain decided; M1+ milestones have scopes. M0 is a
conversation, not a phase — it exits on the owner's call, not on a checklist
reaching zero.

**Implementation gate:** satisfied 2026-08-27 when the owner approved the
ladder and explicitly closed M0.

## M1 — Identity-safe canonical warehouse  `in progress`

Goal: replace the ticker-keyed v1 substrate with the D-014/D-017/D-019 model
and reopen production ingestion only for request segments whose identity can
be proved.

Scope:

- [x] Implement the identity metadata and resolution contracts specified by
      D-014 and the architecture's SQLite metadata section. Produce a report
      that leaves zero/multiple matches explicit instead of guessing.
      Completed 2026-08-27: schema-v2 identity registry, date-segmented alias
      reports, exact-dataset-key vendor-identifier resolution, and recorded
      universe resolution.
- [x] Implement the v2 storage migration, bar publication, and reconciliation
  contracts in the architecture's persistent-data-model and identity/ingestion
  sections, including the D-017 generation boundary and D-019 layout. Those
  documents remain normative for the mechanics; this milestone owns their
  implementation and migration reporting. Completed 2026-08-27: stable
  SHA-256 bucket paths and batch merge-upsert/snapshot publication, whole-root
  v1 quarantine, deterministic fail-closed migration reporting, schema-v3
  instrument coverage, an explicit guarded storage-generation boundary, and
  conservative per-instrument reconciliation.
- [x] Move ingestion and query APIs to `instrument_id`. Ticker conveniences
      require an as-of range and resolve through aliases; active DuckDB views
      never union quarantined v1 files. Completed 2026-08-27: canonical-only
      ingestion primitives and coverage, v2-only DuckDB views/loaders,
      unambiguous alias display views, and explicit-range ticker loaders that
      fail on evidence gaps/conflicts. Operator ingestion is supplied by the
      following completed item.
- [x] Implement the architecture's identity/ingestion flow for all three exact
      dataset keys. Each independently validated segment may make progress while
      an unresolved or conflicting segment remains fail-closed and reported.
      Completed 2026-08-27: alias and exact-dataset identifier evidence is
      partitioned into explicit request segments; timestamp/metadata conflicts
      reject a response before normalization; safe peers retain bucket batching;
      disconnected units cannot bridge coverage; guarded permanent identifiers
      can authorize rename-spanning EOD refreshes; and CLI/JSON reports expose
      every blocked or failed segment. Review hardening also permits a bare
      ticker to refresh a provably single-alias history, treats inactive aliases
      as routine update skips, preserves the newest-first anchor after failures,
      accepts weekend-only evidence boundaries, batches heterogeneous request
      ranges per bucket, and publishes summaries atomically.
- [x] Switch bulk Tiingo bar fetches and fixtures to CSV while preserving
      retries, normalization, response validation, and byte measurement.
      Completed 2026-08-27: EOD and IEX bar requests use validated CSV
      parsing; transport fixtures exercise the existing nullable normalized
      schemas, while cumulative request/wire-byte counters charge HTTP retries,
      partial transport failures, and payloads rejected after transport.
      Review hardening added network retries, RFC-date `Retry-After`, exact
      encoded-byte metering, defensive empty-result handling, and shared field
      contracts.

Exit criteria:

- [ ] A fixture containing a rename, a reused symbol, an evidence gap, and an
  overlapping alias migrates deterministically: resolvable bars appear once
  under the correct stable ids and every unsafe source remains quarantined and
  reported.
- [ ] No active bar row, path, coverage key, or durable research-facing join is
  owned by a ticker string; all three dataset keys require independent
  identity evidence, with tests proving cross-frequency evidence is rejected.
- [ ] Crash-boundary and rerun tests prove unique keys, no falsely bridged
  coverage, atomic per-bucket publication, and convergence after interruption.
- [ ] A controlled real-Tiingo canary validates at least one resolved EOD and
      one resolved IEX request without writing any unresolved segment.
      Production ingestion is then permitted for validated segments;
      unresolved work remains fail-closed and visible.
- [ ] `make check` passes and the migration/operator report documents what
      moved, what remains quarantined, and how to retry safely.

## M2 — Trustworthy scheduled ingestion  `pending`

Goal: make current collection and the long-running historical program safe to
operate unattended within Tiingo's limits, and make data fitness visible before
research consumes it.

Scope:

- [ ] Harden hourly and five-minute planning against the 10,000-row cap and
  range-dependent IEX chunk boundaries (RE-002, RE-004). Filter and label
  sessions through a US exchange calendar with explicit UTC, DST, half-day,
  and vendor bar-label semantics; raw vendor timestamps remain unchanged.
- [ ] Implement the calendar and quality contracts in architecture.md, including
  every minimum check listed there. Checks emit structured findings and never
  silently repair vendor bars; each study remains responsible for declaring
  which findings block that study.
- [ ] Persist observed encoded-response-byte/request accounting and scheduler
  progress.
  Execute D-011's phases, D-013's current-first/budget policy, and D-020's
  breadth-first request-depth sweeps for every manual or scheduled historical
  entry point without duplicating their limits here. Before enforcing the
  vendor bandwidth cap, verify Tiingo's billing-byte basis or budget against a
  conservatively safe interpretation; RE-006 found no compression in current
  bar responses but the published limit does not define its accounting basis.
- [ ] Put ingestion, reconciliation, and later research publication behind the
  shared data-directory process lock. Preserve nonzero CLI exits and bounded
  machine-readable summaries for partial failure.
- [ ] Install the owner's scheduled Linux job for current updates with a bounded
  status record and an actually observed notification path for failure. Start
  phase 1 (seed EOD plus hourly) as the first resumable background backfill.

Exit criteria:

- [ ] Offline boundary fixtures prove chunk planning neither loses nor duplicates
  rows at the row cap, year, alias, DST, holiday, and half-day boundaries; a
  controlled Tiingo run confirms the planner against both intraday frequencies.
- [ ] Scheduler tests prove current work wins; quota stops resume the unvisited
  remainder of the same deterministic sweep; no instrument reaches request
  depth N+1 before every eligible peer receives a depth-N turn; failed or
  identity-blocked segments retain their frontier without stalling safe peers;
  actual bytes are charged even for rejected responses; and neither monthly
  hard cap can be exceeded.
- [ ] Every minimum architecture quality check has a focused fixture and appears in
  structured CLI/library reports on stored data. The mechanism for a consumer
  to declare findings blocking is tested; M3 defines the first study's set.
- [ ] Two consecutive scheduled current-update runs complete on the target server;
  an induced failure returns nonzero, records a bounded diagnostic, and reaches
  the owner's notification channel.
- [ ] Phase 1 is running through the persisted scheduler, its coverage and budget
  state survive restart, and `make check` passes.

## M3 — First persisted study, end to end  `pending`

Goal: deliver the shortest honest research path while the metered backfill
continues: the gap-recovery study from stored EOD/direct-hourly data through
queryable, immutable results.

Scope:

- [ ] Add the D-016 result catalog, typed/canonical parameters, tidy metrics,
  immutable Parquet observations, explicit input-file manifests and content
  fingerprints, failure cleanup, and catalog-filtered DuckDB result loading.
- [ ] Implement the library-level D-015 vectorized event runner and a CLI entry
  point. It selects candidates from stored bars (not universe membership),
  holds the shared lock through publication, applies declared calendar/quality
  gates, and never claims portfolio/order semantics.
- [ ] Implement the coarse gap-recovery study using adjusted EOD prior close/open
  inputs and Tiingo's direct clock-hour checkpoints from 10:00 onward. Include
  stored SPY benchmark-relative evaluation and label the absent 09:30–09:59
  interval explicitly rather than inferring it.
- [ ] Provide one reproducible example notebook that calls the same library runner
  and loads the same published artifacts; notebooks do not contain a second
  execution or publication path.
- [ ] Report stale `running` rows and orphaned result artifacts without selecting
  or deleting them automatically.

Exit criteria:

- [ ] One command runs the study on a representative stored cohort and publishes
  a `succeeded` catalog row, manifest, observations, parameters, and summary
  metrics that are queryable together through DuckDB.
- [ ] Tests inject failures before and after each publication boundary and prove
  that only complete compatible runs load, successful runs are immutable,
  retries receive new ids, and input-fingerprint checks detect changed source
  files.
- [ ] Event counts and returns match a small hand-calculated fixture, candidate
  selection is demonstrably independent of universe membership, and quality
  failures block publication as declared.
- [ ] The CLI output and notebook state the direct-hourly and IEX-volume limits;
  benchmark conventions match the event observations; `make check` passes.

## M4 — Full opening-window study and historical program  `pending`

Goal: finish the planned dataset breadth and extend the coarse result into the
complete five-minute, session-relative opening-window study promised by the
vision, then exercise the research engine with a second strategy.

Scope:

- [ ] Keep the metered scheduler running through D-011 phase 1, phase 2 (20 years
  of EOD for all supported US stocks/ETFs, including delisted listings), and
  phase 3 (seed five-minute history back to 2016-12-12). Begin forward-only
  all-ticker current five-minute collection no later than phase 3, as required
  by D-013.
- [ ] Extend the gap study with exchange-calendar-filtered five-minute observations
  for the opening half-hour and other session-relative windows. Retain direct
  hourly results as their own vendor-frequency checkpoints; do not relabel them
  as session-aligned aggregates.
- [ ] Run final coverage, identity-resolution, and blocking-quality reports for
  each phase. Targets that cannot be safely resolved remain explicit exclusions
  with evidence; milestone completion never authorizes guessed identity.
- [ ] Compare the coarse and full study over their common instruments/sessions,
  publish the complete study as a new schema-versioned run, and update the
  example notebook with the full opening-window workflow.
- [ ] Implement a second focused event study, selected with the owner at M4 start,
  through the same query, quality-gate, evaluation, and publication surfaces to
  prove the D-015 engine generalizes beyond gap recovery.

Exit criteria:

- [ ] Every target in phases 1–3 is either covered to its defined range or listed
  in a durable unresolved/failed report; scheduler records show phase order,
  D-020 breadth-first sweep order, and every billing window remained within
  D-013's limits.
- [ ] Nightly EOD/hourly/five-minute collection is current for every resolvable
  all-ticker target, with honest per-dataset coverage and visible failures.
- [ ] The full study publishes validated opening-half-hour and later-window
  observations through the M3 result path, passes hand-calculated session and
  early-close fixtures, and documents the measured effect of adding five-minute
  coverage relative to the coarse run.
- [ ] The second study publishes through the same runner without a parallel
  framework or study-specific publication path, and its events and metrics
  match a hand-calculated fixture.
- [ ] The vision's end-to-end study and interactive-query success criteria are
  demonstrated on the target server, and `make check` passes.

## Work deliberately outside the committed ladder

- After M3, the owner will revisit the proposed read-only web UI and realtime
  research layer. If either is promoted, it gets a newly scoped milestone and
  decision updates before implementation; neither is an implicit M4 task.
- Corporate-action handling beyond Tiingo's adjusted columns is triggered by
  a concrete study limitation. A stateful portfolio/order simulator is likewise
  triggered only by a confirmed study that needs execution semantics.
- Live or automated execution and non-US-stock/ETF assets remain non-goals,
  not future milestones.
