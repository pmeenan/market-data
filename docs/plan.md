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
      migration; afterward every publishable response row is identity-envelope
      validated, validated segments may proceed, and unresolved segments fail
      closed. See
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

## M1 — Identity-safe canonical warehouse  `done`

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

- [x] A fixture containing a rename, a reused symbol, an evidence gap, and an
  overlapping alias migrates deterministically: resolvable bars appear once
  under the correct stable ids and every unsafe source remains quarantined and
  reported.
- [x] No active bar row, path, coverage key, or durable research-facing join is
  owned by a ticker string; all three dataset keys require independent
  identity evidence, with tests proving cross-frequency evidence is rejected.
- [x] Crash-boundary and rerun tests prove unique keys, no falsely bridged
  coverage, atomic per-bucket publication, and convergence after interruption.
- [x] A controlled real-Tiingo canary validates at least one resolved EOD and
      one resolved IEX request without writing any unresolved segment.
      Production ingestion is then permitted for validated segments;
      unresolved work remains fail-closed and visible.
      Completed 2026-08-27: an isolated AAPL canary over 2025-08-25..26 used
      independently recorded EOD and `intraday_1hour` evidence, made exactly
      one validated request to each endpoint, published 2 EOD and 12 IEX rows,
      and blocked an unresolved peer in both datasets without creating an
      instrument, coverage, or bars for it. Reconciliation reported no issues;
      a rerun made no HTTP requests and left the canonical files unchanged.
- [x] `make check` passes and the migration/operator report documents what
      moved, what remains quarantined, and how to retry safely.

## M2 — Trustworthy scheduled ingestion  `in progress`

Goal: make current collection and the long-running historical program safe to
operate unattended within Tiingo's limits, and make data fitness visible before
research consumes it.

Scope:

- [x] Harden hourly and five-minute planning against the 10,000-row cap and
  range-dependent IEX chunk boundaries (RE-002, RE-004). Filter and label
  sessions through a US exchange calendar with explicit UTC, DST, half-day,
  and vendor bar-label semantics; raw vendor timestamps remain unchanged.
  Completed 2026-08-27: frequency-specific weekday-grid bounds keep every
  request below the silent cap, each chunk fetches through the next XNYS
  session and discards the validated lookahead, cap-sized responses fail
  closed, and session-labelled loaders expose UTC opens/closes, DST,
  half-days, minutes from open, and distinct direct-hourly/five-minute label
  semantics without altering canonical bars. D-021 makes the one-session
  request extension discard-only context so delisted target envelopes remain
  exact rather than being falsely extended.
- [x] Implement the calendar and quality contracts in architecture.md, including
  every minimum check listed there. Checks emit structured findings and never
  silently repair vendor bars; each study remains responsible for declaring
  which findings block that study. Completed 2026-08-27: the XNYS calendar
  surface from the preceding planner work now feeds read-only stored-data checks
  for missing expected sessions, duplicate keys, raw/adjusted OHLC invariants,
  negative values, configurable zero-volume runs, split sanity, off-session
  intraday rows, and per-dataset coverage/lifecycle summaries. Library and CLI
  reports are deterministic and structured; consumer-declared gates block on
  warning/error findings and fail closed when a declared check was not run.
- [x] Persist observed encoded-response-byte/request accounting and scheduler
  progress.
  Execute D-011's phases, D-013's current-first/budget policy, and D-020's
  breadth-first request-depth sweeps for every manual or scheduled historical
  entry point without duplicating their limits here. Before enforcing the
  vendor bandwidth cap, verify Tiingo's billing-byte basis or budget against a
  conservatively safe interpretation; RE-006 found no compression in current
  bar responses but the published limit does not define its accounting basis.
  Completed 2026-08-28: schema-v4 request attempts reserve quota before every
  authenticated transport attempt and settle to observed encoded bytes after
  complete responses, including retries and later-rejected payloads; partial
  attempts and process crashes retain their conservative reservation, while
  orderly failures before any response settle to a known zero bytes. D-028's
  documented midnight-EST billing month and a 64 MB per-response reservation
  enforce D-025's 30-to-39 GB late-month historical admission ramp against
  total usage and the 40 GB current-work ceiling without assuming an
  undocumented billing-byte basis. Manual and
  scheduled history share immutable stable-instrument cohorts, durable
  per-alias frontiers and sweep cursors, exact phase/dataset gates, and one
  maximum-safe request unit per eligible instrument per sweep; ready peers in
  one hash bucket retain D-019's batched publication. The current-first cycle
  records current and historical work separately and will not begin history
  after incomplete current work. Review hardening made overlap refreshes
  effective for intraday data, checkpoints the published prefix of a
  quota-interrupted batch, terminalizes identity/oversized-response blockers,
  permits audited job cancellation/retry, and gives each force invocation a
  fresh resumable job unless an explicit id is supplied.
- [x] Put ingestion, reconciliation, and later research publication behind the
  shared data-directory process lock. Preserve nonzero CLI exits and bounded
  machine-readable summaries for partial failure.
  Completed 2026-08-28: D-022's persistent advisory lock serializes ingestion,
  historical job setup/request turns, reconciliation, migration, and legacy
  bar ranking at library coordinator boundaries; nested coordinators are
  reentrant, while competing threads/processes fail fast with bounded holder
  diagnostics. History yields the lock between durable turns, and cancellation
  remains an available SQLite control signal that stops a live sweep after its
  current turn.
  Cross-process CLI fixtures prove contention makes update, backfill, and
  reconcile exit nonzero, preserves bounded JSON failure summaries, and does
  not create a contended history job. M3's library study runner will acquire
  this same lock through input selection and result publication.
- [x] Install the owner's scheduled Linux job for current updates with a bounded
  nonzero status record. An external notification channel is optional for this
  personal deployment and is not an M2 gate. Start phase 1 (seed EOD plus
  hourly) as the first resumable background backfill.
  Completed 2026-08-28: initialized the target-server v2 warehouse, imported the
  2011--2026 seed universes, registered 574 archive-backed EOD episodes, and
  retained 648 missing-archive cases, 112 overlap groups, and 18 metadata/404
  failures as explicit fail-closed work. The safe EOD pass fetched 282
  additional histories with no failures. D-023 then repaired 62 covered broad
  histories into 125 inferred episodes; the final scan has zero structural or
  OHLC errors. A weekday 23:30 UTC user-systemd current-EOD job is installed and
  enabled; it refreshes latest-universe EOD identity evidence and writes a
  bounded atomic status before returning exit 1 for per-symbol identity/vendor
  exclusions, exit 0 for quota stops, or retryable exit 2 for coordinator,
  configuration, lock, and report-publication failure. D-024's independently
  metered hourly bootstrap then validated 4,316 exact-frequency IEX segments;
  399 empty probes, 111 alias-overlap spans, 666 missing-alias tickers, and 274
  pre-IEX tickers remain explicit exclusions. D-027's schema-v7 ordered program
  then replaced the two fixed phase-1 timers: it adopts those exact terminal
  jobs, freezes a full supported-US phase-2 scope, batches dataset-specific
  identity preparation, and admits only registered later-phase jobs after every
  declared predecessor is terminal. The program and current-update services
  retry exit 2 up to three times at two-minute intervals. The live coordinator
  is enabled, froze 23,078 supported-US instruments, and completed phase 2 with
  accepted exclusions. Its sole final safe range, a recent DSPC episode with
  repeated responses that produced no coverage, was operator-terminalized
  without claiming coverage so the program could begin phase-3 five-minute
  identity preparation before month end.

Exit criteria:

- [x] Offline boundary fixtures prove chunk planning neither loses nor duplicates
  rows at the row cap, year, alias, DST, holiday, and half-day boundaries; a
  controlled Tiingo run confirms the planner against both intraday frequencies.
  Completed 2026-08-28: one parameterized end-to-end fixture now drives both
  frequencies through adjacent old/new aliases, year rollover, DST, holidays,
  early closes, lookahead discard, and multi-chunk publication at the largest
  safe weekday-grid envelopes (9,996 hourly rows and 9,984 five-minute rows),
  proving exact raw-row preservation, unique timestamps, contiguous coverage,
  and calendar-filtered research rows. A metered AAPL canary over
  2025-11-26..2025-12-01 fetched through the next XNYS session on 2025-12-02:
  the 30 hourly and 390 five-minute response rows exactly matched their planner
  envelopes; discard left 24/312 raw target rows and XNYS filtering left the
  expected 15/198 research rows. All three diagnostic/canary requests settled
  to 27,963 observed encoded bytes in the durable ledger, including the first
  hourly probe that exposed the already-documented RE-004 closed-session rows.
- [x] Scheduler tests prove current work wins; quota stops resume the unvisited
  remainder of the same deterministic sweep; no instrument reaches request
  depth N+1 before every eligible peer receives a depth-N turn; retryable
  failures and terminal blocked segments retain their frontier without
  stalling safe peers; definitive HTTP 404s become dormant fail-closed
  exclusions until explicitly retried; actual bytes are charged even for
  rejected responses; and neither monthly hard cap can be exceeded.
  Completed 2026-08-28 with offline restart, retry, quota, phase-order,
  current-first, failure, identity-block, trailing-coverage, partial-batch,
  force-lifecycle, oversized-stream, zero-byte-outage, and dual-byte-ceiling
  fixtures.
- [x] Every minimum architecture quality check has a focused fixture and appears in
  structured CLI/library reports on stored data. The mechanism for a consumer
  to declare findings blocking is tested; M3 defines the first study's set.
  Completed 2026-08-27; corrupt-input fixtures also prove checks do not rewrite
  canonical Parquet. Review hardening made scans bounded DuckDB aggregations,
  made empty scopes and unrun declared checks fail closed, treated missing
  volume as invalid, made zero-volume runs key-unique and calendar-contiguous,
  and separated operational CLI failures from gate failures. `make check`
  passes with 134 tests.
- [ ] Two consecutive scheduled current-update runs complete on the target server;
  an induced failure returns nonzero and records a bounded diagnostic visible
  through the user-systemd result and status JSON.
  The installed service completed two consecutive controlled pre-publication
  runs on 2026-08-28, and induced lock contention produced systemd exit 2 plus
  a 506-byte diagnostic status before a healthy rerun restored the standing
  4.9 KB status. The first actual timer-triggered post-market run completed on
  2026-09-01 with 1,623 fetched, 62 corporate-action refreshes, and 12 explicit
  blockers; one further consecutive actual run remains, so this criterion stays
  unchecked. The scheduler/status checkpoint passed
  `make check` with 188 tests before the phase-1 bootstrap work landed.
- [x] Phase 1 ran through the persisted scheduler, its coverage and budget
  state survive restart, and `make check` passes.
  Seed EOD made its safe persisted pass on 2026-08-28 (282 fetched, zero
  failed), survived cancellation/restart during review, and now resumes an
  aligned post-repair cohort for explicit blockers. The hourly service's live
  first sweep published 37 instruments before an induced SIGTERM at durable
  cursor 41; the interrupted request retained its conservative reservation and
  claimed no coverage. Restarting the same job resumed past cursor 59 and 52
  covered instruments, proving persisted cohort, coverage, and budget state
  survive the process boundary. Both components are now terminal with durable
  exclusions and have been adopted by D-027; the replacement program's offline
  restart/scope/ordering fixtures pass in the 229-test full `make check`.

## M3 — First persisted study, end to end  `in progress`

Goal: deliver the shortest honest research path while the metered backfill
continues: the gap-recovery study from stored EOD/direct-hourly data through
queryable, immutable results.

Scope:

- [x] Add the D-016 result catalog, typed/canonical parameters, tidy metrics,
  immutable Parquet observations, explicit input-file manifests and content
  fingerprints, failure cleanup, and catalog-filtered DuckDB result loading.
  Completed 2026-08-29: schema-v6 catalogs opaque immutable runs, canonical-JSON
  parameters, and dimensioned numeric metrics; the shared-lock publication
  primitive expands one explicit input vintage, records SHA-256 content and
  date-bound manifests, validates and atomically publishes per-run Parquet,
  and removes artifacts on handled failure. Fingerprint verification detects
  added, missing, or changed files by re-expanding persisted input patterns,
  without depending on mtimes. DuckDB loads only explicit, compatible
  `succeeded` paths and rejects missing, mismatched, failed, cross-version, or
  same-version schema-drift selections while ignoring orphan files.
- [x] Implement the library-level D-015 vectorized event runner and a CLI entry
  point. It selects candidates from stored bars (not universe membership),
  holds the shared lock through publication, applies declared calendar/quality
  gates, and never claims portfolio/order semantics. Per D-026, the reusable
  eligibility audit requires only each event's declared contiguous lookback
  through its decision timestamp. Terminal ranges remain reported backfill
  exclusions, not global study gates or reasons to reject a locally complete
  event. Future outcome availability never changes selection, and selected
  events with missing checkpoints remain explicitly counted.
  Completed 2026-08-29: `run_event_study` exposes only explicit selection-bar
  views until a calendar-, identity-, and local-window eligibility audit has
  frozen the selected event frame; outcome views open afterward. The runner
  rejects full-history coverage findings as local study gates, records its
  declared gate/window contract plus shared audit counts, fingerprints the
  selection cohort's alias evidence, keeps selectors filter-only, and requires
  every selected event to retain explicit observations while its aggregate
  outcome is classified mutually exclusively as evaluable or missing-outcome.
  Registered focused studies share the `research-run` CLI boundary, which
  labels its output as event evidence without portfolio/order semantics.
- [ ] Implement the coarse gap-recovery study using adjusted EOD prior close/open
  inputs and Tiingo's direct clock-hour checkpoints from 10:00 onward. Include
  stored SPY benchmark-relative evaluation and label the absent 09:30–09:59
  interval explicitly rather than inferring it.
- [ ] Provide one reproducible example notebook that calls the same library runner
  and loads the same published artifacts; notebooks do not contain a second
  execution or publication path.
- [x] Report stale `running` rows and orphaned result artifacts without selecting
  or deleting them automatically. Completed 2026-08-29: `research-reconcile`
  defaults to a shared-lock dry run; `--apply` is the explicit recovery boundary
  that marks abandoned rows failed and removes partial/unowned directories.

Exit criteria:

- [ ] One command runs the study on a representative stored cohort and publishes
  a `succeeded` catalog row, manifest, observations, parameters, and summary
  metrics that are queryable together through DuckDB.
- [ ] Tests inject failures before and after each publication boundary and prove
  that only complete compatible runs load, successful runs are immutable,
  retries receive new ids, and input-fingerprint checks detect changed source
  files.
- [ ] Event counts and returns match a small hand-calculated fixture, candidate
  selection is demonstrably independent of universe membership and future-bar
  availability, remote history gaps do not exclude a locally complete event,
  missing outcomes remain auditable, and quality failures block publication as
  declared.
- [ ] The CLI output and notebook state the direct-hourly and IEX-volume limits;
  benchmark conventions match the event observations; `make check` passes.

## M4 — Full opening-window study and historical program  `in progress`

Goal: turn the completed historical archive into the intended durable ongoing
collection, extend the coarse result into the complete five-minute,
session-relative opening-window study promised by the vision, then exercise the
research engine with a second strategy.

Scope:

- [x] Complete D-011 phases 1–3 through D-027's metered historical program:
  phase-1 seed EOD/hourly, 20 years of phase-2 EOD for the frozen 23,078-ticker
  supported-US archive (including delisted listings), and phase-3 seed
  five-minute history back to 2016-12-12. All four components are terminal
  with explicit accepted exclusions after D-029 prevented one definitive GBF
  404 from polling forever.
- [x] Implement D-030's durable ongoing collector. Refresh Tiingo's active
  supported-US stock/ETF roster at the start of each post-market weekday cycle
  and update EOD for every resolvable stable listing. After the first complete
  all-active EOD cycle following month end, persist the next rolling liquidity
  snapshot: default top 5,000 by mean EOD `close * volume` across the latest 20
  completed XNYS sessions, at least 15 valid observations, with deterministic
  ties and full ranking provenance. Collect direct hourly and five-minute data
  independently for that fixed cohort during the same overnight window;
  membership changes affect future collection only.
  Completed 2026-09-01: schema v8 content-addresses active supported-list
  snapshots, persists monthly stable-instrument liquidity cohorts, and drives
  ordered EOD/hourly/five-minute identity and data states. Existing coverage
  starts at its trailing edge plus the seven-day correction overlap, so every
  gap from the historical or prior-current stop through the cycle session is
  fetched. A cohort member with no intraday coverage starts forward-only at
  its cohort as-of session and later overlap refreshes cannot move that floor
  backward. D-032 extends unchanged authenticated current EOD listing anchors
  without a per-ticker metadata request; new/changed anchors remain fail-closed.
- [x] Make each current dataset an independently resumable bounded sweep with
  honest target/completed/excluded status. EOD runs first; hourly and
  five-minute run in separate request-budget windows and every batch yields the
  D-022 mutation lock. Completed 2026-09-01: current-mode scheduler jobs retain
  immutable targets, per-range progress, retryable failures, terminal
  exclusions, and shared current-work request/byte accounting. `ongoing run`
  admits automatic work only from 23:30 UTC through 08:00 New York time on the
  next XNYS session, checkpoints each bounded step, and writes
  target/cursor/sweep/exclusion status. The systemd template uses 1,000-turn
  API batches separated by six minutes and a one-second idle delay for
  zero-request transitions. Missing current-session bars and cancellations are
  terminal exclusions for that frozen cycle rather than permanent phase gates;
  only the designated exit 3 is accepted by systemd as partial success.
- [ ] The live metadata database was already migrated to schema v8 by an
  enabled editable-install timer on 2026-09-02 and must not be downgraded.
  After the human commit gate, initialize the production ongoing program,
  replace the interim latest-universe-only EOD timer with the new overnight
  timer, and measure complete end-to-end cycles. Confirm the 1,000-turn/six-
  minute pacing leaves retry headroom under the 10,000/hour and 100,000/day
  limits and finishes or checkpoints before the next morning decision window.
  Adjust measured batch spacing if necessary; the broad collector must never
  poll during the regular session.
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

- [x] Every target in phases 1–3 is either covered to its defined range or listed
  in a durable unresolved/failed report; scheduler records show phase order,
  D-020 breadth-first sweep order, and every billing window remained within
  D-013's limits.
- [ ] Repeated overnight post-market cycles keep EOD current through the most
  recent completed XNYS session for every resolvable active supported listing,
  and keep both direct intraday datasets current for every resolvable member of
  the accepted rolling top-5,000 snapshot. The snapshot provenance,
  new/removed membership, per-dataset sweep progress, budget stops, and
  fail-closed exclusions are queryable and present in bounded operator status.
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
- The likely first realtime shape is D-031's small owner-tagged morning
  watchlist for triggering decisions. It may use Tiingo five-minute updates or
  a broker's read-only market-data API, but it remains separate from the
  overnight bulk collector and canonical Tiingo bars. Promotion requires its
  own source, credential, freshness, and persistence decisions; broker order
  or account mutation remains out of scope.
- Corporate-action handling beyond Tiingo's adjusted columns is triggered by
  a concrete study limitation. A stateful portfolio/order simulator is likewise
  triggered only by a confirmed study that needs execution semantics.
- Live or automated execution and non-US-stock/ETF assets remain non-goals,
  not future milestones.
