# Architecture

> **Status: first full draft (2026-08-26).** This describes the target system.
> The ticker-keyed v1 substrate is quarantined migration input, not an alternate
> supported architecture. Milestone scope and exit criteria live only in
> [plan.md](plan.md).

## Goals and boundaries

The system is a single-user, local market-data warehouse and event-study
toolkit. It favors inspectable files, reproducible scripts, and failure-closed
data handling over service infrastructure or a general backtesting framework.

The load-bearing boundaries are:

- Parquet is the canonical bar and large-result store; SQLite holds small,
  relational metadata; DuckDB is the analytical query engine (D-003, D-016,
  D-019).
- Tiingo is the sole source for canonical warehouse bars. A separately
  approved future read-only broker feed may supply source-labelled morning
  trigger data for a small tagged watchlist, but it cannot silently enter the
  canonical Tiingo archive (D-002, D-031).
- Stable internal `instrument_id` values own bars and research results.
  Symbols are date-ranged aliases and never durable join keys (D-014).
- Universes scope ingestion only. Research selects from the bars that are
  actually stored and applies any liquidity screen to those bars (D-010).
- Research starts with columnar event studies over DuckDB and polars. It does
  not model orders, fills, cash, or overlapping-position constraints until a
  confirmed study requires those semantics (D-015).
- There is no order execution, broker trading/account mutation, multi-user
  service, or non-US-stock/ETF asset path. Read-only broker market data is a
  proposed future input, not trading connectivity (D-007, D-008, D-031).

## System shape

The two main flows share storage but have separate responsibilities:

```text
Tiingo API
    -> client -> identity resolution + response validation -> normalization
    -> staged merge/snapshot -> canonical Parquet bars + SQLite coverage

Canonical bars + SQLite identity
    -> DuckDB views -> calendar/data-quality filters -> strategy event frame
    -> shared evaluation -> SQLite run catalog + Parquet observations
```

The CLI owns operational workflows such as import, backfill, update,
reconciliation, quality reporting, and eventually study execution. The Python
library exposes the same stores and query/research functions for scripts and
notebooks. A future UI, if promoted, is a thin read-only consumer of those
library interfaces rather than a second implementation of warehouse logic.

## Component responsibilities

| Component | Responsibility | Must not own |
| --- | --- | --- |
| Configuration | Resolve the relocatable data directory and secrets from environment | Business logic or persisted state |
| Tiingo client | Authentication, request construction, retries, CSV parsing, and transfer accounting | Instrument identity guesses or writes |
| Identity registry/resolver | Internal instruments, aliases, per-dataset-key vendor identifiers, evidence, and request segments | Fetching bars or silently choosing a listing |
| Ingestion coordinator | Coverage planning, budget priority, validation orchestration, refresh policy, and resumability | Strategy selection |
| Bar store | Normalize schemas and atomically merge or replace Parquet files | Vendor calls or symbol resolution |
| Metadata store | Versioned SQLite schema and transactional small-state updates | Large observations or bar payloads |
| Query surface | Read-only DuckDB views and filtered polars frames | Mutation, universe-membership semantics, or strategy rules |
| Calendar and quality layer | Valid US sessions, bar/session labels, and trustworthy-input checks | Repairing raw vendor data silently |
| Research layer | Event selection, observation construction, evaluation, and result publication | Stateful portfolio claims without defined execution semantics |

These are module boundaries, not a demand for one class or package per row.
The identity migration may split the current modules where it makes the
contracts clearer; it should not introduce a generic repository/service
hierarchy.

## Persistent data model

Everything below `MARKET_DATA_DIR` is relocatable. Persisted paths stored in
SQLite are relative to that root. Temporary files use a sibling name and are
renamed into place only after validation.

### Directory layout

```text
data/
  .market-data.lock
  meta.db
  bars/
    eod/bucket={00..ff}/bars.parquet
    intraday/
      1hour/year={YYYY}/bucket={00..ff}/bars.parquet
      5min/year={YYYY}/bucket={00..ff}/bars.parquet
  quarantine/
    v1-ticker-bars/eod/{ticker}.parquet
    v1-ticker-bars/intraday/{freq}/{ticker}/{year}.parquet
    eod-quality/{operation_id}.parquet
    eod-response/{operation_id}.parquet
  backups/
    eod-episodes-{operation_id}/meta.db
    eod-episodes-{operation_id}/bars/eod/bucket={00..ff}/bars.parquet
  results/
    {study_name}/{run_id}/observations.parquet
    {study_name}/{run_id}/input_files.parquet
```

Per D-022, the persistent `.market-data.lock` is the one advisory canonical
mutation lock for the warehouse. Library-level ingestion, reconciliation,
migration, legacy bar ranking, and research-publication coordinators acquire
it; nested coordinators are reentrant. Current collection holds it across its
declared datasets, while historical work releases it after each durable turn.
A competing thread or process fails fast with bounded PID, operation, and
acquisition-time diagnostics so an unattended overlap cannot hang
indefinitely. Holder metadata is cleared before unlock, but the file is never
unlinked during normal operation because all processes must continue
contending on the same inode. SQLite-only configuration and control writes use
SQLite transactions; notably, cancellation remains available while a history
turn is in flight and takes effect at the next turn boundary.

`instrument_id` is opaque; `run_id` is opaque and filesystem-safe; and
`study_name` is a registered filesystem-safe slug. D-019 assigns an instrument
to one of 256 stable buckets using the first byte of SHA-256 over its UTF-8 id.
A rename or symbol reuse therefore changes neither a bucket nor a join key.
Empty buckets need no placeholder file.

Before any instrument-keyed file is published, the migration moves the whole
v1 `eod/` and `intraday/` roots out of the active namespace and into
`quarantine/v1-ticker-bars/`. Rows are read from quarantine and merged into
their target buckets only after each source file's complete stored range
resolves to one instrument. Query globs read `bars/` exclusively. Ambiguous
files remain quarantined and reported, never unioned with or overwritten by
the new generation.

The M0 layout benchmark chose hash-bucket compaction: it materially reduced
small-file discovery and improved the representative cross-sectional scans
and batched ingestion. See
[parquet-layout-benchmark.md](parquet-layout-benchmark.md). Code outside the
bar store must query globs/views and must not construct or hash these paths
itself.

### Canonical bar schemas

EOD rows are unique by (`instrument_id`, `date`) and retain Tiingo's raw and
adjusted OHLCV fields plus `div_cash` and `split_factor`. Intraday rows are
unique by (`instrument_id`, `ts`, `freq`) and retain unadjusted OHLCV. Each
intraday frequency has its own directory/view, so `freq` may be supplied by
the view rather than repeated physically in every file.

All intraday timestamps are timezone-aware UTC values. Calendar-derived
session date, minutes from open, early-close state, and similar research
columns are derived at query time rather than written back into vendor bars.
A convenient as-of ticker may be exposed by a query view, but canonical joins,
coverage, and outputs use `instrument_id`.

The bar store enforces schema and key uniqueness before publication. Overlap
fetches use merge-upsert. A corporate-action full refresh is a validated
snapshot replacement, because an upsert cannot remove dates omitted by a new
vendor snapshot (D-009).

EOD history jobs that reach `complete` run D-023's idempotent listing-episode
audit; blocked/cancelled jobs never trigger it. A manual audit may partition a
demonstrably discontinuous broad history only when durable source coverage
spans the candidate boundary. It quarantines invalid OHLC, long internal
zero-volume bridges, and too-short inferred fragments. The operation stages
and validates a complete EOD root under the canonical lock, takes a recoverable
metadata/Parquet backup, atomically swaps the directory, and then retires the
broad identity. The next EOD command automatically rolls back a pre-swap
metadata registration or finishes post-swap retirement after an interruption.
New EOD responses quarantine invalid raw rows individually so valid rows can
still publish with honest coverage. DuckDB canonical views disable Hive
partition inference so physical `bucket`/`year` directory keys never leak into
the published bar schema.

### SQLite metadata

SQLite schema migrations are ordered by `PRAGMA user_version`. The target
logical tables are grouped below; exact helper names may evolve, but their
keys and ownership are architectural contracts.

Per D-017, one vocabulary applies to identity validation and coverage: the
`dataset_key` is exactly one of `eod`, `intraday_1hour`, or
`intraday_5min`. The Tiingo endpoint family (`eod` or `iex`) is a transport
attribute, not a dataset key.
The two intraday frequencies share the IEX endpoint but do not share identity
authorization: evidence for `intraday_1hour` does not authorize
`intraday_5min`, or vice versa. Every individual response is validated again
before a write.

Identity and ingestion metadata:

- `instruments`: one row per internal `instrument_id`; lifecycle status and a
  human-readable description are attributes, not identity.
- `instrument_aliases`: ticker, exchange, asset type, and closed effective
  date range keyed to an instrument. Conflicting overlapping aliases remain
  unresolved and visible. The registry helper represents an active/open-ended
  alias consistently with the closed `9999-12-31` sentinel; callers pass no
  end date rather than choosing a snapshot date or today's date.
- `vendor_identifiers`: identifier value and type, instrument, `dataset_key`,
  validated envelope, validation state, and evidence. Adjacent or overlapping
  validated evidence rows for the same identifier, instrument, and exact
  dataset key may jointly cover a request segment; an evidence gap never does.
  Per D-032, a current EOD envelope whose `endDate` advances may reuse its
  authenticated immutable listing anchor only when every other archive field
  is unchanged and the new tail has no competing alias owner; the evidence
  retains both the authenticated anchor and continuation provenance. New or
  changed anchors still require authenticated metadata. This rule never
  applies to IEX.
  A weekend-only interval between trading-day evidence boundaries is a
  non-session continuity marker per D-014, not request or row authorization.
- `identity_episodes`: provenance for archive-bounded or observed-gap EOD
  listing episodes, including the superseded source id when one broad history
  was partitioned, confidence, observed bounds, and the non-key display label
  `TICKER@YYYYMMDD`. Actual aliases retain the vendor ticker. Per D-023,
  overlapping archive records remain fail-closed and inferred episodes require
  a 252-session gap plus at least 20 observations per published fragment.
- `universe`: original (`year`, `ticker`) seed record, rank, and dollar-volume
  value. Resolution to exactly one instrument is recorded separately so the
  imported source value is not destroyed.
- `coverage`: one closed interval per (`instrument_id`, `dataset_key`). It is
  derived state that `reconcile` can atomically rebuild conservatively from
  Parquet; verified-empty tails may be forgotten and safely refetched.
- `storage_state`: the explicit `v1`/`v2` generation marker. Establishing the
  D-017 boundary sets `v2` and clears derived ticker-keyed v1 coverage; legacy
  ingestion/query commands then fail closed until their instrument-keyed paths
  replace them.
- scheduler/request accounting: every authenticated attempt, its current or
  historical work class, conservative pre-request byte reservation, observed
  encoded response bytes, and complete/incomplete state; plus the immutable
  phase/dataset cohort, per-alias-range frontier, per-instrument attempt depth,
  and breadth-first sweep cursor required by D-013 and D-020. Schema v8 also
  gives these jobs an immutable `current` mode and correction-overlap policy;
  coverage remains the only statement of published bars. The hard budget
  follows Tiingo's documented midnight-EST billing month and uses a 64 MB
  response reservation because Tiingo does not publish its byte basis.
  Complete responses settle to actual observed bytes;
  interrupted responses retain the larger reservation, while an orderly
  transport failure before any response exists settles to the known zero-byte
  body. A crashed/unsettled attempt always retains its reservation.
- backfill-program state: one immutable ordered component declaration, frozen
  seed and Tiingo-supported US stock/ETF scopes (including the exact archive
  rows behind the latter), designated history job ids, and a per-component
  identity cursor. Program state distinguishes a missing predecessor from a
  terminal predecessor with accepted exclusions; phase-2/3 jobs outside the
  declaration fail closed.
- ongoing-program state: one immutable collector definition, content-addressed
  active supported-list snapshots, per-session cycle states and identity
  cursors, monthly stable-instrument liquidity cohorts with complete ranking
  provenance, and the three designated current-mode job ids. An interrupted
  cycle always resumes its frozen scopes; a later session cannot silently
  replace them.

Research metadata follows D-016:

- `research_runs`: `run_id`, study name and schema version, status, UTC
  start/completion times, source revision when available, input fingerprint,
  relative observation/manifest paths, observation count, and a bounded error
  summary. Status is one of `running`, `succeeded`, or `failed`.
- `research_parameters`: one row per (`run_id`, parameter name), with the
  value stored as canonical JSON so numbers, strings, booleans, lists, and
  null retain their types.
- `research_metrics`: one row per (`run_id`, metric name, dimensions), with a
  numeric value and optional unit. Dimensions are canonical JSON (for example
  a horizon and gap bucket), allowing tidy SQL comparisons without changing
  the schema for every study.

Successful runs are immutable. A retry receives a new `run_id`; it does not
overwrite prior evidence. Deleting/retaining old runs is an explicit future
maintenance operation, not an automatic side effect of running a study.

### Research result files

`observations.parquet` is the potentially large, study-specific event output.
Every file contains at least `run_id`, `instrument_id`, an event date or
timestamp, an observation/horizon label, and the study's measured value.
Strategy-specific fields such as gap size, entry price, checkpoint price, and
return remain tidy columns. A run has one internally consistent schema;
`study schema version` identifies changes across runs.

The library-level study runner acquires the data-directory process lock, then
expands its input globs once into an explicit sorted path list. That exact list
is passed to DuckDB and written to `input_files.parquet` with each relative
path, content digest, size, relevant date bounds, and the canonical declared
glob set. Its canonical `input_metadata_json` also snapshots alias envelopes
for the stable instruments present in an event run's explicit selection files.
Every declared glob must match; recursive patterns retain their recursive
semantics. The aggregate fingerprint hashes the patterns, metadata snapshot,
and canonical (`relative path`, `content digest`) pairs, so a byte-identical
restore does not change it merely because filesystem mtimes did. Verification
re-expands the recorded patterns and rereads the recorded identity cohort to
detect added, missing, or changed files and changed alias evidence. The lock is
held through the scan, fingerprint, and result publication. The first
implementation need not retain a copy of input bars.

The reproducibility guarantee is D-016's: exact re-execution against an old
input vintage is promised only while the recorded input fingerprint matches.
The fingerprint covers the declared selection and explicit files read, so
changes outside those patterns do not affect that answer. Persisted
observations and metrics remain inspectable even when the fingerprint no longer
matches.

The warehouse remains correction-aware rather than versioned. If a future
study must remain rerunnable after its input fingerprint changes, it requires
immutable input snapshots or a new dataset-versioning decision.

Result loading never globs the results directory. A query helper first reads
explicit artifact paths from `succeeded` catalog rows, verifies that the
selected runs have one study and schema version, and only then gives that path
list to DuckDB. Every selected artifact must have the exact same ordered schema;
column drift without a schema-version change fails before the common bar query
surface is built. This excludes orphan/failed files and prevents silent null
padding. `run_id` joins the observations to parameters and metrics in attached
read-only SQLite.

## Identity and ingestion flow

Identity validation precedes every production write:

For IEX, the stable alias registry supplies candidate envelopes but never
cross-dataset validation. D-024's operator bootstrap independently probes each
exact frequency inside conflict-free candidate segments, meters the request,
and records validated, empty/rejected, or conflicting evidence before
historical scheduling. A probe does not weaken the universal response checks
below; missing aliases, known overlaps, and empty probes remain fail-closed.
Range changes are partitioned at prior probe-evidence boundaries so covered
spans are reused and only uncovered dates need another bounded probe.

1. Resolve the requested ticker and date span into one or more non-overlapping
   request segments, each owned by exactly one instrument and bounded by
   validated alias evidence.
2. Choose an identifier validated for that exact `dataset_key` and segment.
   Split or clamp a request at alias boundaries; never extend a bare ticker
   through an evidence gap or reuse another frequency's validation. D-021's
   IEX next-session `endDate` is discard-only transport context, not an
   extension of the ingestible segment.
3. Fetch into memory/staging and validate every returned timestamp against
   the HTTP request and endpoint metadata where it exists. Reject the entire
   response on conflict. Discard D-021 context rows, then validate every
   retained timestamp against both the request segment and instrument
   envelope before normalization.
4. Normalize to the canonical schema. If a new EOD corporate action requires
   a full refresh, validate that complete snapshot before publishing any frame
   from the triggering operation.
5. Split intraday data at year/storage-file boundaries (D-017), then stage and
   group validated frames by dataset/year/bucket (D-019). A failed response is
   absent from the group and does not block validated peers. Each instrument's
   units are ordered inward from an existing coverage edge (or from the
   scheduler's chosen edge when no coverage exists), so a crash cannot create
   a published interior hole.
6. Atomically rewrite one bucket file with its ready units, then transactionally
   advance coverage for exactly those adjacent units before publishing the
   next group. An isolated retry may rewrite its bucket alone. Empty units may
   advance coverage only after identity validation and the publication-lag
   rule. A failed write never advances coverage.

There is no transaction spanning Parquet and SQLite. The safe ordering makes
Parquet canonical: a crash after one file publication but before its metadata
update causes redundant refetching. `reconcile` only rebuilds an interval when
each instrument's available year partitions form one contiguous sequence; the
presence of other instruments in a bucket-year is irrelevant. It reports and
omits coverage rather than bridging a missing instrument partition.
Verified-empty edges are not recoverable from row data and may be
conservatively refetched. The inverse publication ordering could claim data
exists when it does not and is forbidden.

Historical scheduling is a planning layer over this same idempotent unit. It
refreshes current data first, enforces the vendor-period byte budget, and
advances every phase/dataset history breadth-first: one maximum-safe request
unit from each eligible instrument's newest uncovered frontier per durable
sweep before any instrument receives another older unit (D-011, D-013,
D-020). A quota stop publishes and checkpoints any completed prefix of a bucket
batch, then resumes the unfinished sweep. Retryable failures and terminal
blocked units retain their own frontier but count as attempted turns, so they
do not stall safe peers. A definitive Tiingo HTTP 404 is a terminal vendor
blocker under D-029; transient transport and payload failures remain
retryable. When only terminal identity or vendor blockers remain, the job
becomes `blocked` and no longer holds a later phase. Routine invocations leave
those ranges dormant; `--retry-blocked` reactivates them only after evidence
is repaired or reviewed. Every authenticated attempt reserves request and byte
budget durably before transport, including each retry. It then records encoded
response-body bytes from the raw transport, including retry bodies, partial
reads where the HTTP stack exposes their count, and responses that later fail
validation. Current cycles complete every declared current dataset before
history can run. Historical admission uses total billing-month usage: its 30 GB
ceiling rises in equal daily steps to 39 GB over the final seven Tiingo billing
dates, releasing only reserve current work has not consumed, while current
work retains the 40 GB monthly total ceiling. RE-006's vendor billing basis
remains undocumented, so the response reservation remains deliberately
stricter than an observed-byte ledger.
Each durable historical turn holds D-022's mutation lock through planning,
transport, publication, and checkpoint, then yields it before the next turn.
Cancellation is a concurrent SQLite control signal; an in-flight turn may
finish, but checkpointing preserves the cancelled terminal state and no next
turn begins.

Responses are capped while streaming; an undeclared oversized body is charged,
checkpointed as a durable range blocker, and is not downloaded again on every
automatic resume.

The production D-027 driver owns phase advancement above individual history
jobs. Each invocation performs one bounded action: freeze a missing scope,
prepare one identity batch, or run a bounded prefix of the current component's
breadth-first sweep. Phase 2 uses one persisted Tiingo supported-tickers
snapshot rather than the seed-universe CLI default; phase 3 cannot reuse hourly
identity evidence. A component is admitted only after its complete frozen
scope has exact-dataset identity classifications, and every declared lower
component is terminal. `blocked` is terminal with exclusions, while a missing,
active, or cancelled designated predecessor is not completion.

Ongoing collection is a separate durable current program under D-030–D-035.
The schema-v8 implementation admits automatic work no earlier than 23:30 UTC
after the completed XNYS regular session, preserving the deployed publication
buffer, and stops before 08:00 New York time on the next XNYS session. Each
cycle refreshes Tiingo's supported metadata, content-addresses the active US
stock/ETF listing snapshot, and completes one breadth-first EOD main pass
first. Once only bounded retries remain, the healthy pipeline advances. After
the first such EOD pass following month end, the program calculates and
persists a fixed intraday cohort from safely completed EOD targets: by default
the top 5,000 active stable instruments by
mean canonical EOD `close * volume` over the latest 20 completed XNYS sessions,
requiring at least 15 valid observations. The snapshot records its as-of
session, ranking window and method, metric, rank, and stable membership; it is
an ingestion scope and never a backtest-membership join.

Direct hourly and five-minute sweeps independently consume that snapshot and
retain their exact-frequency identity gates. EOD, hourly, and five-minute each
use a frozen current-mode job with a durable cursor and bounded operator
status. If coverage exists, the immutable target begins at its trailing edge
minus the seven-day correction overlap and therefore fetches every safely
resolvable missing interval through the cycle session. Completion requires
coverage through that session; a partial turn is progress only if it moves the
stored coverage edge, not merely because it reached one selected identity
slice's end. A transient failure or no-progress current response gets at most 40
unsuccessful breadth-first turns (roughly four hours at the production cadence);
successful coverage progress does not consume that allowance. Every attempt
yields to healthy peers, and exhaustion becomes an explicit cycle
exclusion. Retry-only work does not gate the next dataset: after hourly and
five-minute have also received healthy main passes, all still-active jobs take
deferred turns ordered by durable sweep count. The cycle remains partial until
those retries recover or become explicit exclusions. An intraday cohort entrant without prior
coverage begins at the cohort's as-of session, never at the historical IEX
floor, and subsequent correction overlaps are clamped to that forward-only
floor. An already-covered older recovery session retires without a request but
keeps its stable owner in the EOD ranking input. The next session gets a new
job and retries cycle exclusions normally rather than holding the whole program
through the historical
publication-lag horizon. Cancellation has the same cycle-terminal semantics.
Jobs publish/checkpoint a completed batch and yield the shared mutation lock
before the next batch;
hourly and five-minute are scheduled in separate request-budget windows because
5,000 instruments at both frequencies already represent about 10,000 logical
requests before retries. Cohort departures retain their canonical history,
and partial sweeps resume against the same snapshot. No cycle claims freshness
until every declared target is covered through the most recent completed
session or carries an explicit fail-closed exclusion. The broad collector
performs all three dataset sweeps in the overnight window and completes or
checkpoints before the next morning decision window; it never becomes an
in-session polling service.

D-036 scopes the pending M6 trigger surface separately. It consumes a bounded
nightly candidate list, optionally supplemented by owner tags, during declared
strategy monitoring hours. Tiingo is the initial source; a broker market-data
API still requires a separate source approval. Broker
rows remain source-labelled in a hot/decision store unless a later decision
defines a canonical multi-source contract. This path has no order endpoint,
order credential, account mutation, or implicit write into Tiingo-owned
Parquet bars.

## Query, calendar, and quality contracts

`marketdata.query.connect()` is the common read surface. After the identity
migration it creates, when data exists:

- `eod` and `intraday_1hour` / `intraday_5min` views keyed by
  `instrument_id`;
- convenient alias-resolved views that derive the ticker as of each bar date;
- read-only attached `meta` tables; and
- result-loading helpers/views built only from explicit compatible paths in
  successful catalog rows.

Library loaders accept instrument ids as their unambiguous selector. Ticker
convenience functions require a date/as-of range and go through the resolver;
they never translate a symbol to "whatever is current."

The calendar layer supplies valid US exchange sessions, regular-session open
and close in UTC, DST and half-day handling, and explicit bar-label semantics.
Raw Tiingo timestamps remain untouched. Intraday research filters/labels them
against the calendar before calculating session-relative windows. Direct
hourly bars preserve Tiingo's 10:00-through-15:00 clock-hour bins; opening
windows come from 5-minute bars (D-012).

Quality checks produce findings rather than silently editing bars. At minimum
they cover missing expected sessions, duplicate keys, OHLC invariants,
negative values, suspicious zero-volume runs, split sanity, off-session
intraday rows, and coverage/delisting summaries. A study declares which checks
are blocking and records their outcome with the run. IEX-only intraday volume
must not be presented as composite market liquidity.

## Research execution and publication

D-036's [research protocol](research-protocol.md) governs the M3–M6 price,
causality, execution, validation, and scanner contracts. The existing runner
implements publication and eligibility, not the pending as-of feature API,
trade simulator, or scanner. Selection views contain future rows in declared
datasets; callbacks must enforce causality and pass perturbation tests until
that feature boundary lands. EOD adjusted and intraday raw prices must never
be mixed in a return. Bar-close availability is at the interval end.

`marketdata.features` is the shared as-of feature boundary (D-036/D-037):
it registers prior-window EOD features, XNYS session opens, and IEX bar
density views (direct hourly and five-minute) on any DuckDB connection
exposing the canonical views, so the
event runner's explicit-file inputs and a future scanner's nightly snapshot
evaluate identical SQL. Every window ends at the prior completed session; the
decision session contributes only its explicitly timestamped open.

A study is a focused function/script with typed parameters and a versioned
output schema, not a subclass hierarchy. CLI commands, scripts, and notebooks
all enter through the same library-level runner; no supported entry point
bypasses its lock or publication contract. Its execution path is:

1. Acquire the data-directory process lock, register a `running` run, and
   canonicalize parameters.
2. Resolve the explicit input-file list and select candidate events from stored
   bars, never from universe membership. Per D-026, eligibility uses only the
   declared contiguous selection/lookback window through the decision
   timestamp; remote history and later outcome-bar availability are not
   eligibility inputs. Apply calendar and required quality gates.
3. Use DuckDB for cross-file scans, joins, window functions, and large
   filters; pass tidy frames to polars for study-specific transformations.
4. Produce an `instrument_id`-keyed event/observation frame and shared summary
   metrics. Benchmark-relative values use stored comparison series such as
   SPY with the same session and adjustment conventions. Selected events with
   missing later checkpoints remain in the observations with an explicit
   outcome status. Audit metrics separate selected events into mutually
   exclusive evaluable/missing-outcome outcomes and retain lookback, identity,
   and calendar exclusions; declared quality gates block the whole run.
5. Write observations and the input manifest to temporary files, validate
   their schemas/counts, and atomically rename them into the run directory.
6. In one SQLite transaction, insert metrics and mark the run `succeeded`
   with its paths, count, and fingerprint.

The runner gives candidate builders and selectors DuckDB views for declared
selection datasets only; selectors may filter that audited frame but cannot
alter its schema or values, and outcome-dataset views open afterward. Each
candidate declares typed local window bounds. Completed EOD lookbacks end
before the event date; an event-day open is an explicit decision feature, not a
completed EOD bar. Intraday labels must finish by the decision timestamp. The
shared audit requires every expected XNYS EOD or exact-frequency intraday label
in those bounds, plus a valid decision session and identity envelope; a window
with zero expected labels is a declaration error rather than a data gap.
Full-history missing-session and terminal coverage summaries cannot be
declared blocking gates for this local eligibility decision. Row checks over
an explicitly empty candidate/dataset scope pass vacuously, while other unrun
checks still fail closed. Result validation requires every selected event to
retain at least one labeled observation with an explicit `evaluable` or
`missing_outcome` status.

On error, the run is marked `failed` with a bounded diagnostic and temporary
files are removed. A crash can leave a `running` row or an unreferenced final
file, but neither is selected by successful catalog-based result loading; a
dry-run `research-reconcile` command reports them under the shared lock.
`research-reconcile --apply` explicitly marks abandoned rows failed and removes
their partial/unowned directories; no automatic cleanup can delete result
artifacts.

Ingestion and research must not mutate/read the same files concurrently in
the initial single-user implementation. The mutation coordinators and the
library-level study runner enforce the same data-directory process lock; the
CLI merely calls those library boundaries. This avoids a cross-file mixed
vintage while preserving atomic per-file writes. Ad hoc read-only interactive
queries may run concurrently, with the understanding that a long query sees
whatever file versions DuckDB opened and cannot publish a cataloged run.

## Operations and failure visibility

The intended deployment is the owner's Linux server with a virtual
environment, local filesystem, `.env` token, and cron/systemd timer. No secret
is stored in SQLite, Parquet, logs, summaries, or result parameters.

Operational commands return nonzero if any requested segment fails and can
emit machine-readable summaries. The schema-v8 ongoing driver writes a bounded
replace-in-place status record after every step, including program/cycle state,
each dataset's job status/target/cursor/sweep/exclusion summary, request and
observed-byte counts, pending target retries, and the overnight deadline. Its
service paces at most
1,000 turns per six minutes, reserves exit 3 for a terminal cycle with explicit
per-symbol exclusions, and therefore retries ordinary exit-1 CLI/crash failures
as well as exit-2 coordinator/configuration/lock/status failures. A one-second
idle delay prevents zero-request state transitions from spinning. Quota stops
and the 08:00 New York deadline checkpoint cleanly. The production program was
initialized and the replacement timer enabled on 2026-09-02; the interim
latest-universe EOD timer is disabled. Its first overnight cycle exposed
D-034's bounded-retry defect and stopped at the morning checkpoint before
cohort selection. The corrected scheduler will resume that immutable cycle,
defer its two retrying targets, and continue healthy intraday work while they
receive a fresh bounded retry window. On the owner's personal server, the
systemd result plus inspectable status is the required visible failure signal;
an external notification channel may be added later but is not an M2 gate.
`market-data doctor` (D-037) condenses that state into bounded findings:
request rate against Tiingo's hourly/daily limits, current targets retrying
without a successful depth, frozen-target exclusions, cohort coverage freshness
by liquidity rank, and active supported listings that resolve to no
instrument. It exits 1 on an error-level finding and writes an optional JSON
report; it never mutates the warehouse.

Backups treat `data/` as one unit. Parquet is canonical for bars and result
observations, while `meta.db` is required for identity evidence, universe
history, scheduler budgets, and the result catalog. Coverage can be rebuilt
conservatively (possibly forgetting verified-empty edges); identity and run
metadata cannot.

## Correctness invariants

Implementations and tests preserve these properties:

- no bar, coverage row, or research join is durably owned by a ticker string;
- no response writes until its exact-dataset-key identity envelope validates;
- canonical bar keys are unique and rerunning an ingestion unit converges;
- bucket assignment is the stable D-019 SHA-256 function, never a process- or
  language-dependent hash;
- each coverage boundary is justified by a validated published row or an
  identity-validated historical empty response permitted by D-009; reconcile
  never bridges a missing storage partition (D-017);
- one instrument's EOD slice never mixes corporate-action adjustment
  vintages;
- intraday coverage excludes the incomplete current session/day;
- research never gains point-in-time membership semantics from `universe`;
- opening-window results never infer the missing 09:30-09:59 interval from
  direct hourly bars;
- a successful result row references fully published immutable artifacts; and
- portfolio-like performance is not claimed without explicit execution and
  capital-allocation semantics.

## Planning boundary and ordering constraints

This document does not assign milestone scope. [plan.md](plan.md) is the sole
source for milestone names, status, and exit criteria. Its M1–M6 ladder is the
approved implementation sequence (M5/M6 added by D-036); the owner reviewed
the original ladder, closed M0,
and closed M1 after its controlled canary passed on 2026-08-27. M2 is in
progress.

The architecture imposes only these ordering constraints on that planning:

- implement D-019's settled bucket layout as part of the identity migration,
  before instrument-keyed production files are published;
- move the v1 roots out of the active namespace before publishing any
  instrument-keyed files, and complete the identity migration before
  production ingestion resumes;
- make exchange-calendar semantics and the minimum blocking quality checks
  available before publishing research results;
- land the result catalog/publication path with the first persisted study; and
- keep a web UI and general streaming optional; validate strategies in M5
  before relying on M6 alerts. M6 shares as-of features with research and keeps
  timestamped observations separate from canonical bars, without long-lived
  warehouse locks or broker account access.

No architecture-blocking numbered question remains: OQ-1 is answered by
D-015, OQ-8 by D-014, OQ-2/OQ-3 by D-012, OQ-5 by D-010, and OQ-6 by D-016.
