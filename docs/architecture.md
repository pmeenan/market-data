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
- Tiingo is the sole market-data source. Vendor concerns stop at the client,
  identity resolver, and normalization boundary (D-002).
- Stable internal `instrument_id` values own bars and research results.
  Symbols are date-ranged aliases and never durable join keys (D-014).
- Universes scope ingestion only. Research selects from the bars that are
  actually stored and applies any liquidity screen to those bars (D-010).
- Research starts with columnar event studies over DuckDB and polars. It does
  not model orders, fills, cash, or overlapping-position constraints until a
  confirmed study requires those semantics (D-015).
- There is no order execution, broker connection, multi-user service, or
  non-US-stock/ETF asset path (D-007, D-008).

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
  A weekend-only interval between trading-day evidence boundaries is a
  non-session continuity marker per D-014, not request or row authorization.
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
  and breadth-first sweep cursor required by D-013 and D-020. This is
  operational state, not a second statement of bar coverage. The initial hard
  budget uses a 32-day rolling window (longer than any calendar month) and a
  64 MB response reservation because Tiingo does not publish its byte basis or
  billing reset boundary. Complete responses settle to actual observed bytes;
  interrupted responses retain the larger reservation, while an orderly
  transport failure before any response exists settles to the known zero-byte
  body. A crashed/unsettled attempt always retains its reservation.

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
path, content digest, size, and relevant date bounds. The aggregate fingerprint
hashes only the canonical (`relative path`, `content digest`) pairs, so a
byte-identical restore does not change it merely because filesystem mtimes did.
The lock is held through the scan, fingerprint, and result publication. The
first implementation need not retain a copy of input bars.

The reproducibility guarantee is D-016's: exact re-execution against an old
input vintage is promised only while the recorded input fingerprint matches.
The fingerprint covers the explicit files read, so changes elsewhere in the
archive do not affect that answer. Persisted observations and metrics remain
inspectable even when the fingerprint no longer matches.

The warehouse remains correction-aware rather than versioned. If a future
study must remain rerunnable after its input fingerprint changes, it requires
immutable input snapshots or a new dataset-versioning decision.

Result loading never globs the results directory. A query helper first reads
explicit artifact paths from `succeeded` catalog rows, verifies that the
selected runs have one study and schema version, and only then gives that path
list to DuckDB with union-by-name. This excludes orphan/failed files before
schema binding and prevents cross-version type drift from breaking unrelated
runs. `run_id` joins the observations to parameters and metrics in attached
read-only SQLite.

## Identity and ingestion flow

Identity validation precedes every production write:

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
batch, then resumes the unfinished sweep. Failed and identity-blocked units
retain their own frontier but count as attempted turns, so they do not stall
safe peers. When only terminal identity blockers remain, the job becomes
`blocked` and no longer holds a later phase; explicitly rerunning the same job
reactivates those ranges after evidence is repaired. Every authenticated attempt reserves request and byte budget
durably before transport, including each retry. It then records encoded
response-body bytes from the raw transport, including retry bodies, partial
reads where the HTTP stack exposes their count, and responses that later fail
validation. Current cycles complete every declared current dataset before
history can run, while the separate 30 GB rolling history ceiling preserves at
least 10 GB beneath the 40 GB rolling total ceiling. RE-006's vendor billing
basis remains undocumented, so the rolling window and response reservation are
deliberately stricter than an assumed calendar-month/observed-byte ledger.
Each durable historical turn holds D-022's mutation lock through planning,
transport, publication, and checkpoint, then yields it before the next turn.
Cancellation is a concurrent SQLite control signal; an in-flight turn may
finish, but checkpointing preserves the cancelled terminal state and no next
turn begins.

Responses are capped while streaming; an undeclared oversized body is charged,
checkpointed as a durable range blocker, and is not downloaded again on every
automatic resume.

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

A study is a focused function/script with typed parameters and a versioned
output schema, not a subclass hierarchy. CLI commands, scripts, and notebooks
all enter through the same library-level runner; no supported entry point
bypasses its lock or publication contract. Its execution path is:

1. Acquire the data-directory process lock, register a `running` run, and
   canonicalize parameters.
2. Resolve the explicit input-file list and select candidate instruments from
   stored bars, never from universe membership. Apply calendar and required
   quality gates.
3. Use DuckDB for cross-file scans, joins, window functions, and large
   filters; pass tidy frames to polars for study-specific transformations.
4. Produce an `instrument_id`-keyed event/observation frame and shared summary
   metrics. Benchmark-relative values use stored comparison series such as
   SPY with the same session and adjustment conventions.
5. Write observations and the input manifest to temporary files, validate
   their schemas/counts, and atomically rename them into the run directory.
6. In one SQLite transaction, insert metrics and mark the run `succeeded`
   with its paths, count, and fingerprint.

On error, the run is marked `failed` with a bounded diagnostic and temporary
files are removed. A crash can leave a `running` row or an unreferenced final
file, but neither is selected by successful catalog-based result loading; a
maintenance command can report and reconcile those artifacts.

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
emit machine-readable summaries. The nightly job writes a bounded status
record (start/end, counts, bytes, failures) and uses the server's chosen
notification mechanism; merely logging to an unattended file does not satisfy
the vision's visible-failure criterion.

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
source for milestone names, status, and exit criteria. Its M1–M4 ladder is the
approved implementation sequence; the owner reviewed the ladder, closed M0,
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
- treat a web UI or realtime layer as optional until the owner promotes it.

No architecture-blocking numbered question remains: OQ-1 is answered by
D-015, OQ-8 by D-014, OQ-2/OQ-3 by D-012, OQ-5 by D-010, and OQ-6 by D-016.
