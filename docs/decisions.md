# Decision log

Newest first. Every entry: what was decided, why, and what would reopen it.
Entries are for choices that are expensive to reverse or that a future agent
might silently undo — not routine implementation calls; a few per milestone is
the target. Existing entries are never edited into a different decision —
reversing or amending one gets a *new* entry that supersedes it (a status-line
annotation on the old entry is fine). When an entry hangs on a claim about
current technology state, check a current source or run a local experiment —
training knowledge is stale.

**Reading:** scan the D-NNN headings (or grep) and read only the entries your
task touches. Full read is for structural or cross-cutting work.

**Culling:** the log may be periodically pruned — superseded or moot entries
whose context no longer informs anything current are deleted outright; git
history is the archive. D-numbers are never reused.

Format:

```
## D-NNN: Title  (YYYY-MM-DD, status: accepted | proposed | superseded by D-MMM)
Decision / Context / Consequences / Reopen if
```

---

## D-026: Study eligibility is local-window and decision-time causal  (2026-08-29, status: accepted, amends D-010, D-015, D-020, and D-024)

**Decision:** Event studies decide eligibility for each instrument at each
decision timestamp from only the data available through that timestamp. A study
declares its dataset-specific selection/lookback window and requires the
expected observations in that local window to be contiguous and valid. It does
not require the instrument's entire stored history to be complete, resolve
identity gaps outside that window, or treat one ticker as a continuous company
across distinct listing episodes. Canonical bars remain instrument-owned and
all ingestion-time response/identity validation in D-014 and D-024 remains in
force.

The outcome window is deliberately separate from candidate eligibility. Once
selected, an event remains in the study population even if a later checkpoint
is unavailable. Its outcome is recorded as unavailable with a reason rather
than silently dropping the event; event counts distinguish selected,
evaluable, missing-outcome, lookback-incomplete, identity-excluded, and
quality-excluded cases. No future alias end, coverage state, or returned bar
may retroactively decide whether the candidate existed at the decision time.

Terminal history ranges and static identity exclusions remain durable and
visible, but they are accepted exclusions rather than unfinished work that
holds a later backfill phase or a study whose local inputs are complete.
Routine history invocations do not reactivate terminal ranges. An operator
must use an explicit retry after changing or reviewing the underlying evidence.

**Context:** The confirmed strategies hold positions for hours to days and
select each event from a short contiguous lookback. For that research unit, a
distant gap or earlier use of the same ticker has no bearing on a locally
complete ticker/date window. Exhaustively resolving those edges would spend
quota without improving the intended result. Conversely, requiring a future
exit bar before retaining a candidate would introduce lookahead/survivorship
bias; high-price/liquidity filters reduce ordinary delisting exposure but do
not eliminate halts, acquisitions, or vendor omissions.

**Consequences:** M3's runner owns a reusable local-window eligibility audit and
persists exclusion/outcome-status counts with each run. Study features never
group across listing episodes by ticker alone. Phase completion means every
safe target is covered and every remaining target is durably classified; it
does not mean every identity edge was resolved. D-020's blocked terminal state
continues to satisfy predecessor gates, while retries require explicit operator
intent.

**Reopen if:** A confirmed strategy uses issuer continuity, fundamentals,
longitudinal per-security features spanning identity boundaries, or holding
periods long enough that corporate-action/delisting semantics need first-class
simulation.

---

## D-025: Late-month history may consume proven-unused reserve  (2026-08-29, status: accepted, amends D-013 and D-020)

**Decision:** The admission ceiling for every historical request remains 30 GB
of total metered usage until the final seven UTC calendar days of a month. On
those seven dates it advances in equal daily steps through 30, 31.5, 33, 34.5,
36, 37.5, and 39 GB. The check uses total charged usage, not historical usage
alone, so current work already performed consumes the same allowance and only
genuinely unused reserve becomes available to history. Current work is exempt
from this sliding admission ceiling and retains the separate 40 GB total hard
stop. The 64 MB pre-request reservation is included in both checks.

RE-006's conservative 32-day rolling usage window remains the accounting
basis; UTC calendar dates change the admission ceiling but do not assert or
simulate a vendor reset. Consequently, the ceiling returns to 30 GB on the
first of a month while prior attempts remain charged until they age out. This
can pause history longer than Tiingo's actual billing cycle, but cannot spend
an undocumented reset twice.

**Context:** A fixed 10 GB current-work reserve was mostly unused during the
first live phase-1 backfill, leaving substantial bandwidth idle near month end.
The owner prefers to release that reserve gradually once the remaining current
work has had most of the month to reveal its actual cost, while retaining 1 GB
at month end and the 40 GB absolute ceiling for current collection.

**Consequences:** Budget checks and bucket-batch preflights calculate the same
date-dependent ceiling. `market-data status` reports the currently active
ceiling and headroom after the next conservative response reservation. The
schedule is intentionally daily rather than a
last-minute jump, so breadth-first history can make useful progress throughout
the final week. D-013's fixed 30 GB history-only ceiling and permanent 10 GB
reserve no longer govern.

**Reopen if:** Tiingo exposes an authoritative billing ledger/reset timestamp,
current traffic needs more than the retained late-month headroom, or measured
historical throughput needs a smoother intra-day ramp.

---

## D-024: IEX ticker evidence comes from exact-frequency bounded probes  (2026-08-28, status: accepted, amended by D-026; amends D-014)

**Decision:** A stable alias envelope is only a candidate for IEX identity; EOD
validation is never copied into an intraday dataset. For each conflict-free
alias segment intersecting the requested IEX range, the operator bootstrap
makes a bounded request over the segment's latest 20 XNYS sessions plus D-021's
one-session finalization context. A non-empty target-envelope response whose
timestamps all fit the safe request envelope validates the bare ticker only for
that exact frequency and candidate segment. Every later ingestion response is
still independently request-, alias-, and identifier-envelope validated before
publication.

An empty target response is rejected evidence, and an invalid, out-of-request,
or cap-sized response is conflicting evidence. Those terminal outcomes are
persisted so an automatic rerun does not repeatedly spend quota or starve later
segments; an operator may explicitly retry them after reviewing the report.
When a requested range or alias envelope changes, the bootstrap partitions the
candidate at stored evidence boundaries, reuses covered validated or terminal
spans, and probes only newly uncovered dates. Stored envelopes therefore remain
useful without being silently stretched beyond what the response established.
Known alias overlaps remain unresolved without transport. A successful hourly
probe supplies no five-minute evidence, or vice versa. All authenticated probes
use the durable current-work request/byte ledger and the shared mutation lock.

**Context:** Tiingo exposes no complete IEX security master or response
metadata, and RE-005 proved that an EOD-valid permanent identifier can resolve
incorrectly on IEX. The bounded probe is the strongest available independent
endpoint evidence while D-014's date envelope prevents a bare symbol from
crossing a known reuse boundary. On the 2026-08-28 phase-1 seed range,
2016-12-12 through 2026-08-27, 4,715 conflict-free candidate segments were
probed: 4,316 validated with target rows and 399 returned no target rows. An
additional 111 overlap spans remained multiple matches, 666 tickers had no
stable alias envelope, and 274 tickers were wholly before the measured IEX
history range. The initial pass plus the 376 empty-shape confirmations
transferred 31,202,799 encoded bytes.

**Consequences:** `identity bootstrap-intraday` is the operator gate before a
new exact-frequency historical cohort starts. It records the probe range,
lookahead, row counts, observed dates, and outcome in `vendor_identifiers`.
Safe segments can enter the breadth-first scheduler while every missing,
overlapping, empty, or contradictory segment stays visible and fail-closed.
The approach deliberately does not claim that one sample proves vendor history
semantics forever; universal validation of every fetched response remains the
publication boundary.

**Reopen if:** Tiingo supplies a complete versioned IEX security master or
per-row stable identifier, measured range-dependent identity invalidates the
probe-envelope assumption, or a material class of valid listings is empty over
20 sessions and needs a separately evidenced probe policy.

---

## D-023: EOD history is partitioned into evidence-bounded listing episodes  (2026-08-28, status: accepted, amends D-014 and D-022)

**Decision:** One internal instrument represents one continuous listing episode,
not every security that has ever answered to the same bare ticker. Each in-scope
Tiingo supported-tickers archive row creates a distinct archive-bounded episode.
Non-overlapping portions may be requested by bare ticker and retain D-014's full
request/response envelope validation; overlapping archive intervals remain
multiple matches and fail closed.

After an EOD job reaches `complete` (not `blocked` or cancelled), canonical
history is also audited for vendor conflation that the archive does not expose.
A manually invoked audit may run sooner, but a boundary is usable only when
durable source coverage spans it. A boundary is inferred only at a gap of at
least 252 expected XNYS sessions, or at an internal run of at least 252
zero-volume session rows with nonzero observations on both sides. Each
substantive side must contain at least 20 observations. Substantive episodes
receive deterministic internal ids and disjoint date-ranged aliases; an
episode's vendor alias remains the real ticker. A separate display label uses
`TICKER@YYYYMMDD`, rather than mutating the ticker to `.X`/`.X2`, because an
ordinal suffix could change if older evidence is discovered. Large zero-volume
bridges, invalid OHLC rows, and inferred fragments under 20 observations are
moved to a provenance-bearing quarantine instead of being published or used to
invent an identity. Missing prices are never fabricated.

The audit is an idempotent part of completed EOD backfill and is also available
as an explicit dry-run/apply maintenance command. It rewrites a staged complete
EOD root under D-022's lock, validates the staged schema/keys/buckets, preserves
a consistent SQLite plus Parquet backup, and swaps the root before retiring the
superseded broad identity. The next repair or EOD backfill automatically rolls
back an unswapped metadata registration or completes cleanup for an
already-swapped tree. New EOD responses quarantine invalid raw or adjusted OHLC
rows with response provenance while valid rows continue to publication.

**Context:** The first completed 20-year seed EOD load passed structural checks
but exposed multi-year discontinuities inside identities that Tiingo's archive
had represented as one listing. A 2026-08-28 audit found 60 such source
histories, yielding 121 substantive episodes, plus 1,908 zero-bridge, invalid,
or too-sparse rows requiring quarantine. Several histories visibly joined old
delisted companies to unrelated recent listings. Retaining one broad alias
would silently merge securities in research; relying only on duplicated
archive rows would leave these cases undetected. The subsequent validated
history pass exposed two more covered broad histories; the live warehouse now
contains 125 inferred episodes across 62 reused symbols.

**Consequences:** `identity_episodes` records archive or observed-gap
provenance, confidence, display label, and observed bounds. The original broad
source instrument remains as provenance but loses aliases, EOD identifiers,
coverage, and canonical bars after replacement. Research and joins continue to
use internal ids; display labels are conveniences only. A short fragment stays
quarantined unless later explicit evidence justifies recovering it. This rule
is EOD-only: hourly and five-minute evidence must still be established
independently under D-014.

**Reopen if:** Tiingo supplies a complete immutable security master and stable
EOD identifier history, a corporate-action source can distinguish a long
trading halt from symbol reuse, or measured false splits show that the gap and
minimum-observation thresholds need versioning.

---

## D-022: Canonical mutations share one persistent data-directory lock  (2026-08-28, status: accepted)

**Decision:** Every library coordinator that can publish or replace canonical
Parquet, rebuild its coverage, cross the storage-generation boundary, or read
bars for cataloged research uses the same advisory process lock at
`data/.market-data.lock`. The lock file is persistent and is never unlinked
during normal operation, so every process continues to lock the same inode.
Nested coordinators are reentrant. Contention fails closed with a nonzero
operational result and bounded holder metadata.

Current collection holds the lock through its declared datasets. Historical
backfill holds it only for one durable request-depth turn (including a D-019
same-bucket batch), then releases it before planning the next turn so current
work and other operators are not excluded for an entire sweep. Job resolution
and initialization are locked. Cancellation is the deliberate exception: it
is a SQLite-only control signal that may run while a historical turn owns the
lock; the runner completes any already-started durable turn, preserves the
cancelled terminal state while checkpointing, and stops before the next turn.
Other SQLite-only configuration/control writes rely on SQLite transactions;
the legacy universe rank is locked because it reads bar files.

**Context:** Atomic per-file replacement does not prevent a reader from seeing
a mixed multi-file vintage or two coordinators from planning against stale
coverage. D-016 already required the eventual study runner to share an
ingestion lock from input selection through result publication. A persistent
advisory lock is the smallest single-server mechanism that covers both flows.
Holding it for a complete network-bound historical sweep, however, would block
scheduled current updates for hours and would make the existing durable cancel
operation unable to stop a runaway sweep.

**Consequences:** Lock ownership is enforced at library coordinator boundaries,
not only by the CLI. The holder record contains bounded PID, operation, and UTC
acquisition time, is written through an unbuffered descriptor, and is cleared
before unlock; the empty persistent file remains. Read-only ad hoc queries may
still run concurrently but cannot publish cataloged results. A history command
may stop on contention between turns and is safely resumable from its durable
cursor. M3 must use this same lock for its manifest scan and publication.

**Reopen if:** The warehouse moves to a concurrent-writer database or a
manifest/pointer protocol that supplies equivalent multi-file snapshots; the
target filesystem does not provide reliable local advisory locking; or measured
turn duration requires a queued/current-priority lock protocol.

---

## D-021: IEX finalization lookahead is discard-only transport context  (2026-08-27, status: accepted, amends D-014 and D-020)

**Decision:** An identity-validated historical IEX target unit may extend its
HTTP `endDate` only through the first XNYS session strictly after the target
end. That extension is transport context required to make Tiingo return the
target's final bars; it is not part of the ingestible identity segment. Every
returned row must remain inside the HTTP request and agree with identifier
metadata where present. The full response is charged and checked against the
planner envelope and 10,000-row cap before discard. Rows after the target end
are then discarded before normalization, publication, or coverage accounting;
every retained row must still fall inside D-014's resolved alias and
exact-dataset identity envelope. No other request-bound expansion is allowed.

**Context:** RE-004 established that Tiingo can omit a session's final hourly
or five-minute bars unless `endDate` reaches the next exchange session.
Requiring the resolved alias and vendor-identifier interval itself to cover
that later session makes the final historical unit impossible for every
delisted instrument, because its evidence correctly ends on its last listing
day. It also turns a response-shaping workaround into false evidence that the
instrument existed later. Treating lookahead rows as ingestible would violate
D-014; treating the narrowly bounded request extension as context preserves
the identity boundary and the survivorship-safe delisted backfill goal.

**Consequences:** D-014's requirement that every publishable row be inside the
instrument envelope remains unchanged, but its former wording that every
returned row be inside the ingestible segment is narrowed for this one
discard-only IEX context. A bare ticker may be used only when the target range
itself resolves uniquely; any context row is structurally/metadata validated
and discarded even if a later alias exists. Tests cover a delisted final unit,
lookahead discard, metadata conflict, request-cap accounting, and refusal to
extend beyond one next session. D-020's safe request envelope includes this
context in its row bound without advancing the instrument frontier through it.

**Reopen if:** Tiingo makes range results stable without lookahead, supplies
complete pagination/finalization semantics, or measurement shows that a later
`endDate` can change the identity of rows dated inside an otherwise validated
target segment.

---

## D-020: Historical backfills advance breadth-first by request depth  (2026-08-27, status: accepted, amended by D-025 and D-026; amends D-011 and D-013)

**Decision:** Every historical backfill in phases 1–3, whether started
manually or by the persisted scheduler, advances breadth-first within its
current phase and exact dataset. A sweep gives each eligible instrument at
most one request-sized unit from its newest uncovered frontier before any
instrument receives a second, older unit. Each unit spans as far backward as
the endpoint- and frequency-specific safe request envelope allows. It never
relies on a response reaching Tiingo's silent row cap as proof that the range
was complete.

The cohort order and sweep cursor are deterministic and durable. If an hourly,
daily, or monthly limit stops work partway through a sweep, the next run
resumes with the instruments that have not yet received that turn; it does not
restart at the first instrument or deepen an early ticker. A successful or
verified-empty unit advances only that instrument's frontier. A failed or
identity-blocked unit records an outcome for its turn but does not
advance its frontier, so validated peers can progress after the full sweep
without guessing identity or falsely covering the failed range. Current
collection retains D-013's priority over all historical sweeps, and D-011's
phase order is unchanged.

If all remaining ranges are terminally identity-blocked, the job records a
`blocked` terminal state. That durable unresolved report satisfies the phase
gate without claiming coverage. Routine reruns leave its ranges dormant;
`backfill ... --retry-blocked` explicitly reactivates runtime-blocked ranges
after identity evidence is repaired or reviewed.
Operators can cancel an obsolete active/blocked job without deleting its audit
trail, so a superseded request cannot orphan a permanent predecessor gate.

**Context:** Tiingo silently limits historical IEX responses to 10,000 rows,
while request quotas reset hourly and daily. A ticker-at-a-time traversal can
consume a quota window on a few deep histories and leave most of the cohort
with no recent history. It can also starve later tickers after every restart.
Breadth-first sweeps maximize useful cross-sectional depth at each completed
request layer and make quota interruptions fair and resumable. The owner
generalized D-013's five-minute date-band preference to every historical
backfill on 2026-08-27.

**Consequences:** M2's range planner chooses the largest conservatively safe
unit independently for EOD, hourly, and five-minute data, including IEX's
required lookahead/discard behavior. Scheduler state includes the phase,
dataset, cohort snapshot, per-instrument frontier, sweep order/cursor, and
last-attempt status. Tests must prove a quota stop resumes the remainder
of the same sweep, no instrument gets request depth N+1 before every eligible
peer gets a depth-N turn, and failures or identity blocks neither advance that
target nor stall safe peers. D-013 continues to own the special five-minute
budget and current-data priority, as amended by D-025's late-month ramp.

**Reopen if:** Tiingo provides explicit complete pagination with different
economics, the owner changes phase priority, or measured scheduler overhead
makes a different fairness unit materially better while preserving comparable
cross-sectional depth.

## D-019: Canonical bars use stable hash-bucket compaction  (2026-08-27, status: accepted, amends D-003, D-009, and D-017)

**Decision:** Instrument-keyed canonical bars use 256 stable hash buckets,
selected by the first byte of SHA-256 over the UTF-8 `instrument_id`. EOD has
one Parquet file per non-empty bucket. Each intraday dataset has one file per
year and bucket. The exact active paths are
`bars/eod/bucket={00..ff}/bars.parquet` and
`bars/intraday/{1hour|5min}/year={YYYY}/bucket={00..ff}/bars.parquet`.
Bucket assignment is a storage concern: canonical keys and coverage remain
instrument-based, and code must use the shared bucket function rather than an
implementation-dependent language hash.

Validated responses are staged independently and grouped by
(dataset, year when intraday, bucket) for publication. One atomic bucket-file
rewrite may merge any validated subset; a failed response is excluded and
does not block validated peers. Normal current updates and date-band
backfills batch all ready frames for a bucket so that file is rewritten once.
An isolated retry may rewrite one bucket immediately. Every response still
advances its own coverage only after the file rename succeeds. Reconciliation
checks year continuity per instrument, not from the mere presence of a shared
bucket-year file.

An EOD corporate-action snapshot replacement removes and replaces only that
instrument's complete slice while atomically rewriting its bucket file. The
snapshot must pass D-009 and D-014 validation before it joins the preserved
slices for other instruments; this retains the one-vintage-per-instrument
invariant without treating a shared physical file as one adjustment vintage.

**Context:** The reproducible M0 benchmark generated 39,468,000 canonical
five-minute-shaped rows (1,000 instruments, two full-depth instrument-years)
per candidate. Relative to 2,000 per-instrument-year files, 64 year/bucket
files per year reduced file count 93.6% and bytes 14.6%. Median warm DuckDB
queries improved 3.86x for a one-session cross section, 2.11x for a grouped
20-session event shape, and 1.15x for a full scan. A single-instrument
merge/write was 6.14x slower because it rewrote the shared bucket, but a
64-instrument/four-complete-bucket publish was 2.31x faster. Sixty-four buckets
at 1,000 instruments modeled 15.6 instruments per bucket; 256 buckets at the
5,403-symbol seed scale yields a comparable 21.1. Full method, results, and
limitations are in
[parquet-layout-benchmark.md](parquet-layout-benchmark.md).

**Consequences:** M1 builds the new active namespace directly in the compact
layout; it does not first copy v1 into per-instrument files. Bar-store point
reads compute the bucket, while cross-sectional consumers continue through
DuckDB views/globs. The coordinator needs a staging/grouping boundary, and a
rare isolated update has bounded write amplification of roughly one bucket's
instrument population. File publication remains atomic and crash-safe under
D-017 because every unit affects one canonical file; a crash after rename but
before coverage commit only causes conservative refetching. Backups and file
discovery handle hundreds to low thousands of files per dataset instead of
tens of thousands of small files.

**Reopen if:** Real instrument counts make bucket files materially larger than
the modeled density, isolated-update latency becomes operationally important,
full-scale cold-cache scans cease to be interactive, or an atomic
manifest/append-compaction design demonstrates a better read/write balance.
Re-benchmark before changing bucket count, hash, partitions, row groups, or
batching.

## D-018: Lock and continuously check the Python toolchain  (2026-08-26, status: accepted, amends D-001)

**Decision:** The uv version declared in `pyproject.toml` manages a committed
universal lockfile and exact development environments; the MIT-licensed
Setuptools build backend is exactly pinned in the build contract. Ruff is the
sole formatter/linter, using its stable formatter plus `E4`, `E7`, `E9`, `F`,
`I`, `B`, and `UP` lint families. A single `make check` entry point uses one
locked environment invocation to verify lint, formatting, offline tests, and
dependency licenses. GitHub Actions runs it on Python 3.11 and 3.12 for pull
requests and pushes to `main`. CI action revisions are pinned; the uv action
and bootstrap script both read the sole uv-version constraint from
`pyproject.toml`.

D-001 continues to require permissive direct dependencies. The audit also
compares every registry package/version in the universal lock plus every exact
build requirement against a committed, normalized SPDX inventory. This covers
platform-marked packages even when they are not installed locally. It permits
one required named exception: `certifi`, an existing `requests` dependency, is
MPL-2.0 and is consumed unmodified. Removing or changing that package fails the
check until D-018 and the exception are deliberately updated; another MPL
dependency also fails closed.

**Context:** The project had lower-bounded dependencies and an ignored virtual
environment, so two installs could test different code. Tests were the only
automated check. A local metadata audit confirmed that all direct runtime and
development dependencies are BSD, MIT, or Apache licensed. It also exposed the
pre-existing `certifi` mismatch with D-001: MPL-2.0 is file-level copyleft, not
permissive, despite arriving transitively through the Apache-licensed
`requests`. Replacing the established HTTP client solely to remove an
unmodified CA-certificate package would add application risk without changing
the project's licensing, while silently treating all transitive dependencies
as out of policy would make the audit misleading.

`uv.lock` is cross-platform and locks exact versions while retaining
`pyproject.toml` as the range-based package contract. Auditing the lock rather
than only the installed environment is necessary because marker-selected
packages such as Windows-only `colorama` are otherwise invisible on the Linux
server and CI runner. Stored SPDX expressions keep policy separate from the
free-text spellings exposed by older package metadata. Ruff replaces separate
formatter, import sorter, and baseline lint tools. GitHub Actions is available
on the existing origin and gives the human commit gate a visible, repeatable
signal without adding a local hook that can be skipped or forgotten.

**Consequences:** Dependency changes update `pyproject.toml`, `uv.lock`, and
`dependency-licenses.toml`; any missing or stale review fails closed. The
license inventory records the normalized SPDX conclusion and where it was
verified in the package's own metadata. Developers run `uv sync --locked
--extra dev`, `make check`, and optionally `make format`. `make check` finds a
repo-local uv before falling back to `PATH`; `tools/install-uv` bootstraps the
version declared in `pyproject.toml`. Updating uv or a pinned CI action is an
explicit maintenance change. The lock supports the declared Python range, but
CI exercises the minimum and current project versions (3.11 and 3.12), not
every future interpreter admitted by `>=3.11`.

**Reopen if:** The repository leaves GitHub; uv's lock ceases to be portable
enough for the server; Ruff cannot express a needed check; Python support
changes; or `requests`/`certifi` is replaced so the MPL exception can be
removed.

## D-017: Identity migration isolates storage generations and preserves contiguous coverage  (2026-08-26, status: accepted, amended by D-019; amends D-003, D-009, and D-014)

**Decision:** Instrument-keyed canonical bars live under a new active
`data/bars/` root, preserving the current per-instrument EOD and
per-instrument-year intraday granularity unless the M0 layout benchmark
supports changing it. Before the first instrument-keyed file is published,
the migration moves the complete v1 `data/eod/` and `data/intraday/` roots to
`data/quarantine/v1-ticker-bars/`. Active query globs read only `data/bars/`;
files copy out of quarantine only after their complete ranges resolve to one
instrument.

The shared identity/coverage `dataset_key` vocabulary is exactly `eod`,
`intraday_1hour`, and `intraday_5min`. Tiingo endpoint family (`eod` or `iex`)
is transport metadata, not an authorization key. Vendor-identifier evidence
is keyed by the exact dataset key, so validating one IEX frequency does not
authorize another; every response is still envelope-validated before writing.

Because D-009 coverage remains one closed interval, each ingestion publication
unit may affect at most one canonical Parquet file. Intraday responses crossing
a year boundary are split and published one file at a time, ordered inward
from a coverage edge, with coverage advanced after each unit. Reconciliation
never takes a blind minimum/maximum across disconnected year partitions: it
reports the gap and omits that coverage entry. Rebuilding from Parquet may
forget verified-empty edges and refetch them, but it must never bridge a
missing partition.

**Context:** Review of the M0 architecture draft found three ways its earlier
wording could violate the settled fail-closed model. Reusing `data/eod/` and
`data/intraday/` for both generations could mix ticker- and instrument-keyed
schemas or overwrite a colliding filename. Calling both endpoint families and
stored frequencies "dataset" could apply validation evidence at the wrong
scope. Finally, atomically renaming individual files does not make a multi-year
publish atomic; a crash plus min/max reconciliation could turn missing middle
years into falsely covered history.

**Consequences:** The migration has a generation boundary that is visible on
disk and cheap to roll back before production resumes. Identity lookups and
coverage use one unambiguous key. Intraday planning must split at year
boundaries and cannot publish a non-adjacent partition. `reconcile` becomes
conservative and reports disconnected partitions instead of manufacturing a
single interval. The M0 layout benchmark still decides granularity; this
decision only isolates the namespaces safely.

**Reopen if:** The layout benchmark adopts a different versioned root or an
atomic manifest/pointer scheme replaces file-at-a-time publication and the
single-interval coverage model.

## D-016: Research runs publish cataloged immutable artifacts  (2026-08-26, status: accepted)

**Decision:** Persist each research execution under a new opaque `run_id`.
Small run metadata, canonical-JSON parameters, and tidy numeric metrics live
in versioned SQLite tables. Potentially large event/observation rows live in
`data/results/{study_name}/{run_id}/observations.parquet`, keyed by `run_id`
and `instrument_id`; an input-file manifest is stored beside them and its
aggregate fingerprint is cataloged with the run. Paths in SQLite are relative
to the relocatable data root.

The library-level study runner holds the data-directory process lock from
input selection through result publication. It requires every declared input
glob to match, expands them once, and passes that explicit file list to both
DuckDB and the manifest builder. The manifest retains the canonical glob set;
the aggregate fingerprint hashes those patterns plus canonical (relative path,
content digest) pairs, not filesystem mtimes, so a byte-identical restore
preserves the fingerprint while a newly matching file invalidates it.

Result publication is append-only and failure-safe. Parquet artifacts are
written and validated under temporary names, renamed into place, then the
metrics and `succeeded` state are committed together in SQLite. Failed or
interrupted runs are never selected by successful catalog-based result
loading. A retry creates a new run rather than mutating a completed one.
DuckDB loads observations through the same query surface as bars, using
explicit compatible artifact paths read from successful SQLite catalog rows
rather than a results-directory glob. Runs that claim one study schema version
must have exactly equal artifact schemas; union-by-name null padding is not a
compatibility mechanism.

The manifest identifies the input vintage but does not turn the
correction-aware bar archive into versioned storage. Persisted observations
remain auditable after Tiingo restates bars; exact re-execution against an old
vintage is promised only while its recorded input fingerprint still matches.

**Context:** Feature triage answered OQ-6 with SQLite for
metadata/parameters/metrics and Parquet for large outputs, but left the
publication and reproducibility contract for the M0 architecture draft.
SQLite is appropriate for filtering and comparing small run records; tidy
Parquet preserves the long-form, `instrument_id`-keyed shape chosen by D-015
without bloating `meta.db`. Atomic publication matters because a notebook or
later UI must not mistake a half-written output for a completed study.

Retaining complete historical bar snapshots for every run would multiply the
warehouse and conflict with its current rolling-correction model. The input
manifest plus immutable derived observations provides honest auditability
without claiming a versioned data lake.

**Consequences:** The first persisted-study work adds a versioned result
catalog, study schema versions, input fingerprints, atomic artifact
publication, catalog-filtered DuckDB loading, and stale-running/orphan
reporting. Every result observation uses stable instrument identity.
Parameters must not contain secrets. Removing old runs requires an explicit
maintenance operation; automatic overwrite or reuse of a successful `run_id`
is forbidden. Dry-run reconciliation reports abandoned `running` rows and
unowned directories; an explicit apply is required to fail/clean them.

**Reopen if:** A study must be exactly rerunnable after source corrections, in
which case immutable input snapshots or dataset versioning are required; or
result volume/query patterns make the per-run layout materially inefficient.

## D-015: Research uses a project-native DuckDB/polars event engine  (2026-08-26, status: accepted, amended by D-026)

**Decision:** The research layer starts as a small, project-native vectorized
engine over the existing DuckDB/polars query stack. Strategies select events
and produce tidy, `instrument_id`-keyed observation frames; shared research
code owns reusable evaluation, parameter/run recording, and result
persistence. The default path is columnar SQL/dataframe work, not a Python
row-by-row simulation loop.

Do not add a general portfolio/order simulator until a confirmed study needs
stateful capital allocation, overlapping-position constraints, fill
semantics, or another execution behavior that an event frame cannot express.
At that point, implement the smallest required simulator or repeat the
library comparison against that concrete acceptance case.

**Context:** The OQ-1 spike implemented the gap-recovery acceptance case both
ways on deterministic representative data: 300 instruments, 1,000 sessions,
300,000 EOD rows, 900,000 intraday checkpoint rows, and 40,686 qualifying gap
events. Both prototypes produced the same 122,058 observations and identical
checkpoint summaries. The DuckDB/polars implementation kept the warehouse's
long-form shape and completed in 0.220 seconds (0.216 warm), versus 6.970
seconds (2.843 warm) for vectorbt 1.1.0; first-run peak RSS was 361 MiB versus
710 MiB. The vectorbt prototype was also longer (71 versus 51 lines) because
it had to pivot dense matrices, simulate a separate portfolio for each exit
horizon, and still calculate event-specific metrics outside the library.

vectorbt's portfolio accounting is useful but premature for the first study,
whose output is a conditional return distribution rather than an executable
portfolio. Adoption is independently blocked by D-001: vectorbt 1.1.0's own
license is Apache 2.0 with Commons Clause, not a permissive
Apache-2.0-compatible license. The full method, results, limitations, and
current-source links are in
[backtest-engine-spike.md](backtest-engine-spike.md).

**Consequences:** M3 builds focused research primitives and the gap study,
not a framework-shaped abstraction layer. DuckDB performs Parquet scans,
windowing, and large filters; polars handles strategy transformations and
tidy outputs. Results remain compatible with OQ-6's settled SQLite-metadata +
Parquet-observations shape. Existing dependencies suffice. Portfolio metrics
must not be presented as strategy evidence until portfolio/execution
semantics are explicitly defined.

**Reopen if:** A confirmed strategy requires stateful execution semantics,
the long-form columnar path fails representative full-scale benchmarks, or a
permissively licensed library demonstrably removes more project complexity
than it adds for a concrete study.

## D-014: Stable instruments own bars; symbols are date-ranged aliases  (2026-08-26, status: accepted, amended by D-017, D-021, and D-023; amends D-003, D-004, D-009, and D-011)

**Decision:** The warehouse's durable identity is an opaque internal
`instrument_id`, not a ticker string and not a vendor identifier. SQLite will
hold instruments and their date-ranged symbol aliases (ticker, exchange,
asset type, start date, end date). Coverage, Parquet paths, and bar rows are
keyed by `instrument_id`; ticker is resolved as-of the bar date for display
and research. A Tiingo `permaTicker`, when available, is stored as an optional
vendor identifier alongside the instrument rather than used as the primary
key.

Vendor request identifiers are validated separately for each dataset. A
permaTicker that correctly resolves EOD is not assumed to resolve IEX. **Every
response, including an apparently unambiguous bare-ticker response, is
validated before any bar or coverage write:** the request segment must belong
to exactly one resolved instrument; every returned row must fall inside both
that segment and the instrument's validated alias envelope; and endpoint
metadata, where available, must agree with the stored identity. Any violation
rejects the complete response. An empty response can advance coverage only
after the request identifier/segment passes identity validation and D-009's
publication-lag rule. D-021 defines the sole exception to the returned-row
boundary: IEX may request through the next session as discard-only transport
context, but no row outside the validated target envelope can be normalized,
written, or used for coverage.

A bare ticker may identify one request segment only when the segment is wholly
contained in one validated alias interval, no other known record for that
ticker overlaps the segment, and bare-ticker metadata matches that alias. A
nominal 20-year backfill is split and clamped to validated alias boundaries;
it never extends before an alias start or after its end to fill the target.
Missing, overlapping, or incomplete archive evidence makes the segment
unresolved rather than relaxing the rule. History outside the known envelope
requires a validated permaTicker or manual/vendor evidence attaching another
alias to the same instrument. A full-history refresh likewise requires a
dataset identifier validated for the instrument's complete stored envelope;
a bare alias cannot replace a multi-alias instrument snapshot. Ingestion never
selects the newest listing, merges histories, or guesses across an evidence
gap.

Tiingo archive boundaries are trading-day dates. A Saturday/Sunday-only gap
between otherwise adjacent validated units may connect the derived coverage
interval because no US stock/ETF bar can exist there; it does not authorize a
request or returned row for those dates. Weekday holidays remain ordinary
evidence gaps until M2's exchange calendar can identify them explicitly.

Universe rows retain the imported ticker string as historical source data but
must resolve to an instrument through aliases overlapping that universe year.
Zero or multiple matches are explicit resolution failures. No Tiingo-backed
production ingestion—nightly update, historical backfill, or corporate-action
refresh—may write the v1 ticker-keyed warehouse after this decision. After the
M1 identity migration, validated request segments may ingest while each
unresolved segment remains individually blocked. “All tickers” means all
resolved listing instruments, not one row per distinct current symbol.

**Context:** The OQ-8 spike measured Tiingo's 2026-08-26 public
supported-tickers archive using the same US exchange/stock/ETF/USD filter as
the application. Its 24,074 rows contain 23,042 distinct symbols: 993 symbols
have multiple records (2,025 records). The seed CSV contains 5,403 distinct
symbols; 282 are reused (577 records), and 229 of those have multiple records
intersecting Tiingo's measured IEX history since 2016-12-12. The archive has
date ranges but no permaTicker; 462 duplicated symbols have overlapping record
ranges, so ranges alone are not a universally unique identity mapping.

Authenticated probes showed that Tiingo does possess stable identifiers but
does not expose a complete, uniformly safe resolver on this account. EOD
queries by known permaTicker separated the old 2009–2013 ACOM/Ancestry history
from the 2026 ACOM ETF, while bare `ACOM` returned only the ETF. Bare `ALTR`
returned 2017–2025 Altair, not the older Altera listing. The search endpoint
is documented as early beta and omitted exact identities in probes—including
returning no exact ALTR result; fundamentals metadata is a smaller add-on
universe rather than a complete price master. Most importantly, IEX queried
with the old ACOM permaTicker returned 80 rows dated 2026-07-17 through
2026-08-05, outside that instrument's 2013 end date, while the current ETF
permaTicker returned none.
Full measurements and source links are in
[instrument-identity-spike.md](instrument-identity-spike.md).

**Consequences:** M1 starts with a schema/data-path migration and an identity
resolution report before any production ingestion resumes. Internal IDs must
be persisted in both SQLite and canonical Parquet so `reconcile` does not
depend on mutable symbol mappings. The resolver caches source evidence and
validation state per dataset, rejects conflicting aliases, and leaves
unresolved records visible
for vendor support/manual mapping. Existing bare-ticker files are quarantined
during migration until their complete date range resolves to exactly one
instrument; a file crossing multiple identity envelopes or an evidence gap is
reported, not auto-assigned or combined with new data. Query views may expose
convenient ticker columns, but joins and result persistence use
`instrument_id` to prevent cross-security merges.

**Reopen if:** Tiingo publishes a complete security master with stable IDs and
historical aliases for EOD and IEX, or measurements establish different
identity semantics. Such a source can replace the resolver input, but does not
remove the need for stable warehouse identity unless its IDs and guarantees
are contractually durable.

## D-013: Five-minute history fills newest-first with a 30 GB monthly hard cap  (2026-08-26, status: accepted, amended by D-020 and D-025; amends D-011)

**Decision:** The phase-3 seed-ticker 5-minute backfill proceeds in global
date bands from the most recent completed session backward toward 2016-12-12.
Within each band, cover the seed universe before advancing to an older band;
do not complete one ticker's history while other seed tickers lack current
coverage.

Current 5-minute collection has priority over historical work. Each daily run
refreshes the current all-ticker target from D-011 first. Historical 5-minute
transfer has a hard ceiling of **30 GB in each vendor billing month**. The
remaining 10 GB of the 40 GB plan allowance is unavailable to history and is
reserved for daily refreshes and other ongoing work; unused reserve does not
roll into the historical allowance near month end.

The scheduler may stop history below 30 GB if actual use plus projected daily
refreshes through the next bandwidth reset would otherwise threaten the
overall 40 GB cap. When either limit is reached, history stops cleanly and
resumes from its persisted oldest completed band after the vendor budget
resets. Historical work never delays current collection or borrows from its
10 GB reserve.

The initial M2 enforcement (2026-08-28) is intentionally stricter while
Tiingo's billing basis and reset boundary remain undocumented (RE-006): both
ceilings use a 32-day rolling window, every response reserves 64 MB before
transport, complete bodies settle to observed encoded bytes, and incomplete
bodies retain the reservation. An orderly failure before any response exists
settles the body to a known zero bytes; an interrupted process or partial body
keeps the reservation. This may resume later than the vendor's actual reset,
but it cannot spend the current-work reserve on history. The policy can
relax to an authoritative billing window only after Tiingo exposes or an
account measurement proves that mapping.

**Context:** The owner wants useful recent 5-minute coverage first and wants
the historical backfill capped at 30 GB/month, leaving 10 GB of headroom under
the 40 GB monthly limit so current data can still be refreshed daily. A
ticker-at-a-time traversal would leave an incomplete recent cross-section,
while a history-first budget policy could make the freshest data stale near
month end.

**Consequences:** D-011's phase order is unchanged: seed 5-minute history
still follows the EOD and hourly phases. D-011's ongoing all-ticker 5-minute
collection now starts no later than phase 3 rather than waiting for every
historical band to finish. The scheduler needs persisted band progress,
separate current-versus-history byte accounting, and hard stops at both 30 GB
of historical transfer and 40 GB total. The initial conservative rolling
window supplies the current-work reserve without guessing a vendor reset or
refresh forecast; a verified billing window may replace it later. At the
measured 68.5 GB seed-list projection, phase 3 requires at least three billing
windows. Recent cross-sectional history becomes usable incrementally while the
backfill works toward 2016-12-12.

**Reopen if:** The Tiingo cap/reset rules change, measured ongoing work cannot
fit within the 10 GB reserve, or the owner changes the 30 GB ceiling, phase
order, or preference for cross-sectional date coverage.

## D-012: Preserve vendor hourly bars; derive opening windows from 5-minute data  (2026-08-26, status: accepted)

**Decision:** Store Tiingo's direct `resampleFreq=1hour` output as the
`intraday_1hour` vendor dataset, but use it only for clock-hour checkpoints
from 10:00 onward. Any opening-window or session-relative hourly analysis is
derived from stored 5-minute bars after filtering against the exchange
calendar. Intraday IEX volume is not used for composite liquidity screens or
absolute cross-sectional volume thresholds; EOD composite volume is the
default liquidity input. A study may use IEX volume descriptively or as a
within-instrument measure only after validating that narrower use.

**Context:** The OQ-2/OQ-3 spike measured AAPL, CROX, and SPY through
2026-08-25. Both frequencies begin 2016-12-12. On sampled normal sessions,
direct hourly rows labelled 10:00–15:00 exactly matched the same-clock-hour
groups of twelve 5-minute rows, while the six 09:30–09:55 rows had no hourly
counterpart. Long-range endpoint responses also synthesized weekday grids on
holidays and after early closes, independent of `forceFill`, so row presence
cannot define a session. Tiingo documents historical IEX volume as trades on
IEX only. Full measurements are in [intraday-spike.md](intraday-spike.md).

**Consequences:** D-011's phase order stays intact: cheap direct hourly data
can support a coarse first pass using EOD open plus 10:00-and-later recovery
checkpoints, but the complete opening-window study waits for phase 3's
5-minute history. The exchange-calendar feature is a correctness dependency,
not optional polish. Research code must preserve frequency/semantics rather
than presenting direct hourly data as a 09:30 session-aligned bar. The
measured depth is adequate for the first study but does not cover the seed
record's 2011–2016 years.

**Reopen if:** Tiingo changes its direct resampling convention, the study is
reframed to exclude the opening half-hour, or a validated consolidated Tiingo
intraday feed replaces IEX for this dataset.

## D-011: Backfill scope, priority, and API budget  (2026-08-26, status: accepted, amended by D-013, D-014, and D-020)

**Decision:** The dataset is built in phases, bounded by the owner's Tiingo
Power-tier budget:

1. **Phase 1 — seed tickers, EOD + 1-hour** (the 5,403 distinct tickers in
   the 2011–2026 seed CSV): EOD daily back 20 years, plus 1-hour intraday
   through the available history (measured from 2016-12-12 in the OQ-2
   spike). The hourly component is about 5.4 GB for the seed list; the EOD
   component is additional and must be included in scheduler accounting.
2. **Phase 2 — all tickers, EOD:** EOD daily back 20 years for *all*
   Tiingo-supported US stocks/ETFs, including delisted ones.
3. **Phase 3 — seed tickers, 5-minute:** intraday back through the available
   history, measured from 2016-12-12.
   5-minute is confirmed intraday scope alongside hourly, but its history
   backfill runs **last** — EOD and 1-hour backfills complete before any
   5-minute history is fetched, to keep the heavy transfer (the bulk of the
   bandwidth budget) from crowding out everything else (owner's call,
   2026-08-26).
4. **Ongoing:** once the backfill is past, nightly collection extends to
   5-minute (and hourly/EOD) data for all tickers, not just the seed list.
   All-ticker 5-minute coverage is forward-only — no all-ticker 5-minute
   *history* backfill is planned.

**Every historical backfill phase (1–3) is gated on OQ-8 (instrument
identity).** The warehouse keys everything by bare ticker, but symbols get
reused across distinct securities (review, 2026-08-26: ~1,000 duplicated
symbols in the filtered supported-tickers list, **282 of them in the seed
CSV** covering 577 Tiingo records — e.g. ACOM, seeded for 2011 as the
2009–2013 NASDAQ stock, is also a 2026 NYSE Arca ETF). A bare-ticker fetch
could pull the wrong security or silently merge two, so the identity
decision (features.md OQ-8) must land before *any* historical backfill
starts — seed phases included. The OQ-8 spike may narrow this gate if
endpoint measurements prove a specific dataset or frequency safe.

*(2026-08-26 annotation: D-014 answers OQ-8. It temporarily gates every
Tiingo-backed production write—including D-011 ongoing collection—until the
M1 identity migration. After migration, the gate becomes per request segment:
validated instruments may proceed and unresolved segments may not.)*

Power-tier limits as published 2026-08-26 (tiingo.com/about/pricing): 10,000
requests/hour, 100,000/day, **40 GB bandwidth/month**, ~110k unique
symbols/month. Bandwidth is the binding constraint, so **bulk fetches use
`format=csv`, not JSON** — CSV responses carry no repeated field names and
run roughly half to a third the bytes per bar (owner's call, 2026-08-26; the
client switch landed in M1 on 2026-08-27). With CSV, the OQ-2 spike
measured a 68.5 GB seed-list projection for 5-minute history, within the
original ~40–75 GB estimate, i.e. **1–2 months only if phase 3 could use the
full vendor cap**. D-013 originally hard-capped history at 30 GB/month, making
the operational minimum three billing windows; D-025's later reserve release
can reduce that to two. The EOD phases and operational
overhead are additional; all-ticker EOD is still projected at well under a
month. Backfill runs through a budget-aware scheduler (confirmed feature,
features.md):
track requests and bytes against the hourly/daily/monthly caps, stop when a
window is spent, resume in the next — never let a backfill run blow the
month's bandwidth. Ongoing all-ticker nightly collection is comfortably
inside the caps (~1–2 GB/month with CSV).

**Context:** Backfill scope and priority stated by the owner at the M0 triage
(2026-08-26); tier limits checked against Tiingo's current pricing page the
same day. Resolves OQ-7.

**Consequences:** Phase ordering is owner intent — don't reorder it to
"optimize". The backfill completing is measured in months, not hours; M2
planning must treat it as a long-running metered process with resumable
state (which D-009's coverage intervals already provide). The Tiingo client's
M1 move to CSV parsing for bulk endpoints includes CSV transport fixtures and
in-memory request/wire-byte measurement; durable scheduler accounting lands in
M2. The OQ-2 measurement calibrates the initial
projection; the scheduler must continue tracking actual bytes/ticker because
listing lifetimes and payload sizes vary.

An authenticated 2026-08-27 follow-up offered identity, gzip, Brotli, and
Zstandard encodings separately and together to representative EOD and IEX CSV
requests. Tiingo returned every variant unencoded with identical
`Content-Length` and raw-body size. The client therefore permits normal HTTP
content negotiation and meters encoded bytes from the raw transport rather
than forcing identity. Tiingo's published limit does not define whether its
ledger charges encoded or decoded bytes; M2 must validate that mapping before
using the client counter as the authoritative budget ledger (RE-006).

**Reopen if:** Tiingo changes tier limits, the measured bytes/ticker differs
wildly from the estimate, or the owner's study needs reprioritize which data
arrives first.

## D-010: Universes are dataset seed filters, not backtest membership  (2026-08-26, status: accepted, amended by D-026; amends D-004; identity consequences amended by D-014)

**Decision:** The per-year dollar-volume universes exist to choose which
symbols seed dataset ingestion (preferring large, stable names for the initial
backfill) — they are **not** a point-in-time membership constraint on
backtests. Research code selects stored instruments by `instrument_id` from
price/volume data (identity amended by D-014); if a strategy needs a liquidity
screen, it computes one from the dataset, not from the universe table.
Survivorship-bias protection comes
from the data itself — phase 2 of D-011 backfills *all* tickers including
delisted ones — rather than from membership joins. Per-year storage remains
as the historical record of how the dataset was seeded.

**Context:** Owner's clarification at the M0 triage (2026-08-26): "the
dollar volume was only used for filtering an initial list of tickers to seed
the dataset with… it isn't being used in the actual strategy itself."

**Consequences:** OQ-5 (which year's membership governs which trading days)
is dissolved — there is no membership semantics to settle. Universe
provenance metadata (proposed in features.md) loses its purpose and is
rejected. D-009's `update --universe` default stays a pragmatic ingestion
scoping knob (its "provisional pending OQ-5" caveat is lifted) and becomes
moot once ongoing all-ticker collection (D-011) lands. New universe years
stop being needed once collection covers everything (resolves OQ-4). The
survivorship-bias guarantee additionally depends on distinct securities
sharing a symbol never merging. *(2026-08-26 annotation: D-014 answers OQ-8,
replaces D-011's broad historical gate with an M1 migration gate followed by
per-segment fail-closed validation, and makes research joins instrument-keyed.)*

**Reopen if:** A study reintroduces universe-membership-based selection —
that would resurrect point-in-time semantics and the lookahead question,
and needs its own decision.

## D-009: EOD coverage and refresh policy  (2026-08-26, status: accepted, amended by D-017 and D-019; identity key amended by D-014; provisional-default caveat lifted by D-010)

**Decision:** Ingestion state is a per-(ticker, dataset) coverage *interval*
[first_date, last_date], not a single high watermark; backfills fill missing
leading history and coverage is rebuildable from Parquet (`market-data
reconcile`). Nightly updates refetch a 7-day rolling overlap
(`REFRESH_WINDOW_DAYS`). An empty response marks a range covered only when it
ends ≥5 days in the past (`PUBLICATION_LAG_DAYS`; 1 day for intraday). A
split or dividend observed past the old coverage edge triggers a
full-history refetch for that ticker, so Tiingo's adjusted columns stay one
consistent vintage per file (adjusted columns remain the canonical basis;
raw-bars-canonical was considered and not adopted). The refetched snapshot is
validated — non-empty, reaches the prior coverage edge, retains every
previously stored date through that edge, contains every date from the
triggering fetch, and agrees with the triggering fetch's div/split values —
then *replaces* the file atomically (merge-upsert cannot remove stale dates
a snapshot omits). Any disagreement is a reported failure that keeps the
existing file for retry/manual inspection, never a silent success.
Intraday coverage never advances into the current day: a partial session is
written but stays refreshable until the day completes. `reconcile` rebuilds
the coverage table by full atomic replacement, so entries for vanished files
don't survive. Partial ingestion failures exit nonzero so cron notices.
Nightly `update` defaults to the MAX(year) universe with `--all-universes`
as the opt-out. *(2026-08-26 annotation: this default was originally marked
provisional pending OQ-5. D-010 dissolved OQ-5 — universes are ingestion
seed filters with no backtest-effective dates — so the MAX(year) default is
settled as a pragmatic ingestion scope, and becomes moot when D-011's
ongoing all-ticker collection supersedes universe-scoped updates.)*

*(2026-08-26 identity annotation: D-014 replaces `ticker` with
`instrument_id` as the coverage/file owner and requires identity-envelope
validation before every bar or coverage write. A full refresh must cover the
instrument's complete stored envelope; the interval, overlap, and vintage
policies otherwise remain.)*

**Context:** Adopted from an external substrate review the owner forwarded
(2026-08-26), which demonstrated that the original single-watermark model
could not perform the documented rank-year-then-full-history backfill, and
that appends without a refresh policy can mix adjustment vintages. Tiingo
documents EOD corrections arriving through the evening and adjusted values
incorporating splits/dividends
(https://www.tiingo.com/documentation/end-of-day).

**Consequences:** Future agents must not "simplify" ingestion back to a
watermark, advance coverage on empty recent responses, or append past a new
corporate action without the full refresh. Costs: nightly updates refetch ~7
extra days per ticker; a corporate action refetches that ticker's full
history (one request).

**Reopen if:** Tiingo request quotas make the refresh overlap or full
refreshes too expensive, or a study needs raw-bars-canonical with locally
derived adjustments (that supersedes the vintage-refresh approach).

## D-008: Asset scope is US stocks and ETFs only  (2026-08-26, status: accepted)

**Decision:** The dataset covers US-listed stocks and ETFs. No options,
futures, or crypto.

**Context:** Stated by the project owner during workflow scaffolding. The
owner's seed universe (SPY, IWM, AAPL, QQQ, ...) is already stocks + ETFs, and
Tiingo's daily feed covers exactly this scope.

**Consequences:** One data source, one bar schema, one universe model.
Options/futures would each require a different vendor and storage design.

**Reopen if:** A study genuinely requires another asset class; crypto would be
the cheapest addition (Tiingo has endpoints), derivatives the most expensive.

## D-007: No live or automated trade execution  (2026-08-26, status: accepted)

**Decision:** This is a research/backtesting tool. Orders never leave it — no
broker connectivity, no execution, including in any future realtime layer.

**Context:** Stated by the project owner during workflow scaffolding
("out of scope").

**Consequences:** No broker-API surface, credentials handling, or
order-safety design anywhere in the architecture. A future realtime layer is
signal display for the owner, nothing more.

**Reopen if:** The owner explicitly changes the project's charter; that would
be a major new decision with its own risk analysis, not an amendment.

## D-006: AI-developed, human-directed workflow (lean process)  (2026-08-26, status: accepted)

**Decision:** Agents implement from the project documentation; the human
directs, reviews, and is the sole committer. Process weight is the lean
default — one agent, one pass, one human scan; reviews on demand
([workflow.md](workflow.md)).

**Context:** The owner adopted the scaffold-project workflow for this repo and
stated it is a personal tool (single user, no SLA, reversible deploys).

**Consequences:** Agents never commit or push. The docs in `docs/` are the
project's long-term memory and must be kept accurate as changes land. No
mandatory multi-agent review structure.

**Reopen if:** The project gains real users or hard-to-reverse deploys — then
process dials up, recorded as a new decision.

## D-005: Interface is CLI + Python library first  (2026-08-26, status: accepted)

**Decision:** Operations run through the `market-data` CLI (cron-friendly);
research runs through the importable `marketdata` library. A web UI and
realtime layer are later, optional additions on top of the same library.

**Context:** Chosen by the owner at kickoff from CLI-first / web-now /
notebook-first options.

**Consequences:** No web framework dependency today. Everything a UI would
need must be reachable through the library, keeping a later UI thin.

**Reopen if:** Coverage browsing or results review becomes painful enough at
the terminal that the proposed web UI gets promoted (features.md).

## D-004: Annual point-in-time universes ranked by dollar volume  (2026-08-26, status: accepted, amended by D-010 and D-014)

**Decision:** The ticker universe is stored per year, ranked by a
dollar-volume metric, seeded from the owner's CSV
(`Year,Ticker,MedianDollarVolume`) and extendable via the built-in
Tiingo-candidates + ranking flow. Backtests use the membership for the year
being simulated. *(2026-08-26: this last sentence is superseded by D-010 —
universes seed ingestion; backtests select from stored data.)*

**Context:** The owner's stated plan: seed the ticker list annually by dollar
volume to strategically pick which stocks are in the dataset. Per-year storage
was chosen so historical membership is preserved (survivorship-bias-aware).

**Consequences:** The `universe` table is keyed (year, ticker). *(2026-08-26:
the original requirements here — research code joins membership
point-in-time, and the open OQ-5 timing question — are superseded/dissolved
by D-010; the table remains as the seeding record and an ingestion scope.
D-014 further requires each source `(year, ticker)` row to resolve through
date-ranged aliases to exactly one instrument before it can scope ingestion;
zero or multiple matches fail closed.)*

**Reopen if:** Studies need finer-grained (e.g. quarterly) membership or a
different selection metric.

## D-003: Storage is Parquet + DuckDB with SQLite metadata  (2026-08-26, status: accepted, amended by D-009, D-014, D-017, and D-019)

**Decision:** Bars are stored as zstd Parquet files (per-ticker EOD,
per-ticker-year intraday), queried via DuckDB; small relational state
(universes, coverage, ticker registry) lives in SQLite. No database server.
Ingestion is coverage-driven (originally single-watermark; amended to
coverage intervals by D-009) with atomic merge-upsert writes. If a realtime
tool materializes, a server DB may be added as a hot layer, but the Parquet
archive stays canonical.

**Context:** Chosen by the owner at kickoff from DuckDB+Parquet / PostgreSQL /
SQLite / MySQL options, on the analysis that backtesting is an analytical,
read-heavy, batch-written, single-user workload where columnar storage wins
and a row-store server adds admin cost without benefit at intraday scale.

**Consequences:** Zero database administration; backups are file copies; the
warehouse relocates via `MARKET_DATA_DIR`. Concurrent-writer scenarios are out
of design until a realtime layer forces them.

**Reopen if:** A realtime/serving workload with concurrent writers arrives, or
single-box scans stop being interactive at the dataset's actual size.

## D-002: Python implementation; Tiingo as sole data source  (2026-08-26, status: accepted)

**Decision:** The tool is written in Python (polars, DuckDB, click, requests).
Market data comes exclusively from Tiingo's API — daily endpoint for EOD
(adjusted columns included), IEX endpoint for intraday. The API token lives in
`.env` and is never committed or logged.

**Context:** Both stated by the owner at kickoff: Tiingo was a given ("using
Tiingo through the API"); Python was chosen from Python/Go/TypeScript/Rust for
its quant/dataframe ecosystem.

**Consequences:** No multi-vendor abstraction layer; code may be
Tiingo-shaped. Intraday inherits the IEX feed's limits (bounded history,
unadjusted, IEX-only volume) — measured in the M0 spike, not assumed.

**Reopen if:** Tiingo pricing/coverage stops fitting, or a study needs data
Tiingo cannot provide (that becomes a second-source decision, not a silent
addition).

## D-001: Apache-2.0 license, permissive dependencies  (2026-08-26, status: accepted, amended by D-018)

**Decision:** The project is licensed Apache-2.0 (LICENSE committed at repo
creation by the owner). Dependencies must carry permissive,
Apache-2.0-compatible licenses, verified against each package's own metadata.

**Context:** The owner created the repo with the Apache-2.0 text before
kickoff. Current dependencies (click, duckdb, polars, pyarrow, python-dotenv,
requests, pytest, responses) are all permissive (BSD/MIT/Apache).

**Consequences:** Copyleft dependencies are excluded except for D-018's named,
transitive `certifi` exception.

**Reopen if:** The owner relicenses; dependency policy would follow.
