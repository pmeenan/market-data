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

Every manual historical command now creates or resumes a schema-v4 scheduler
job and completes at most its current breadth-first sweep.
Each eligible stable instrument gets one maximum-safe request unit before any
peer deepens; `--max-units` can stop even earlier, and `--job-id` names an
explicit rerunnable job. `--phase 1|2|3` applies D-011's dataset and predecessor
gates. Production phase-2/3 jobs must also be designated by a durable D-027
program; a missing predecessor or an ad hoc later-phase job fails closed. An
omitted `--end` is frozen when the job is first created, so the same
command still resolves to that job on later days. Re-run an active job to
continue the next sweep. Terminal ranges remain dormant on routine timer and
manual invocations; after repairing or reviewing their evidence,
`--retry-blocked` explicitly reactivates them. Each `--force` invocation creates
a fresh job; supply its printed `--job-id` to resume that exact force run. A
superseded active/blocked job can be released from phase gating without erasing
its audit trail with
`market-data backfill cancel JOB_ID`:

```bash
market-data backfill eod --phase 1 --start 2006-08-28
market-data backfill intraday --phase 1 --freq 1hour \
  --start 2016-12-12 --max-units 500
```

Research eligibility is local to each event, not to an instrument's complete
warehouse history. A study declares the contiguous lookback it needs through
the decision timestamp; identity or coverage gaps outside that window do not
exclude the event. Outcome availability is evaluated only after selection, so
a missing future checkpoint is retained and reported rather than used to
remove the candidate retroactively (D-026).

Cataloged research publication records every required input glob, its expanded
files, and the alias-envelope snapshot used by event selection. Fingerprint
verification detects new, missing, or changed matching files and changed
selection identity. Interrupted `running` rows and unowned result directories
are reported without mutation by default; cleanup requires an explicit apply:

```bash
market-data research-reconcile
market-data research-reconcile --apply
```

Before a new warehouse can ingest EOD, bootstrap conservative identity
evidence from Tiingo's public supported-tickers archive plus its authenticated
EOD metadata endpoint. A unique archive record is admitted only when ticker,
exchange, and date envelope all agree with authenticated metadata. Reused
tickers get one internal listing episode per archive record; non-overlapping
date segments can proceed, while archive overlaps, missing records,
mismatches, and 404s remain reported and fail closed. The authenticated calls
use the durable current-work request/byte ledger.

```bash
market-data identity bootstrap-eod \
  --summary-json data/operations/identity-bootstrap-eod.json
```

Intraday identifiers are established independently for each exact frequency;
EOD evidence is never copied into IEX. The bootstrap partitions stable alias
envelopes around known overlaps, probes the latest 20 XNYS sessions plus the
one-session finalization context, and validates only segments with target-range
rows. Empty or contradictory responses are persisted as fail-closed evidence
so reruns skip them unless `--retry-blocked` is explicit. Probes use the same
durable current-work ledger as other authenticated requests:

```bash
market-data identity bootstrap-intraday --freq 1hour \
  --start 2016-12-12 --end 2026-08-27 \
  --summary-json data/operations/identity-bootstrap-hourly-phase1.json
```

After an EOD history job reaches `complete` (never merely `blocked` or
cancelled), the command automatically runs the idempotent D-023 episode audit.
It conservatively splits a broad vendor history only at a gap of at least 252
expected XNYS sessions (including a long internal zero-volume bridge) that is
spanned by durable source coverage, keeps the real ticker as the alias, and
records a stable display label such as `PCS@20070419`. Invalid OHLC, long
zero-volume bridges, and inferred fragments under 20 rows are quarantined
instead of published. Invalid OHLC rows in newly fetched EOD responses are
likewise written with raw response provenance under
`data/quarantine/eod-response/`, while valid peers in that response continue.
The apply operation records a recoverable SQLite/Parquet backup under
`data/backups/`. Inspect or run the coverage-aware repair independently with:

```bash
market-data identity repair-eod-episodes --dry-run \
  --summary-json data/operations/eod-episode-repair-plan.json
market-data identity repair-eod-episodes --apply \
  --summary-json data/operations/eod-episode-repair-applied.json
```

The target server uses one D-027 user-systemd program timer instead of fixed
per-phase timers. The one-time initialization adopts the exact terminal phase-1
jobs and freezes the seed scope:

```bash
market-data backfill program-init
```

Each `program-step` invocation then performs one bounded durable action: freeze
the phase-2 Tiingo-supported US stock/ETF archive scope, prepare at most 250
identities, or advance at most 500 historical instrument turns. Phase 2 cannot
fall back to the 5,403-ticker seed CLI default, and phase 3 cannot begin until
phase 2 is terminal and the complete seed cohort has independent five-minute
identity classifications. Inspect without API calls or durable mutation using
`market-data backfill program-status`. The timer atomically writes bounded
status to `data/operations/backfill-program-v1-status.json`; exit 1 represents
durable per-symbol exclusions, while coordinator failures receive up to three
two-minute retries. A definitive Tiingo HTTP 404 becomes a dormant fail-closed
range exclusion without claiming coverage; `--retry-blocked` is required to
reactivate it after review. Current collection retains its independent priority
and budget.

The former interim current-EOD timer ran at 23:30 UTC Monday through Friday. Its
single locked command refreshed exact EOD identity evidence for the latest
universe, then collected bars, so a new session was never requested through
stale snapshot boundaries. It atomically replaced
`data/operations/current-eod-status.json` with start/end timestamps, request and
observed-byte counts, outcome counts, and at most 100 details per diagnostic
category. Per-symbol fail-closed identity and vendor failures use exit 1 and are
accepted by the service; quota stops are clean. Coordinator, configuration,
locking, and status-publication failures use exit 2 and receive up to three
two-minute retries before the user service remains failed. Manual forensic runs
can still use `--summary-json` for the complete segment report instead of the
bounded `--status-json` record.

That interim service is now disabled. The schema-v8 ongoing program was
initialized and its replacement timer enabled on 2026-09-02. Each cycle freezes
the active Tiingo-supported US stock/ETF roster and updates EOD for every
resolvable listing. It persists a
monthly top-5,000 cohort ranked by mean EOD `close * volume` over the latest 20
completed XNYS sessions, then runs independently resumable direct-hourly and
five-minute sweeps for that cohort.

D-032 prevents the active roster from consuming one metadata request per
listing every night. An unchanged, unique EOD listing whose prior envelope was
authenticated can extend its advancing supported-list `endDate` locally; any
new or changed immutable listing field still requires metadata or fails closed.
The continuation never authorizes IEX and every bar response remains
identity-envelope validated.

Per D-033, every current-mode target with existing coverage begins at that
coverage's trailing edge minus the seven-day correction overlap. It therefore
fills every safely resolvable interval from the historical or prior-current
stop through the cycle session rather than fetching only the last day. A new
intraday cohort member
with no stored intraday bars begins forward-only at the cohort's as-of session
and later correction overlaps cannot move that lower bound backward. A
completed request with no bar through the cycle session, or a transient request
failure, remains retryable for at most 40 unsuccessful breadth-first turns;
successful coverage progress does not consume the allowance. At the production
six-minute cadence that is roughly four hours; every attempt yields to healthy
peers. Once a dataset's healthy main pass is done, retry-only tickers no longer
gate the next dataset. A new monthly cohort excludes EOD targets that did not
complete safely; after the healthy five-minute pass, pending EOD, hourly, and
five-minute jobs retry in sweep-balanced order. Exhaustion becomes an explicit
cycle-scoped exclusion, while the cycle remains visibly partial until every
retry recovers or exhausts. Deterministic identity blockers, 404s, oversized
responses, and cancellation are immediate terminal exclusions.
The automatic driver refuses work before 23:30 UTC or after 08:00 New York time
on the next XNYS session; an explicit `step --session` is reserved for manual
recovery. Inspecting status never calls Tiingo and reports the pending retry
count for each dataset:

```bash
market-data ongoing init
market-data ongoing status
market-data ongoing step --session YYYY-MM-DD --max-units 1000
market-data ongoing run \
  --status-json data/operations/ongoing-main-v1-status.json
```

`ongoing run` checkpoints each bounded action, paces API-bearing batches, and
continues an older unfinished frozen cycle before creating the requested
session. EOD completes before the monthly rank, hourly, and five-minute states.
The systemd template uses 1,000 turns followed by six minutes of headroom and
stops cleanly at the morning boundary or a durable quota limit. Exit 3 is
reserved for a terminal cycle with explicit exclusions and is the only
nonzero status accepted by the service; ordinary exit-1 CLI/crash failures and
exit-2 operational failures retry.

D-036 scopes a pending M6 decision tool for bounded nightly candidates and
optional owner tags during strategy-defined monitoring hours. It will use the
same signal functions as replay, with timestamped shadow validation. A broker
market-data feed still requires a separate source decision. The tool cannot
place orders, access brokerage accounts, or silently merge observations into
canonical Tiingo bars.

On this server the ongoing templates are installed under
`~/.config/systemd/user/`, `market-data-ongoing.timer` is enabled, and user
lingering is enabled so collection continues after logout. The completed
historical-program timer and interim current-EOD timer are disabled. Reinstall
the live templates with:

```bash
install -Dm0644 -t ~/.config/systemd/user deploy/systemd/market-data-ongoing.*
systemctl --user daemon-reload
systemctl --user enable --now market-data-ongoing.timer
loginctl enable-linger "$USER"
```

The already-completed one-time production cutover was:

```bash
install -Dm0644 -t ~/.config/systemd/user deploy/systemd/market-data-ongoing.*
systemctl --user daemon-reload
market-data ongoing init
systemctl --user disable --now market-data-current-eod.timer
systemctl --user enable --now market-data-ongoing.timer
```

The live `meta.db` is schema v8 and contains `ongoing-main-v1`; stashing or
reverting its supporting code would make the enabled timer fail. Because the
console script is an editable install, reviewed scheduler changes become live
for the next timer invocation without reinstalling the unit.

Cancellation is also available while a sweep is running. Any already-started
request turn finishes and checkpoints safely, then the sweep stops before the
next instrument turn.

Authenticated attempts reserve quota in SQLite before transport and settle to
the encoded body bytes observed afterward; retries and rejected payloads count,
and incomplete transfers retain their reservation. Tiingo's documented
billing month starts at midnight EST on the first; its billing-byte basis
remains unpublished, so enforcement uses that calendar boundary with a 64 MB
response allowance. Historical work is admitted against total billing-month
usage up to 30 GB normally; over the final seven Tiingo billing dates that
ceiling rises daily through 31.5, 33, 34.5, 36, 37.5, and 39 GB, releasing only
reserve that current work has not consumed. Current work retains the separate
40 GB monthly ceiling.
An orderly connection failure before any response exists settles at zero body
bytes; crashes and partial bodies retain the full reservation. Responses are
capped while streaming so an oversized undeclared body becomes a durable
blocker rather than a repeating download.
`market-data status` displays billing-month observed and budgeted usage plus the
active historical admission ceiling and request-admissible headroom after the
next 64 MB reservation.

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
Schema v4 names the canonical SQL table `meta.coverage`, retains the old shape
explicitly as `meta.ticker_coverage_v1`, and adds durable request-attempt and
historical scheduler state.

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

Use a declared consistent price basis: `adj_*` columns are split- and
dividend-adjusted, while raw columns reflect prices as traded. Same-session
returns use raw entry and exit prices; never divide raw intraday prices by
adjusted EOD prices. Cross-session holdings require explicit action handling.
See [the research protocol](docs/research-protocol.md) for decision-time
availability, zero-fee 401(k) assumptions, validation, and M5/M6 scope.

## Layout

```
data/                       (gitignored; set MARKET_DATA_DIR to relocate)
  meta.db                   SQLite: identities, generation, universes, coverage
  bars/eod/bucket=ab/bars.parquet
  bars/intraday/1hour/year=2025/bucket=ab/bars.parquet
  results/STUDY/RUN_ID/
    observations.parquet
    input_files.parquet
  quarantine/v1-ticker-bars/
    migration-report.json  durable per-source migration outcome
src/marketdata/
  bar_fields.py              shared Tiingo bar-field contract
  calendar.py                XNYS sessions, IEX request bounds, bar labels
  quality.py                 structured checks + consumer-declared gates
  research.py                vectorized event runner + cataloged publication
  research_layout.py         shared safe result-layout contract
  ongoing.py                 durable overnight EOD/intraday program
  scheduler.py               durable budgets + breadth-first history sweeps
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

Milestones **M2 (trustworthy scheduled ingestion)**, **M3 (first persisted
study)**, and **M4 (full opening-window study and historical program)** are in
progress. M2's
cap-safe intraday request planner, XNYS calendar/session-label surface, and
structured quality/gating layer are implemented, as are durable request-budget
accounting, current-first breadth-first history scheduling, and shared process
locking. The terminal phase-1 jobs fetched 282 additional EOD histories,
represent 62 reused symbols as 125 inferred episodes, and validated 4,316
exact-frequency hourly IEX segments while retaining honest exclusions. The
enabled D-027 program timer has replaced both fixed phase-1 timers, frozen an
immutable 23,078-instrument supported-US phase-2 cohort, completed its safe EOD
work with accepted exclusions, completed phase-3 five-minute identity
preparation, and completed its historical transfer with accepted fail-closed
exclusions. The interim nightly current-EOD timer completed one actual
post-market run before being replaced. External failure notification is
optional for this personal deployment. D-030/D-031's schema-v8 all-active EOD
and monthly
top-5,000 hourly/five-minute overnight collector, gap bridging, CLI, status,
tests, and replacement systemd templates are implemented. The production
program and replacement timer are live. Its first 2026-09-02 cycle exposed an
identity-split retry loop after completing 13,154 EOD targets and explicitly
excluding 817; D-034's bounded retry fix and D-035's deferred cross-dataset
retry queue are implemented and await the next scheduled recovery run.
Complete measured EOD/hourly/five-minute cycles remain.
M1 closed on
2026-08-27 after its controlled EOD/IEX canary passed. Production ingestion is
permitted only for validated segments; unresolved work remains fail-closed and
visible. M3's immutable result catalog, input manifests/fingerprints, strict
compatible-result loading, explicit interrupted-run reconciliation, and
local-window vectorized event runner/CLI boundary, the shared as-of feature
path, and the coarse `gap_recovery` study are implemented (D-037 freezes the
ingestion substrate to bug fixes until the owner reviews its first result).

D-036 adds **M5 (execution-aware validation)** and **M6 (read-only opportunity
scanning)** as pending milestones. Neither simulator nor scanner is implemented.

Health checks read durable state only and exit 1 on an error-level finding
(request rate against Tiingo's limits, current targets retrying without
progress, frozen-target exclusions, cohort coverage freshness by rank, and
active listings that resolve to no instrument):

```bash
market-data doctor --summary-json data/operations/health.json
```

The first study is a registered built-in. It publishes an immutable cataloged
run with frozen chronological periods; `notebooks/gap_recovery.ipynb` loads
the same artifacts:

```bash
market-data research-run gap_recovery
market-data research-run gap_recovery --parameters-json '{"gap_threshold": -0.05}'
```

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
