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

## D-011: Backfill scope, priority, and API budget  (2026-08-26, status: accepted)

**Decision:** The dataset is built in phases, bounded by the owner's Tiingo
Power-tier budget:

1. **Phase 1 — seed tickers, EOD + 1-hour** (the 5,403 distinct tickers in
   the 2011–2026 seed CSV): EOD daily back 20 years, plus 1-hour intraday
   back up to 10 years (the IEX feed likely caps out around 9; the OQ-2
   spike measures the real depth). Cheap: a few GB total.
2. **Phase 2 — all tickers, EOD:** EOD daily back 20 years for *all*
   Tiingo-supported US stocks/ETFs, including delisted ones.
3. **Phase 3 — seed tickers, 5-minute:** intraday back up to 10 years.
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

Power-tier limits as published 2026-08-26 (tiingo.com/about/pricing): 10,000
requests/hour, 100,000/day, **40 GB bandwidth/month**, ~110k unique
symbols/month. Bandwidth is the binding constraint, so **bulk fetches use
`format=csv`, not JSON** — CSV responses carry no repeated field names and
run roughly half to a third the bytes per bar (owner's call, 2026-08-26; the
client currently requests JSON and switches in M1). With CSV, 5-minute
history for the seed list is estimated at ~40–75 GB of transfer, i.e.
**1–2 months of budget**, and all-ticker EOD well under a month. Backfill
runs through a budget-aware scheduler (confirmed feature, features.md):
track requests and bytes against the hourly/daily/monthly caps, stop when a
window is spent, resume in the next — never let a backfill run blow the
month's bandwidth. Ongoing all-ticker nightly collection is comfortably
inside the caps (~1–2 GB/month with CSV).

**Context:** Backfill scope and priority stated by the owner at the M0 triage
(2026-08-26); tier limits checked against Tiingo's current pricing page the
same day. Resolves OQ-7.

**Consequences:** Phase ordering is owner intent — don't reorder it to
"optimize". The backfill completing is measured in months, not hours; M1
planning must treat it as a long-running metered process with resumable
state (which D-009's coverage intervals already provide). The Tiingo client
must move from `format=json` to CSV parsing for bulk endpoints (M1, with
tests updated to CSV fixtures). Bandwidth estimates are back-of-envelope;
the OQ-2 spike and the scheduler measure actual bytes/ticker (CSV) early
and re-project.

**Reopen if:** Tiingo changes tier limits, the measured bytes/ticker differs
wildly from the estimate, or the owner's study needs reprioritize which data
arrives first.

## D-010: Universes are dataset seed filters, not backtest membership  (2026-08-26, status: accepted, amends D-004)

**Decision:** The per-year dollar-volume universes exist to choose which
tickers the dataset ingests (preferring large, stable names for the initial
backfill) — they are **not** a point-in-time membership constraint on
backtests. Research code selects tickers from the stored price/volume data
directly; if a strategy needs a liquidity screen, it computes one from the
dataset, not from the universe table. Survivorship-bias protection comes
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
survivorship-bias guarantee additionally depends on resolving reused ticker
symbols (OQ-8) — distinct securities sharing a symbol must not merge; see
D-011's backfill gate.

**Reopen if:** A study reintroduces universe-membership-based selection —
that would resurrect point-in-time semantics and the lookahead question,
and needs its own decision.

## D-009: EOD coverage and refresh policy  (2026-08-26, status: accepted, provisional-default caveat lifted by D-010)

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

## D-004: Annual point-in-time universes ranked by dollar volume  (2026-08-26, status: accepted, backtest-membership role removed by D-010)

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
by D-010; the table remains as the seeding record and an ingestion scope.)*

**Reopen if:** Studies need finer-grained (e.g. quarterly) membership or a
different selection metric.

## D-003: Storage is Parquet + DuckDB with SQLite metadata  (2026-08-26, status: accepted, ingestion-state model amended by D-009)

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

## D-001: Apache-2.0 license, permissive dependencies  (2026-08-26, status: accepted)

**Decision:** The project is licensed Apache-2.0 (LICENSE committed at repo
creation by the owner). Dependencies must carry permissive,
Apache-2.0-compatible licenses, verified against each package's own metadata.

**Context:** The owner created the repo with the Apache-2.0 text before
kickoff. Current dependencies (click, duckdb, polars, pyarrow, python-dotenv,
requests, pytest, responses) are all permissive (BSD/MIT/Apache).

**Consequences:** Copyleft dependencies are excluded.

**Reopen if:** The owner relicenses; dependency policy would follow.
