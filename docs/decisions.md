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

## D-015: Research uses a project-native DuckDB/polars event engine  (2026-08-26, status: accepted)

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

**Consequences:** M2 builds focused research primitives and the gap study,
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

## D-014: Stable instruments own bars; symbols are date-ranged aliases  (2026-08-26, status: accepted, amends D-003, D-004, D-009, and D-011)

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
publication-lag rule.

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

## D-013: Five-minute history fills newest-first with a 30 GB monthly hard cap  (2026-08-26, status: accepted, amends D-011)

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
separate current-versus-history byte accounting, the vendor bandwidth-reset
boundary, a refresh-cost forecast, and hard stops at both 30 GB of historical
transfer and 40 GB total. At the measured 68.5 GB seed-list projection, phase
3 requires at least three billing windows. Recent cross-sectional history
becomes usable incrementally while the backfill works toward 2016-12-12.

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

## D-011: Backfill scope, priority, and API budget  (2026-08-26, status: accepted, amended by D-013 and D-014)

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
client currently requests JSON and switches in M1). With CSV, the OQ-2 spike
measured a 68.5 GB seed-list projection for 5-minute history, within the
original ~40–75 GB estimate, i.e. **1–2 months only if phase 3 could use the
full vendor cap**. D-013 instead hard-caps history at 30 GB/month, making the
operational minimum three billing windows. The EOD phases and operational
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
"optimize". The backfill completing is measured in months, not hours; M1
planning must treat it as a long-running metered process with resumable
state (which D-009's coverage intervals already provide). The Tiingo client
must move from `format=json` to CSV parsing for bulk endpoints (M1, with
tests updated to CSV fixtures). The OQ-2 measurement calibrates the initial
projection; the scheduler must continue tracking actual bytes/ticker because
listing lifetimes and payload sizes vary.

**Reopen if:** Tiingo changes tier limits, the measured bytes/ticker differs
wildly from the estimate, or the owner's study needs reprioritize which data
arrives first.

## D-010: Universes are dataset seed filters, not backtest membership  (2026-08-26, status: accepted, amends D-004; identity consequences amended by D-014)

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

## D-009: EOD coverage and refresh policy  (2026-08-26, status: accepted, identity key amended by D-014; provisional-default caveat lifted by D-010)

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

## D-003: Storage is Parquet + DuckDB with SQLite metadata  (2026-08-26, status: accepted, amended by D-009 and D-014)

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
