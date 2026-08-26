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

## D-009: EOD coverage and refresh policy  (2026-08-26, status: accepted)

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
as the opt-out — but that default is **provisional**: if OQ-5 resolves to
"year-Y rankings are effective in Y+1", then during year Y the MAX(year)
universe is *future-effective*, and the default must become "latest
*effective* universe" (or cron should pass `--universe YEAR` explicitly)
once effective-date metadata exists. Do not treat the current default as
settled.

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

## D-004: Annual point-in-time universes ranked by dollar volume  (2026-08-26, status: accepted)

**Decision:** The ticker universe is stored per year, ranked by a
dollar-volume metric, seeded from the owner's CSV
(`Year,Ticker,MedianDollarVolume`) and extendable via the built-in
Tiingo-candidates + ranking flow. Backtests use the membership for the year
being simulated.

**Context:** The owner's stated plan: seed the ticker list annually by dollar
volume to strategically pick which stocks are in the dataset. Per-year storage
was chosen so historical membership is preserved (survivorship-bias-aware).

**Consequences:** The `universe` table is keyed (year, ticker); research code
must join against it point-in-time rather than using the latest list. Which
year's list governs which trading days is still open (OQ-5).

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
