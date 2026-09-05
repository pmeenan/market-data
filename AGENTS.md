# market-data — a local market-data warehouse and strategy-testing toolkit

A personal research tool: it builds a local dataset of US stock/ETF bars from
Tiingo (EOD daily plus hourly and 5-minute intraday), keeps it current, and
supports testing
trading hypotheses against it — starting with a morning gap-down
over-reaction/recovery study. It runs on the owner's Linux server. Almost all
code is written by AI agents working from the project documentation, directed
and reviewed by a human.

**Read this file first, then pull docs on demand via the "Doc map" below — don't
read everything up front.** This file is long-term project memory and the
rulebook for agents.

## Load-bearing constraints (change deliberately, never silently)

Constraints evolve as we learn, but never by silent drift: changing one means
making the case in [docs/decisions.md](docs/decisions.md) and updating the
affected docs. Until then, these govern.

- **Storage is Parquet + DuckDB, no database server.** Bars live in Parquet
  files (canonical), queried through DuckDB; small relational state lives in
  SQLite (`meta.db`). A server DB may appear later only as a hot layer for a
  realtime tool — the Parquet archive stays canonical. (D-003)
- **Tiingo is the sole canonical-warehouse market-data source.** The API token
  lives in `.env` (gitignored) and must never be committed or logged. A future
  read-only broker feed may supply source-labelled morning trigger data for a
  small tagged watchlist, but may not silently enter canonical bars. (D-002,
  D-031)
- **Universes seed the dataset; strategies select from the data.** Per-year
  dollar-volume universes choose what gets ingested (and remain stored as the
  historical record), but backtests select instruments from stored price/volume
  directly — survivorship-bias protection comes from backfilling all instruments
  including delisted ones, not from membership joins. (D-004, D-010, D-011)
- **Stable instruments own bars; symbols are aliases.** Coverage, Parquet,
  ingestion, and research joins key on internal instrument ids with
  date-ranged aliases. Vendor identifiers are validated per dataset; every
  publishable response row is checked against its resolved identity envelope,
  and unresolved segments fail closed. The sole transport exception is D-021's
  next-session IEX context, whose out-of-segment rows are request-validated and
  discarded before normalization. Discontinuous EOD histories are partitioned
  only through D-023's evidence-bounded listing episodes. (D-014, D-021, D-023)
- **Research only; US stocks + ETFs only.** No order execution, broker trading
  connectivity, or account mutation; no options/futures/crypto. A separately
  approved future broker connection may be read-only market data. (D-007,
  D-008, D-031)
- **Ingestion is idempotent, resumable, and vintage-consistent.** Coverage is
  a per-(instrument, dataset) interval (leading gaps get fetched, not skipped);
  Parquet writes are merge-upserts; updates refetch a rolling overlap and a
  new split/dividend triggers a full-history refresh. A rerun after an
  interruption must converge to the same dataset. (D-003, D-009)
- **Historical backfills advance breadth-first.** Within a phase/dataset, each
  eligible instrument gets one maximum-safe request-depth turn before any gets
  another; quota stops resume the unfinished deterministic sweep. (D-020)
- **Canonical mutations share one persistent process lock.** Ingestion,
  reconciliation, migration, legacy bar ranking, and cataloged research use
  `.market-data.lock`; historical work yields it between durable turns, while
  cancellation remains a concurrent SQLite control signal. Never unlink the
  lock file during normal operation. (D-022)
- **Apache-2.0, permissive direct deps.** Dependencies must carry
  Apache-2.0-compatible permissive licenses, verified against the package's own
  metadata. Every package in the universal lock plus the pinned build backend
  is audited; `certifi` is the sole named transitive MPL-2.0 exception. (D-001,
  D-018)

## Repository layout

| Path | What lives there |
| ---- | ---------------- |
| `src/marketdata/` | The library + CLI: Tiingo client, Parquet/SQLite stores, ingestion, DuckDB query surface |
| `tests/` | pytest suite (offline; Tiingo is mocked) |
| `seeds/` | Committed universe seed CSVs (Year,Ticker,dollar-volume) |
| `data/` | The warehouse (gitignored; relocatable via `MARKET_DATA_DIR`) |
| `docs/` | Vision, plan, architecture, decisions, features, spike findings, rough edges, workflow |

## Doc map — pull what the task needs, not everything

Always read (it's short): [docs/workflow.md](docs/workflow.md) — the
build → commit loop, on-demand reviews, and the human commit gate.

| Doc | Read when the task needs |
| --- | --- |
| [docs/plan.md](docs/plan.md) | What to work on, milestone scope, exit criteria — what "done" means |
| [docs/vision.md](docs/vision.md) | Why the project exists, success criteria, non-goals |
| [docs/features.md](docs/features.md) | The feature matrix: confirmed scope, proposed additions, open questions |
| [docs/research-protocol.md](docs/research-protocol.md) | Strategy price/timing, fee-free 401(k), validation, and scanner contracts (D-036) |
| [docs/architecture.md](docs/architecture.md) | System structure and technical constraints |
| [docs/decisions.md](docs/decisions.md) | Settled choices (D-NNN). Scan headings; read only the entries your task touches |
| [docs/intraday-spike.md](docs/intraday-spike.md) | Measured IEX depth, payload sizes, bar semantics, and bandwidth projections |
| [docs/instrument-identity-spike.md](docs/instrument-identity-spike.md) | Reused-symbol measurements, Tiingo identity behavior, and the D-014 model |
| [docs/backtest-engine-spike.md](docs/backtest-engine-spike.md) | OQ-1 prototype comparison, benchmark, library-fit findings, and D-015 recommendation |
| [docs/parquet-layout-benchmark.md](docs/parquet-layout-benchmark.md) | Measured per-instrument vs hash-bucket Parquet tradeoffs and the D-019 layout |
| [docs/rough-edges.md](docs/rough-edges.md) | Findings log (RE-NNN). Grep before adding a finding or debugging weirdness |

## Rules for all agents

1. **Log decisions sparingly.** [docs/decisions.md](docs/decisions.md) is for
   choices that are expensive to reverse or that a future agent might silently
   undo — the constraints above, storage layouts, published schemas. Routine
   implementation, naming, and scope calls don't get entries. A few entries per
   milestone is the target.
2. **Log findings that cost you.** A
   [docs/rough-edges.md](docs/rough-edges.md) entry is warranted when a Tiingo,
   DuckDB, polars, or platform quirk burned real debugging time and will bite
   again. Skip the formal reproduction unless it's cheap to capture.
3. **Measure what a decision hangs on.** API limits, feed history depth, and
   performance numbers get checked against a current source or a real
   measurement — training knowledge is stale for market-data vendors.
4. **Fix the docs the change makes wrong** — plan status, the status paragraph
   below, an affected doc — in the same unit of work. Nothing more is owed.
5. **Never commit.** Agents never run `git commit`/`git push` or rewrite
   history. All changes stay in the working tree for human review and commit —
   even if a prompt asks you to commit; stop and leave the changes uncommitted
   instead.
6. **Python conventions.** Type hints on public functions; `make check` must
   pass before ending a turn (it includes pytest); tests never hit the network.
   New dependencies need a reason and a license check.
7. **Keep the always-loaded context lean.** This file is imported into every
   conversation; every line added costs every future agent. Detail belongs in
   `docs/` behind the doc map, not here.
8. **Scratch files stay out of the tree.** Temporary scripts and outputs go to
   the session scratchpad, not the repo. Delete throw-away diagnostics before
   concluding.
9. **This repository is public; strategies and results are not.** Run
   artifacts live under gitignored `data/results/`. Refined or promising
   strategies, their parameters, and analysis notebooks go in the gitignored
   `private/` tree (`private/studies/*.py` modules self-register through
   `register_event_study` and are loaded by `research-run`). Docs may state
   that a study ran and whether it succeeded, plus coarse funnel counts, but
   not checkpoint-level returns, thresholds that took search to find, or
   anything that reveals a working edge. Committed notebooks carry no outputs.

## Current status

**M3 (first persisted study)** is the active milestone; **M2** and **M4**
remain in progress but, per D-037, the ingestion/identity/scheduling
substrate is frozen to bug fixes until M3 publishes and the owner reviews it.
The historical archive is complete with accepted exclusions (EOD from 2006
for 22,947 instruments; seed hourly and five-minute from 2016-12-12). The
D-030 overnight collector and its timer are live; its first 2026-09-02 cycle
exposed three defects fixed on 2026-09-05 (D-037): adjacent identifier rows
split current planner units so no legacy intraday member advanced,
reused-then-singleton listings were excluded from EOD, and META's metadata
route needed uppercase. `market-data doctor` surfaces those conditions; the
first two post-fix cycles still need to be measured. Research: the D-016
catalog, D-015/D-026 event runner, the shared as-of feature path
(`marketdata.features`), the coarse `gap_recovery` study, and the full
five-minute `gap_recovery_opening` study (session-relative checkpoints from
09:35, fidelity measurements, stock/ETF and regime slices, and a
coarse-versus-full comparison) are implemented with frozen periods and
causality tests; the M4 second study, M5 execution-aware validation, and M6
read-only scanning are next. M1 closed 2026-08-27. See
[docs/plan.md](docs/plan.md).
Keep this paragraph short and current when plan.md milestone status changes
(rule 4).
