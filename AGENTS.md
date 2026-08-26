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
- **Tiingo is the sole market-data source.** The API token lives in `.env`
  (gitignored) and must never be committed or logged. (D-002)
- **Universes seed the dataset; strategies select from the data.** Per-year
  dollar-volume universes choose what gets ingested (and remain stored as the
  historical record), but backtests select tickers from stored price/volume
  directly — survivorship-bias protection comes from backfilling all tickers
  including delisted ones, not from membership joins. (D-004, D-010, D-011)
- **Research only; US stocks + ETFs only.** No order execution or broker
  connectivity, ever; no options/futures/crypto. (D-007, D-008)
- **Ingestion is idempotent, resumable, and vintage-consistent.** Coverage is
  a per-(ticker, dataset) interval (leading gaps get fetched, not skipped);
  Parquet writes are merge-upserts; updates refetch a rolling overlap and a
  new split/dividend triggers a full-history refresh. A rerun after an
  interruption must converge to the same dataset. (D-003, D-009)
- **Apache-2.0, permissive deps only.** Dependencies must carry
  Apache-2.0-compatible permissive licenses, verified against the package's own
  metadata. (D-001)

## Repository layout

| Path | What lives there |
| ---- | ---------------- |
| `src/marketdata/` | The library + CLI: Tiingo client, Parquet/SQLite stores, ingestion, DuckDB query surface |
| `tests/` | pytest suite (offline; Tiingo is mocked) |
| `seeds/` | Committed universe seed CSVs (Year,Ticker,dollar-volume) |
| `data/` | The warehouse (gitignored; relocatable via `MARKET_DATA_DIR`) |
| `docs/` | Vision, plan, architecture, decisions, features, rough edges, workflow |

## Doc map — pull what the task needs, not everything

Always read (it's short): [docs/workflow.md](docs/workflow.md) — the
build → commit loop, on-demand reviews, and the human commit gate.

| Doc | Read when the task needs |
| --- | --- |
| [docs/plan.md](docs/plan.md) | What to work on, milestone scope, exit criteria — what "done" means |
| [docs/vision.md](docs/vision.md) | Why the project exists, success criteria, non-goals |
| [docs/features.md](docs/features.md) | The feature matrix: confirmed scope, proposed additions, open questions |
| [docs/architecture.md](docs/architecture.md) | System structure and technical constraints |
| [docs/decisions.md](docs/decisions.md) | Settled choices (D-NNN). Scan headings; read only the entries your task touches |
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
6. **Python conventions.** Type hints on public functions; `pytest` must pass
   before ending a turn (`.venv/bin/pytest`); tests never hit the network. New
   dependencies need a reason and a license check.
7. **Keep the always-loaded context lean.** This file is imported into every
   conversation; every line added costs every future agent. Detail belongs in
   `docs/` behind the doc map, not here.
8. **Scratch files stay out of the tree.** Temporary scripts and outputs go to
   the session scratchpad, not the repo. Delete throw-away diagnostics before
   concluding.

## Current status

Milestone **M0 (plan the plan)** — the data-warehouse substrate (ingestion,
storage, CLI) was scaffolded before this workflow was adopted and works, with
tests. Feature triage is done (2026-08-26): scope, backfill phasing (D-011),
and the universe reframing (D-010) are settled; the intraday, engine, and
instrument-identity (OQ-8 — gates all historical backfills) spikes,
architecture draft, and real milestone ladder remain. The backtest layer
does not exist yet. See [docs/plan.md](docs/plan.md). Keep this
paragraph short and current when plan.md milestone status changes (rule 4).
