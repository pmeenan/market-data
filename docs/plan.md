# Plan

**This is a living document.** Milestones will be re-scoped, re-ordered, split,
or added as planning conversations and findings come in. That churn is
expected; what is *not* allowed is silent change. Scope changes get a
decision-log entry; progress is reflected here by checking boxes and updating
status lines as work lands.

Check a box only when the item is done and verified; partially done items stay
unchecked, optionally with a note.

**Status legend:** `pending` · `in progress` · `done` · `parked`

## M0 — Plan the plan  `in progress`

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
- [ ] Feature triage: walk features.md with the owner; promote or reject every
      `proposed` row; answer the open questions; record significant calls in
      decisions.md.
- [ ] Spike — intraday depth & semantics (OQ-2, OQ-3): fetch hourly bars for a
      handful of representative tickers (large-cap, mid-cap, ETF), measure how
      far back coverage goes, check bar alignment to the 9:30 open and
      IEX-volume caveats. Output: a short findings note + rough-edges entries
      for surprises.
- [ ] Spike — backtest engine (OQ-1): prototype the gap-recovery study once
      with a hand-rolled polars/DuckDB loop and once with an existing library;
      compare effort and fit. Output: recommendation recorded in decisions.md.
- [ ] Settle universe-membership semantics for backtests (OQ-5) and record the
      decision.
- [ ] First full draft of architecture.md (research layer + results storage,
      OQ-6).
- [ ] Toolchain hardening decisions: lint/format (e.g. ruff), CI or a local
      pre-commit check, dependency locking, license audit. Record in
      decisions.md.
- [ ] Benchmark the Parquet file layout (per-ticker vs yearly/bucketed
      compaction) with representative cross-sectional scans and intraday
      ingestion before revisiting D-003's layout.
- [ ] Rewrite the provisional ladder below into real milestones with exit
      criteria.

**Exit criteria:** the owner has walked features.md and says the plan is good
enough to build from; the architecture-blocking open questions (OQ-1, OQ-2,
OQ-5) are answered; toolchain decided; M1+ milestones have scopes. M0 is a
conversation, not a phase — it exits on the owner's call, not on a checklist
reaching zero.

## Provisional milestone ladder  `pending — to be rewritten in M0`

Ordered by risk: the riskiest substrate (intraday coverage) and one end-to-end
research path before breadth. Sketch only — do not start work from these
entries, and note that they freely reference `proposed` features.md rows;
nothing here pre-empts the M0 triage.

- **M1 — Data substrate hardening.** Hourly intraday ingestion verified
  end-to-end against real Tiingo data; seed CSV imported and full EOD backfill
  run for the universes; nightly cron in place with visible failures; ruff +
  license audit wired in.
- **M2 — First study end-to-end.** The gap-recovery study runs from stored
  data to summary statistics with the chosen engine, using point-in-time
  universes; results persisted (per OQ-6).
- **M3 — Research breadth.** Data-quality checks, coverage reports,
  benchmark-relative metrics, a second strategy to prove the engine
  generalizes.
- **M4 — Web UI (if promoted).** Coverage browser + backtest-result viewer on
  the server's public IP.
- **M5 — Realtime layer (if promoted).** Intraday-refreshed signals for the
  owner; still research-only (no execution, D-007).
