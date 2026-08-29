# Vision

## What this is

A personal, locally-hosted market-data warehouse and strategy-testing toolkit.
The problem it solves: testing trading hypotheses requires clean, complete,
survivorship-bias-aware historical data, and hosted backtesting platforms are
opaque about exactly those properties. The answer here is to own the dataset —
build it locally from Tiingo, control which tickers are in it (annual
universes ranked by dollar volume), keep it current with a nightly job, and
run strategy studies directly against it with DuckDB/polars.

The first study is concrete: find stocks that opened significantly lower than
the prior close and measure whether they tend to recover over the next few
hours — i.e., is there a morning window where some stocks over-react and
partially rebound? Daily bars can approximate this; direct hourly bars provide
checkpoints from 10:00 onward, while 5-minute bars are required to measure the
opening half-hour and session-relative windows properly (D-012).

## Who it's for

- The project owner — the only user. A research tool, not a product.
- (Secondary, aspirational) A future realtime variant of the same tool, still
  for the owner's use.

## Success criteria

- The morning gap-recovery hypothesis can be tested end-to-end — data → signal
  → summary statistics — from a single reproducible script, selecting tickers
  from the stored data itself. Eligibility requires only the declared
  contiguous lookback through each decision timestamp; outcome availability
  cannot retroactively remove a selected event (D-026). The broader dataset
  still includes delisted tickers so exclusions remain measurable rather than
  silently current-only (D-010, D-011).
- For any seeded year, the universe and its full EOD history can be queried in
  interactive time (seconds, not minutes) on the Linux server.
- A nightly cron `market-data update` keeps EOD data current to the most recent
  trading day without manual intervention, and failures are visible rather than
  silent.
- Backfills interrupted at any point converge to an identical dataset when
  rerun (coverage intervals + merge-upsert writes).
- Intraday coverage is honest about its limits: Tiingo's IEX feed begins
  2016-12-12, direct hourly bars omit 09:30–09:59, and volume is IEX-only —
  the measured depth, semantics, and adequacy for the gap study are recorded,
  not assumed (D-012).

## Non-goals

- **Live or automated trading.** No order execution, no broker connectivity —
  removes a whole class of risk and regulatory surface (D-007).
- **Asset classes beyond US stocks and ETFs.** No options, futures, or crypto;
  keeps one data source and one storage schema (D-008).
- **Multi-user service.** No accounts, no SLA; a web UI, if built, is a
  convenience for the owner, not a product.
- **Tick-level data.** Five-minute bars are the finest resolution required by
  the stated opening-window study; tick/1-minute storage costs are not
  justified by any current goal.
- **Being a general data-vendor abstraction.** The code may stay
  Tiingo-shaped; a second source would be a recorded decision, not a day-one
  abstraction.
