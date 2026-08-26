# Architecture

> **Status: skeleton.** The first full draft is an M0 exit criterion; what is
> written here now is the load-bearing shape already settled (and largely
> built), so drafting can build on it rather than re-derive it.

## Fixed points (from decisions)

- **Storage (D-003):** Parquet is canonical bar storage — one file per ticker
  for EOD (`data/eod/{TICKER}.parquet`), one per ticker-year for intraday
  (`data/intraday/{freq}/{TICKER}/{year}.parquet`), zstd-compressed, each file
  carrying a `ticker` column. SQLite (`data/meta.db`) holds relational state:
  ticker registry, per-year universes, coverage intervals. DuckDB is the query
  engine over Parquet globs with the SQLite DB attached read-only. No database
  server.
- **Ingestion (D-003, D-009):** coverage is tracked as a per-(ticker,
  dataset) *interval* [first, last] — backfills fetch missing leading as well
  as trailing history, and coverage is rebuildable from the Parquet files
  (`market-data reconcile`). Parquet writes are atomic merge-upserts keyed on
  date/timestamp — idempotent and resumable by construction. Nightly updates
  refetch a rolling overlap window to absorb corrections/restated
  adjustments; an empty response only marks a range covered once it is old
  enough to be past publication lag; a newly observed split/dividend
  triggers a full-history refresh so one file never mixes adjustment
  vintages (see D-009 for parameters).
- **Source (D-002):** Tiingo only. EOD comes from the daily endpoint (with
  split/dividend-adjusted columns); intraday from the IEX endpoint
  (unadjusted, bounded history). Token in `.env`.
- **Universes (D-004, D-010):** per-year membership ranked by dollar volume,
  kept as the record of how the dataset was seeded and as an ingestion
  scope. The research layer selects tickers from the stored data directly —
  survivorship-bias protection comes from backfilling all tickers including
  delisted ones (D-011), not from membership joins.
- **Interface (D-005):** `market-data` CLI for operations; `marketdata` Python
  library for research. Any web UI or realtime layer sits on top of the same
  library and archive.

## Expected shape (to be validated in the M0 draft)

Where a bullet below leans on a `proposed` features.md row, it is a design
assumption to confirm during feature triage, not settled scope.

- A **research layer** (engine per OQ-1) that loads bars through `query.py`,
  selects tickers and screens from the stored data (D-010), and produces
  per-run artifacts — likely a `backtest/` module with strategies as small
  Python classes/functions.
- **Results persistence** (OQ-6): probably run manifests + metric tables under
  `data/results/`, queryable through the same DuckDB surface.
- **Hourly bars** land in the existing intraday store as `freq="1hour"` and
  retain Tiingo's fixed clock-hour semantics (10:00–15:00; the opening half
  hour is absent). Opening-window or session-relative bars are derived from
  `freq="5min"` after exchange-calendar filtering (D-012).
- **Operations**: a nightly cron running `market-data update`; failure
  visibility mechanism TBD (exit codes + mail, or a status file the owner
  checks). The budget scheduler prioritizes current collection, hard-caps
  historical 5-minute transfer at 30 GB per billing month, reserves the other
  10 GB for current/ongoing work, and fills global date bands from newest to
  oldest (D-013).
- A future **web UI** (proposed) would be a thin read-only FastAPI app over
  the library; a future **realtime layer** (proposed) would add a hot store
  while the Parquet archive stays canonical (D-003).

## Open architecture questions

The numbered open questions live in [features.md](features.md) (OQ-1..OQ-8).
The architecture-blocking ones still open are OQ-1 (engine) and OQ-8
(instrument identity for reused ticker symbols — gates all D-011 historical
backfill phases). OQ-2/OQ-3 were answered by the intraday spike and D-012;
OQ-5 was dissolved and OQ-6 answered at the 2026-08-26 triage (results: SQLite
metadata + Parquet outputs).
