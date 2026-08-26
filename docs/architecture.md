# Architecture

> **Status: skeleton.** The first full draft is an M0 exit criterion; what is
> written here now is the load-bearing shape already settled (and largely
> built), so drafting can build on it rather than re-derive it.

## Fixed points (from decisions)

- **Storage (D-003, D-014):** Parquet is canonical bar storage. The v1 layout
  that M1 migrates from is one file per ticker for EOD
  (`data/eod/{TICKER}.parquet`) and one per ticker-year for intraday
  (`data/intraday/{freq}/{TICKER}/{year}.parquet`), zstd-compressed, with a
  `ticker` column in every file. The D-014 target keys paths and bar rows by an
  opaque stable `instrument_id`; symbol, exchange, and effective dates become
  aliases in SQLite. Coverage is keyed by instrument rather than symbol.
  DuckDB remains the query engine over Parquet globs with SQLite attached
  read-only; no database server.
- **Ingestion (D-003, D-009, D-014):** coverage is tracked as a per-(instrument,
  dataset) *interval* [first, last] — backfills fetch missing leading as well
  as trailing history, and coverage is rebuildable from the Parquet files
  (`market-data reconcile`). Parquet writes are atomic merge-upserts keyed on
  date/timestamp — idempotent and resumable by construction. Nightly updates
  refetch a rolling overlap window to absorb corrections/restated
  adjustments; an empty response only marks a range covered once it is old
  enough to be past publication lag; a newly observed split/dividend
  triggers a full-history refresh so one file never mixes adjustment
  vintages (see D-009 for parameters). Vendor request identifiers are stored
  and validated per dataset: a permaTicker proven for EOD is not assumed safe
  for IEX. Every response is checked against its requested segment and the
  resolved instrument envelope; unresolved or conflicting segments fail
  closed regardless of whether the ticker appears unique.
- **Source (D-002):** Tiingo only. EOD comes from the daily endpoint (with
  split/dividend-adjusted columns); intraday from the IEX endpoint
  (unadjusted, bounded history). Token in `.env`.
- **Universes (D-004, D-010, D-014):** per-year membership ranked by dollar volume,
  kept as the record of how the dataset was seeded and as an ingestion
  scope. Seed ticker strings are resolved against date-ranged aliases for the
  seed year; ambiguity is an error, never an implicit choice of the current
  listing. The research layer selects instruments from stored data directly —
  survivorship-bias protection comes from backfilling all instruments including
  delisted ones (D-011), not from membership joins.
- **Interface (D-005):** `market-data` CLI for operations; `marketdata` Python
  library for research. Any web UI or realtime layer sits on top of the same
  library and archive.

## Expected shape (to be validated in the M0 draft)

Where a bullet below leans on a `proposed` features.md row, it is a design
assumption to confirm during feature triage, not settled scope.

- A **research layer** using D-015's project-native vectorized event engine:
  DuckDB scans/windowing over Parquet, polars strategy transformations, and
  tidy `instrument_id`-keyed observation frames. It loads bars through
  `query.py`, selects instruments and screens from stored data (D-010,
  D-014), and produces per-run artifacts. A portfolio/order simulator is
  deferred until a confirmed study requires stateful execution semantics.
- **Results persistence** (OQ-6): probably run manifests + metric tables under
  `data/results/`, queryable through the same DuckDB surface.
- **Hourly bars** land in the existing intraday store as `freq="1hour"` and
  retain Tiingo's fixed clock-hour semantics (10:00–15:00; the opening half
  hour is absent). Opening-window or session-relative bars are derived from
  `freq="5min"` after exchange-calendar filtering (D-012).
- **Operations**: after the D-014 M1 migration, a nightly cron running
  `market-data update`; failure visibility mechanism TBD (exit codes + mail,
  or a status file the owner checks). The budget scheduler prioritizes current
  collection, hard-caps
  historical 5-minute transfer at 30 GB per billing month, reserves the other
  10 GB for current/ongoing work, and fills global date bands from newest to
  oldest (D-013).
- A future **web UI** (proposed) would be a thin read-only FastAPI app over
  the library; a future **realtime layer** (proposed) would add a hot store
  while the Parquet archive stays canonical (D-003).

## Open architecture questions

The numbered open questions live in [features.md](features.md) (OQ-1..OQ-8).
No architecture-blocking numbered question remains: OQ-1 was answered by the
backtest-engine spike and D-015; OQ-8 by the instrument-identity spike and
D-014; OQ-2/OQ-3 by the intraday spike and D-012; OQ-5 was dissolved and
OQ-6 answered at the 2026-08-26 triage (results: SQLite metadata + Parquet
outputs).
