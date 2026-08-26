# Instrument identity spike

Measured 2026-08-26 for OQ-8. This spike asks whether ticker strings can
safely identify the securities in historical EOD and IEX backfills, what
stable identity Tiingo exposes, and what the warehouse should key on.

## Outcome

No Tiingo surface available to this Power-tier account is both complete and
consistent enough to be the warehouse primary key. D-014 adopts an internal
stable `instrument_id` with date-ranged symbol aliases. Tiingo permaTickers
are useful vendor identifiers when discovered, but must be validated per
dataset. Every response is checked against its request segment and resolved
instrument envelope; unresolved segments fail closed even when the ticker
appears unique.

This answers OQ-8. No Tiingo-backed production ingestion may write the v1
ticker-keyed warehouse after the decision. Following the M1 migration,
validated request segments may proceed; ingestion may not silently skip,
merge, or mark coverage for unresolved segments.

## Public archive measurement

The input was Tiingo's public `supported_tickers.zip`, whose embedded CSV was
dated 2026-08-26 06:05. Rows were filtered exactly as
`seed_candidates_from_tiingo`: NYSE, NASDAQ, NYSE Arca, AMEX, or BATS; Stock
or ETF; USD; non-empty start date. Tickers were normalized to uppercase.

| Measure | Result |
| --- | ---: |
| Archive rows, all markets/types | 108,444 |
| Filtered US stock/ETF rows | 24,074 |
| Distinct filtered ticker strings | 23,042 |
| Ticker strings with multiple filtered records | 993 |
| Records belonging to those reused strings | 2,025 |
| Maximum records for one ticker string | 6 |
| Reused strings with overlapping record ranges | 462 |
| Distinct seed ticker strings | 5,403 |
| Reused seed ticker strings | 282 |
| Records belonging to reused seed strings | 577 |
| Reused seed strings with multiple records intersecting IEX history (2016-12-12 onward) | 229 |
| Their IEX-relevant records | 469 |

The archive columns are only `ticker`, `exchange`, `assetType`,
`priceCurrency`, `startDate`, and `endDate`. It has no permanent identifier or
security name. Date ranges help but do not uniquely solve identity: 462 reused
strings have overlapping record ranges. For example, seed ticker AAC has one
NYSE stock record dated 2014-10-02–2021-04-19 and another dated
2021-03-25–2023-11-06.

The seed CSV adds a second warning. Resolving every `(year, ticker)` row solely
by archive-range overlap yielded 25,148 single matches, 121 double matches,
and 2,380 zero matches. The zero-match group includes historical aliases such
as 2011 GOOG and RIMM. The supported archive is therefore neither a complete
historical alias master nor sufficient by itself to attach every seed row to
one security.

## Authenticated endpoint probes

Requests used the project's `.env` credential without logging the token. The
probes were deliberately small and summarized dates/counts rather than saving
vendor data in the repository.

### EOD

`ACOM` is an explicit reuse case in the public archive:

| Listing | Archive range | Search permaTicker |
| --- | --- | --- |
| Ancestry.com, NASDAQ stock | 2009-11-05–2013-01-14 | `US000000008464` |
| Harbor Active Commodity ETF, NYSE Arca | 2026-06-17–2026-08-25 | `US000000142176` |

- Bare `ACOM` metadata and prices selected only the ETF (48 rows through
  2026-08-25 in the measured request), even when the request began in 2009.
- EOD prices queried by the old permaTicker returned 802 rows from 2009-11-05
  through 2013-01-14.
- EOD prices queried by the new permaTicker returned only the ETF history.
- Bare `ALTR` returned Altair Engineering from 2017-11-01 through its 2025
  acquisition, not the older Altera security that the archive also lists.

Known permanent IDs can therefore disambiguate EOD histories, while bare
symbols cannot.

### Identity discovery

Tiingo's search endpoint returned distinct permanent IDs for both exact ACOM
matches, but the official documentation calls search early beta and says not
to build production code on it. The same probes returned no exact ALTR result
and no historical US ADPT result, despite both having multiple archive rows.

The fundamentals metadata endpoint documents permaTicker as a primary key and
did contain multiple identities for some recycled stocks (AAC and ACI), but
Tiingo documents fundamentals as a 5,500+ equity add-on universe. It omitted
the historical ADPT and Altera identities and is not a complete stock/ETF
price security master. EOD metadata by permaTicker returned 404 even when EOD
prices by that identifier succeeded, so it cannot fill the alias fields.

### IEX

The old Ancestry.com ACOM permaTicker returned 80 hourly rows dated 2026-07-17
through 2026-08-05—more than thirteen years after that listing ended. The
current ETF permaTicker returned no IEX rows for the same range. This is a
hard validation failure, regardless of whether it reflects account
entitlement, stale vendor mapping, or endpoint behavior. A permaTicker proven
for EOD cannot be assumed safe for IEX.

Tiingo's changelog says EOD and IEX permanent-ID queries are available for
accounts with permaTickers enabled, but it does not promise identical mapping
or expose a complete price security master. The observed result requires
dataset-specific validation.

## Architecture implication

The M1 migration should establish:

1. `instruments`: an opaque stable internal id plus optional vendor IDs and
   descriptive metadata.
2. `symbol_aliases`: instrument id, ticker, exchange, asset type, effective
   date range, source snapshot/evidence, and resolution status.
3. Coverage and canonical Parquet paths/rows keyed by instrument id. Query
   views may resolve a ticker as-of the bar date, but never join histories on
   ticker alone.
4. Dataset-specific request keys and universal validation. Every returned date
   must fit both the request segment and resolved instrument envelope;
   endpoint metadata must agree where it exists. Bare-ticker requests are
   clamped to one non-overlapping validated alias interval; missing,
   overlapping, or out-of-envelope history stays unresolved rather than
   extending a nominal backfill range.
5. A resolution report that distinguishes safe, unresolved, and conflicting
   records. After migration, ingestion can make progress on safe segments
   without weakening the requirement to eventually include delisted/reused
   securities.

The migration design must also preserve `reconcile`: internal IDs belong in
canonical Parquet, so SQLite state can be rebuilt without re-resolving mutable
vendor symbols. Existing ticker-keyed files remain quarantined until their
complete date ranges resolve to one instrument; crossing multiple envelopes
or an evidence gap is a reported conflict, not an automatic migration.

## Sources

- [Tiingo EOD documentation](https://www.tiingo.com/documentation/end-of-day):
  supported archive purpose and bare-ticker metadata fields.
- [Tiingo search documentation](https://www.tiingo.com/documentation/utilities/search):
  permaTicker in results and the early-beta warning.
- [Tiingo fundamentals documentation](https://www.tiingo.com/documentation/fundamentals):
  permanent-ID definition and the smaller add-on coverage universe.
- [Tiingo API changelog](https://www.tiingo.com/documentation/general/changelog):
  EOD/IEX permaTicker support is conditional on account enablement.
