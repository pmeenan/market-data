# Rough edges — findings log

Tiingo, DuckDB, polars, and platform bugs, quirks, surprising limits,
performance cliffs, and missing capabilities encountered while building
market-data. Log the ones that burned real debugging time and will bite
again — this is a save-future-you log, not a compliance artifact.

**Before adding:** grep for the API/library involved to avoid duplicates.
**Before debugging weirdness:** check here first — it may be known.

A good entry says what environment it happened in and what was observed vs.
expected; include a reproduction when it's cheap to capture.

Format:

```
## RE-NNN: Title  (YYYY-MM-DD, status: open | fixed-upstream | worked-around | wontfix)
Environment / Repro or measurement / Observed / Expected / Impact / Links
```

Newest first. RE-numbers are never reused.

---

## RE-010: An IEX 404 can pin the final breadth-first sweep  (2026-09-01, status: worked-around)

**Environment:** D-027 program scheduler, phase-3 five-minute history, Tiingo
IEX CSV endpoint.

**Repro/measurement:** GBF had validated five-minute identity evidence and
contiguous stored coverage from 2018-11-21 through 2026-08-27. Its next older
request (`2018-05-29..2018-11-20`) returned HTTP 404 with a four-byte response.
The request ledger accumulated 41 such responses. Once every peer was covered
or terminal, ten timer runs in the final observed hour each attempted only GBF
and advanced nothing.

**Observed:** `TiingoClient` correctly did not retry a 404 within one logical
request, but raised the generic exception also used for transient transport
failures. Validated ingestion therefore left the range active, and every new
sweep retried it indefinitely.

**Expected:** A definitive resource-absence response is fail-closed and
terminal for that immutable historical range: retain no coverage claim, expose
the exact exclusion, and require explicit operator intent to retry it.

**Impact/workaround:** D-029 gives HTTP 404 a distinct type, records IEX probe
404s as rejected evidence, and maps validated history 404s to durable terminal
blockers. The live GBF range became terminal on the next request and the
program completed with exclusions. Generic transport and payload failures
remain retryable.

---

## RE-009: A recent empty EOD range can poll indefinitely at a phase gate  (2026-08-29, status: open)

**Environment:** D-027 program scheduler, EOD phase-2 terminal sweep, Tiingo
CSV, five-day empty-response publication lag.

**Repro/measurement:** The sole remaining safe phase-2 range was the
archive-bounded `DSPC@2021-05-19` episode ending 2026-08-26. On 2026-08-29,
21 complete requests produced no coverage; 20 responses were the normal
94-byte header-only CSV. Each completed sweep retried that same range, recorded
`coverage did not advance`, and left the job active.

**Observed:** Once all peers were terminal, every program invocation spent
about four minutes scanning the 23,759-target cohort, made one DSPC request,
then waited for the five-minute timer cadence. API usage appeared stationary
and phase 3 could not start, even though the range would become eligible for
verified-empty coverage when the publication lag expired.

**Expected:** A no-progress result whose only blocker is the deterministic
publication-lag date should persist a not-before checkpoint, or otherwise stop
without polling the full cohort and phase gate on every timer invocation.

**Impact/workaround:** The operator may accept the exact range as a terminal
exclusion after reviewing its repeated complete responses; this must retain no
coverage claim and a durable reason. The 2026-08-29 live workaround did so for
DSPC after taking an integrity-checked SQLite backup, allowing phase 2 to become
terminal and phase-3 identity preparation to begin. A general scheduler fix
remains open.

---

## RE-008: Empty IEX CSV omits the implicit date column  (2026-08-28, status: worked-around)

**Environment:** Authenticated Tiingo historical IEX REST endpoint with
`format=csv` and explicit OHLCV columns; measured during the phase-1 hourly
identity bootstrap.

**Repro/measurement:** All 376 initially failed probes returned a successful,
header-only CSV containing the requested `open,high,low,close,volume` columns
but no `date` column. Repeating the same bounded requests after recognizing
that shape produced 376 empty results and no transport failures. Populated IEX
responses include `date` even though the request's explicit `columns` parameter
does not name it.

**Observed:** Tiingo's implicit timestamp column is absent from the header when
the IEX result has zero rows.

**Expected:** An empty CSV retains the same schema as a populated response, as
the EOD endpoint does.

**Impact:** The CSV parser accepts a missing `date` only when the body has a
valid header, every other required field is present, and there are zero data
rows. A populated response missing `date`, or any other missing field, still
fails closed. This distinction lets an identity probe persist honest empty
evidence instead of misclassifying it as a retryable transport/schema failure.

---

## RE-007: Tiingo can conflate unrelated listings inside one archive record  (2026-08-28, status: worked-around)

**Environment:** Then-current 2006-08-28 through 2026-08-27 seed EOD warehouse,
Tiingo supported-tickers archive and EOD CSV, measured 2026-08-28.

**Repro/measurement:** A full XNYS-session continuity scan over 4,454 stored
histories found 60 broad source identities with at least one 252-session
missing or internal zero-volume gap and at least 20 observations on two sides.
They partition into 121 substantive episodes. Examples included old delisted
companies followed years later by unrelated recent listings even though the
archive/bootstrap had supplied one broad record. The same scan found 1,908
invalid OHLC, internal zero-bridge, or too-sparse rows outside publishable
episodes.

**Observed:** Supported-tickers record cardinality is not sufficient evidence
that a bare-symbol EOD history belongs to one security. Some conflated histories
also contain long calendar-contiguous zero-volume bridges rather than absent
rows.

**Expected:** One archive record and its date envelope describe one continuous
listing history, or the vendor exposes a stable security id for each underlying
listing.

**Impact:** D-023 adds a terminal, idempotent EOD episode audit. It partitions
only across conservative XNYS-session discontinuities, retains real tickers as
date-ranged aliases, quarantines suspicious bridges/short fragments/invalid
OHLC, and leaves evidence gaps or overlaps unresolved. Archive uniqueness must
not be treated as proof that the returned history is one security.

The following validated history pass exposed two more covered broad histories
(`MMV` and `RCM`), which the same idempotent rule split into four episodes. The
live total is therefore 62 repaired source symbols and 125 inferred episodes;
the original 1,908-row quarantine is unchanged.

---

## RE-006: Tiingo bar CSV is uncompressed and billing-byte semantics are undocumented  (2026-08-27, status: worked-around)

**Environment:** Authenticated Tiingo EOD and historical IEX REST endpoints,
CSV, Power tier; measured 2026-08-27 with Requests streaming raw bodies.

**Repro/measurement:** The same AAPL 5-minute response was requested with
`Accept-Encoding` set to identity, gzip, Brotli, Zstandard, and
`gzip, br, zstd`; every response had no `Content-Encoding` and the same
107,179-byte `Content-Length` and raw-body length. A ten-year AAPL EOD response
behaved identically at 353,190 bytes. The responses exposed no bandwidth or
usage headers. Tiingo's documentation names a monthly bandwidth limit but does
not state whether it counts encoded or decoded response bytes. A historical
empty EOD CSV range returned the normal 94-byte header row, not JSON `[]`.

**Observed:** Current bar endpoints do not use gzip, Brotli, or Zstandard even
when explicitly offered, so encoded and decoded body sizes are currently
identical. The future relationship between observable transport bytes and the
vendor's billing ledger is unspecified.

**Expected:** A bandwidth-limited bulk API either compresses repetitive CSV or
documents why not, and defines the byte counter clients must enforce.

**Impact:** Do not force identity encoding. Meter encoded bytes through the
HTTP raw-stream count so future automatic decompression cannot corrupt the
measurement. M2's initial hard budget used a 32-day rolling window and retains
a 64 MB reservation before each response; complete responses settle to
observed encoded bytes, while partial/unknown transfers retain the reservation.
Keep exact JSON `[]`, header-only CSV, empty bodies, and BOM-only bodies as
tested empty-result variants. See
Tiingo's [general API documentation](https://www.tiingo.com/documentation/general).

**Update (2026-09-01):** The linked general documentation explicitly states
that monthly bandwidth resets on the first at midnight EST. D-028 replaces the
32-day byte window with that billing-month boundary; the byte basis remains
undocumented, so the response reservation and conservative settlement rules
remain necessary.

## RE-005: Tiingo identity surfaces are incomplete and dataset-dependent  (2026-08-26, status: open)

**Environment:** Tiingo public supported-tickers archive plus authenticated
EOD, IEX, utilities/search, and fundamentals metadata endpoints; Power tier;
measured 2026-08-26.

**Repro/measurement:** The public archive has date-ranged duplicate symbol
records but no permaTicker. Bare `ACOM` EOD returned only the 2026 ETF, while
EOD requests using two permanent IDs correctly returned the 2009–2013
Ancestry.com history and the ETF history separately. Search returned both ACOM
IDs but missed historical US ADPT and returned no exact ALTR identity at all;
fundamentals metadata covered some recycled stocks but is not the complete
stock/ETF price master. An IEX request using Ancestry.com's old ACOM
permaTicker unexpectedly returned 80 rows dated in 2026, all after that
security's 2013 end date; the current ETF permaTicker returned no IEX rows.

**Observed:** A bare ticker selects one of several securities, stable-ID
discovery is incomplete, and a permaTicker validated for EOD can resolve
differently on IEX.

**Expected:** One complete vendor security master and one stable identifier
with consistent semantics across price datasets.

**Impact:** Follow D-014: use an internal instrument id and validate every
response against its dataset, request segment, and expected identity envelope;
fail closed on any unresolved segment, even if its ticker appears unique.
Never infer IEX identity safety from a successful EOD probe.
See [instrument-identity-spike.md](instrument-identity-spike.md) and Tiingo's
[search documentation](https://www.tiingo.com/documentation/utilities/search)
and [changelog](https://www.tiingo.com/documentation/general/changelog).

## RE-004: IEX resampling depends on request range  (2026-08-26, status: worked-around)

**Environment:** Tiingo historical IEX REST endpoint, CSV, Power tier,
measured 2026-08-26.

**Repro/measurement:** A 120-day AAPL 5-minute request spanning 2025-09-01,
2025-11-27, 2025-11-28, and 2025-12-25 returned a complete 78-row weekday
grid for every date, including zero-volume holiday rows and rows after the
scheduled 13:00 early close; `forceFill=false` and `forceFill=true` produced
the same result. Single-day holiday requests returned no rows, and a
single-day 2025-11-28 request with `forceFill=false` returned 54 rows through
13:55 — twelve bars past the scheduled 13:00 close, so it was not a clean
session series either. Separately, an AAPL request ending 2017-04-10 returned
only five hourly rows through 14:00 / 65 five-minute rows through 14:50;
extending `endDate` to 2017-04-11 returned the missing 15:00 hour / remaining
13 five-minute rows. A Friday 2017-04-07 probe remained truncated when
`endDate` was Saturday or Sunday and became complete only when `endDate`
reached Monday 2017-04-10.

**Observed:** The same date's result changes with the surrounding request
range, and explicit `forceFill=false` does not prevent long-range synthetic
rows.

**Expected:** A date range only partitions a stable underlying series, and
`forceFill=false` excludes synthetic non-trading intervals.

**Impact:** Never infer sessions from row presence or zero volume. The M2
planner now fetches every bounded chunk through the next XNYS session,
request-validates and discards D-021's context rows, and
merge-upserts/deduplicates only target-envelope rows; session-labelled loaders
separately filter raw bars through the same calendar. [NYSE calendar](https://www.nyse.com/markets/hours-calendars),
[Tiingo IEX docs](https://www.tiingo.com/documentation/iex).

## RE-003: Direct IEX hourly bars omit the opening half-hour  (2026-08-26, status: wontfix)

**Environment:** Tiingo historical IEX REST endpoint, AAPL/CROX/SPY,
`resampleFreq=1hour` and `5min`, measured 2026-08-26.

**Repro/measurement:** On a normal session, hourly timestamps were
10:00–15:00 Eastern. Each hourly OHLCV row exactly matched the twelve
five-minute rows in that wall-clock hour. The six 09:30–09:55 bars had no
hourly counterpart.

**Observed:** `1hour` means whole clock hours, not one-hour bins anchored at
the 09:30 exchange open; the first half-hour is dropped.

**Expected:** A naïve consumer could reasonably interpret “hourly bars” as
covering the whole regular session from the open.

**Impact:** Keep the vendor hourly dataset for 10:00-and-later checkpoints,
but derive opening-window/session-relative bins from 5-minute data (D-012).

## RE-002: Historical IEX responses silently cap at 10,000 rows  (2026-08-26, status: worked-around)

**Environment:** Tiingo historical IEX REST endpoint, CSV, Power tier,
measured 2026-08-26.

**Repro/measurement:** Requests for 2010-01-01 through 2026-08-25 returned
exactly 10,000 rows with HTTP 200 and no truncation marker. For AAPL, hourly
appeared to start 2020-04-06 while 5-minute appeared to start 2026-02-26.
Bounded requests proved both actually begin 2016-12-12.

**Observed:** The endpoint returns the newest 10,000 rows and silently drops
the older prefix.

**Expected:** Either the full requested range or an explicit pagination/error
signal.

**Impact:** Every historical IEX fetch must use bounded chunks and reject a
response with 10,000 rows as potentially truncated. A range probe using one
large request will report a false history depth. The M2 planner now derives
frequency-specific weekday-grid bounds below 10,000 (including its
next-session lookahead), records the bound on each request unit, and rejects
both cap-sized responses and responses that exceed the planned envelope.

## RE-001: DuckDB→polars conversion requires pyarrow  (2026-08-26, status: worked-around)

**Environment:** Python 3.12, duckdb 1.x, polars 1.x, Linux.

**Observed:** `con.pl()` raised `ModuleNotFoundError: No module named
'pyarrow'` even though polars itself no longer depends on pyarrow — DuckDB's
polars bridge goes through Arrow.

**Expected:** duckdb + polars installed ⇒ `.pl()` works.

**Impact:** `pyarrow` is a required dependency in pyproject.toml solely for
this bridge; don't remove it when pruning deps.
