# Intraday depth and semantics spike

Measured 2026-08-26 against Tiingo's historical IEX endpoint. This answers
features.md OQ-2 and OQ-3; D-012 records the resulting policy.

## Method

Fetched AAPL (large-cap stock), CROX (mid-cap stock), and SPY (ETF) at
`resampleFreq=1hour` and `resampleFreq=5min`, explicitly requesting
`format=csv` and `open,high,low,close,volume`. Requests used identity encoding
so byte counts are the actual CSV response bytes, not decompressed sizes.
An authenticated follow-up on 2026-08-27 offered identity, gzip, Brotli, and
Zstandard encodings to representative IEX and EOD CSV requests; all variants
were returned unencoded with identical raw sizes. Identity therefore did not
inflate this spike's transfer measurement, although Tiingo does not document
whether its monthly ledger would charge encoded or decoded bytes if transport
compression is enabled later (RE-006).

A single long request is not a valid depth probe: the endpoint silently keeps
only the newest 10,000 rows (RE-002). The final measurement therefore used
30 bounded 120-day requests per ticker/frequency. Each measurement request
extended its `endDate` by seven calendar days, which crossed at least one
later trading day for every window in this measured range, and discarded the
lookahead rows. A Friday probe confirmed that weekend-only lookahead does not
restore missing final bars (RE-004). The transfer totals include this
conservative safety overhead; production ingestion should query through the
next exchange-calendar session.

Queries ending 2016-12-11 returned no rows for all six ticker/frequency pairs;
queries beginning 2016-12-12 returned data. The measured end was the most
recent completed day, 2026-08-25.

## Results

| Representative | Frequency | Observed range | Unique rows | CSV transfer |
| --- | --- | --- | ---: | ---: |
| AAPL | 1 hour | 2016-12-12 – 2026-08-25 | 15,180 | 0.968 MiB |
| CROX | 1 hour | 2016-12-12 – 2026-08-25 | 15,180 | 0.896 MiB |
| SPY | 1 hour | 2016-12-12 – 2026-08-25 | 15,180 | 0.976 MiB |
| AAPL | 5 minute | 2016-12-12 – 2026-08-25 | 197,340 | 12.375 MiB |
| CROX | 5 minute | 2016-12-12 – 2026-08-25 | 197,331 | 11.423 MiB |
| SPY | 5 minute | 2016-12-12 – 2026-08-25 | 197,340 | 12.468 MiB |

Mean transfer was 0.946 MiB/ticker for hourly and 12.089 MiB/ticker for
5-minute data. Straight multiplication by the 5,403 seed symbols projects
about **5.4 GB hourly** and **68.5 GB 5-minute**. This is an upper-bound-style
calibration: newly listed and delisted instruments have shorter histories,
while the safe request overlap adds a little transfer. It validates D-011's
40–75 GB estimate for seed-list 5-minute history and implies roughly 1.7
Power-tier bandwidth months for that phase before EOD and operational
overhead at the full vendor cap. D-013's 30 GB/month historical hard cap makes
the operational minimum three billing windows.

The nearly 9 years 9 months of available history is enough temporal depth for
the first gap-recovery study (roughly 2,400 real sessions after calendar
filtering), though it cannot extend the intraday study back to the 2011 start
of the seed record.

## Bar semantics

- Timestamps are exchange-local wall-clock timestamps with an explicit
  `-05:00` or `-04:00` offset, so DST is represented in the values.
- Five-minute regular-session bars are start-labelled from 09:30 through
  15:55 on a normal session.
- Direct hourly bars are start-labelled fixed clock-hour bins at 10:00,
  11:00, …, 15:00. On a sampled normal session for all three tickers, every
  hourly OHLCV row exactly matched the twelve 5-minute rows with the same
  clock hour.
- The six 09:30–09:55 five-minute bars have **no direct hourly counterpart**.
  Direct `1hour` data therefore cannot measure recovery during the first 30
  minutes after the open and must not be described as a session-aligned bar
  beginning at 09:30.
- Multi-day responses can contain force-filled weekday grids on market
  holidays and after scheduled early closes even with `forceFill=false`.
  Single-day responses are not safe session definitions either: the sampled
  2025-11-28 early close included twelve 5-minute rows after the scheduled
  13:00 close (RE-004). An exchange calendar, not row presence or zero volume,
  defines valid sessions and bar cutoffs.
- Historical `volume` is explicitly IEX-only. Across the bulk responses,
  11.6–11.9% of hourly rows and 11.8–18.9% of 5-minute rows had zero volume;
  many are synthetic non-session rows, but sparse IEX trading remains an
  instrument-dependent concern after calendar filtering.

## Outcome

Keep Tiingo's direct hourly data as a cheap, separately named vendor dataset
for checkpoints from 10:00 onward. The first coarse gap study may combine the
EOD opening price with those later checkpoints, but any opening-window or
session-relative hourly analysis must resample stored 5-minute bars using the
exchange calendar. Do not use IEX-only intraday volume for liquidity screens
or absolute cross-sectional volume thresholds; use composite EOD volume for
liquidity and treat intraday volume as descriptive unless a study validates a
narrower use.

Tiingo now documents a beta consolidated-equity intraday endpoint, but this
spike did not substitute that endpoint for the settled IEX source. If a future
study requires consolidated intraday volume, evaluating and adopting it is a
separate source/semantics decision.

Sources: [Tiingo IEX documentation](https://www.tiingo.com/documentation/iex),
[Tiingo general API formats](https://www.tiingo.com/documentation/general),
[Tiingo IEX product history](https://www.tiingo.com/products/iex-api), and
[NYSE hours and holiday calendar](https://www.nyse.com/markets/hours-calendars).
