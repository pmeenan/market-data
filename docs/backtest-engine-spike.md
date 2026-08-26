# Backtest engine spike

Measured 2026-08-26 to answer OQ-1: should the research layer use a small
DuckDB/polars engine or adopt an existing backtesting library?

## Acceptance case

The prototype implements the first study's essential path:

1. derive each instrument's prior close from EOD bars;
2. select sessions with an open at least 3% below the prior close and EOD
   volume of at least 1 million shares;
3. join the signal to 10:00, 11:00, and 12:00 price checkpoints;
4. emit one row per event/checkpoint with gap return, open-to-checkpoint
   return, and fraction of the gap recovered; and
5. aggregate count, mean/median return, and mean recovery fraction by
   checkpoint.

This is an event study, not yet a portfolio simulation: each signal is an
independent observation and the first question is the conditional return
distribution. Fees, slippage, capital constraints, overlapping positions,
and order fills become meaningful only if a later study asks a portfolio or
execution question.

The benchmark used deterministic synthetic data because production ingestion
is paused for the D-014 identity migration and no representative warehouse is
available yet. Its shape is representative of the intended cross-sectional
workflow: 300 stable instrument ids over 1,000 sessions (300,000 EOD rows),
three checkpoints per session (900,000 intraday rows), and 40,686 selected
gap events. The prices deliberately contain a known recovery effect. This
tests implementation fit and relative overhead, not strategy validity or the
eventual full-universe Parquet layout.

## Prototypes

### DuckDB + polars

DuckDB scanned EOD Parquet and computed the partitioned prior-close window
and signal filter. Polars joined the resulting event rows to long-form
intraday checkpoints, derived the study metrics, and grouped the result.
The prototype stayed in the warehouse's native long-form,
`instrument_id`-keyed representation.

### vectorbt 1.1.0

vectorbt was chosen as the library candidate because OQ-1 names it and its
vectorized design is closer to this project than an event-driven trading
engine. The prototype used the documented
[`Portfolio.from_signals`](https://vectorbt.dev/api/portfolio/base/#vectorbt.portfolio.base.Portfolio.from_signals)
path: pandas reshaped each checkpoint into dense time-by-instrument price and
signal matrices, then three separate portfolios simulated buying the open and
selling at each checkpoint. A separate pandas join was still needed to
calculate the study-specific recovery fraction.

The current package is actively maintained and supports this project's
Python version: the
[`v1.1.0` release](https://github.com/polakowo/vectorbt/releases/tag/v1.1.0)
publishes wheels for Python 3.11 through 3.14. It is not admissible under
D-001, however. Its own
[`v1.1.0` license](https://github.com/polakowo/vectorbt/blob/v1.1.0/LICENSE.md)
is "Apache 2.0 with Commons Clause," which restricts selling the software and
is not a permissive Apache-2.0-compatible dependency. The wheel metadata also
omits a license expression/classifier.

A clean isolated `pip install vectorbt==1.1.0` selected Plotly 7.0.0 and then
failed on `import vectorbt`: vectorbt's built-in template referenced the
removed `scattermapbox` property. The package declares only
`plotly>=4.12.0` in its
[`v1.1.0` dependency metadata](https://github.com/polakowo/vectorbt/blob/v1.1.0/pyproject.toml),
so the resolver cannot avoid that incompatibility. Pinning `plotly<6` in the
throw-away environment allowed the spike to proceed. This is not the primary
reason to reject the package, but it is real dependency overhead.

## Results

Both implementations produced exactly 122,058 observation rows and identical
counts and summary values at every checkpoint:

| Checkpoint | Observations | Mean return | Median return | Mean gap recovered |
| --- | ---: | ---: | ---: | ---: |
| 10:00 | 40,686 | 1.9890% | 1.8041% | 33.9746% |
| 11:00 | 40,686 | 3.0400% | 2.6600% | 51.9394% |
| 12:00 | 40,686 | 3.8652% | 3.3316% | 66.0530% |

Timings are wall-clock measurements on the project server. "In-script" starts
after imports have completed and includes Parquet reads, transformations,
simulation, and aggregation. Peak RSS comes from `/usr/bin/time`; it includes
the Python runtime and imported libraries. End-to-end process wall time was
0.37 seconds versus 8.66 seconds on the first run and 0.37 versus 4.52 seconds
on a second fresh process that could reuse compiled caches.

| Implementation | Prototype lines | First in-script run | Warm in-script run | Peak RSS (first) |
| --- | ---: | ---: | ---: | ---: |
| DuckDB + polars | 51 | 0.220 s | 0.216 s | 361 MiB |
| vectorbt | 71 | 6.970 s | 2.843 s | 710 MiB |

The vectorbt wheel declares 17 direct runtime dependencies, while DuckDB and
polars are already project dependencies. More importantly, vectorbt's
portfolio model did not remove study code: the prototype was longer because
it had to build dense matrices and run a portfolio per exit horizon, then
leave the library to compute event-specific metrics. Its strengths—cash and
position accounting, orders, stops, and large parameter grids—do not answer
the first study's present question.

These figures are directional, not a general performance claim about
vectorbt. They cover one representative event-study shape, with one cold and
one warm process run, and the synthetic dataset is much smaller than the
eventual warehouse. The exact runtime is less important than the observed
data-shape and abstraction mismatch.

## Recommendation

Build a small project-native, vectorized research layer on the existing
DuckDB/polars stack (D-015). Strategy code should select events and return
tidy `instrument_id`-keyed observation frames; shared code should validate
parameters, calculate reusable evaluation metrics, and persist run metadata
plus Parquet observations. Keep SQL/polars operations lazy or streaming where
practical and do not introduce a Python row-by-row loop as the default engine.

Do not build a general order-matching engine speculatively. If a confirmed
study later needs portfolio state, simultaneous capital allocation, order
fill semantics, or path-dependent execution, add the smallest simulator that
meets that study and repeat the library evaluation against its acceptance
case. A permissively licensed library may be adopted then; vectorbt itself
would additionally require a license-policy change or a differently licensed
release.
