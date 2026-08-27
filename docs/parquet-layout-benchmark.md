# Parquet layout benchmark

Measured 2026-08-27 to choose the D-014 instrument-keyed layout before the M1
migration makes that choice expensive to change.

## Question and candidates

The current v1 layout creates one EOD file per ticker and one intraday file per
ticker-year. At the 5,403-symbol seed size and measured IEX history depth, the
instrument-keyed equivalent would approach 54,000 files per intraday
frequency. That is attractive for isolated writes but makes every
cross-sectional query discover and inspect thousands of small files.

The benchmark compared two layouts over identical canonical intraday rows:

- **per instrument/year:** one file per instrument-year, matching the current
  granularity; and
- **year/hash bucket:** a fixed set of bucket files per year, with every
  instrument assigned to one stable bucket.

The candidate run used 64 buckets for 1,000 instruments, or 15.6 instruments
per bucket. D-019 adopts 256 buckets for the 5,403-symbol seed scale, or 21.1
seed instruments per bucket. Those densities are close enough for the
measured read/write amplification to inform the production choice while
keeping the benchmark compact. Synthetic ids were assigned round-robin to
make every candidate bucket's population deterministic; production uses the
stable SHA-256 mapping, whose physical tradeoff depends on the resulting
population rather than the hash implementation.

## Method

`tools/benchmark_parquet_layout.py` generated two years of deterministic
five-minute-shaped data: 1,000 instruments, 19,734 rows per instrument-year,
and 39,468,000 rows per layout. Rows use the target `instrument_id`, UTC
timestamp, OHLCV schema and 78 bars per synthetic session. The benchmark uses
synthetic values because production ingestion remains paused for the identity
migration; it evaluates physical layout, not vendor semantics or a strategy.

DuckDB 1.5.5 wrote zstd Parquet with 122,880-row groups. Query results are the
median of five measured runs with a warm-up, a fresh DuckDB connection per
query, four threads, and alternating layout order. OS caches were warm. Four
query shapes covered a one-session cross section, a 20-session grouped event
shape, a full-history aggregate, and one instrument's full history.

The ingestion measurement staged an already-validated five-day overlap in a
DuckDB temporary table, then timed merge-deduplication, zstd write, and atomic
rename. It therefore measures canonical publication, not vendor transfer or
response validation. The single-instrument case shows worst-case bucket write
amplification. The batch case updates all 64 instruments in four complete
buckets, matching the scheduler's breadth-first cross-sectional direction and
measuring each bucket rewrite once.

Run on the project Linux server (24 logical CPUs, NVMe storage, Python 3.12.3):

```console
.venv/bin/uv run --isolated --locked --extra dev \
  python tools/benchmark_parquet_layout.py
```

The generated layouts occupied a temporary directory outside the repository.
Setup took 43.9 seconds and was excluded from all results.

## Results

| Physical result | Per instrument/year | Year/hash bucket | Change |
| --- | ---: | ---: | ---: |
| Parquet files | 2,000 | 128 | -93.6% |
| Bytes | 848,447,956 | 724,760,818 | -14.6% |

| Warm query, median | Per instrument/year | Year/hash bucket | Bucket speedup |
| --- | ---: | ---: | ---: |
| One-session cross section | 0.1388 s | 0.0360 s | 3.86x |
| 20-session event shape | 0.1864 s | 0.0884 s | 2.11x |
| Full-history aggregate | 0.2394 s | 0.2090 s | 1.15x |
| One-instrument history through the common glob | 0.0287 s | 0.0094 s | 3.07x |

| Atomic overlap publication, median | Per instrument/year | Year/hash bucket | Result |
| --- | ---: | ---: | ---: |
| One instrument | 0.0114 s | 0.0698 s | bucket 6.14x slower |
| 64 instruments / four complete buckets | 0.6643 s | 0.2873 s | bucket 2.31x faster |

The compact layout wins the cross-sectional workload and full-universe-style
publication, while the expected tradeoff appears clearly for isolated writes.
At target density a bucket-year should contain roughly 21 complete seed
instrument histories, so a single failed/retried response remains a bounded
rewrite rather than a warehouse-scale compaction.

These are directional server measurements, not general DuckDB claims. They do
not simulate cold page cache, the final all-listed-instrument count, real
price compression ratios, network time, or concurrent writers (which remain
out of scope). Directory order differs from v1 but file granularity and row
contents are the compared variables. The benchmark also does not credit a
per-instrument direct-path query because research must use the common DuckDB
view; the bar store may still exploit the known bucket for point reads.

## Outcome

Use stable 256-way hash buckets for canonical instrument-keyed bars (D-019).
EOD uses one file per non-empty bucket; intraday uses one file per
frequency/year/bucket. This removes the small-file penalty before data is
migrated and aligns normal update/backfill publication with D-020's
cross-sectional request sweeps.

The ingestion coordinator stages and validates responses independently, then
groups publishable frames by dataset/year/bucket. A failed response is omitted
from its batch and does not block validated peers. A single response can still
publish by rewriting only its bucket, accepting the measured bounded latency.
Atomic rename remains the file commit point, and SQLite coverage advances only
after that commit. EOD corporate-action refreshes replace one instrument's
complete slice within an atomic bucket-file rewrite so adjustment vintages
cannot mix.

The checked-in benchmark remains a calibration tool, not a performance test in
`make check`. Re-run it before changing the bucket count, partition key,
Parquet row-group settings, or ingestion batching model.
