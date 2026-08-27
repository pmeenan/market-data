#!/usr/bin/env python3
"""Benchmark per-instrument versus year/bucket Parquet layouts.

The generated data has the intended canonical intraday schema and 78 bars per
session. It is deterministic and contains no vendor data. Setup time is not a
benchmark result; measured operations use already-written, OS-cached files.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import tempfile
import time
from pathlib import Path

import duckdb

BAR_COLUMNS = "instrument_id, ts, open, high, low, close, volume"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instruments", type=int, default=1_000)
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument("--rows-per-year", type=int, default=19_734)
    parser.add_argument("--buckets", type=int, default=64)
    parser.add_argument("--update-buckets", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="new directory for generated files (default: a temporary directory)",
    )
    return parser.parse_args()


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def rows_sql(year: int, rows: int, instrument_source: str) -> str:
    start = f"{year}-01-02 14:30:00+00"
    return f"""
        SELECT
            printf('inst_%06d', i) AS instrument_id,
            TIMESTAMPTZ '{start}'
                + (bar // 78) * INTERVAL '1 day'
                + (bar % 78) * INTERVAL '5 minutes' AS ts,
            50.0 + (hash(i, bar, 1) % 500000) / 10000.0 AS open,
            50.1 + (hash(i, bar, 2) % 500000) / 10000.0 AS high,
            49.9 + (hash(i, bar, 3) % 500000) / 10000.0 AS low,
            50.0 + (hash(i, bar, 4) % 500000) / 10000.0 AS close,
            CAST(100 + (hash(i, bar, 5) % 100000) AS BIGINT) AS volume
        FROM ({instrument_source}) instruments
        CROSS JOIN range({rows}) bars(bar)
    """


def copy_query(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({query}) TO '{sql_path(path)}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
    )


def generate_layouts(
    root: Path,
    instruments: int,
    years: list[int],
    rows: int,
    buckets: int,
    threads: int,
) -> tuple[Path, Path]:
    per_instrument = root / "per_instrument"
    bucketed = root / "bucketed"
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    for year in years:
        for instrument in range(instruments):
            query = rows_sql(year, rows, f"SELECT {instrument}::BIGINT AS i")
            copy_query(
                con,
                query,
                per_instrument / f"year={year}" / f"inst_{instrument:06d}.parquet",
            )
        for bucket in range(buckets):
            query = rows_sql(
                year,
                rows,
                f"SELECT * FROM range({bucket}, {instruments}, {buckets}) ids(i)",
            )
            copy_query(
                con,
                query,
                bucketed / f"year={year}" / f"bucket={bucket:03d}" / "bars.parquet",
            )
    con.close()
    return per_instrument, bucketed


def layout_stats(root: Path) -> dict[str, int]:
    files = list(root.rglob("*.parquet"))
    return {
        "files": len(files),
        "bytes": sum(path.stat().st_size for path in files),
    }


def validate_layouts(layouts: dict[str, Path], expected_rows: int) -> dict[str, int]:
    signatures: dict[str, tuple[int, int]] = {}
    for name, root in layouts.items():
        con = duckdb.connect()
        count, volume = con.execute(
            f"""
            SELECT count(*), CAST(sum(volume) AS HUGEINT)
            FROM read_parquet('{sql_path(root / "**" / "*.parquet")}')
            """
        ).fetchone()
        con.close()
        signatures[name] = (count, volume)
    if len(set(signatures.values())) != 1:
        raise RuntimeError(f"layout signatures differ: {signatures}")
    count, volume = next(iter(signatures.values()))
    if count != expected_rows:
        raise RuntimeError(f"expected {expected_rows} rows, found {count}")
    return {"rows": count, "volume_sum": volume}


def benchmark_queries(
    layouts: dict[str, Path],
    years: list[int],
    rows_per_year: int,
    repeats: int,
    threads: int,
) -> dict[str, dict[str, float]]:
    last_year = years[-1]
    middle_session = rows_per_year // 78 // 2
    day_start = f"{last_year}-01-02 14:30:00+00"
    queries = {
        "one_session_cross_section": f"""
            SELECT count(*), sum(close), sum(volume)
            FROM read_parquet('{{glob}}')
            WHERE ts >= TIMESTAMPTZ '{day_start}'
                + {middle_session} * INTERVAL '1 day'
              AND ts < TIMESTAMPTZ '{day_start}'
                + {middle_session + 1} * INTERVAL '1 day'
        """,
        "twenty_session_event_shape": f"""
            SELECT count(*), sum(session_return)
            FROM (
                SELECT instrument_id, CAST(ts AS DATE) AS session_date,
                    arg_max(close, ts) / arg_min(open, ts) - 1 AS session_return
                FROM read_parquet('{{glob}}')
                WHERE ts >= TIMESTAMPTZ '{day_start}'
                    + {middle_session} * INTERVAL '1 day'
                  AND ts < TIMESTAMPTZ '{day_start}'
                    + {middle_session + 20} * INTERVAL '1 day'
                GROUP BY instrument_id, session_date
            )
        """,
        "full_history_scan": """
            SELECT count(*), sum(close - open), sum(volume)
            FROM read_parquet('{glob}')
        """,
        "one_instrument_history": """
            SELECT count(*), sum(close), sum(volume)
            FROM read_parquet('{glob}')
            WHERE instrument_id = 'inst_000000'
        """,
    }
    samples: dict[str, dict[str, list[float]]] = {
        name: {query_name: [] for query_name in queries} for name in layouts
    }
    # Rotate layout order so cache warmth does not always favor one candidate.
    layout_items = list(layouts.items())
    for repeat in range(repeats + 1):
        ordered = layout_items if repeat % 2 == 0 else list(reversed(layout_items))
        for layout_name, root in ordered:
            glob = sql_path(root / "**" / "*.parquet")
            for query_name, template in queries.items():
                con = duckdb.connect()
                con.execute(f"SET threads = {threads}")
                started = time.perf_counter()
                con.execute(template.format(glob=glob)).fetchone()
                elapsed = time.perf_counter() - started
                con.close()
                if repeat:
                    samples[layout_name][query_name].append(elapsed)
    return {
        layout_name: {
            query_name: statistics.median(values)
            for query_name, values in query_samples.items()
        }
        for layout_name, query_samples in samples.items()
    }


def prepare_incoming(
    con: duckdb.DuckDBPyConnection, path: Path, instrument_ids: list[str]
) -> None:
    ids = ", ".join(f"'{instrument_id}'" for instrument_id in instrument_ids)
    con.execute("DROP TABLE IF EXISTS incoming")
    con.execute(
        f"""
        CREATE TEMP TABLE incoming AS
        SELECT * REPLACE (close + 0.000001 AS close)
        FROM read_parquet('{sql_path(path)}')
        WHERE instrument_id IN ({ids})
          AND ts >= (SELECT max(ts) - INTERVAL '5 days'
                     FROM read_parquet('{sql_path(path)}'))
        """
    )


def merge_rewrite(con: duckdb.DuckDBPyConnection, path: Path) -> float:
    tmp = path.with_suffix(".parquet.tmp")
    started = time.perf_counter()
    copy_query(
        con,
        f"""
        SELECT {BAR_COLUMNS}
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY instrument_id, ts ORDER BY incoming DESC
            ) AS row_number
            FROM (
                SELECT *, false AS incoming
                FROM read_parquet('{sql_path(path)}')
                UNION ALL
                SELECT *, true AS incoming FROM incoming
            )
        )
        WHERE row_number = 1
        ORDER BY instrument_id, ts
        """,
        tmp,
    )
    tmp.replace(path)
    return time.perf_counter() - started


def benchmark_ingestion(
    per_instrument: Path,
    bucketed: Path,
    year: int,
    instruments: int,
    buckets: int,
    update_buckets: int,
    repeats: int,
    threads: int,
) -> dict[str, dict[str, float]]:
    if update_buckets > buckets:
        raise ValueError("update-buckets cannot exceed buckets")
    selected = [
        instrument
        for instrument in range(instruments)
        if instrument % buckets < update_buckets
    ]
    selected_by_bucket = {
        bucket: [
            instrument for instrument in selected if instrument % buckets == bucket
        ]
        for bucket in range(update_buckets)
    }
    samples: dict[str, list[float]] = {
        "per_instrument_single": [],
        "bucketed_single": [],
        "per_instrument_batch": [],
        "bucketed_batch": [],
    }
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    for repeat in range(repeats + 1):
        single_id = "inst_000000"
        per_path = per_instrument / f"year={year}" / f"{single_id}.parquet"
        prepare_incoming(con, per_path, [single_id])
        per_single = merge_rewrite(con, per_path)

        bucket_path = bucketed / f"year={year}" / "bucket=000" / "bars.parquet"
        prepare_incoming(con, bucket_path, [single_id])
        bucket_single = merge_rewrite(con, bucket_path)

        per_batch = 0.0
        for instrument in selected:
            instrument_id = f"inst_{instrument:06d}"
            path = per_instrument / f"year={year}" / f"{instrument_id}.parquet"
            prepare_incoming(con, path, [instrument_id])
            per_batch += merge_rewrite(con, path)

        bucket_batch = 0.0
        for bucket, members in selected_by_bucket.items():
            path = bucketed / f"year={year}" / f"bucket={bucket:03d}" / "bars.parquet"
            ids = [f"inst_{instrument:06d}" for instrument in members]
            prepare_incoming(con, path, ids)
            bucket_batch += merge_rewrite(con, path)

        if repeat:
            samples["per_instrument_single"].append(per_single)
            samples["bucketed_single"].append(bucket_single)
            samples["per_instrument_batch"].append(per_batch)
            samples["bucketed_batch"].append(bucket_batch)
    con.close()
    return {
        "seconds": {
            name: statistics.median(values) for name, values in samples.items()
        },
        "updated_instruments": len(selected),
        "updated_buckets": update_buckets,
    }


def main() -> None:
    args = parse_args()
    if (
        min(
            args.instruments,
            args.years,
            args.rows_per_year,
            args.buckets,
            args.update_buckets,
            args.repeats,
            args.threads,
        )
        <= 0
    ):
        raise SystemExit("all numeric arguments must be positive")
    if args.buckets > args.instruments:
        raise SystemExit("buckets cannot exceed instruments")
    if args.years > 2024:
        raise SystemExit("years cannot exceed 2024 with the fixed 2024 end year")
    if args.output_dir:
        root = args.output_dir.resolve()
        root.mkdir(parents=True, exist_ok=False)
        temporary = False
    else:
        root = Path(tempfile.mkdtemp(prefix="market-data-layout-benchmark-"))
        temporary = True

    years = list(range(2024 - args.years + 1, 2025))
    setup_started = time.perf_counter()
    per_instrument, bucketed = generate_layouts(
        root,
        args.instruments,
        years,
        args.rows_per_year,
        args.buckets,
        args.threads,
    )
    setup_seconds = time.perf_counter() - setup_started
    layouts = {"per_instrument": per_instrument, "bucketed": bucketed}
    expected_rows = args.instruments * args.years * args.rows_per_year
    result = {
        "parameters": {
            "instruments": args.instruments,
            "years": args.years,
            "rows_per_instrument_year": args.rows_per_year,
            "total_rows_per_layout": expected_rows,
            "buckets_per_year": args.buckets,
            "query_repeats": args.repeats,
            "threads": args.threads,
        },
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
        },
        "layout": {
            "per_instrument": layout_stats(per_instrument),
            "bucketed": layout_stats(bucketed),
        },
        "validation": validate_layouts(layouts, expected_rows),
        "query_median_seconds": benchmark_queries(
            layouts,
            years,
            args.rows_per_year,
            args.repeats,
            args.threads,
        ),
        "ingestion_median": benchmark_ingestion(
            per_instrument,
            bucketed,
            years[-1],
            args.instruments,
            args.buckets,
            args.update_buckets,
            args.repeats,
            args.threads,
        ),
        "setup_seconds_not_benchmarked": setup_seconds,
        "files_retained_at": str(root),
        "temporary_output": temporary,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
