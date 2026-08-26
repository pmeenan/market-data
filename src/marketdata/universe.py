"""Building and managing the annual ticker universe.

The intended workflow, per year:
  1. Seed candidates - either import your own CSV, or pull Tiingo's
     supported-tickers list filtered to US-listed stocks.
  2. Backfill EOD data for the candidates covering the ranking year.
  3. Rank candidates by average daily dollar volume computed from the
     stored bars, and keep the top N as that year's universe.

Each year's universe is stored separately as the record of how the dataset
was seeded. Universes scope ingestion, not backtests — research code selects
tickers from the stored bars directly (D-010).
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from marketdata.store import BarStore, MetaStore


# Header aliases (lowercased, underscores stripped) accepted for the
# dollar-volume metric column on import.
_DV_ALIASES = {"avgdollarvolume", "mediandollarvolume", "dollarvolume", "adv"}


def import_csv(
    meta: MetaStore, path: Path | str, year: int | None = None
) -> tuple[dict[int, int], list[str]]:
    """Import universe lists from CSV. Returns ({year: ticker_count},
    warnings).

    Requires a `ticker` column. A `year` column imports multiple years at
    once; without one, `year` must be given. Optional columns: `rank`, and
    a dollar-volume metric (avg_dollar_volume / MedianDollarVolume /
    dollar_volume / adv). Missing ranks are derived from the dollar-volume
    column (descending) when present, else from file order.

    The whole file is validated and deduplicated before anything is written,
    and all years are replaced in one atomic transaction. Duplicate
    (year, ticker) rows with identical values collapse silently; conflicting
    duplicates keep the higher dollar volume and produce a warning.
    """

    def norm(name: str) -> str:
        return name.lower().replace("_", "").strip()

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        fields = {norm(c): c for c in reader.fieldnames or []}
        if "ticker" not in fields:
            raise ValueError("CSV must have a 'ticker' column")
        if "year" not in fields and year is None:
            raise ValueError("CSV has no 'year' column, so a year must be specified")
        dv_field = next((fields[a] for a in _DV_ALIASES if a in fields), None)

        warnings: list[str] = []
        seen: dict[tuple[int, str], dict] = {}
        for lineno, row in enumerate(reader, start=2):
            ticker = row[fields["ticker"]].strip().upper()
            if not ticker:
                raise ValueError(f"line {lineno}: empty ticker")
            try:
                row_year = int(row[fields["year"]]) if "year" in fields else year
                dv = float(row[dv_field]) if dv_field and row.get(dv_field) else None
                rank = (
                    int(row[fields["rank"]])
                    if "rank" in fields and row.get(fields["rank"])
                    else None
                )
            except ValueError as e:
                raise ValueError(f"line {lineno}: {e}") from None
            entry = {"ticker": ticker, "rank": rank, "avg_dollar_volume": dv}
            key = (row_year, ticker)
            prior = seen.get(key)
            if prior is None:
                seen[key] = entry
            elif prior != entry:
                keep = max(
                    prior, entry, key=lambda e: (e["avg_dollar_volume"] or 0.0)
                )
                warnings.append(
                    f"conflicting duplicate {row_year}/{ticker}: kept "
                    f"dollar volume {keep['avg_dollar_volume']}"
                )
                seen[key] = keep

    by_year: dict[int, list[dict]] = {}
    for (row_year, _), entry in seen.items():
        by_year.setdefault(row_year, []).append(entry)
    for entries in by_year.values():
        if any(e["rank"] is None for e in entries):
            if dv_field:
                entries.sort(key=lambda e: -(e["avg_dollar_volume"] or 0.0))
            for i, e in enumerate(entries):
                e["rank"] = i + 1
    meta.replace_universes(by_year)
    return {y: len(es) for y, es in sorted(by_year.items())}, warnings


def rank_by_dollar_volume(
    meta: MetaStore, bars: BarStore, year: int, top_n: int, min_days: int = 60
) -> int:
    """Rank all tickers with stored EOD data by average daily dollar volume
    over `year`, and store the top N as that year's universe.

    `min_days` filters out tickers that only traded a sliver of the year
    (IPOs, delistings mid-January, data gaps).
    """
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT ticker,
               avg(close * volume) AS adv,
               count(*)            AS days
        FROM read_parquet('{bars.eod_glob()}')
        WHERE date >= ? AND date <= ?
        GROUP BY ticker
        HAVING days >= ?
        ORDER BY adv DESC
        LIMIT ?
        """,
        [f"{year}-01-01", f"{year}-12-31", min_days, top_n],
    ).fetchall()
    entries = [
        {"ticker": t, "rank": i + 1, "avg_dollar_volume": adv}
        for i, (t, adv, _days) in enumerate(rows)
    ]
    meta.set_universe(year, entries)
    return len(entries)


def seed_candidates_from_tiingo(
    meta: MetaStore,
    supported: list[dict[str, str]],
    *,
    exchanges: tuple[str, ...] = ("NYSE", "NASDAQ", "NYSE ARCA", "AMEX", "BATS"),
    asset_types: tuple[str, ...] = ("Stock", "ETF"),
    active_in_year: int | None = None,
) -> list[str]:
    """Filter Tiingo's supported-tickers dump to plausible candidates and
    register them in the tickers table. Returns the candidate ticker list."""
    out = []
    for row in supported:
        if row.get("exchange") not in exchanges:
            continue
        if row.get("assetType") not in asset_types:
            continue
        if row.get("priceCurrency") not in (None, "", "USD"):
            continue
        start, end = row.get("startDate") or "", row.get("endDate") or ""
        if not start:
            continue
        if active_in_year is not None:
            if start > f"{active_in_year}-12-31" or (end and end < f"{active_in_year}-01-01"):
                continue
        out.append(
            {
                "ticker": row["ticker"],
                "name": None,
                "exchange": row.get("exchange"),
                "asset_type": row.get("assetType"),
                "start_date": start or None,
                "end_date": end or None,
            }
        )
    meta.upsert_tickers(out)
    return [r["ticker"].upper() for r in out]
