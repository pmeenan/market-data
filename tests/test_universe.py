from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from marketdata import universe
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import EOD_SCHEMA


def _synthetic_eod(ticker: str, close: float, volume: int, days: int = 100):
    start = date(2024, 1, 2)
    dates = [start + timedelta(days=i) for i in range(days)]
    return pl.DataFrame(
        {
            "ticker": [ticker] * days,
            "date": dates,
            "open": [close] * days,
            "high": [close] * days,
            "low": [close] * days,
            "close": [close] * days,
            "volume": [volume] * days,
            "adj_open": [close] * days,
            "adj_high": [close] * days,
            "adj_low": [close] * days,
            "adj_close": [close] * days,
            "adj_volume": [volume] * days,
            "div_cash": [0.0] * days,
            "split_factor": [1.0] * days,
        }
    ).cast(EOD_SCHEMA)


def test_rank_by_dollar_volume(tmp_path):
    bars = BarStore(tmp_path)
    bars.write_eod("BIG", _synthetic_eod("BIG", close=100.0, volume=1_000_000))
    bars.write_eod("MID", _synthetic_eod("MID", close=50.0, volume=500_000))
    bars.write_eod("TINY", _synthetic_eod("TINY", close=5.0, volume=10_000))
    bars.write_eod(
        "BRIEF", _synthetic_eod("BRIEF", close=200.0, volume=2_000_000, days=10)
    )

    with MetaStore(tmp_path / "meta.db") as meta:
        n = universe.rank_by_dollar_volume(meta, bars, 2024, top_n=2, min_days=60)
        assert n == 2
        rows = meta.universe(2024)
        # BRIEF is excluded by min_days despite the largest dollar volume
        assert [r["ticker"] for r in rows] == ["BIG", "MID"]
        assert rows[0]["rank"] == 1


def test_import_csv_single_year(tmp_path):
    csv_path = tmp_path / "u.csv"
    csv_path.write_text("ticker,rank\naapl,1\nmsft,2\n")
    with MetaStore(tmp_path / "meta.db") as meta:
        counts, warnings = universe.import_csv(meta, csv_path, year=2023)
        assert counts == {2023: 2}
        assert warnings == []
        assert [r["ticker"] for r in meta.universe(2023)] == ["AAPL", "MSFT"]


def test_import_csv_multi_year_dollar_volume(tmp_path):
    csv_path = tmp_path / "u.csv"
    csv_path.write_text(
        "Year,Ticker,MedianDollarVolume\n"
        "2011,SPY,21300259166.0\n"
        "2011,AAPL,4391122778.0\n"
        "2011,QQQ,2399985068.0\n"
        "2012,AAPL,5000000000.0\n"
        "2012,SPY,20000000000.0\n"
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        counts, warnings = universe.import_csv(meta, csv_path)
        assert counts == {2011: 3, 2012: 2}
        assert warnings == []
        rows = meta.universe(2011)
        assert [(r["ticker"], r["rank"]) for r in rows] == [
            ("SPY", 1),
            ("AAPL", 2),
            ("QQQ", 3),
        ]
        assert rows[0]["avg_dollar_volume"] == 21300259166.0
        # 2012 ranks derived from dollar volume, not file order
        assert [r["ticker"] for r in meta.universe(2012)] == ["SPY", "AAPL"]
        assert meta.universe_years() == [2011, 2012]


def test_import_csv_duplicates(tmp_path):
    csv_path = tmp_path / "u.csv"
    csv_path.write_text(
        "Year,Ticker,MedianDollarVolume\n"
        "2015,GCI,102980729.0\n"
        "2015,GCI,102980729.0\n"  # identical dup: collapses silently
        "2015,GCI,76832649.5\n"  # conflicting dup: keep max, warn
        "2015,SPY,999.0\n"
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        counts, warnings = universe.import_csv(meta, csv_path)
        assert counts == {2015: 2}
        assert len(warnings) == 1 and "GCI" in warnings[0]
        rows = {r["ticker"]: r["avg_dollar_volume"] for r in meta.universe(2015)}
        assert rows["GCI"] == 102980729.0


def test_import_real_seed(tmp_path):
    """The committed seed must import cleanly (offline check of real data)."""
    seed = (
        Path(__file__).resolve().parent.parent
        / "seeds"
        / "universe_by_dollar_volume.csv"
    )
    if not seed.exists():
        pytest.skip("seed file not present")
    with MetaStore(tmp_path / "meta.db") as meta:
        counts, warnings = universe.import_csv(meta, seed)
        assert len(counts) >= 10  # 2011..2026
        assert all(n > 0 for n in counts.values())
        # known conflicting duplicates resolve with a warning, not an error
        assert len(warnings) == 4


def test_import_csv_requires_year(tmp_path):
    csv_path = tmp_path / "u.csv"
    csv_path.write_text("ticker\naapl\n")
    with MetaStore(tmp_path / "meta.db") as meta:
        with pytest.raises(ValueError):
            universe.import_csv(meta, csv_path)
