from datetime import date, datetime, timedelta, timezone

import polars as pl

from marketdata.config import Config
from marketdata.query import connect, load_eod, load_intraday
from marketdata.store.bars import BarStore, EOD_SCHEMA, INTRADAY_SCHEMA


def _config(tmp_path) -> Config:
    return Config(data_dir=tmp_path, tiingo_token=None)


def _eod(ticker: str, days: int = 5) -> pl.DataFrame:
    start = date(2024, 1, 2)
    dates = [start + timedelta(days=i) for i in range(days)]
    n = len(dates)
    return pl.DataFrame({
        "ticker": [ticker] * n, "date": dates,
        "open": [1.0] * n, "high": [1.0] * n, "low": [1.0] * n, "close": [1.0] * n,
        "volume": [10] * n,
        "adj_open": [1.0] * n, "adj_high": [1.0] * n, "adj_low": [1.0] * n,
        "adj_close": [1.0] * n, "adj_volume": [10] * n,
        "div_cash": [0.0] * n, "split_factor": [1.0] * n,
    }).cast(EOD_SCHEMA)


def _hourly(ticker: str) -> pl.DataFrame:
    ts = [
        datetime(2024, 6, 3, 14, 30, tzinfo=timezone.utc) + timedelta(hours=i)
        for i in range(6)
    ]
    n = len(ts)
    return pl.DataFrame({
        "ticker": [ticker] * n, "ts": ts,
        "open": [1.0] * n, "high": [1.0] * n, "low": [1.0] * n, "close": [1.0] * n,
        "volume": [10] * n,
    }).cast(INTRADAY_SCHEMA)


def test_hourly_view_and_loader(tmp_path):
    config = _config(tmp_path)
    bars = BarStore(tmp_path)
    bars.write_eod("AAPL", _eod("AAPL"))
    bars.write_intraday("AAPL", _hourly("AAPL"), freq="1hour")

    con = connect(config)
    views = {r[0] for r in con.execute(
        "SELECT view_name FROM duckdb_views() WHERE NOT internal"
    ).fetchall()}
    assert {"eod", "intraday_1hour"} <= views
    assert con.execute("SELECT count(*) FROM intraday_1hour").fetchone()[0] == 6

    df = load_intraday(config, ["aapl"], start="2024-06-03", end="2024-06-03", freq="1hour")
    assert df.height == 6
    assert df["ticker"].unique().to_list() == ["AAPL"]


def test_load_eod_filters(tmp_path):
    config = _config(tmp_path)
    bars = BarStore(tmp_path)
    bars.write_eod("AAPL", _eod("AAPL"))
    bars.write_eod("MSFT", _eod("MSFT"))
    df = load_eod(config, ["AAPL"], start="2024-01-03")
    assert df["ticker"].unique().to_list() == ["AAPL"]
    assert df["date"].min() == date(2024, 1, 3)
